use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{executor::block_on, future::BoxFuture};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Observed {
    source_calls: AtomicUsize,
    combined: AtomicUsize,
    values: Mutex<Vec<i64>>,
    batch_sizes: Mutex<Vec<usize>>,
}

struct Source {
    batches: VecDeque<RecordBatch>,
    observed: Arc<Observed>,
}

impl SourceExecutor for Source {
    fn next_batch<'a>(
        &'a mut self,
        _cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            self.observed.source_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.batches.pop_front())
        })
    }
}

struct Sink {
    observed: Arc<Observed>,
    stop_after: Option<usize>,
}

impl SinkExecutor for Sink {
    fn sink<'a>(
        &'a mut self,
        _cancel: &'a Cancel,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            assert!(input.num_rows() > 0);
            self.observed
                .batch_sizes
                .lock()
                .unwrap()
                .push(input.num_rows());
            let mut collected = self.observed.values.lock().unwrap();
            collected.extend(values(input));
            Ok(
                if self
                    .stop_after
                    .is_some_and(|limit| collected.len() >= limit)
                {
                    SinkResult::Finished
                } else {
                    SinkResult::NeedMoreInput
                },
            )
        })
    }

    fn combine(self: Box<Self>, _cancel: &Cancel) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.observed.combined.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct Processor<F, G>(F, G);

impl<F, G> ProcessExecutor for Processor<F, G>
where
    F: FnMut(&Cancel, &RecordBatch) -> Result<ProcessResult> + Send + 'static,
    G: FnMut(&Cancel) -> Result<Option<RecordBatch>> + Send + 'static,
{
    fn execute(&mut self, cancel: &Cancel, input: &RecordBatch) -> Result<ProcessResult> {
        (self.0)(cancel, input)
    }

    fn finish(&mut self, cancel: &Cancel) -> Result<Option<RecordBatch>> {
        (self.1)(cancel)
    }
}

fn processor(
    f: impl FnMut(&Cancel, &RecordBatch) -> Result<ProcessResult> + Send + 'static,
) -> Box<dyn ProcessExecutor> {
    processor_with_finish(f, |_| Ok(None))
}

fn processor_with_finish(
    execute: impl FnMut(&Cancel, &RecordBatch) -> Result<ProcessResult> + Send + 'static,
    finish: impl FnMut(&Cancel) -> Result<Option<RecordBatch>> + Send + 'static,
) -> Box<dyn ProcessExecutor> {
    Box::new(Processor(execute, finish))
}

fn buffering(finish_rows: usize) -> Box<dyn ProcessExecutor> {
    let buffered = Arc::new(Mutex::new(VecDeque::new()));
    let input_buffer = buffered.clone();
    processor_with_finish(
        move |_, input| {
            input_buffer.lock().unwrap().extend(values(input));
            Ok(ProcessResult::NeedMoreInput(input.slice(0, 0)))
        },
        move |_| {
            let mut buffered = buffered.lock().unwrap();
            if buffered.is_empty() {
                return Ok(None);
            }
            let rows = finish_rows.min(buffered.len());
            Ok(Some(batch(&buffered.drain(..rows).collect::<Vec<_>>())))
        },
    )
}

fn batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn executor(
    batches: Vec<RecordBatch>,
    processors: Vec<Box<dyn ProcessExecutor>>,
    stop_after: Option<usize>,
) -> (PipelineExecutor, Arc<Observed>) {
    let observed = Arc::new(Observed::default());
    (
        PipelineExecutor {
            source: Box::new(Source {
                batches: batches.into(),
                observed: observed.clone(),
            }),
            processors,
            sink: Box::new(Sink {
                observed: observed.clone(),
                stop_after,
            }),
        },
        observed,
    )
}

fn config() -> PipelineExecutionConfig {
    PipelineExecutionConfig {
        batch_rows: 2,
        yield_batches: 2,
    }
}

#[test]
fn drains_nested_continuations_before_advancing_input() {
    let mut position = 0;
    let inputs = Arc::new(Mutex::new(Vec::new()));
    let calls = inputs.clone();
    let split = processor(move |_, input| {
        calls.lock().unwrap().push(values(input));
        let output = input.slice(position, 1);
        position += 1;
        if position == input.num_rows() {
            position = 0;
            Ok(ProcessResult::NeedMoreInput(output))
        } else {
            Ok(ProcessResult::MoreResult(output))
        }
    });
    let mut repeat = false;
    let twice = processor(move |_, input| {
        repeat = !repeat;
        Ok(if repeat {
            ProcessResult::MoreResult(input.clone())
        } else {
            ProcessResult::NeedMoreInput(input.clone())
        })
    });
    let (exec, observed) = executor(
        vec![batch(&[1, 2, 3]), batch(&[4])],
        vec![split, twice],
        None,
    );
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1, 1, 2, 2, 3, 3, 4, 4]);
    assert_eq!(
        *inputs.lock().unwrap(),
        [vec![1, 2], vec![1, 2], vec![3], vec![4]]
    );
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_output_skips_downstream_without_losing_continuations_or_new_inputs() {
    let mut empty = false;
    let produce = processor(move |_, input| {
        empty = !empty;
        Ok(if empty {
            ProcessResult::MoreResult(input.slice(0, 0))
        } else {
            ProcessResult::NeedMoreInput(input.clone())
        })
    });
    let filter = processor(|_, input| {
        assert!(input.num_rows() > 0);
        Ok(ProcessResult::NeedMoreInput(if values(input)[0] == 1 {
            input.slice(0, 0)
        } else {
            input.clone()
        }))
    });
    let (exec, observed) = executor(vec![batch(&[1]), batch(&[2])], vec![produce, filter], None);
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [2]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn processor_finished_discards_upstream_continuations_and_combines_successfully() {
    let upstream_calls = Arc::new(AtomicUsize::new(0));
    let calls = upstream_calls.clone();
    let expand = processor(move |_, input| {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(ProcessResult::MoreResult(input.clone()))
    });
    let limit = processor_with_finish(
        |_, input| Ok(ProcessResult::Finished(input.slice(0, 1))),
        |_| panic!("an already finished processor must not be finished again"),
    );
    let (exec, observed) = executor(vec![batch(&[1, 2]), batch(&[3])], vec![expand, limit], None);
    let cancel = Cancel::new();
    block_on(exec.execute(cancel.clone(), config())).unwrap();
    assert!(!cancel.is_cancelled());
    assert_eq!(*observed.values.lock().unwrap(), [1]);
    assert_eq!(upstream_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn sink_finished_stops_pending_outputs() {
    let expand = processor(|_, input| Ok(ProcessResult::MoreResult(input.slice(0, 1))));
    let (exec, observed) = executor(vec![batch(&[1, 2]), batch(&[3])], vec![expand], Some(1));
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn continuation_error_does_not_combine() {
    let mut first = true;
    let fail = processor(move |_, input| {
        if first {
            first = false;
            Ok(ProcessResult::MoreResult(input.clone()))
        } else {
            Err(Error::Execution("continuation failed".into()))
        }
    });
    let (exec, observed) = executor(vec![batch(&[1]), batch(&[2])], vec![fail], None);
    assert!(
        matches!(block_on(exec.execute(Cancel::new(), config())), Err(Error::Execution(message)) if message == "continuation failed")
    );
    assert_eq!(*observed.values.lock().unwrap(), [1]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
}

#[test]
fn empty_continuations_yield_and_can_be_cancelled_without_a_runtime() {
    let infinite = processor(|_, input| Ok(ProcessResult::MoreResult(input.slice(0, 0))));
    let (exec, observed) = executor(vec![batch(&[1])], vec![infinite], None);
    let cancel = Cancel::new();
    let (result, ()) = block_on(async {
        futures::join!(exec.execute(cancel.clone(), config()), async {
            yield_now().await;
            cancel.cancel();
        })
    });
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(observed.values.lock().unwrap().is_empty());
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
}

#[test]
fn pipeline_without_processors_still_splits_source_morsels() {
    let (exec, observed) = executor(vec![batch(&[]), batch(&[1, 2, 3])], vec![], None);
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1, 2, 3]);
    assert_eq!(*observed.batch_sizes.lock().unwrap(), [2, 1]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn finishes_buffers_in_order_and_drains_tail_continuations() {
    let mut repeat = false;
    let twice = processor(move |_, input| {
        repeat = !repeat;
        Ok(if repeat {
            ProcessResult::MoreResult(input.clone())
        } else {
            ProcessResult::NeedMoreInput(input.clone())
        })
    });
    let (exec, observed) = executor(
        vec![batch(&[1, 2, 3]), batch(&[4])],
        vec![buffering(1), twice, buffering(3)],
        None,
    );
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1, 1, 2, 2, 3, 3, 4, 4]);
    assert_eq!(*observed.batch_sizes.lock().unwrap(), [3, 3, 2]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_source_still_finishes_and_empty_tail_batches_do_not_end_finishing() {
    let mut tail = VecDeque::from([batch(&[]), batch(&[1]), batch(&[]), batch(&[2])]);
    let generate = processor_with_finish(
        |_, _| panic!("empty source must not execute"),
        move |_| Ok(tail.pop_front()),
    );
    let (exec, observed) = executor(vec![], vec![generate, buffering(2)], None);
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1, 2]);
    assert_eq!(*observed.batch_sizes.lock().unwrap(), [2]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn early_process_stop_finishes_downstream_but_not_upstream() {
    let expand = processor_with_finish(
        |_, input| Ok(ProcessResult::MoreResult(input.clone())),
        |_| panic!("stopped upstream must not finish"),
    );
    let mut calls = 0;
    let limit = processor_with_finish(
        move |_, input| {
            calls += 1;
            Ok(if calls == 1 {
                ProcessResult::NeedMoreInput(input.clone())
            } else {
                ProcessResult::Finished(input.slice(0, 0))
            })
        },
        |_| panic!("finished processor must not finish"),
    );
    let (exec, observed) = executor(
        vec![batch(&[1, 2, 3]), batch(&[4])],
        vec![expand, limit, buffering(1)],
        None,
    );
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1, 2]);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn process_stop_during_finishing_discards_upstream_tail_and_drains_final_output() {
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let calls = finish_calls.clone();
    let generate = processor_with_finish(
        |_, _| panic!("empty source must not execute"),
        move |_| {
            assert_eq!(calls.fetch_add(1, Ordering::SeqCst), 0);
            Ok(Some(batch(&[1, 2])))
        },
    );
    let limit = processor_with_finish(
        |_, input| Ok(ProcessResult::Finished(input.slice(0, 1))),
        |_| panic!("finished processor must not finish"),
    );
    let mut repeat = false;
    let twice = processor(move |_, input| {
        repeat = !repeat;
        Ok(if repeat {
            ProcessResult::MoreResult(input.clone())
        } else {
            ProcessResult::NeedMoreInput(input.clone())
        })
    });
    let (exec, observed) = executor(vec![], vec![generate, limit, twice, buffering(1)], None);
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1, 1]);
    assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn sink_stop_during_finishing_skips_remaining_tail_and_downstream_finish() {
    let mut first = true;
    let generate = processor_with_finish(
        |_, _| panic!("empty source must not execute"),
        move |_| {
            assert!(first, "sink stopped accepting input");
            first = false;
            Ok(Some(batch(&[1])))
        },
    );
    let expand = processor_with_finish(
        |_, input| Ok(ProcessResult::MoreResult(input.clone())),
        |_| panic!("finished sink must stop finishing"),
    );
    let (exec, observed) = executor(vec![], vec![generate, expand], Some(1));
    block_on(exec.execute(Cancel::new(), config())).unwrap();
    assert_eq!(*observed.values.lock().unwrap(), [1]);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
}

#[test]
fn finish_error_does_not_combine_or_finish_downstream() {
    let mut first = true;
    let fail = processor_with_finish(
        |_, _| panic!("empty source must not execute"),
        move |_| {
            if first {
                first = false;
                Ok(Some(batch(&[1])))
            } else {
                Err(Error::Execution("finish failed".into()))
            }
        },
    );
    let downstream = processor_with_finish(
        |_, input| Ok(ProcessResult::NeedMoreInput(input.clone())),
        |_| panic!("failed pipeline must not finish downstream"),
    );
    let (exec, observed) = executor(vec![], vec![fail, downstream], None);
    assert!(
        matches!(block_on(exec.execute(Cancel::new(), config())), Err(Error::Execution(message)) if message == "finish failed")
    );
    assert_eq!(*observed.values.lock().unwrap(), [1]);
    assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
}

#[test]
fn empty_finish_outputs_yield_and_can_be_cancelled_without_a_runtime() {
    let infinite = processor_with_finish(
        |_, _| panic!("empty source must not execute"),
        |_| Ok(Some(batch(&[]))),
    );
    let (exec, observed) = executor(vec![], vec![infinite], None);
    let cancel = Cancel::new();
    let (result, ()) = block_on(async {
        futures::join!(exec.execute(cancel.clone(), config()), async {
            yield_now().await;
            cancel.cancel();
        })
    });
    assert!(matches!(result, Err(Error::Cancelled)));
    assert!(observed.values.lock().unwrap().is_empty());
    assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
}
