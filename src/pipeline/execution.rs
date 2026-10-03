// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pipeline task execution and pipeline-graph scheduling.
use super::{Pipeline, PipelineGraph};
use crate::{
    Shutdown,
    error::{Error, Result},
    exec::{
        GlobalExecContextRef, ProcessExecutor, ProcessResult, SinkExecutor, SinkResult,
        SourceExecutor,
    },
};
use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::{
    StreamExt,
    future::{Either, select},
    stream::FuturesUnordered,
};
use std::future::Future;
use std::{collections::VecDeque, sync::Arc};

pub trait Executor: Send + Sync + 'static {
    type JoinError: std::fmt::Display + Send + 'static;
    type Handle<T>: Future<Output = std::result::Result<T, Self::JoinError>> + Send + 'static
    where
        T: Send + 'static;

    /// Schedule the future immediately and return its completion handle.
    fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

#[derive(Clone, Copy, Debug)]
pub struct PipelineExecutionConfig {
    pub batch_rows: usize,
    /// Maximum execute/finish/sink calls between yields, including empty outputs.
    pub yield_batches: usize,
}

impl Default for PipelineExecutionConfig {
    fn default() -> Self {
        Self {
            batch_rows: 2048,
            yield_batches: 16,
        }
    }
}

impl PipelineExecutionConfig {
    fn validate(self) -> Result<()> {
        if self.batch_rows == 0 || self.yield_batches == 0 {
            return Err(Error::InvalidPlan(
                "batch and yield sizes must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Owns one graph execution and the resources supplied by its caller.
pub struct PipelineGraphExecutor<E> {
    graph: PipelineGraph,
    task_executor: E,
    shutdown: Shutdown,
    shutdown_guard: ShutdownGuard,
    config: PipelineExecutionConfig,
    parallelism: usize,
}

impl PipelineGraphExecutor<()> {
    pub fn new(graph: PipelineGraph) -> Self {
        let (shutdown, shutdown_guard) = asyncband::shutdown::new();
        Self {
            graph,
            task_executor: (),
            shutdown,
            shutdown_guard,
            config: PipelineExecutionConfig::default(),
            parallelism: 1,
        }
    }
}

impl<E> PipelineGraphExecutor<E> {
    pub fn with_task_executor<T: Executor>(self, task_executor: T) -> PipelineGraphExecutor<T> {
        PipelineGraphExecutor {
            graph: self.graph,
            task_executor,
            shutdown: self.shutdown,
            shutdown_guard: self.shutdown_guard,
            config: self.config,
            parallelism: self.parallelism,
        }
    }

    /// Returns the caller's shutdown capability. Requesting shutdown is explicit;
    /// awaiting this handle also waits until all execution guards are dropped.
    pub fn shutdown(&self) -> Shutdown {
        self.shutdown.clone()
    }

    pub fn with_config(mut self, config: PipelineExecutionConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

/// Per-role global contexts shared by all tasks in this pipeline.
struct PipelineGlobalContext {
    source: GlobalExecContextRef,
    processors: Vec<GlobalExecContextRef>,
    sink: GlobalExecContextRef,
}

impl Pipeline {
    /// Initializes this pipeline, runs its workers, then finalizes it.
    /// On failure, returns immediately without requesting shutdown. The caller
    /// owns shutdown and must stop remaining work if execution cannot continue.
    pub async fn execute<E: Executor>(
        self,
        task_executor: Arc<E>,
        shutdown_guard: ShutdownGuard,
        config: PipelineExecutionConfig,
        parallelism: usize,
    ) -> Result<()> {
        config.validate()?;
        if parallelism == 0 {
            return Err(Error::InvalidPlan(
                "task parallelism must be positive".into(),
            ));
        }
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::Cancelled);
        }
        let global = self.init_global_context(&shutdown_guard)?;
        let executors = (0..parallelism)
            .map(|_| self.new_executor(&global))
            .collect::<Result<Vec<_>>>()?;
        let mut tasks = FuturesUnordered::new();
        for executor in executors {
            tasks.push(task_executor.spawn(executor.execute(shutdown_guard.clone(), config)));
        }

        while let Some(result) = tasks.next().await {
            result.map_err(|error| Error::Execution(format!("task failed: {error}")))??;
        }
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::Cancelled);
        }
        self.finalize(&global, &shutdown_guard).await
    }

    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<PipelineGlobalContext> {
        let sink = self.sink.init_global_context(shutdown_guard)?;
        let processors = self
            .processors
            .iter()
            .map(|process| process.init_global_context(shutdown_guard))
            .collect::<Result<Vec<_>>>()?;
        let source = self.source.init_global_context(shutdown_guard)?;
        Ok(PipelineGlobalContext {
            source,
            processors,
            sink,
        })
    }

    fn new_executor(&self, global: &PipelineGlobalContext) -> Result<PipelineExecutor> {
        let source = self.source.new_executor(global.source.clone())?;
        let processors = self
            .processors
            .iter()
            .zip(&global.processors)
            .map(|(operator, global)| operator.new_executor(global.clone()))
            .collect::<Result<Vec<_>>>()?;
        let sink = self.sink.new_executor(global.sink.clone())?;
        Ok(PipelineExecutor {
            source,
            processors,
            sink,
        })
    }

    async fn finalize(
        &self,
        global: &PipelineGlobalContext,
        shutdown_guard: &ShutdownGuard,
    ) -> Result<()> {
        self.source
            .finalize(global.source.clone(), shutdown_guard)
            .await?;
        self.sink
            .finalize(global.sink.clone(), shutdown_guard)
            .await
    }
}

/// Task-local execution state. Every task gets a fully initialized executor.
struct PipelineExecutor {
    source: Box<dyn SourceExecutor>,
    processors: Vec<Box<dyn ProcessExecutor>>,
    sink: Box<dyn SinkExecutor>,
}
impl PipelineExecutor {
    async fn execute(
        mut self,
        shutdown_guard: ShutdownGuard,
        config: PipelineExecutionConfig,
    ) -> Result<()> {
        let mut work_since_yield = 0;
        // Pending calls form a depth-first stack. A MoreResult continuation stays
        // below its output so downstream drains before the input is reused.
        let mut pending: Vec<(usize, RecordBatch)> = Vec::with_capacity(self.processors.len() + 1);
        let mut morsel: Option<RecordBatch> = None;
        let mut offset = 0;
        // Once input ends, finish processors in order. Their outputs must drain
        // through the remaining pipeline before finishing the next processor.
        let mut finishing = None;
        loop {
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::Cancelled);
            }
            if let Some((index, input)) = pending.pop() {
                if index == self.processors.len() {
                    if self.sink.sink(&shutdown_guard, &input).await? == SinkResult::Finished {
                        break;
                    }
                } else {
                    let output = match self.processors[index].execute(&input)? {
                        ProcessResult::NeedMoreInput(output) => output,
                        ProcessResult::MoreResult(output) => {
                            pending.push((index, input));
                            output
                        }
                        ProcessResult::Finished(output) => {
                            pending.clear();
                            morsel = None;
                            finishing = Some(index + 1);
                            output
                        }
                    };
                    if output.num_rows() > 0 {
                        pending.push((index + 1, output));
                    }
                }
            } else if let Some(index) = finishing {
                if index == self.processors.len() {
                    break;
                }
                match self.processors[index].finish()? {
                    Some(output) => {
                        if output.num_rows() > 0 {
                            pending.push((index + 1, output));
                        }
                    }
                    None => finishing = Some(index + 1),
                }
            } else {
                if morsel.is_none() {
                    morsel = self.source.next_batch(&shutdown_guard).await?;
                    offset = 0;
                }
                let Some(input) = &morsel else {
                    finishing = Some(0);
                    continue;
                };
                if input.num_rows() == 0 {
                    morsel = None;
                    yield_now().await;
                    continue;
                }
                let rows = config.batch_rows.min(input.num_rows() - offset);
                pending.push((0, input.slice(offset, rows)));
                offset += rows;
                if offset == input.num_rows() {
                    morsel = None;
                }
                continue;
            }
            // Include empty continuations and finish outputs so draining cannot
            // prevent cancellation or cooperative yield.
            work_since_yield += 1;
            if work_since_yield >= config.yield_batches {
                work_since_yield = 0;
                yield_now().await;
            }
        }
        pending.clear();
        self.sink.combine(&shutdown_guard).await
    }
}

pub async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

impl<E: Executor> PipelineGraphExecutor<E> {
    /// Starts dependency-free pipelines, then releases consumers after completion.
    /// Errors are returned without requesting shutdown; the caller can use the
    /// handle from shutdown() to stop and join remaining work. Dropping this
    /// future does not request shutdown either.
    pub async fn execute(self) -> Result<()> {
        self.config.validate()?;
        let task_executor = Arc::new(self.task_executor);
        if self.parallelism == 0 {
            return Err(Error::InvalidPlan(
                "task parallelism must be positive".into(),
            ));
        }
        let shutdown_guard = self.shutdown_guard;
        let mut graph = self.graph;
        let mut remaining_dependencies =
            graph.dependencies.iter().map(Vec::len).collect::<Vec<_>>();
        let mut ready = remaining_dependencies
            .iter()
            .enumerate()
            .filter_map(|(id, &count)| (count == 0).then_some(id))
            .collect::<VecDeque<_>>();
        let mut pipelines = std::mem::take(&mut graph.pipelines)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let mut finished = 0;
        let mut running = FuturesUnordered::new();
        loop {
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::Cancelled);
            }
            while let Some(id) = ready.pop_front() {
                let pipeline = pipelines[id].take().expect("pipeline started once");
                let task = task_executor.spawn(pipeline.execute(
                    task_executor.clone(),
                    shutdown_guard.clone(),
                    self.config,
                    self.parallelism,
                ));
                running.push(async move { (id, task.await) });
            }
            if running.is_empty() {
                return if finished == pipelines.len() {
                    Ok(())
                } else {
                    Err(Error::Execution(
                        "pipeline graph has no runnable pipeline".into(),
                    ))
                };
            }
            let completed = match select(
                Box::pin(shutdown_guard.shutdown_requested()),
                running.next(),
            )
            .await
            {
                Either::Left(_) => return Err(Error::Cancelled),
                Either::Right((completed, _)) => completed,
            };
            let (id, result) = completed.expect("running pipeline exists");
            result.map_err(|error| Error::Execution(format!("task failed: {error}")))??;
            finished += 1;
            for &dependent in &graph.dependents[id] {
                let remaining = &mut remaining_dependencies[dependent];
                *remaining -= 1;
                if *remaining == 0 {
                    ready.push_back(dependent);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
            _shutdown_guard: &'a ShutdownGuard,
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
            _shutdown_guard: &'a ShutdownGuard,
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

        fn combine(self: Box<Self>, _shutdown_guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.observed.combined.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct Processor<F, G>(F, G);

    impl<F, G> ProcessExecutor for Processor<F, G>
    where
        F: FnMut(&RecordBatch) -> Result<ProcessResult> + Send + 'static,
        G: FnMut() -> Result<Option<RecordBatch>> + Send + 'static,
    {
        fn execute(&mut self, input: &RecordBatch) -> Result<ProcessResult> {
            (self.0)(input)
        }

        fn finish(&mut self) -> Result<Option<RecordBatch>> {
            (self.1)()
        }
    }

    fn processor(
        f: impl FnMut(&RecordBatch) -> Result<ProcessResult> + Send + 'static,
    ) -> Box<dyn ProcessExecutor> {
        processor_with_finish(f, || Ok(None))
    }

    fn processor_with_finish(
        execute: impl FnMut(&RecordBatch) -> Result<ProcessResult> + Send + 'static,
        finish: impl FnMut() -> Result<Option<RecordBatch>> + Send + 'static,
    ) -> Box<dyn ProcessExecutor> {
        Box::new(Processor(execute, finish))
    }

    fn buffering(finish_rows: usize) -> Box<dyn ProcessExecutor> {
        let buffered = Arc::new(Mutex::new(VecDeque::new()));
        let input_buffer = buffered.clone();
        processor_with_finish(
            move |input| {
                input_buffer.lock().unwrap().extend(values(input));
                Ok(ProcessResult::NeedMoreInput(input.slice(0, 0)))
            },
            move || {
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
        let split = processor(move |input| {
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
        let twice = processor(move |input| {
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
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
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
        let produce = processor(move |input| {
            empty = !empty;
            Ok(if empty {
                ProcessResult::MoreResult(input.slice(0, 0))
            } else {
                ProcessResult::NeedMoreInput(input.clone())
            })
        });
        let filter = processor(|input| {
            assert!(input.num_rows() > 0);
            Ok(ProcessResult::NeedMoreInput(if values(input)[0] == 1 {
                input.slice(0, 0)
            } else {
                input.clone()
            }))
        });
        let (exec, observed) =
            executor(vec![batch(&[1]), batch(&[2])], vec![produce, filter], None);
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [2]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn processor_finished_discards_upstream_continuations_and_combines_successfully() {
        let upstream_calls = Arc::new(AtomicUsize::new(0));
        let calls = upstream_calls.clone();
        let expand = processor(move |input| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(ProcessResult::MoreResult(input.clone()))
        });
        let limit = processor_with_finish(
            |input| Ok(ProcessResult::Finished(input.slice(0, 1))),
            || panic!("an already finished processor must not be finished again"),
        );
        let (exec, observed) =
            executor(vec![batch(&[1, 2]), batch(&[3])], vec![expand, limit], None);
        let (_shutdown, shutdown_guard) = asyncband::shutdown::new();
        block_on(exec.execute(shutdown_guard.clone(), config())).unwrap();
        assert!(!shutdown_guard.is_shutdown_requested());
        assert_eq!(*observed.values.lock().unwrap(), [1]);
        assert_eq!(upstream_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sink_finished_stops_pending_outputs() {
        let expand = processor(|input| Ok(ProcessResult::MoreResult(input.slice(0, 1))));
        let (exec, observed) = executor(vec![batch(&[1, 2]), batch(&[3])], vec![expand], Some(1));
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn continuation_error_does_not_combine() {
        let mut first = true;
        let fail = processor(move |input| {
            if first {
                first = false;
                Ok(ProcessResult::MoreResult(input.clone()))
            } else {
                Err(Error::Execution("continuation failed".into()))
            }
        });
        let (exec, observed) = executor(vec![batch(&[1]), batch(&[2])], vec![fail], None);
        assert!(
            matches!(block_on(exec.execute(asyncband::shutdown::new().1, config())), Err(Error::Execution(message)) if message == "continuation failed")
        );
        assert_eq!(*observed.values.lock().unwrap(), [1]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn empty_continuations_yield_and_can_be_cancelled_without_a_runtime() {
        let infinite = processor(|input| Ok(ProcessResult::MoreResult(input.slice(0, 0))));
        let (exec, observed) = executor(vec![batch(&[1])], vec![infinite], None);
        let (shutdown, shutdown_guard) = asyncband::shutdown::new();
        let (result, ()) = block_on(async {
            futures::join!(exec.execute(shutdown_guard.clone(), config()), async {
                yield_now().await;
                shutdown.request_shutdown();
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
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2, 3]);
        assert_eq!(*observed.batch_sizes.lock().unwrap(), [2, 1]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn finishes_buffers_in_order_and_drains_tail_continuations() {
        let mut repeat = false;
        let twice = processor(move |input| {
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
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 1, 2, 2, 3, 3, 4, 4]);
        assert_eq!(*observed.batch_sizes.lock().unwrap(), [3, 3, 2]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn empty_source_still_finishes_and_empty_tail_batches_do_not_end_finishing() {
        let mut tail = VecDeque::from([batch(&[]), batch(&[1]), batch(&[]), batch(&[2])]);
        let generate = processor_with_finish(
            |_| panic!("empty source must not execute"),
            move || Ok(tail.pop_front()),
        );
        let (exec, observed) = executor(vec![], vec![generate, buffering(2)], None);
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2]);
        assert_eq!(*observed.batch_sizes.lock().unwrap(), [2]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn early_process_stop_finishes_downstream_but_not_upstream() {
        let expand = processor_with_finish(
            |input| Ok(ProcessResult::MoreResult(input.clone())),
            || panic!("stopped upstream must not finish"),
        );
        let mut calls = 0;
        let limit = processor_with_finish(
            move |input| {
                calls += 1;
                Ok(if calls == 1 {
                    ProcessResult::NeedMoreInput(input.clone())
                } else {
                    ProcessResult::Finished(input.slice(0, 0))
                })
            },
            || panic!("finished processor must not finish"),
        );
        let (exec, observed) = executor(
            vec![batch(&[1, 2, 3]), batch(&[4])],
            vec![expand, limit, buffering(1)],
            None,
        );
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn process_stop_during_finishing_discards_upstream_tail_and_drains_final_output() {
        let finish_calls = Arc::new(AtomicUsize::new(0));
        let calls = finish_calls.clone();
        let generate = processor_with_finish(
            |_| panic!("empty source must not execute"),
            move || {
                assert_eq!(calls.fetch_add(1, Ordering::SeqCst), 0);
                Ok(Some(batch(&[1, 2])))
            },
        );
        let limit = processor_with_finish(
            |input| Ok(ProcessResult::Finished(input.slice(0, 1))),
            || panic!("finished processor must not finish"),
        );
        let mut repeat = false;
        let twice = processor(move |input| {
            repeat = !repeat;
            Ok(if repeat {
                ProcessResult::MoreResult(input.clone())
            } else {
                ProcessResult::NeedMoreInput(input.clone())
            })
        });
        let (exec, observed) = executor(vec![], vec![generate, limit, twice, buffering(1)], None);
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 1]);
        assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sink_stop_during_finishing_skips_remaining_tail_and_downstream_finish() {
        let mut first = true;
        let generate = processor_with_finish(
            |_| panic!("empty source must not execute"),
            move || {
                assert!(first, "sink stopped accepting input");
                first = false;
                Ok(Some(batch(&[1])))
            },
        );
        let expand = processor_with_finish(
            |input| Ok(ProcessResult::MoreResult(input.clone())),
            || panic!("finished sink must stop finishing"),
        );
        let (exec, observed) = executor(vec![], vec![generate, expand], Some(1));
        block_on(exec.execute(asyncband::shutdown::new().1, config())).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1]);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn finish_error_does_not_combine_or_finish_downstream() {
        let mut first = true;
        let fail = processor_with_finish(
            |_| panic!("empty source must not execute"),
            move || {
                if first {
                    first = false;
                    Ok(Some(batch(&[1])))
                } else {
                    Err(Error::Execution("finish failed".into()))
                }
            },
        );
        let downstream = processor_with_finish(
            |input| Ok(ProcessResult::NeedMoreInput(input.clone())),
            || panic!("failed pipeline must not finish downstream"),
        );
        let (exec, observed) = executor(vec![], vec![fail, downstream], None);
        assert!(
            matches!(block_on(exec.execute(asyncband::shutdown::new().1, config())), Err(Error::Execution(message)) if message == "finish failed")
        );
        assert_eq!(*observed.values.lock().unwrap(), [1]);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn empty_finish_outputs_yield_and_can_be_cancelled_without_a_runtime() {
        let infinite = processor_with_finish(
            |_| panic!("empty source must not execute"),
            || Ok(Some(batch(&[]))),
        );
        let (exec, observed) = executor(vec![], vec![infinite], None);
        let (shutdown, shutdown_guard) = asyncband::shutdown::new();
        let (result, ()) = block_on(async {
            futures::join!(exec.execute(shutdown_guard.clone(), config()), async {
                yield_now().await;
                shutdown.request_shutdown();
            })
        });
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(observed.values.lock().unwrap().is_empty());
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }
}
