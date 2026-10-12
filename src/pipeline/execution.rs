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
use crate::arrow::record_batch::RecordBatch;
use crate::suboperator::ProgramExecutionContext;
use crate::{
    Shutdown,
    error::{Error, Result},
};
use asyncband::shutdown::ShutdownGuard;
use futures::{
    FutureExt, StreamExt,
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
    /// Maximum completed batches before explicitly yielding the coordinator.
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

impl Pipeline {
    /// One async coordinator owns the source and a bounded pool of local state.
    /// Only synchronous Program calls move to Rayon; no worker owns an async
    /// source cursor or an executor object.
    pub async fn execute(
        mut self,
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
        self.sink.start(&shutdown_guard)?;
        self.source.start(&shutdown_guard)?;
        // A single worker has no CPU parallelism to schedule, so run inline
        // and yield after bounded work instead of dispatching each batch.
        if parallelism == 1 {
            let mut local = self.program.new_execution_context();
            self.program.initialize(&mut local)?;
            let mut completed = 0;
            while let Some(batch) = self.source.next_batch(&shutdown_guard).await? {
                if shutdown_guard.is_shutdown_requested() {
                    return Err(Error::Cancelled);
                }
                for offset in (0..batch.num_rows()).step_by(config.batch_rows) {
                    let rows = config.batch_rows.min(batch.num_rows() - offset);
                    if offset == 0 && rows == batch.num_rows() {
                        run_inline(&self.program, &batch, &mut local, &shutdown_guard)?;
                    } else {
                        run_inline(
                            &self.program,
                            &batch.slice(offset, rows),
                            &mut local,
                            &shutdown_guard,
                        )?;
                    }
                    self.sink.flush(&mut local, &shutdown_guard).await?;
                    if local.stopped {
                        break;
                    }
                    completed += 1;
                    if completed % config.yield_batches == 0 {
                        yield_now().await;
                    }
                }
                if local.stopped {
                    break;
                }
                if batch.num_rows() == 0 {
                    yield_now().await;
                }
            }
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::Cancelled);
            }
            self.sink.combine(local, &shutdown_guard).await?;
            self.source.finish(&shutdown_guard).await?;
            return self.sink.finish(&shutdown_guard).await;
        }
        let source_guard = &shutdown_guard;
        let mut source = Box::pin(futures::stream::try_unfold(
            self.source.as_mut(),
            |source| async move {
                Ok::<_, Error>(
                    source
                        .next_batch(source_guard)
                        .await?
                        .map(|batch| (batch, source)),
                )
            },
        ));
        let program = Arc::new(self.program);
        let mut idle = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            let mut local = program.new_execution_context();
            program.initialize(&mut local)?;
            idle.push(local);
        }
        let mut jobs = FuturesUnordered::new();
        let mut morsel: Option<RecordBatch> = None;
        let mut offset = 0;
        let mut eof = false;
        let mut stop = false;
        let mut completed = 0;
        loop {
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::Cancelled);
            }
            if !stop
                && let Some(input) = &morsel
                && !idle.is_empty()
            {
                let rows = config.batch_rows.min(input.num_rows() - offset);
                let batch = if offset == 0 && rows == input.num_rows() {
                    morsel.take().unwrap()
                } else {
                    let batch = input.slice(offset, rows);
                    offset += rows;
                    if offset == input.num_rows() {
                        morsel = None;
                    }
                    batch
                };
                // Amortize dispatch over immediately available input only. Unfold
                // retains a pending source read, so feedback/async I/O is never
                // cancelled or blocked waiting for a full work chunk.
                let mut batches = vec![batch];
                if morsel.is_none() {
                    while batches.len() < 128 && !eof {
                        match source.next().now_or_never() {
                            Some(Some(Ok(batch))) if batch.num_rows() > config.batch_rows => {
                                morsel = Some(batch);
                                offset = 0;
                                break;
                            }
                            Some(Some(Ok(batch))) if batch.num_rows() != 0 => batches.push(batch),
                            Some(Some(Ok(_))) => break,
                            Some(Some(Err(e))) => return Err(e),
                            Some(None) => eof = true,
                            None => break,
                        }
                    }
                }
                let mut local = idle.pop().unwrap();
                let code = program.clone();
                let guard = shutdown_guard.clone();
                jobs.push(on_cpu(move || {
                    for batch in batches {
                        code.run(&batch, &mut local, &guard)?;
                        if local.stopped {
                            break;
                        }
                    }
                    Ok(local)
                }));
                continue;
            }
            if (eof || stop) && jobs.is_empty() {
                break;
            }
            if !stop && !eof && morsel.is_none() && !idle.is_empty() {
                // This borrow stays alive while CPU completions are drained.
                // Early stop drops only the read future; the source remains
                // owned by the Pipeline and can still finish its resources.
                let mut next = Box::pin(async { source.next().await.transpose() });
                loop {
                    enum Event {
                        Input(Result<Option<RecordBatch>>),
                        Done(Result<ProgramExecutionContext>),
                    }
                    let event = async {
                        if jobs.is_empty() {
                            Event::Input(next.as_mut().await)
                        } else {
                            match select(next.as_mut(), jobs.next()).await {
                                Either::Left((batch, _)) => Event::Input(batch),
                                Either::Right((local, _)) => Event::Done(local.unwrap()),
                            }
                        }
                    };
                    let event = match select(
                        Box::pin(shutdown_guard.shutdown_requested()),
                        Box::pin(event),
                    )
                    .await
                    {
                        Either::Left(_) => return Err(Error::Cancelled),
                        Either::Right((event, _)) => event,
                    };
                    match event {
                        Event::Input(batch) => {
                            match batch? {
                                Some(batch) if batch.num_rows() != 0 => {
                                    morsel = Some(batch);
                                    offset = 0;
                                }
                                Some(_) => yield_now().await,
                                None => eof = true,
                            }
                            break;
                        }
                        Event::Done(local) => {
                            stop |= return_local(
                                self.sink.as_mut(),
                                local?,
                                &mut idle,
                                &shutdown_guard,
                            )
                            .await?;
                            completed += 1;
                            if completed % config.yield_batches == 0 {
                                yield_now().await;
                            }
                            if stop {
                                break;
                            }
                        }
                    }
                }
            } else {
                let local = match select(Box::pin(shutdown_guard.shutdown_requested()), jobs.next())
                    .await
                {
                    Either::Left(_) => return Err(Error::Cancelled),
                    Either::Right((local, _)) => local.expect("pending CPU work")?,
                };
                stop |= return_local(self.sink.as_mut(), local, &mut idle, &shutdown_guard).await?;
                completed += 1;
                if completed % config.yield_batches == 0 {
                    yield_now().await;
                }
            }
            if stop {
                morsel = None;
            }
        }
        // Combine only after every CPU job succeeds. Empty input still combines
        // initialized local states, which is required for a global COUNT = 0.
        for local in idle {
            self.sink.combine(local, &shutdown_guard).await?;
        }
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::Cancelled);
        }
        drop(source);
        self.source.finish(&shutdown_guard).await?;
        self.sink.finish(&shutdown_guard).await
    }
}

fn run_inline(
    program: &crate::suboperator::ProgramContext,
    batch: &RecordBatch,
    local: &mut ProgramExecutionContext,
    guard: &ShutdownGuard,
) -> Result<()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        program.run(batch, local, guard)
    }))
    .unwrap_or_else(|_| Err(Error::Execution("CPU task panicked".into())))
}

async fn return_local(
    sink: &mut dyn crate::exec::SinkExec,
    mut local: ProgramExecutionContext,
    idle: &mut Vec<ProgramExecutionContext>,
    guard: &ShutdownGuard,
) -> Result<bool> {
    let stopped = local.stopped;
    sink.flush(&mut local, guard).await?;
    idle.push(local);
    Ok(stopped)
}

/// Rayon runs synchronous work only. Catch worker panics and return them through
/// the completion channel rather than unwinding across the async runtime.
fn on_cpu<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> impl Future<Output = Result<T>> {
    let (tx, rx) = futures::channel::oneshot::channel();
    rayon::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
            .unwrap_or_else(|_| Err(Error::Execution("CPU task panicked".into())));
        let _ = tx.send(result);
    });
    async move {
        rx.await
            .map_err(|_| Error::Execution("CPU task did not return its state".into()))?
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
    use crate::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use crate::exec::{SinkExec, SourceExec};
    use crate::suboperator::{
        ExecutionMode, ProgramControl, StateBuilder, StateId, SuboperatorScopeBuilder,
    };
    use futures::{executor::block_on, future::BoxFuture};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct Observed {
        source_calls: AtomicUsize,
        cursors: AtomicUsize,
        locals: AtomicUsize,
        combined: AtomicUsize,
        values: Mutex<Vec<i64>>,
        batch_sizes: Mutex<Vec<usize>>,
        batches: Mutex<Vec<RecordBatch>>,
    }

    struct Source {
        batches: VecDeque<RecordBatch>,
        observed: Arc<Observed>,
    }

    impl SourceExec for Source {
        fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
            self.observed.cursors.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
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

    struct RecordingState {
        observed: Arc<Observed>,
        stop_after: Option<usize>,
    }

    impl RecordingState {
        fn sink(&mut self, input: &RecordBatch) -> Result<ProgramControl> {
            assert!(input.num_rows() > 0);
            self.observed.batches.lock().unwrap().push(input.clone());
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
                    ProgramControl::Stop
                } else {
                    ProgramControl::Continue
                },
            )
        }
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
    fn config() -> PipelineExecutionConfig {
        PipelineExecutionConfig {
            batch_rows: 2,
            yield_batches: 1,
        }
    }
    struct SinkDefinition {
        state: StateId,
        observed: Arc<Observed>,
    }
    impl SinkExec for SinkDefinition {
        fn combine<'a>(
            &'a mut self,
            mut local: ProgramExecutionContext,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                local
                    .take_state(self.state)
                    .unwrap()
                    .into_inner::<RecordingState>()?;
                self.observed.combined.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    fn executor(
        batches: Vec<RecordBatch>,
        mut builder: SuboperatorScopeBuilder,
        stop_after: Option<usize>,
    ) -> (Pipeline, Arc<Observed>) {
        let observed = Arc::new(Observed::default());
        let monitor = observed.clone();
        let state = builder.state(StateBuilder::new(move || {
            monitor.locals.fetch_add(1, Ordering::SeqCst);
            Ok(RecordingState {
                observed: monitor.clone(),
                stop_after,
            })
        }));
        builder
            .batch_update(state, |state, batch| {
                state.get_mut::<RecordingState>()?.sink(batch)
            })
            .unwrap();
        let pipeline = Pipeline::new(
            Box::new(Source {
                batches: batches.into(),
                observed: observed.clone(),
            }),
            builder,
            Box::new(SinkDefinition {
                state,
                observed: observed.clone(),
            }),
            ExecutionMode::Compiled,
        )
        .unwrap();
        (pipeline, observed)
    }
    fn empty_program() -> SuboperatorScopeBuilder {
        SuboperatorScopeBuilder::new()
    }
    #[test]
    fn source_morsels_split_and_empty_batches_are_skipped() {
        let (exec, observed) = executor(vec![batch(&[]), batch(&[1, 2, 3])], empty_program(), None);
        block_on(exec.execute(asyncband::shutdown::new().1, config(), 1)).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2, 3]);
        assert_eq!(*observed.batch_sizes.lock().unwrap(), [2, 1]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn whole_batches_keep_their_arrow_owners() {
        let input = batch(&[1, 2]);
        let owner = input.column(0).clone();
        let (exec, observed) = executor(vec![input], empty_program(), None);
        block_on(exec.execute(asyncband::shutdown::new().1, config(), 1)).unwrap();
        assert!(Arc::ptr_eq(
            &owner,
            observed.batches.lock().unwrap()[0].column(0)
        ));
    }
    #[test]
    fn sink_stop_does_not_read_more_input_and_combines_once() {
        let (exec, observed) = executor(
            vec![batch(&[1, 2, 3]), batch(&[4])],
            empty_program(),
            Some(1),
        );
        block_on(exec.execute(asyncband::shutdown::new().1, config(), 1)).unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn program_errors_do_not_publish_partial_results() {
        use crate::{
            expr::{ExpressionResultType, scalar::*},
            operator::{Projection, ProjectionExpression},
        };
        let x = ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, false))
            .into_ref();
        let division = FunctionExpression::binary(
            FunctionKind::Divide,
            x,
            ConstantExpression::int64(Some(0)).into_ref(),
            DataType::Int64,
            false,
        )
        .into_ref();
        let mut builder = SuboperatorScopeBuilder::new();
        Projection::new(vec![ProjectionExpression::new(division, "x")])
            .build_suboperator(&mut builder)
            .unwrap();
        let (exec, observed) = executor(vec![batch(&[1])], builder, None);
        assert!(block_on(exec.execute(asyncband::shutdown::new().1, config(), 1)).is_err());
        assert!(observed.values.lock().unwrap().is_empty());
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn one_source_and_bounded_reused_locals_for_many_batches() {
        let (exec, observed) = executor(
            vec![batch(&(0..100).collect::<Vec<_>>())],
            empty_program(),
            None,
        );
        block_on(exec.execute(asyncband::shutdown::new().1, config(), 3)).unwrap();
        assert_eq!(observed.cursors.load(Ordering::SeqCst), 1);
        assert_eq!(observed.locals.load(Ordering::SeqCst), 3);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 3);
        let mut values = observed.values.lock().unwrap().clone();
        values.sort();
        assert_eq!(values, (0..100).collect::<Vec<_>>());
    }
    #[test]
    fn cancellation_does_not_read_or_combine() {
        let (exec, observed) = executor(vec![batch(&[1, 2])], empty_program(), None);
        let (shutdown, guard) = asyncband::shutdown::new();
        shutdown.request_shutdown();
        assert!(matches!(
            block_on(exec.execute(guard, config(), 1)),
            Err(Error::Cancelled)
        ));
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 0);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn empty_source_still_returns_local_state_for_combine() {
        let (exec, observed) = executor(vec![], empty_program(), None);
        block_on(exec.execute(asyncband::shutdown::new().1, config(), 1)).unwrap();
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }
    // The second source read cannot finish until the first batch is flushed.
    // This catches coordinators that await source input while neglecting CPU
    // completions, and accidental cancellation/recreation of pending reads.
    struct FeedbackCursor {
        read: usize,
        ready: Option<futures::channel::oneshot::Receiver<()>>,
    }
    impl SourceExec for FeedbackCursor {
        fn next_batch<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
            Box::pin(async move {
                let read = self.read;
                self.read += 1;
                match read {
                    0 => Ok(Some(batch(&[1]))),
                    1 => {
                        self.ready
                            .take()
                            .expect("one persistent read")
                            .await
                            .map_err(|_| Error::Execution("lost flush notification".into()))?;
                        Ok(Some(batch(&[2])))
                    }
                    _ => Ok(None),
                }
            })
        }
    }
    struct FeedbackSink {
        send: Option<futures::channel::oneshot::Sender<()>>,
        state: StateId,
    }
    impl SinkExec for FeedbackSink {
        fn flush<'a>(
            &'a mut self,
            local: &'a mut ProgramExecutionContext,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let updated = local.state_mut(self.state)?.get_mut::<bool>()?;
                assert!(*updated);
                *updated = false;
                if let Some(send) = self.send.take() {
                    let _ = send.send(());
                }
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn pending_source_read_keeps_progress_while_completed_batches_flush() {
        let (send, receive) = futures::channel::oneshot::channel();
        let mut builder = SuboperatorScopeBuilder::new();
        let state = builder.state(StateBuilder::new(|| Ok(false)));
        builder
            .batch_update(state, |state, _| {
                assert!(rayon::current_thread_index().is_some());
                *state.get_mut::<bool>()? = true;
                Ok(ProgramControl::Continue)
            })
            .unwrap();
        let pipeline = Pipeline::new(
            Box::new(FeedbackCursor {
                read: 0,
                ready: Some(receive),
            }),
            builder,
            Box::new(FeedbackSink {
                send: Some(send),
                state,
            }),
            ExecutionMode::Compiled,
        )
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.execute(asyncband::shutdown::new().1, config(), 2),
        )
        .await
        .expect("source and flush deadlocked")
        .unwrap();
    }
    // A source whose second read never completes must still be finalized after
    // a CPU program stops. Keep the source owned by the coordinator, not the read.
    struct PendingStopSource {
        first: bool,
        pending: Arc<std::sync::atomic::AtomicBool>,
        finished: Arc<AtomicUsize>,
    }
    impl SourceExec for PendingStopSource {
        fn next_batch<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
            Box::pin(async move {
                if self.first {
                    self.first = false;
                    return Ok(Some(batch(&[1])));
                }
                self.pending.store(true, Ordering::SeqCst);
                std::future::pending().await
            })
        }
        fn finish<'a>(&'a mut self, _: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn early_stop_drops_pending_read_but_finishes_owned_source() {
        let pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(Observed::default());
        let mut builder = SuboperatorScopeBuilder::new();
        let monitor = observed.clone();
        let state = builder.state(StateBuilder::new(move || {
            Ok(RecordingState {
                observed: monitor.clone(),
                stop_after: None,
            })
        }));
        builder
            .batch_update(state, |state, batch| {
                state.get_mut::<RecordingState>()?.sink(batch)
            })
            .unwrap();
        let read_pending = pending.clone();
        let wait_state = builder.state(StateBuilder::new(move || Ok(read_pending.clone())));
        builder
            .batch_update(wait_state, |state, _| {
                let pending = state.get_mut::<Arc<std::sync::atomic::AtomicBool>>()?;
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                while !pending.load(Ordering::SeqCst) {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "second read never polled"
                    );
                    std::thread::yield_now();
                }
                Ok(ProgramControl::Stop)
            })
            .unwrap();
        let pipeline = Pipeline::new(
            Box::new(PendingStopSource {
                first: true,
                pending,
                finished: finished.clone(),
            }),
            builder,
            Box::new(SinkDefinition {
                state,
                observed: observed.clone(),
            }),
            ExecutionMode::Compiled,
        )
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.execute(asyncband::shutdown::new().1, config(), 2),
        )
        .await
        .expect("stop must interrupt pending source read")
        .unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn cpu_panics_are_returned_as_errors() {
        let result = block_on(on_cpu(|| -> Result<()> {
            panic!("intentional worker panic")
        }));
        assert!(matches!(result, Err(Error::Execution(_))));
    }
}
