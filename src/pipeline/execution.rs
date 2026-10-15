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
use crate::exec::{
    ConsumeStatus, Cpu, CpuJob, LocalSink, LocalSinkContext, SinkExec, StateContext,
    partition_count,
};
use crate::kernel::{LocalState, Program};
use crate::{
    Shutdown,
    error::{Error, ErrorContext, ErrorKind, Result, ResultExt},
};
use asyncband::shutdown::ShutdownGuard;
use futures::{
    FutureExt, StreamExt,
    future::{Either, select},
    stream::FuturesUnordered,
};
use std::future::Future;
use std::{collections::VecDeque, sync::Arc};

const JOB_ROWS: usize = 16; // Multiples of the configured batch_rows.

struct ExecutorCpu<E> {
    executor: Arc<E>,
    parallelism: usize,
}
impl<E: Executor> Cpu for ExecutorCpu<E> {
    fn spawn(&self, job: CpuJob) -> futures::future::BoxFuture<'static, Result<()>> {
        Box::pin(on_cpu(self.executor.as_ref(), job))
    }
    fn parallelism(&self) -> usize {
        self.parallelism
    }
}

async fn finish_sink(sink: &mut dyn SinkExec, guard: &ShutdownGuard, cpu: &dyn Cpu) -> Result<()> {
    match select(
        Box::pin(guard.shutdown_requested()),
        sink.finish(guard, cpu),
    )
    .await
    {
        Either::Left(_) => Err(Error::cancelled()),
        Either::Right((result, _)) => result,
    }
}

pub trait Executor: Send + Sync + 'static {
    type JoinError: std::error::Error + Send + Sync + 'static;
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
            return Err(Error::invalid_plan(
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
    /// Program calls run as tasks of `executor`, the one pool of the caller's
    /// threads: a job is a bounded run of chunks with no locks or shared
    /// state, so it returns its thread soon and async tasks run between jobs.
    /// No worker owns an async source cursor or an executor object.
    pub async fn execute<E: Executor>(
        mut self,
        executor: Arc<E>,
        shutdown_guard: ShutdownGuard,
        config: PipelineExecutionConfig,
        parallelism: usize,
    ) -> Result<()> {
        config.validate()?;
        if parallelism == 0 {
            return Err(Error::invalid_plan(
                "task parallelism must be positive".into(),
            ));
        }
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        self.sink.start(&shutdown_guard).with_location()?;
        self.source.start(&shutdown_guard).with_location()?;
        let ctx = LocalSinkContext {
            batch_rows: config.batch_rows,
            partitions: partition_count(parallelism),
        };
        let cpu = ExecutorCpu {
            executor: executor.clone(),
            parallelism,
        };
        // Ordered pipelines have exactly one instance, even with many workers.
        if parallelism == 1 || self.ordered {
            let mut state = self.program.new_local()?;
            let mut local = self.sink.local_sink(&ctx)?;
            let mut completed = 0;
            let idle = self.program.idle(&state);
            'input: while !idle {
                let batch = match select(
                    Box::pin(shutdown_guard.shutdown_requested()),
                    self.source.next_batch(&shutdown_guard),
                )
                .await
                {
                    Either::Left(_) => return Err(Error::cancelled()),
                    Either::Right((batch, _)) => batch.with_location()?,
                };
                let Some(batch) = batch else { break };
                for offset in (0..batch.num_rows()).step_by(config.batch_rows) {
                    if shutdown_guard.is_shutdown_requested() {
                        return Err(Error::cancelled());
                    }
                    let rows = config.batch_rows.min(batch.num_rows() - offset);
                    let input = if offset == 0 && rows == batch.num_rows() {
                        batch.clone()
                    } else {
                        batch.slice(offset, rows)
                    };
                    let chunk = run_inline(&self.program, &input, &mut state, &shutdown_guard)?
                        .filter(|c| c.num_rows() != 0);
                    let status =
                        catch_cpu(|| local.consume(chunk, &mut StateContext::new(&mut state)))?;
                    if status != ConsumeStatus::End {
                        catch_cpu(|| {
                            local.finish(
                                status,
                                &mut StateContext::new(&mut state),
                                &shutdown_guard,
                            )
                        })?;
                        if shutdown_guard.is_shutdown_requested() {
                            return Err(Error::cancelled());
                        }
                        self.sink
                            .consume(
                                status,
                                &mut *local,
                                StateContext::new(&mut state),
                                &shutdown_guard,
                            )
                            .await
                            .with_location()?;
                        if status == ConsumeStatus::Stop {
                            break 'input;
                        }
                    }
                    completed += 1;
                    if completed % config.yield_batches == 0 {
                        yield_now().await;
                    }
                }
                if batch.num_rows() == 0 {
                    yield_now().await;
                }
            }
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let (mut state, mut local) = if parallelism > 1 {
                let guard = shutdown_guard.clone();
                let finish = on_cpu(executor.as_ref(), move || {
                    finish_pair((state, local), &guard)
                });
                match select(
                    Box::pin(shutdown_guard.shutdown_requested()),
                    Box::pin(finish),
                )
                .await
                {
                    Either::Left(_) => return Err(Error::cancelled()),
                    Either::Right((pair, _)) => pair?,
                }
            } else {
                catch_cpu(|| finish_pair((state, local), &shutdown_guard))?
            };
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.sink
                .consume(
                    ConsumeStatus::End,
                    &mut *local,
                    StateContext::new(&mut state),
                    &shutdown_guard,
                )
                .await
                .with_location()?;
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.source.finish(&shutdown_guard).await.with_location()?;
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            return finish_sink(self.sink.as_mut(), &shutdown_guard, &cpu)
                .await
                .with_location();
        }
        let source_guard = &shutdown_guard;
        let mut source = Some(Box::pin(futures::stream::try_unfold(
            self.source.as_mut(),
            |source| async move {
                Ok::<_, Error>(
                    source
                        .next_batch(source_guard)
                        .await
                        .with_location()?
                        .map(|batch| (batch, source)),
                )
            },
        )));
        let program = Arc::new(self.program);
        let mut idle = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            idle.push((program.new_local()?, self.sink.local_sink(&ctx)?));
        }
        let mut jobs = FuturesUnordered::new();
        let mut pending: VecDeque<RecordBatch> = VecDeque::new();
        let mut morsel: Option<RecordBatch> = None;
        let mut offset = 0;
        // A program that does nothing on any input does not read it.
        let mut eof = idle.first().is_some_and(|l| program.idle(&l.0));
        let mut stop = false;
        let mut completed = 0;
        loop {
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            if !stop && (!pending.is_empty() || morsel.is_some()) && !idle.is_empty() {
                let mut batches = Vec::with_capacity(128);
                let mut job_rows = 0usize;
                let row_limit = JOB_ROWS.saturating_mul(config.batch_rows);
                while batches.len() < 128 && job_rows < row_limit {
                    if let Some(batch) = pending.pop_front() {
                        job_rows += batch.num_rows();
                        batches.push(batch);
                    } else {
                        break;
                    }
                }
                if batches.is_empty() {
                    let input = morsel.as_ref().unwrap();
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
                    batches.push(batch);
                    job_rows += rows;
                }
                // Unfold retains a pending read across polls. Only coalesce
                // immediately available input, so handoffs can unblock I/O.
                if pending.is_empty() && morsel.is_none() {
                    while batches.len() < 128 && job_rows < row_limit && !eof {
                        match source.as_mut().unwrap().next().now_or_never() {
                            Some(Some(Ok(batch))) if batch.num_rows() > config.batch_rows => {
                                morsel = Some(batch);
                                offset = 0;
                                break;
                            }
                            Some(Some(Ok(batch))) if batch.num_rows() != 0 => {
                                job_rows += batch.num_rows();
                                batches.push(batch);
                            }
                            Some(Some(Ok(_))) => break,
                            Some(Some(Err(e))) => return Err(e),
                            Some(None) => eof = true,
                            None => break,
                        }
                    }
                }
                let (mut state, mut local) = idle.pop().unwrap();
                let code = program.clone();
                let guard = shutdown_guard.clone();
                jobs.push(on_cpu(executor.as_ref(), move || {
                    let mut batches = batches.into_iter();
                    while let Some(batch) = batches.next() {
                        if guard.is_shutdown_requested() {
                            return Err(Error::cancelled());
                        }
                        let chunk = code
                            .run(&batch, &mut state, &guard)?
                            .filter(|c| c.num_rows() != 0);
                        let status = local.consume(chunk, &mut StateContext::new(&mut state))?;
                        if status != ConsumeStatus::End {
                            local.finish(status, &mut StateContext::new(&mut state), &guard)?;
                            return Ok(Completed {
                                pair: (state, local),
                                handoff: Some(status),
                                leftover: batches.collect(),
                            });
                        }
                    }
                    Ok(Completed {
                        pair: (state, local),
                        handoff: None,
                        leftover: vec![],
                    })
                }));
                continue;
            }
            if (eof || stop) && jobs.is_empty() && pending.is_empty() && morsel.is_none() {
                break;
            }
            if !stop && !eof && pending.is_empty() && morsel.is_none() && !idle.is_empty() {
                // This borrow stays alive while CPU completions are drained.
                // Early stop drops only the read future; the source remains
                // owned by the Pipeline and can still finish its resources.
                let mut next =
                    Box::pin(async { source.as_mut().unwrap().next().await.transpose() });
                loop {
                    enum Event {
                        Input(Result<Option<RecordBatch>>),
                        Done(Result<Completed>),
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
                        Either::Left(_) => return Err(Error::cancelled()),
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
                        Event::Done(completed_job) => {
                            stop = receive_completed(
                                self.sink.as_mut(),
                                completed_job?,
                                &mut idle,
                                &mut pending,
                                stop,
                                &shutdown_guard,
                            )
                            .await?;
                            completed += 1;
                            if completed % config.yield_batches == 0 {
                                yield_now().await;
                            }
                            if stop || !pending.is_empty() {
                                break;
                            }
                        }
                    }
                }
            } else {
                let local = match select(Box::pin(shutdown_guard.shutdown_requested()), jobs.next())
                    .await
                {
                    Either::Left(_) => return Err(Error::cancelled()),
                    Either::Right((local, _)) => local.expect("pending CPU work")?,
                };
                stop = receive_completed(
                    self.sink.as_mut(),
                    local,
                    &mut idle,
                    &mut pending,
                    stop,
                    &shutdown_guard,
                )
                .await?;
                completed += 1;
                if completed % config.yield_batches == 0 {
                    yield_now().await;
                }
            }
            if stop {
                source = None;
                pending.clear();
                morsel = None;
            }
        }
        drop(source);
        // Stage 2 completes for every instance before any End is handed off.
        let finishes = idle
            .into_iter()
            .map(|pair| {
                let guard = shutdown_guard.clone();
                on_cpu(executor.as_ref(), move || finish_pair(pair, &guard))
            })
            .collect::<Vec<_>>();
        let pairs = match select(
            Box::pin(shutdown_guard.shutdown_requested()),
            Box::pin(futures::future::try_join_all(finishes)),
        )
        .await
        {
            Either::Left(_) => return Err(Error::cancelled()),
            Either::Right((pairs, _)) => pairs?,
        };
        // Stage 3 keeps the pool order, independent of finish-job completion.
        for (mut state, mut local) in pairs {
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.sink
                .consume(
                    ConsumeStatus::End,
                    &mut *local,
                    StateContext::new(&mut state),
                    &shutdown_guard,
                )
                .await
                .with_location()?;
        }
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        self.source.finish(&shutdown_guard).await.with_location()?;
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        finish_sink(self.sink.as_mut(), &shutdown_guard, &cpu)
            .await
            .with_location()
    }
}

type Instance = (LocalState, Box<dyn LocalSink>);
struct Completed {
    pair: Instance,
    handoff: Option<ConsumeStatus>,
    leftover: Vec<RecordBatch>,
}

fn finish_pair((mut state, mut local): Instance, guard: &ShutdownGuard) -> Result<Instance> {
    if guard.is_shutdown_requested() {
        return Err(Error::cancelled());
    }
    local.finish(
        ConsumeStatus::End,
        &mut StateContext::new(&mut state),
        guard,
    )?;
    Ok((state, local))
}

fn catch_cpu<T>(work: impl FnOnce() -> Result<T>) -> Result<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)).unwrap_or_else(|_| Err(panicked()))
}

fn run_inline(
    program: &Program,
    batch: &RecordBatch,
    local: &mut LocalState,
    guard: &ShutdownGuard,
) -> Result<Option<RecordBatch>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        program.run(batch, local, guard)
    }))
    .unwrap_or_else(|_| Err(panicked()))
}

#[cold]
fn panicked() -> Error {
    Error::new(ErrorKind::TaskFailed, "CPU task panicked")
}

/// Hands off prepared data, requeues unprocessed batches in source order,
/// and returns the same instance to the bounded pool.
async fn receive_completed(
    sink: &mut dyn SinkExec,
    done: Completed,
    idle: &mut Vec<Instance>,
    pending: &mut VecDeque<RecordBatch>,
    mut stop: bool,
    guard: &ShutdownGuard,
) -> Result<bool> {
    let (mut state, mut local) = done.pair;
    if let Some(status) = done.handoff {
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        sink.consume(status, &mut *local, StateContext::new(&mut state), guard)
            .await
            .with_location()?;
        if status == ConsumeStatus::Stop {
            stop = true;
        }
        if status == ConsumeStatus::Flush && !stop {
            for batch in done.leftover.into_iter().rev() {
                pending.push_front(batch);
            }
        }
    }
    idle.push((state, local));
    Ok(stop)
}

/// Runs CPU work as a task of `executor`. Worker panics are caught and
/// returned as errors rather than unwinding across the async runtime.
fn on_cpu<E: Executor, T: Send + 'static, W: FnOnce() -> Result<T> + Send + 'static>(
    executor: &E,
    work: W,
) -> impl Future<Output = Result<T>> + use<E, T, W> {
    let task = executor.spawn(async move {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
            .unwrap_or_else(|_| Err(panicked()))
    });
    async move {
        task.await
            .map_err(|source| {
                Error::new(ErrorKind::TaskFailed, "CPU task did not return its state")
                    .with_source(source)
            })
            .and_then(|result| result)
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
            return Err(Error::invalid_plan(
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
                return Err(Error::cancelled());
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
                    Err(Error::internal(
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
                Either::Left(_) => return Err(Error::cancelled()),
                Either::Right((completed, _)) => completed,
            };
            let (id, result) = completed.expect("running pipeline exists");
            result
                .map_err(|source| {
                    Error::new(ErrorKind::TaskFailed, "pipeline task failed").with_source(source)
                })
                .and_then(|result| result)
                .with_context(|| ErrorContext::new("pipeline.execute").field("pipeline", id))?;
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
    use crate::column::{ColumnContext, ColumnId, ColumnType};
    use crate::exec::{SourceExec, downcast_local};
    use crate::kernel::KernelBuilder;
    use futures::{executor::block_on, future::BoxFuture};

    /// Runs each task on a thread of its own.
    struct ThreadTaskExecutor;

    impl Executor for ThreadTaskExecutor {
        type JoinError = futures::channel::oneshot::Canceled;
        type Handle<T>
            = futures::channel::oneshot::Receiver<T>
        where
            T: Send + 'static;

        fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static,
        {
            let (send, receive) = futures::channel::oneshot::channel();
            std::thread::spawn(move || {
                let _ = send.send(block_on(task));
            });
            receive
        }
    }
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct Observed {
        source_calls: AtomicUsize,
        cursors: AtomicUsize,
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

    /// Records consumed chunks and finishes after `stop_after` values.
    struct RecordingSink {
        observed: Arc<Observed>,
        stop_after: Option<usize>,
    }

    impl LocalSink for RecordingSink {
        fn consume(
            &mut self,
            chunk: Option<RecordBatch>,
            _: &mut StateContext<'_>,
        ) -> Result<ConsumeStatus> {
            let Some(chunk) = chunk else {
                return Ok(ConsumeStatus::End);
            };
            assert!(chunk.num_rows() > 0);
            self.observed
                .batch_sizes
                .lock()
                .unwrap()
                .push(chunk.num_rows());
            let mut collected = self.observed.values.lock().unwrap();
            collected.extend(values(&chunk));
            self.observed.batches.lock().unwrap().push(chunk);
            Ok(
                if self
                    .stop_after
                    .is_some_and(|limit| collected.len() >= limit)
                {
                    ConsumeStatus::Stop
                } else {
                    ConsumeStatus::End
                },
            )
        }
        fn finish(
            &mut self,
            _: ConsumeStatus,
            _: &mut StateContext<'_>,
            _: &ShutdownGuard,
        ) -> Result<()> {
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
    impl SinkExec for RecordingSink {
        fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            Ok(Box::new(Self {
                observed: self.observed.clone(),
                stop_after: self.stop_after,
            }))
        }
        fn consume<'a>(
            &'a mut self,
            status: ConsumeStatus,
            local: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                downcast_local::<Self>(local)?;
                if status == ConsumeStatus::End {
                    self.observed.combined.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            })
        }
        fn finish<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            _cpu: &'a dyn crate::exec::Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
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
    /// A program over one non-nullable Int64 column producing `build`'s output.
    fn program(
        build: impl FnOnce(&mut ColumnContext, ColumnId) -> crate::expr::scalar::ScalarExprRef,
    ) -> Program {
        let mut columns = ColumnContext::new();
        let input = columns.allocate(ColumnType::new(DataType::Int64, false));
        let expression = build(&mut columns, input);
        let mut builder = KernelBuilder::new(&columns);
        builder.bind_inputs(&[input]).unwrap();
        let errors = expression.build_kernels(&mut builder).unwrap();
        builder.raise(errors).unwrap();
        Program::compile(builder.finish(Some(&[expression.output()])).unwrap()).unwrap()
    }
    fn passthrough() -> Program {
        program(|_, input| crate::expr::scalar::ReferenceExpression::new(input).into_ref())
    }
    fn executor(
        batches: Vec<RecordBatch>,
        program: Program,
        stop_after: Option<usize>,
    ) -> (Pipeline, Arc<Observed>) {
        let observed = Arc::new(Observed::default());
        let pipeline = Pipeline::new(
            Box::new(Source {
                batches: batches.into(),
                observed: observed.clone(),
            }),
            program,
            Box::new(RecordingSink {
                observed: observed.clone(),
                stop_after,
            }),
        );
        (pipeline, observed)
    }
    #[test]
    fn source_morsels_split_and_empty_batches_are_skipped() {
        let (exec, observed) = executor(vec![batch(&[]), batch(&[1, 2, 3])], passthrough(), None);
        block_on(exec.execute(
            Arc::new(ThreadTaskExecutor),
            asyncband::shutdown::new().1,
            config(),
            1,
        ))
        .unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2, 3]);
        assert_eq!(*observed.batch_sizes.lock().unwrap(), [2, 1]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 3);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn whole_batches_keep_their_arrow_owners() {
        let input = batch(&[1, 2]);
        let owner = input.column(0).clone();
        let (exec, observed) = executor(vec![input], passthrough(), None);
        block_on(exec.execute(
            Arc::new(ThreadTaskExecutor),
            asyncband::shutdown::new().1,
            config(),
            1,
        ))
        .unwrap();
        assert!(Arc::ptr_eq(
            &owner,
            observed.batches.lock().unwrap()[0].column(0)
        ));
    }
    #[test]
    fn stop_does_not_read_more_input_and_hands_off_end_once() {
        let (exec, observed) =
            executor(vec![batch(&[1, 2, 3]), batch(&[4])], passthrough(), Some(1));
        block_on(exec.execute(
            Arc::new(ThreadTaskExecutor),
            asyncband::shutdown::new().1,
            config(),
            1,
        ))
        .unwrap();
        assert_eq!(*observed.values.lock().unwrap(), [1, 2]);
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn program_errors_do_not_publish_partial_results() {
        use crate::expr::scalar::*;
        let division = program(|columns, input| {
            let zero = columns.allocate(ColumnType::new(DataType::Int64, false));
            let output = columns.allocate(ColumnType::new(DataType::Int64, false));
            FunctionExpression::binary(
                FunctionKind::Divide,
                ReferenceExpression::new(input).into_ref(),
                ConstantExpression::int64(Some(0), zero).into_ref(),
                output,
            )
            .into_ref()
        });
        let (exec, observed) = executor(vec![batch(&[1])], division, None);
        assert!(
            block_on(exec.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                1
            ))
            .is_err()
        );
        assert!(observed.values.lock().unwrap().is_empty());
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn one_source_and_bounded_reused_locals_for_many_batches() {
        let (exec, observed) = executor(
            vec![batch(&(0..100).collect::<Vec<_>>())],
            passthrough(),
            None,
        );
        block_on(exec.execute(
            Arc::new(ThreadTaskExecutor),
            asyncband::shutdown::new().1,
            config(),
            3,
        ))
        .unwrap();
        assert_eq!(observed.cursors.load(Ordering::SeqCst), 1);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 3);
        let mut values = observed.values.lock().unwrap().clone();
        values.sort();
        assert_eq!(values, (0..100).collect::<Vec<_>>());
    }
    #[test]
    fn cancellation_does_not_read_or_handoff() {
        let (exec, observed) = executor(vec![batch(&[1, 2])], passthrough(), None);
        let (shutdown, guard) = asyncband::shutdown::new();
        shutdown.request_shutdown();
        assert!(matches!(
            block_on(exec.execute(Arc::new(ThreadTaskExecutor), guard, config(), 1)),
            Err(ref error) if error.kind() == ErrorKind::Cancelled
        ));
        assert_eq!(observed.source_calls.load(Ordering::SeqCst), 0);
        assert_eq!(observed.combined.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn empty_source_still_hands_off_end() {
        let (exec, observed) = executor(vec![], passthrough(), None);
        block_on(exec.execute(
            Arc::new(ThreadTaskExecutor),
            asyncband::shutdown::new().1,
            config(),
            1,
        ))
        .unwrap();
        assert_eq!(observed.combined.load(Ordering::SeqCst), 1);
    }
    // The second source read cannot finish until the first chunk is consumed.
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
                            .map_err(|_| Error::internal("lost consume notification".into()))?;
                        Ok(Some(batch(&[2])))
                    }
                    _ => Ok(None),
                }
            })
        }
    }
    struct FeedbackSink {
        send: Option<futures::channel::oneshot::Sender<()>>,
        consumed: usize,
    }
    struct HandoffLocal {
        status: ConsumeStatus,
        chunks: usize,
    }
    impl LocalSink for HandoffLocal {
        fn consume(
            &mut self,
            chunk: Option<RecordBatch>,
            _: &mut StateContext<'_>,
        ) -> Result<ConsumeStatus> {
            self.chunks += usize::from(chunk.is_some());
            Ok(self.status)
        }
        fn finish(
            &mut self,
            _: ConsumeStatus,
            _: &mut StateContext<'_>,
            _: &ShutdownGuard,
        ) -> Result<()> {
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
    impl SinkExec for FeedbackSink {
        fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            Ok(Box::new(HandoffLocal {
                status: ConsumeStatus::Flush,
                chunks: 0,
            }))
        }
        fn consume<'a>(
            &'a mut self,
            _: ConsumeStatus,
            local: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let local = downcast_local::<HandoffLocal>(local)?;
                self.consumed += std::mem::take(&mut local.chunks);
                if let Some(send) = self.send.take() {
                    let _ = send.send(());
                }
                Ok(())
            })
        }
        fn finish<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            _cpu: &'a dyn crate::exec::Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                assert_eq!(self.consumed, 2);
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn pending_source_read_keeps_progress_while_completed_chunks_are_consumed() {
        let (send, receive) = futures::channel::oneshot::channel();
        let pipeline = Pipeline::new(
            Box::new(FeedbackCursor {
                read: 0,
                ready: Some(receive),
            }),
            passthrough(),
            Box::new(FeedbackSink {
                send: Some(send),
                consumed: 0,
            }),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                2,
            ),
        )
        .await
        .expect("source and consume deadlocked")
        .unwrap();
    }
    // A source whose second read never completes must still be finalized after
    // the sink finishes. Keep the source owned by the coordinator, not the read.
    struct PendingStopSource {
        first: bool,
        pending: Arc<AtomicBool>,
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
    /// Finishes on the first chunk, once the second source read is pending.
    struct StopSink {
        pending: Arc<AtomicBool>,
        combined: Arc<AtomicUsize>,
    }
    impl SinkExec for StopSink {
        fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            Ok(Box::new(HandoffLocal {
                status: ConsumeStatus::Stop,
                chunks: 0,
            }))
        }
        fn consume<'a>(
            &'a mut self,
            status: ConsumeStatus,
            local: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                assert!(
                    self.pending.load(Ordering::SeqCst),
                    "second read never polled"
                );
                downcast_local::<HandoffLocal>(local)?.chunks = 0;
                if status == ConsumeStatus::End {
                    self.combined.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            })
        }
        fn finish<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            _cpu: &'a dyn crate::exec::Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    #[tokio::test]
    async fn early_stop_drops_pending_read_but_finishes_owned_source() {
        let pending = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicUsize::new(0));
        let combined = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::new(
            Box::new(PendingStopSource {
                first: true,
                pending: pending.clone(),
                finished: finished.clone(),
            }),
            passthrough(),
            Box::new(StopSink {
                pending,
                combined: combined.clone(),
            }),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                2,
            ),
        )
        .await
        .expect("finish must interrupt pending source read")
        .unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(combined.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn cpu_panics_are_returned_as_errors() {
        let result = block_on(on_cpu(&ThreadTaskExecutor, || -> Result<()> {
            panic!("intentional worker panic")
        }));
        assert!(matches!(result, Err(ref error) if error.kind() == ErrorKind::TaskFailed));
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Failure {
        None,
        ConsumeError,
        ConsumePanic,
        FinishError,
        FinishPanic,
        WaitForCancel,
    }

    #[derive(Default)]
    struct ProtocolLog {
        created: AtomicUsize,
        runs: AtomicUsize,
        finalized: AtomicBool,
        preparing: AtomicUsize,
        finish_threads: Mutex<Vec<std::thread::ThreadId>>,
        output: Mutex<Vec<i64>>,
        events: Mutex<Vec<(usize, ConsumeStatus, bool)>>,
    }
    struct ProtocolSink {
        log: Arc<ProtocolLog>,
        failure: Failure,
        flush: bool,
        stop_after: Option<usize>,
    }
    struct ProtocolLocal {
        id: usize,
        log: Arc<ProtocolLog>,
        failure: Failure,
        flush: bool,
        stop_after: Option<usize>,
        seen: usize,
        values: Vec<i64>,
        ready: Option<ConsumeStatus>,
    }
    impl LocalSink for ProtocolLocal {
        fn consume(
            &mut self,
            chunk: Option<RecordBatch>,
            _: &mut StateContext<'_>,
        ) -> Result<ConsumeStatus> {
            assert!(self.ready.is_none(), "handoff must reset the local");
            self.log.runs.fetch_add(1, Ordering::SeqCst);
            match self.failure {
                Failure::ConsumeError => return Err(Error::internal("consume failed".into())),
                Failure::ConsumePanic => panic!("consume panicked"),
                _ => {}
            }
            if let Some(chunk) = chunk {
                self.values.extend(values(&chunk));
                self.seen += chunk.num_rows();
            }
            Ok(if self.stop_after.is_some_and(|n| self.seen >= n) {
                ConsumeStatus::Stop
            } else if self.flush {
                ConsumeStatus::Flush
            } else {
                ConsumeStatus::End
            })
        }
        fn finish(
            &mut self,
            status: ConsumeStatus,
            _: &mut StateContext<'_>,
            guard: &ShutdownGuard,
        ) -> Result<()> {
            assert!(self.ready.is_none(), "one preparation per handoff");
            self.log.preparing.fetch_add(1, Ordering::SeqCst);
            self.log
                .finish_threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            match self.failure {
                Failure::FinishError => return Err(Error::internal("finish failed".into())),
                Failure::FinishPanic => panic!("finish panicked"),
                Failure::WaitForCancel => {
                    while !guard.is_shutdown_requested() {
                        std::thread::yield_now();
                    }
                    return Err(Error::cancelled());
                }
                _ => {}
            }
            self.ready = Some(status);
            self.log
                .events
                .lock()
                .unwrap()
                .push((self.id, status, false));
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
    impl SinkExec for ProtocolSink {
        fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            Ok(Box::new(ProtocolLocal {
                id: self.log.created.fetch_add(1, Ordering::SeqCst),
                log: self.log.clone(),
                failure: self.failure,
                flush: self.flush,
                stop_after: self.stop_after,
                seen: 0,
                values: vec![],
                ready: None,
            }))
        }
        fn consume<'a>(
            &'a mut self,
            status: ConsumeStatus,
            local: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let local = downcast_local::<ProtocolLocal>(local)?;
                assert_eq!(local.ready.take(), Some(status));
                if status == ConsumeStatus::End {
                    let events = self.log.events.lock().unwrap();
                    assert_eq!(
                        events
                            .iter()
                            .filter(|(_, s, handoff)| *s == ConsumeStatus::End && !handoff)
                            .count(),
                        self.log.created.load(Ordering::SeqCst),
                        "all preparations precede any End handoff"
                    );
                }
                self.log.output.lock().unwrap().append(&mut local.values);
                self.log
                    .events
                    .lock()
                    .unwrap()
                    .push((local.id, status, true));
                Ok(())
            })
        }
        fn finish<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            _cpu: &'a dyn crate::exec::Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.log.finalized.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    fn protocol_pipeline(
        batches: Vec<RecordBatch>,
        program: Program,
        failure: Failure,
        flush: bool,
        stop_after: Option<usize>,
    ) -> (Pipeline, Arc<ProtocolLog>) {
        let log = Arc::new(ProtocolLog::default());
        let pipeline = Pipeline::new(
            Box::new(Source {
                batches: batches.into(),
                observed: Arc::new(Observed::default()),
            }),
            program,
            Box::new(ProtocolSink {
                log: log.clone(),
                failure,
                flush,
                stop_after,
            }),
        );
        (pipeline, log)
    }
    fn assert_ends(log: &ProtocolLog, instances: usize) {
        assert!(log.finalized.load(Ordering::SeqCst));
        let events = log.events.lock().unwrap();
        for id in 0..instances {
            for handed_off in [false, true] {
                assert_eq!(
                    events
                        .iter()
                        .filter(|&&e| e == (id, ConsumeStatus::End, handed_off))
                        .count(),
                    1
                );
            }
        }
    }

    #[test]
    fn flush_requeues_coalesced_batches_without_loss_and_ordered_flush_keeps_order() {
        for (workers, ordered) in [(1, false), (4, false), (4, true)] {
            let input = (0..350).map(|i| batch(&[i])).collect();
            let (mut pipeline, log) =
                protocol_pipeline(input, passthrough(), Failure::None, true, None);
            pipeline.ordered = ordered;
            block_on(pipeline.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                workers,
            ))
            .unwrap();
            let mut output = log.output.lock().unwrap().clone();
            if workers > 1 && !ordered {
                output.sort();
            }
            assert_eq!(output, (0..350).collect::<Vec<_>>());
            assert_ends(&log, if ordered { 1 } else { workers });
        }
    }

    #[test]
    fn flush_leftovers_go_before_pending_batches_in_their_original_order() {
        let log = Arc::new(ProtocolLog::default());
        let mut sink = ProtocolSink {
            log,
            failure: Failure::None,
            flush: true,
            stop_after: None,
        };
        let mut local = sink
            .local_sink(&LocalSinkContext {
                batch_rows: 2,
                partitions: 1,
            })
            .unwrap();
        let mut state = LocalState::default();
        let guard = asyncband::shutdown::new().1;
        local
            .finish(
                ConsumeStatus::Flush,
                &mut StateContext::new(&mut state),
                &guard,
            )
            .unwrap();
        let done = Completed {
            pair: (state, local),
            handoff: Some(ConsumeStatus::Flush),
            leftover: vec![batch(&[1]), batch(&[2])],
        };
        let mut pending = VecDeque::from([batch(&[3])]);
        let mut idle = vec![];
        assert!(
            !block_on(receive_completed(
                &mut sink,
                done,
                &mut idle,
                &mut pending,
                false,
                &guard
            ))
            .unwrap()
        );
        assert_eq!(
            pending.iter().flat_map(values).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    fn idle_program() -> Program {
        use crate::kernel::{
            ExpandKind, StateBuilder,
            table::{Table, TableState},
        };
        let columns = ColumnContext::new();
        let table = Arc::new(Table::new(Arc::new(Schema::empty()), &[], &[], false).unwrap());
        let mut builder = KernelBuilder::new(&columns);
        let state = builder.state(StateBuilder::new(move || {
            Ok(TableState {
                table: table.clone(),
            })
        }));
        let (domain, _) = builder.expand(ExpandKind::Product { state }).unwrap();
        builder.set_current_domain(domain).unwrap();
        let program = Program::compile(builder.finish(Some(&[])).unwrap()).unwrap();
        assert!(program.idle(&program.new_local().unwrap()));
        program
    }

    #[test]
    fn empty_and_idle_programs_finish_every_instance_without_consuming() {
        for workers in [1, 4] {
            for idle in [false, true] {
                let (pipeline, log) = protocol_pipeline(
                    if idle { vec![batch(&[1])] } else { vec![] },
                    if idle { idle_program() } else { passthrough() },
                    Failure::None,
                    false,
                    None,
                );
                block_on(pipeline.execute(
                    Arc::new(ThreadTaskExecutor),
                    asyncband::shutdown::new().1,
                    config(),
                    workers,
                ))
                .unwrap();
                assert_eq!(log.runs.load(Ordering::SeqCst), 0);
                assert_ends(&log, workers);
            }
        }
    }

    #[test]
    fn ordered_stop_preserves_prefix_and_still_hands_off_end() {
        for workers in [1, 4] {
            let (mut pipeline, log) = protocol_pipeline(
                vec![batch(&[0, 1, 2, 3, 4, 5]), batch(&[6])],
                passthrough(),
                Failure::None,
                true,
                Some(4),
            );
            pipeline.ordered = true;
            block_on(pipeline.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                workers,
            ))
            .unwrap();
            assert_eq!(*log.output.lock().unwrap(), [0, 1, 2, 3]);
            assert_ends(&log, 1);
        }
    }

    #[test]
    fn local_errors_and_panics_never_publish() {
        for (workers, ordered) in [(1, false), (3, false), (3, true)] {
            for failure in [
                Failure::ConsumeError,
                Failure::ConsumePanic,
                Failure::FinishError,
                Failure::FinishPanic,
            ] {
                for flush in [false, true] {
                    let (mut pipeline, log) =
                        protocol_pipeline(vec![batch(&[1])], passthrough(), failure, flush, None);
                    pipeline.ordered = ordered;
                    let error = block_on(pipeline.execute(
                        Arc::new(ThreadTaskExecutor),
                        asyncband::shutdown::new().1,
                        config(),
                        workers,
                    ))
                    .unwrap_err();
                    let kind = if matches!(failure, Failure::ConsumePanic | Failure::FinishPanic) {
                        ErrorKind::TaskFailed
                    } else {
                        ErrorKind::Internal
                    };
                    assert_eq!(
                        error.kind(),
                        kind,
                        "{failure:?}, {workers}, {ordered}, {flush}"
                    );
                    assert!(!log.finalized.load(Ordering::SeqCst));
                    assert!(!log.events.lock().unwrap().iter().any(|e| e.2));
                }
            }
        }
    }

    #[tokio::test]
    async fn cancellation_during_parallel_end_preparation_does_not_handoff() {
        for ordered in [false, true] {
            let (mut pipeline, log) =
                protocol_pipeline(vec![], passthrough(), Failure::WaitForCancel, false, None);
            pipeline.ordered = ordered;
            let (shutdown, guard) = asyncband::shutdown::new();
            let future = pipeline.execute(Arc::new(ThreadTaskExecutor), guard, config(), 3);
            let cancel = async {
                while log.preparing.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                shutdown.request_shutdown();
            };
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                futures::join!(future, cancel)
            })
            .await
            .unwrap();
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled);
            assert!(!log.finalized.load(Ordering::SeqCst));
            assert!(!log.events.lock().unwrap().iter().any(|e| e.2));
            shutdown.await;
        }
    }
    struct InflightLog {
        started: std::sync::Barrier,
        stopped: (Mutex<bool>, std::sync::Condvar),
        ids: AtomicUsize,
        runs: AtomicUsize,
        ends: Mutex<Vec<usize>>,
        handoffs: Mutex<Vec<ConsumeStatus>>,
    }
    struct InflightLocal {
        id: usize,
        log: Arc<InflightLog>,
    }
    struct InflightSink(Arc<InflightLog>);
    impl LocalSink for InflightLocal {
        fn consume(
            &mut self,
            _: Option<RecordBatch>,
            _: &mut StateContext<'_>,
        ) -> Result<ConsumeStatus> {
            self.log.runs.fetch_add(1, Ordering::SeqCst);
            self.log.started.wait();
            if self.id == 0 {
                return Ok(ConsumeStatus::Stop);
            }
            let (lock, wake) = &self.log.stopped;
            let _stopped = wake
                .wait_while(lock.lock().unwrap(), |stopped| !*stopped)
                .unwrap();
            Ok(ConsumeStatus::Flush)
        }
        fn finish(
            &mut self,
            _: ConsumeStatus,
            _: &mut StateContext<'_>,
            _: &ShutdownGuard,
        ) -> Result<()> {
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
    impl SinkExec for InflightSink {
        fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            Ok(Box::new(InflightLocal {
                id: self.0.ids.fetch_add(1, Ordering::SeqCst),
                log: self.0.clone(),
            }))
        }
        fn consume<'a>(
            &'a mut self,
            status: ConsumeStatus,
            local: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                let local = downcast_local::<InflightLocal>(local)?;
                if status == ConsumeStatus::End {
                    self.0.ends.lock().unwrap().push(local.id);
                } else {
                    self.0.handoffs.lock().unwrap().push(status);
                }
                if status == ConsumeStatus::Stop {
                    *self.0.stopped.0.lock().unwrap() = true;
                    self.0.stopped.1.notify_all();
                }
                Ok(())
            })
        }
        fn finish<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            _cpu: &'a dyn crate::exec::Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    #[tokio::test]
    async fn stop_drains_inflight_flushes_and_ends_without_dispatching_more() {
        let log = Arc::new(InflightLog {
            started: std::sync::Barrier::new(3),
            stopped: (Mutex::new(false), std::sync::Condvar::new()),
            ids: AtomicUsize::new(0),
            runs: AtomicUsize::new(0),
            ends: Mutex::new(vec![]),
            handoffs: Mutex::new(vec![]),
        });
        let pipeline = Pipeline::new(
            Box::new(Source {
                batches: VecDeque::from([batch(&(0..100).collect::<Vec<_>>())]),
                observed: Arc::new(Observed::default()),
            }),
            passthrough(),
            Box::new(InflightSink(log.clone())),
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                3,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(log.runs.load(Ordering::SeqCst), 3);
        assert_eq!(
            *log.handoffs.lock().unwrap(),
            [
                ConsumeStatus::Stop,
                ConsumeStatus::Flush,
                ConsumeStatus::Flush
            ]
        );
        let mut ends = log.ends.lock().unwrap().clone();
        ends.sort();
        assert_eq!(ends, [0, 1, 2]);
    }
    #[test]
    fn outputless_runs_still_call_local_consume_and_end_finishes_on_the_expected_thread() {
        let caller = std::thread::current().id();
        for (workers, ordered) in [(1, false), (3, true), (3, false)] {
            let (mut pipeline, log) = protocol_pipeline(
                vec![batch(&[1, 2, 3])],
                Program::empty(),
                Failure::None,
                false,
                None,
            );
            pipeline.ordered = ordered;
            block_on(pipeline.execute(
                Arc::new(ThreadTaskExecutor),
                asyncband::shutdown::new().1,
                config(),
                workers,
            ))
            .unwrap();
            assert_eq!(log.runs.load(Ordering::SeqCst), 2);
            assert!(log.output.lock().unwrap().is_empty());
            assert_ends(&log, if ordered { 1 } else { workers });
            for &thread in log.finish_threads.lock().unwrap().iter() {
                assert_eq!(thread == caller, workers == 1);
            }
        }
    }
    #[test]
    fn cpu_spawn_submits_before_its_future_is_polled_and_run_all_orders_results() {
        let cpu = ExecutorCpu {
            executor: Arc::new(ThreadTaskExecutor),
            parallelism: 4,
        };
        let (send, receive) = std::sync::mpsc::channel();
        let job = cpu.spawn(Box::new(move || {
            send.send(()).unwrap();
            Ok(())
        }));
        receive
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("spawn must be eager");
        block_on(job).unwrap();
        let jobs = (0usize..8)
            .map(|i| Box::new(move || Ok(i)) as Box<dyn FnOnce() -> Result<usize> + Send>)
            .collect();
        assert_eq!(
            block_on(crate::exec::run_all(&cpu, jobs)).unwrap(),
            (0..8).collect::<Vec<_>>()
        );
        assert_eq!([0, 1, 2, 3, 4].map(partition_count), [1, 1, 4, 8, 8]);
    }

    struct CpuFinishSink {
        failure: u8,
        workers: usize,
        contexts: Arc<Mutex<Vec<usize>>>,
        started: Arc<AtomicBool>,
        published: Arc<AtomicBool>,
    }
    impl SinkExec for CpuFinishSink {
        fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            self.contexts.lock().unwrap().push(ctx.partitions);
            Ok(Box::new(RecordingSink {
                observed: Arc::new(Observed::default()),
                stop_after: None,
            }))
        }
        fn consume<'a>(
            &'a mut self,
            _: ConsumeStatus,
            _: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn finish<'a>(
            &'a mut self,
            guard: &'a ShutdownGuard,
            cpu: &'a dyn Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                assert_eq!(cpu.parallelism(), self.workers);
                let mut jobs: Vec<Box<dyn FnOnce() -> Result<usize> + Send>> = vec![];
                for i in 0..3 {
                    let guard = guard.clone();
                    let started = self.started.clone();
                    let failure = self.failure;
                    jobs.push(Box::new(move || {
                        if i == 0 {
                            started.store(true, Ordering::SeqCst);
                            match failure {
                                1 => return Err(Error::internal("finish job failed".into())),
                                2 => panic!("finish job panicked"),
                                3 => {
                                    while !guard.is_shutdown_requested() {
                                        std::thread::yield_now();
                                    }
                                }
                                _ => {}
                            }
                        }
                        if guard.is_shutdown_requested() {
                            return Err(Error::cancelled());
                        }
                        Ok(i)
                    }));
                }
                assert_eq!(crate::exec::run_all(cpu, jobs).await?, [0, 1, 2]);
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                self.published.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    #[test]
    fn final_cpu_jobs_respect_ordered_partition_count_and_never_publish_on_failure() {
        for (workers, ordered) in [(1, false), (4, false), (4, true)] {
            for failure in 0..4 {
                let contexts = Arc::new(Mutex::new(vec![]));
                let started = Arc::new(AtomicBool::new(false));
                let published = Arc::new(AtomicBool::new(false));
                let mut pipeline = Pipeline::new(
                    Box::new(Source {
                        batches: vec![batch(&[1, 2])].into(),
                        observed: Arc::new(Observed::default()),
                    }),
                    passthrough(),
                    Box::new(CpuFinishSink {
                        failure,
                        workers,
                        contexts: contexts.clone(),
                        started: started.clone(),
                        published: published.clone(),
                    }),
                );
                pipeline.ordered = ordered;
                let (shutdown, guard) = asyncband::shutdown::new();
                let canceller = if failure == 3 {
                    let shutdown = shutdown.clone();
                    Some(std::thread::spawn(move || {
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(2);
                        while !started.load(Ordering::SeqCst)
                            && std::time::Instant::now() < deadline
                        {
                            std::thread::yield_now();
                        }
                        shutdown.request_shutdown();
                    }))
                } else {
                    None
                };
                let result = block_on(pipeline.execute(
                    Arc::new(ThreadTaskExecutor),
                    guard,
                    config(),
                    workers,
                ));
                match failure {
                    0 => result.unwrap(),
                    1 => assert_eq!(result.unwrap_err().kind(), ErrorKind::Internal),
                    2 => assert_eq!(result.unwrap_err().kind(), ErrorKind::TaskFailed),
                    _ => assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled),
                }
                assert_eq!(published.load(Ordering::SeqCst), failure == 0);
                assert_eq!(
                    *contexts.lock().unwrap(),
                    vec![partition_count(workers); if ordered { 1 } else { workers }]
                );
                if let Some(canceller) = canceller {
                    canceller.join().unwrap();
                }
                block_on(shutdown);
            }
        }
    }
}
