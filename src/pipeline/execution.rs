//! Pipeline task execution and pipeline-graph scheduling.
use super::{Pipeline, PipelineGraph};
use crate::{
    Cancel, Error, Result,
    exec::{
        GlobalExecContextRef, ProcessExecutor, ProcessResult, SinkExecutor, SinkResult,
        SourceExecutor,
    },
};
use arrow::record_batch::RecordBatch;
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
            return Err(Error::Plan("batch and yield sizes must be positive".into()));
        }
        Ok(())
    }
}

/// Owns one graph execution and the resources supplied by its caller.
pub struct PipelineGraphExecutor<E> {
    graph: PipelineGraph,
    task_executor: E,
    cancel: Cancel,
    config: PipelineExecutionConfig,
    parallelism: usize,
}

impl PipelineGraphExecutor<()> {
    pub fn new(graph: PipelineGraph) -> Self {
        Self {
            graph,
            task_executor: (),
            cancel: Cancel::new(),
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
            cancel: self.cancel,
            config: self.config,
            parallelism: self.parallelism,
        }
    }

    pub fn with_cancel(mut self, cancel: Cancel) -> Self {
        self.cancel = cancel;
        self
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

struct CancelOnDrop(Cancel);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
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
    pub async fn execute<E: Executor>(
        self,
        task_executor: Arc<E>,
        cancel: Cancel,
        config: PipelineExecutionConfig,
        parallelism: usize,
    ) -> Result<()> {
        config.validate()?;
        if parallelism == 0 {
            return Err(Error::Plan("task parallelism must be positive".into()));
        }
        let local_cancel = cancel.child();
        let _guard = CancelOnDrop(local_cancel.clone());
        let global = self.init_global_context(&local_cancel)?;
        let executors = (0..parallelism)
            .map(|_| self.new_executor(&global))
            .collect::<Result<Vec<_>>>()?;
        let mut tasks = FuturesUnordered::new();
        for executor in executors {
            tasks.push(task_executor.spawn(executor.execute(local_cancel.clone(), config)));
        }

        let mut failure = None;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    local_cancel.cancel();
                    failure.get_or_insert(error);
                }
                Err(error) => {
                    local_cancel.cancel();
                    failure
                        .get_or_insert_with(|| Error::Execution(format!("task failed: {error}")));
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }

        self.finalize(&global, &local_cancel).await
    }

    fn init_global_context(&self, cancel: &Cancel) -> Result<PipelineGlobalContext> {
        let sink = self.sink.init_global_context(cancel)?;
        let processors = self
            .processors
            .iter()
            .map(|process| process.init_global_context(cancel))
            .collect::<Result<Vec<_>>>()?;
        let source = self.source.init_global_context(cancel)?;
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

    async fn finalize(&self, global: &PipelineGlobalContext, cancel: &Cancel) -> Result<()> {
        self.source.finalize(global.source.clone(), cancel).await?;
        self.sink.finalize(global.sink.clone(), cancel).await
    }
}

/// Task-local execution state. Every task gets a fully initialized executor.
struct PipelineExecutor {
    source: Box<dyn SourceExecutor>,
    processors: Vec<Box<dyn ProcessExecutor>>,
    sink: Box<dyn SinkExecutor>,
}
impl PipelineExecutor {
    async fn execute(mut self, cancel: Cancel, config: PipelineExecutionConfig) -> Result<()> {
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
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if let Some((index, input)) = pending.pop() {
                if index == self.processors.len() {
                    if self.sink.sink(&cancel, &input).await? == SinkResult::Finished {
                        break;
                    }
                } else {
                    let output = match self.processors[index].execute(&cancel, &input)? {
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
                match self.processors[index].finish(&cancel)? {
                    Some(output) => {
                        if output.num_rows() > 0 {
                            pending.push((index + 1, output));
                        }
                    }
                    None => finishing = Some(index + 1),
                }
            } else {
                if morsel.is_none() {
                    morsel = self.source.next_batch(&cancel).await?;
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
        self.sink.combine(&cancel).await
    }
}

pub(crate) async fn yield_now() {
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
    pub async fn execute(self) -> Result<()> {
        self.config.validate()?;
        let task_executor = Arc::new(self.task_executor);
        if self.parallelism == 0 {
            return Err(Error::Plan("task parallelism must be positive".into()));
        }
        let graph_cancel = self.cancel.child();
        let _guard = CancelOnDrop(graph_cancel.clone());
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
        let mut failure = None;
        loop {
            if failure.is_none() {
                if graph_cancel.is_cancelled() {
                    failure = Some(Error::Cancelled);
                } else {
                    while let Some(id) = ready.pop_front() {
                        let pipeline = pipelines[id].take().expect("pipeline started once");
                        let task = task_executor.spawn(pipeline.execute(
                            task_executor.clone(),
                            graph_cancel.clone(),
                            self.config,
                            self.parallelism,
                        ));
                        running.push(async move { (id, task.await) });
                    }
                }
            }
            if running.is_empty() {
                return match failure {
                    Some(error) => Err(error),
                    None if finished == pipelines.len() => Ok(()),
                    None => Err(Error::Execution(
                        "pipeline graph has no runnable pipeline".into(),
                    )),
                };
            }
            let completed = if failure.is_none() {
                match select(Box::pin(graph_cancel.cancelled()), running.next()).await {
                    Either::Left(_) => {
                        failure = Some(Error::Cancelled);
                        continue;
                    }
                    Either::Right((completed, _)) => completed,
                }
            } else {
                running.next().await
            };
            let (id, result) = completed.expect("running pipeline exists");
            match result {
                Ok(Ok(())) if failure.is_none() => {
                    finished += 1;
                    for &dependent in &graph.dependents[id] {
                        let remaining = &mut remaining_dependencies[dependent];
                        *remaining -= 1;
                        if *remaining == 0 {
                            ready.push_back(dependent);
                        }
                    }
                }
                Ok(Err(error)) if failure.is_none() => {
                    graph_cancel.cancel();
                    failure = Some(error);
                }
                Err(error) if failure.is_none() => {
                    graph_cancel.cancel();
                    failure = Some(Error::Execution(format!("task failed: {error}")));
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
