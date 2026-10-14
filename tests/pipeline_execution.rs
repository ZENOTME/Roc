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

use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::{channel::oneshot, future::BoxFuture};
use roc::{
    error::{Error, ErrorKind, Result},
    exec::{SinkExec, SourceExec},
    kernel::Program,
    pipeline::{Executor, Pipeline, PipelineExecutionConfig, PipelineGraph, PipelineGraphExecutor},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

struct ThreadTaskExecutor;

impl Executor for ThreadTaskExecutor {
    type JoinError = oneshot::Canceled;
    type Handle<T>
        = oneshot::Receiver<T>
    where
        T: Send + 'static;

    fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (send, receive) = oneshot::channel();
        std::thread::spawn(move || {
            let _ = send.send(futures::executor::block_on(task));
        });
        receive
    }
}

struct TokioTaskExecutor(tokio::runtime::Handle);

impl Executor for TokioTaskExecutor {
    type JoinError = tokio::task::JoinError;
    type Handle<T>
        = tokio::task::JoinHandle<T>
    where
        T: Send + 'static;

    fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.spawn(task)
    }
}

#[tokio::test]
async fn tokio_handle_can_be_used_directly() {
    let executor = TokioTaskExecutor(tokio::runtime::Handle::current());
    assert_eq!(executor.spawn(async { 42 }).await.unwrap(), 42);
}

struct EmptySource {
    prior_finishes: Arc<AtomicUsize>,
    required_finishes: Option<usize>,
}

impl SourceExec for EmptySource {
    fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
        if self
            .required_finishes
            .is_some_and(|n| self.prior_finishes.load(Ordering::SeqCst) != n)
        {
            return Err(Error::internal(
                "dependent pipeline started too early".into(),
            ));
        }
        Ok(())
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async { Ok(None) })
    }
}
struct WaitForCancelSource(mpsc::Sender<ShutdownGuard>);
impl SourceExec for WaitForCancelSource {
    fn start(&mut self, guard: &ShutdownGuard) -> Result<()> {
        self.0.send(guard.clone()).unwrap();
        Ok(())
    }
    fn next_batch<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            guard.shutdown_requested().await;
            Err(Error::cancelled())
        })
    }
}
struct FailingSource;
impl SourceExec for FailingSource {
    fn next_batch<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async { Err(Error::new(ErrorKind::External, "source failed")) })
    }
}
struct CountingSink(Arc<AtomicUsize>);
impl SinkExec for CountingSink {
    fn finish<'a>(&'a mut self, _: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[test]
fn runs_pipeline_tasks_and_waits_for_dependencies_to_finalize() {
    let finishes = Arc::new(AtomicUsize::new(0));
    let pipelines = [None, None, Some(2)]
        .into_iter()
        .map(|required_finishes| {
            Pipeline::new(
                Box::new(EmptySource {
                    prior_finishes: finishes.clone(),
                    required_finishes,
                }),
                Program::empty(),
                Box::new(CountingSink(finishes.clone())),
            )
        })
        .collect();
    let graph = PipelineGraph::new(pipelines, vec![vec![], vec![], vec![0, 1]]).unwrap();
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(ThreadTaskExecutor)
        .with_parallelism(2);

    let shutdown = executor.shutdown();
    futures::executor::block_on(executor.execute()).unwrap();
    futures::executor::block_on(async {
        assert!(futures::poll!(std::pin::pin!(shutdown)).is_ready());
    });
    assert_eq!(finishes.load(Ordering::SeqCst), 3);
}

#[test]
fn caller_can_cancel_execution_without_a_tokio_runtime() {
    let (started, observed_start) = mpsc::channel();
    let graph = PipelineGraph::new(
        vec![Pipeline::new(
            Box::new(WaitForCancelSource(started)),
            Program::empty(),
            Box::new(CountingSink(Arc::new(AtomicUsize::new(0)))),
        )],
        vec![vec![]],
    )
    .unwrap();
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(ThreadTaskExecutor)
        .with_parallelism(2);
    let shutdown = executor.shutdown();
    let running = std::thread::spawn(move || futures::executor::block_on(executor.execute()));
    observed_start
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    shutdown.request_shutdown();
    assert!(
        matches!(running.join().unwrap(), Err(ref error) if error.kind() == ErrorKind::Cancelled)
    );
    futures::executor::block_on(shutdown);
}

#[test]
fn source_failure_does_not_combine_or_request_shutdown() {
    let finishes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::new(
        Box::new(FailingSource),
        Program::empty(),
        Box::new(CountingSink(finishes.clone())),
    );
    let (shutdown, guard) = asyncband::shutdown::new();
    let observer = guard.clone();
    let result = futures::executor::block_on(pipeline.execute(
        Arc::new(ThreadTaskExecutor),
        guard,
        PipelineExecutionConfig::default(),
        2,
    ));
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::External);
    assert_eq!(error.message(), "source failed");
    assert!(!observer.is_shutdown_requested());
    drop(observer);
    futures::executor::block_on(shutdown);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}

#[test]
fn graph_failure_leaves_other_pipelines_for_caller_to_cancel() {
    let finishes = Arc::new(AtomicUsize::new(0));
    let (started, observed_start) = mpsc::channel();
    let pipelines = vec![
        Pipeline::new(
            Box::new(FailingSource),
            Program::empty(),
            Box::new(CountingSink(finishes.clone())),
        ),
        Pipeline::new(
            Box::new(WaitForCancelSource(started)),
            Program::empty(),
            Box::new(CountingSink(finishes.clone())),
        ),
    ];
    let executor =
        PipelineGraphExecutor::new(PipelineGraph::new(pipelines, vec![vec![], vec![]]).unwrap())
            .with_task_executor(ThreadTaskExecutor)
            .with_parallelism(2);
    let shutdown = executor.shutdown();
    let result = futures::executor::block_on(executor.execute());
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::External);
    assert_eq!(error.message(), "source failed");
    let context = error.frames().last().unwrap().context.as_ref().unwrap();
    assert_eq!(context.operation, "pipeline.execute");
    assert_eq!(context.fields, [("pipeline", "0".to_string())]);
    let observer = observed_start
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert!(!observer.is_shutdown_requested());
    drop(observer);
    shutdown.request_shutdown();
    futures::executor::block_on(shutdown);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}

#[test]
fn dropping_graph_execution_does_not_request_shutdown() {
    let (started, observed_start) = mpsc::channel();
    let graph = PipelineGraph::new(
        vec![Pipeline::new(
            Box::new(WaitForCancelSource(started)),
            Program::empty(),
            Box::new(CountingSink(Arc::new(AtomicUsize::new(0)))),
        )],
        vec![vec![]],
    )
    .unwrap();
    let executor = PipelineGraphExecutor::new(graph).with_task_executor(ThreadTaskExecutor);
    let shutdown = executor.shutdown();
    futures::executor::block_on(async {
        let mut running = Box::pin(executor.execute());
        assert!(futures::poll!(&mut running).is_pending());
        let watch = observed_start
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        drop(running);
        assert!(!watch.is_shutdown_requested());
        drop(watch);
        shutdown.await;
    });
}
