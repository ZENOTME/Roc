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
    error::{Error, Result},
    exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult, SourceExec, SourceExecutor},
    pipeline::{Executor, Pipeline, PipelineExecutionConfig, PipelineGraph, PipelineGraphExecutor},
};
use std::sync::{
    Arc, Barrier,
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
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        if let Some(required_finishes) = self.required_finishes
            && self.prior_finishes.load(Ordering::SeqCst) != required_finishes
        {
            return Err(Error::Execution(
                "dependent pipeline started too early".into(),
            ));
        }
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(EmptySourceExecutor))
    }

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct EmptySourceExecutor;
impl SourceExecutor for EmptySourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async { Ok(None) })
    }
}

struct WaitForCancelSource(mpsc::Sender<ShutdownGuard>);

impl SourceExec for WaitForCancelSource {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        self.0.send(_shutdown_guard.clone()).unwrap();
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(WaitForCancelExecutor))
    }

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct WaitForCancelExecutor;

impl SourceExecutor for WaitForCancelExecutor {
    fn next_batch<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            shutdown_guard.shutdown_requested().await;
            Err(Error::Cancelled)
        })
    }
}

struct FailingSource {
    next_worker: AtomicUsize,
    cancelled_workers: Arc<AtomicUsize>,
    started: Arc<Barrier>,
}

impl SourceExec for FailingSource {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }

    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(FailingSourceExecutor {
            worker: self.next_worker.fetch_add(1, Ordering::SeqCst),
            cancelled_workers: self.cancelled_workers.clone(),
            started: self.started.clone(),
        }))
    }

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct FailingSourceExecutor {
    worker: usize,
    cancelled_workers: Arc<AtomicUsize>,
    started: Arc<Barrier>,
}

impl SourceExecutor for FailingSourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            // This test runs each worker on its own thread. Ensure both are in
            // next_batch before failure, so cancellation cannot skip the call.
            self.started.wait();
            if self.worker == 0 {
                Err(Error::Execution("source failed".into()))
            } else {
                shutdown_guard.shutdown_requested().await;
                self.cancelled_workers.fetch_add(1, Ordering::SeqCst);
                Err(Error::Cancelled)
            }
        })
    }
}

struct CountingSink(Arc<AtomicUsize>);
impl SinkExec for CountingSink {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        Ok(Box::new(CountingSinkExecutor))
    }

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct CountingSinkExecutor;
impl SinkExecutor for CountingSinkExecutor {
    fn sink<'a>(
        &'a mut self,
        _shutdown_guard: &'a ShutdownGuard,
        _input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async { Ok(SinkResult::NeedMoreInput) })
    }

    fn combine(self: Box<Self>, _shutdown_guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn runs_pipeline_tasks_and_waits_for_dependencies_to_finalize() {
    let finishes = Arc::new(AtomicUsize::new(0));
    let pipelines = [None, None, Some(2)]
        .into_iter()
        .map(|required_finishes| Pipeline {
            source: Box::new(EmptySource {
                prior_finishes: finishes.clone(),
                required_finishes,
            }),
            processors: vec![],
            sink: Box::new(CountingSink(finishes.clone())),
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
        vec![Pipeline {
            source: Box::new(WaitForCancelSource(started)),
            processors: vec![],
            sink: Box::new(CountingSink(Arc::new(AtomicUsize::new(0)))),
        }],
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
    assert!(matches!(running.join().unwrap(), Err(Error::Cancelled)));
    futures::executor::block_on(shutdown);
}

#[test]
fn worker_failure_returns_without_shutdown_and_caller_stops_remaining_workers() {
    let cancelled_workers = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline {
        source: Box::new(FailingSource {
            next_worker: AtomicUsize::new(0),
            cancelled_workers: cancelled_workers.clone(),
            started: Arc::new(Barrier::new(2)),
        }),
        processors: vec![],
        sink: Box::new(CountingSink(finishes.clone())),
    };
    let (shutdown, shutdown_guard) = asyncband::shutdown::new();
    let observer = shutdown_guard.clone();
    let result = futures::executor::block_on(pipeline.execute(
        Arc::new(ThreadTaskExecutor),
        shutdown_guard,
        PipelineExecutionConfig::default(),
        2,
    ));
    assert!(matches!(result, Err(Error::Execution(message)) if message == "source failed"));
    assert!(!observer.is_shutdown_requested());
    drop(observer);
    assert_eq!(cancelled_workers.load(Ordering::SeqCst), 0);
    futures::executor::block_on(shutdown);
    assert_eq!(cancelled_workers.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}

#[test]
fn graph_failure_returns_before_caller_requests_shutdown() {
    let cancelled_workers = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline {
        source: Box::new(FailingSource {
            next_worker: AtomicUsize::new(0),
            cancelled_workers: cancelled_workers.clone(),
            started: Arc::new(Barrier::new(2)),
        }),
        processors: vec![],
        sink: Box::new(CountingSink(finishes.clone())),
    };
    let graph = PipelineGraph::new(vec![pipeline], vec![vec![]]).unwrap();
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(ThreadTaskExecutor)
        .with_parallelism(2);
    let shutdown = executor.shutdown();
    let result = futures::executor::block_on(executor.execute());
    assert!(matches!(result, Err(Error::Execution(message)) if message == "source failed"));
    assert_eq!(cancelled_workers.load(Ordering::SeqCst), 0);
    futures::executor::block_on(shutdown);
    assert_eq!(cancelled_workers.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}

#[test]
fn dropping_graph_execution_does_not_request_shutdown() {
    let (started, observed_start) = mpsc::channel();
    let graph = PipelineGraph::new(
        vec![Pipeline {
            source: Box::new(WaitForCancelSource(started)),
            processors: vec![],
            sink: Box::new(CountingSink(Arc::new(AtomicUsize::new(0)))),
        }],
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
