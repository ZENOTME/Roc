use arrow::record_batch::RecordBatch;
use futures::{channel::oneshot, future::BoxFuture};
use roc::{
    CancellationToken, Error, Result,
    exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkStatus, SourceExec, SourceExecutor},
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
    fn init_global_context(
        &self,
        _batch_rows: usize,
        _cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef> {
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
}

struct EmptySourceExecutor;
impl SourceExecutor for EmptySourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        _ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async { Ok(None) })
    }
}

struct WaitForCancelSource(mpsc::Sender<()>);

impl SourceExec for WaitForCancelSource {
    fn init_global_context(
        &self,
        _batch_rows: usize,
        _cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        self.0.send(()).unwrap();
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(WaitForCancelExecutor))
    }
}

struct WaitForCancelExecutor;

impl SourceExecutor for WaitForCancelExecutor {
    fn next_batch<'a>(
        &'a mut self,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            ctx.cancelled().await;
            Err(Error::Cancelled)
        })
    }
}

struct FailingSource {
    next_worker: AtomicUsize,
    cancelled_workers: Arc<AtomicUsize>,
}

impl SourceExec for FailingSource {
    fn init_global_context(
        &self,
        _batch_rows: usize,
        _cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }

    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(FailingSourceExecutor {
            worker: self.next_worker.fetch_add(1, Ordering::SeqCst),
            cancelled_workers: self.cancelled_workers.clone(),
        }))
    }
}

struct FailingSourceExecutor {
    worker: usize,
    cancelled_workers: Arc<AtomicUsize>,
}

impl SourceExecutor for FailingSourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if self.worker == 0 {
                Err(Error::Execution("source failed".into()))
            } else {
                ctx.cancelled().await;
                self.cancelled_workers.fetch_add(1, Ordering::SeqCst);
                Err(Error::Cancelled)
            }
        })
    }
}

struct CountingSink(Arc<AtomicUsize>);
impl SinkExec for CountingSink {
    fn init_global_context(
        &self,
        _batch_rows: usize,
        _cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        Ok(Box::new(CountingSinkExecutor))
    }

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _ctx: &'a CancellationToken,
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
        _ctx: &'a CancellationToken,
        _input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        Box::pin(async { Ok(SinkStatus::NeedMoreInput) })
    }

    fn combine<'a>(self: Box<Self>, _ctx: &'a CancellationToken) -> BoxFuture<'a, Result<()>> {
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
        .with_parallelism(2)
        .with_cancel(CancellationToken::new());

    futures::executor::block_on(executor.execute()).unwrap();
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
    let cancel = CancellationToken::new();
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(ThreadTaskExecutor)
        .with_parallelism(2)
        .with_cancel(cancel.clone());
    let running = std::thread::spawn(move || futures::executor::block_on(executor.execute()));
    observed_start
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    cancel.cancel();
    assert!(matches!(running.join().unwrap(), Err(Error::Cancelled)));
}

#[test]
fn worker_failure_cancels_siblings_and_skips_finalize() {
    let cancelled_workers = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline {
        source: Box::new(FailingSource {
            next_worker: AtomicUsize::new(0),
            cancelled_workers: cancelled_workers.clone(),
        }),
        processors: vec![],
        sink: Box::new(CountingSink(finishes.clone())),
    };
    let result = futures::executor::block_on(pipeline.execute(
        Arc::new(ThreadTaskExecutor),
        CancellationToken::new(),
        PipelineExecutionConfig::default(),
        2,
    ));
    assert!(matches!(result, Err(Error::Execution(message)) if message == "source failed"));
    assert_eq!(cancelled_workers.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}
