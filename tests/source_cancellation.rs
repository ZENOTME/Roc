use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use futures::{executor::block_on, future::BoxFuture, poll};
use roc::{
    error::{Error, Result},
    exec::{ExchangeSourceExec, ScanExec, SourceExec},
    operator::{
        ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink, ExchangeSourceOperator,
        ScanConsumer, ScanHandle, ScanOperator, ScanRequest, ScanStorage,
    },
};
use std::{
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Service {
    guards: Mutex<Vec<ShutdownGuard>>,
    finished: Arc<AtomicUsize>,
    fail_start: bool,
}

impl Service {
    fn start(&self, shutdown_guard: ShutdownGuard) -> Result<Arc<Handle>> {
        self.guards.lock().unwrap().push(shutdown_guard.clone());
        if self.fail_start {
            return Err(Error::Execution("start failed".into()));
        }
        Ok(Arc::new(Handle {
            _shutdown_guard: shutdown_guard,
            finished: self.finished.clone(),
        }))
    }

    fn guard(&self, index: usize) -> ShutdownGuard {
        self.guards.lock().unwrap()[index].clone()
    }
}

impl ScanStorage for Service {
    type StorageTaskDesc = ();

    fn start_scan(&self, request: ScanRequest<()>) -> Result<Arc<dyn ScanHandle>> {
        Ok(self.start(request.into_parts().1)?)
    }
}

impl ExchangeService for Service {
    fn start_input(
        &self,
        _exchange: usize,
        shutdown_guard: &ShutdownGuard,
    ) -> Result<Arc<dyn ExchangeHandle>> {
        Ok(self.start(shutdown_guard.clone())?)
    }

    fn create_sink(&self, _exchange: usize, _schema: &SchemaRef) -> Result<Box<dyn ExchangeSink>> {
        unreachable!("source-only test")
    }
}

struct Handle {
    _shutdown_guard: ShutdownGuard,
    finished: Arc<AtomicUsize>,
}

impl Handle {
    async fn finish(&self) -> Result<()> {
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl ScanHandle for Handle {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        Box::new(Consumer)
    }

    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.finish())
    }
}

impl ExchangeHandle for Handle {
    fn consumer(&self) -> Box<dyn ExchangeConsumer> {
        Box::new(Consumer)
    }

    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.finish())
    }
}

struct Consumer;

impl ScanConsumer for Consumer {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(std::future::pending())
    }
}

impl ExchangeConsumer for Consumer {
    fn next(&mut self) -> BoxFuture<'_, Option<RecordBatch>> {
        Box::pin(std::future::pending())
    }
}

fn source(exchange: bool, service: Arc<Service>) -> Box<dyn SourceExec> {
    if exchange {
        Box::new(ExchangeSourceExec::new(ExchangeSourceOperator::new(
            0, service,
        )))
    } else {
        Box::new(ScanExec::new(ScanOperator::new((), service)))
    }
}

#[test]
fn sources_observe_the_callers_shared_shutdown_without_forwarding() {
    for exchange in [false, true] {
        let (shutdown, guard) = asyncband::shutdown::new();
        let (_other_shutdown, other_guard) = asyncband::shutdown::new();
        let service = Arc::new(Service::default());
        let source = source(exchange, service.clone());
        let global = source.init_global_context(&guard).unwrap();
        let same_execution = source.init_global_context(&guard).unwrap();
        let other_execution = source.init_global_context(&other_guard).unwrap();
        let mut worker = source.new_executor(global.clone()).unwrap();
        block_on(async {
            let mut next = pin!(worker.next_batch(&guard));
            assert!(poll!(&mut next).is_pending());
            shutdown.request_shutdown();
            // Service observers see the request before the executor is polled.
            assert!(service.guard(0).is_shutdown_requested());
            assert!(service.guard(1).is_shutdown_requested());
            assert!(!service.guard(2).is_shutdown_requested());
            assert!(matches!(
                poll!(&mut next),
                std::task::Poll::Ready(Err(Error::Cancelled))
            ));
        });
        service.guards.lock().unwrap().clear();
        drop((worker, global, same_execution, guard));
        block_on(shutdown);
        drop(other_execution);
        assert!(!other_guard.is_shutdown_requested());
    }
}

#[test]
fn shutdown_waits_for_the_last_source_owner_to_release_its_guard() {
    for exchange in [false, true] {
        let (shutdown, guard) = asyncband::shutdown::new();
        let service = Arc::new(Service::default());
        let source = source(exchange, service.clone());
        let global = source.init_global_context(&guard).unwrap();
        let first = source.new_executor(global.clone()).unwrap();
        let second = source.new_executor(global.clone()).unwrap();
        drop((global, guard));
        assert!(!service.guard(0).is_shutdown_requested());
        service.guards.lock().unwrap().clear();
        block_on(async {
            let mut completion = pin!(shutdown);
            assert!(poll!(&mut completion).is_pending());
            drop(first);
            assert!(poll!(&mut completion).is_pending());
            drop(second);
            assert!(poll!(&mut completion).is_ready());
        });
        assert_eq!(service.finished.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn normal_finalization_and_drop_do_not_request_shutdown() {
    for exchange in [false, true] {
        let (_shutdown, guard) = asyncband::shutdown::new();
        let service = Arc::new(Service::default());
        let source = source(exchange, service.clone());
        let global = source.init_global_context(&guard).unwrap();
        block_on(source.finalize(global, &guard)).unwrap();
        assert!(!guard.is_shutdown_requested());
        assert!(!service.guard(0).is_shutdown_requested());
        assert_eq!(service.finished.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn failed_source_initialization_does_not_request_shutdown_or_leak_guards() {
    for exchange in [false, true] {
        let (shutdown, guard) = asyncband::shutdown::new();
        let service = Arc::new(Service {
            fail_start: true,
            ..Default::default()
        });
        let source = source(exchange, service.clone());
        assert!(
            matches!(source.init_global_context(&guard), Err(Error::Execution(message)) if message == "start failed")
        );
        assert!(!service.guard(0).is_shutdown_requested());
        service.guards.lock().unwrap().clear();
        drop(guard);
        block_on(async {
            assert!(poll!(pin!(shutdown)).is_ready());
        });
    }
}
