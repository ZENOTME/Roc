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

use super::{GlobalExecContextRef, SourceExec, SourceExecutor};
use crate::{
    error::{Error, Result},
    operator::{ScanConsumer, ScanHandle, ScanOperator, ScanRequest},
};
use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::{future::Future, sync::Arc, task::Poll};

pub struct ScanExec<StorageTaskDesc> {
    operator: ScanOperator<StorageTaskDesc>,
}

impl<StorageTaskDesc> ScanExec<StorageTaskDesc> {
    pub fn new(operator: ScanOperator<StorageTaskDesc>) -> Self {
        Self { operator }
    }
}

impl<StorageTaskDesc> SourceExec for ScanExec<StorageTaskDesc>
where
    StorageTaskDesc: Clone + Send + Sync + 'static,
{
    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        let handle = self.operator.storage().start_scan(ScanRequest::new(
            self.operator.source().clone(),
            shutdown_guard.clone(),
        ))?;
        Ok(Arc::new(ScanGlobalContext { handle }))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global.downcast::<ScanGlobalContext>().map_err(|_| {
            Error::Execution("scan source received an invalid global context".into())
        })?;
        Ok(Box::new(ScanExecutor {
            consumer: global.handle.consumer(),
            _global: global,
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global.downcast::<ScanGlobalContext>().map_err(|_| {
                Error::Execution("scan source received an invalid global context".into())
            })?;
            global.handle.finish().await
        })
    }
}

struct ScanGlobalContext {
    handle: Arc<dyn ScanHandle>,
}

struct ScanExecutor {
    consumer: Box<dyn ScanConsumer>,
    _global: Arc<ScanGlobalContext>,
}

impl SourceExecutor for ScanExecutor {
    fn next_batch<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let cancelled = shutdown_guard.shutdown_requested();
            let next = self.consumer.next();
            futures::pin_mut!(cancelled, next);
            futures::future::poll_fn(|cx| {
                // Preserve cancellation priority on every poll, including when
                // both the previously pending read and shutdown become ready.
                if shutdown_guard.is_shutdown_requested() {
                    return Poll::Ready(Err(Error::Cancelled));
                }
                match next.as_mut().poll(cx) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        // Register only when the read blocks. Polling the wait
                        // also rechecks shutdown, closing the registration race.
                        match cancelled.as_mut().poll(cx) {
                            Poll::Ready(()) => Poll::Ready(Err(Error::Cancelled)),
                            Poll::Pending => Poll::Pending,
                        }
                    }
                }
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asyncband::shutdown::Shutdown;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::Context,
    };

    struct TestHandle;
    impl ScanHandle for TestHandle {
        fn consumer(&self) -> Box<dyn ScanConsumer> {
            unreachable!("the test constructs its consumer directly")
        }
        fn finish(&self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct PendingThenReady {
        polls: Arc<AtomicUsize>,
        shutdown_during_poll: Option<Shutdown>,
    }
    impl ScanConsumer for PendingThenReady {
        fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
            Box::pin(futures::future::poll_fn(|_| {
                if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    if let Some(shutdown) = &self.shutdown_during_poll {
                        shutdown.request_shutdown();
                    }
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(None))
                }
            }))
        }
    }

    fn executor(consumer: PendingThenReady) -> ScanExecutor {
        ScanExecutor {
            consumer: Box::new(consumer),
            _global: Arc::new(ScanGlobalContext {
                handle: Arc::new(TestHandle),
            }),
        }
    }

    #[test]
    fn shutdown_keeps_priority_when_a_pending_read_becomes_ready() {
        let (shutdown, guard) = asyncband::shutdown::new();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut executor = executor(PendingThenReady {
            polls: polls.clone(),
            shutdown_during_poll: None,
        });
        let mut next = executor.next_batch(&guard);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(next.as_mut().poll(&mut cx).is_pending());
        shutdown.request_shutdown();
        assert!(matches!(
            next.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Cancelled))
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_during_data_poll_is_seen_before_returning_pending() {
        let (shutdown, guard) = asyncband::shutdown::new();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut executor = executor(PendingThenReady {
            polls: polls.clone(),
            shutdown_during_poll: Some(shutdown),
        });
        let mut next = executor.next_batch(&guard);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            next.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Cancelled))
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
}
