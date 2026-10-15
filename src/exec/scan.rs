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

use super::{BatchSlices, LocalSource, LocalSourceContext, SourceExec};
use crate::arrow::record_batch::RecordBatch;
use crate::{
    error::{Error, Result, ResultExt},
    operator::{ScanConsumer, ScanHandle, ScanOperator, ScanRequest},
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::{future::Future, sync::Arc, task::Poll};

pub struct ScanExec<StorageTaskDesc> {
    operator: ScanOperator<StorageTaskDesc>,
    handle: Option<Arc<dyn ScanHandle>>,
}

impl<StorageTaskDesc> ScanExec<StorageTaskDesc> {
    pub fn new(operator: ScanOperator<StorageTaskDesc>) -> Self {
        Self {
            operator,
            handle: None,
        }
    }
}

impl<StorageTaskDesc> SourceExec for ScanExec<StorageTaskDesc>
where
    StorageTaskDesc: Clone + Send + Sync + 'static,
{
    fn start(&mut self, guard: &ShutdownGuard) -> Result<()> {
        if self.handle.is_some() {
            return Err(Error::internal("scan already started".into()));
        }
        let handle = self
            .operator
            .storage()
            .start_scan(ScanRequest::new(
                self.operator.source().clone(),
                guard.clone(),
            ))
            .with_location()?;
        self.handle = Some(handle);
        Ok(())
    }
    fn finish<'a>(&'a mut self, _: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if let Some(handle) = self.handle.take() {
                handle.finish().await.with_location()?;
            }
            Ok(())
        })
    }
    fn local_source(&self, ctx: &LocalSourceContext) -> Result<Box<dyn LocalSource>> {
        Ok(Box::new(ScanLocalSource {
            consumer: self
                .handle
                .as_ref()
                .ok_or_else(|| Error::internal("scan is not started".into()))?
                .consumer(),
            slices: BatchSlices::default(),
            batch_rows: ctx.batch_rows,
        }))
    }
}

struct ScanLocalSource {
    consumer: Box<dyn ScanConsumer>,
    slices: BatchSlices,
    batch_rows: usize,
}
impl LocalSource for ScanLocalSource {
    fn next<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if shutdown_guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            if let Some(batch) = self.slices.next(self.batch_rows) {
                return Ok(Some(batch));
            }
            let cancelled = shutdown_guard.shutdown_requested();
            let next = self.consumer.next();
            futures::pin_mut!(cancelled, next);
            futures::future::poll_fn(|cx| {
                // Preserve cancellation priority on every poll, including when
                // both the previously pending read and shutdown become ready.
                if shutdown_guard.is_shutdown_requested() {
                    return Poll::Ready(Err(Error::cancelled()));
                }
                match next.as_mut().poll(cx) {
                    Poll::Ready(result) => Poll::Ready(result.with_location()),
                    Poll::Pending => {
                        // Register only when the read blocks. Polling the wait
                        // also rechecks shutdown, closing the registration race.
                        match cancelled.as_mut().poll(cx) {
                            Poll::Ready(()) => Poll::Ready(Err(Error::cancelled())),
                            Poll::Pending => Poll::Pending,
                        }
                    }
                }
            })
            .await
            .map(|batch| {
                batch.map(|batch| {
                    if batch.num_rows() == 0 {
                        return batch;
                    }
                    self.slices.set(batch);
                    self.slices.next(self.batch_rows).unwrap()
                })
            })
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

    fn executor(consumer: PendingThenReady) -> ScanLocalSource {
        ScanLocalSource {
            consumer: Box::new(consumer),
            slices: BatchSlices::default(),
            batch_rows: 2048,
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
        let mut next = executor.next(&guard);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(next.as_mut().poll(&mut cx).is_pending());
        shutdown.request_shutdown();
        assert!(matches!(
            next.as_mut().poll(&mut cx),
            Poll::Ready(Err(ref error)) if error.kind() == crate::error::ErrorKind::Cancelled
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
        let mut next = executor.next(&guard);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            next.as_mut().poll(&mut cx),
            Poll::Ready(Err(ref error)) if error.kind() == crate::error::ErrorKind::Cancelled
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
}
