use super::{GlobalExecContextRef, SourceExec, SourceExecutor};
use crate::Cancel;
use crate::{
    Error, Result,
    operator::{ScanConsumer, ScanHandle, ScanOperator, ScanRequest},
};
use arrow::record_batch::RecordBatch;
use futures::{FutureExt, future::BoxFuture};
use std::sync::Arc;

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
    fn init_global_context(&self, cancel: &Cancel) -> Result<GlobalExecContextRef> {
        let cancel = cancel.child();
        let handle = self
            .operator
            .storage()
            .start_scan(ScanRequest::new(self.operator.source().clone(), cancel))?;
        Ok(Arc::new(handle))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global.downcast::<Arc<dyn ScanHandle>>().map_err(|_| {
            Error::Execution("scan source received an invalid global context".into())
        })?;
        Ok(Box::new(ScanExecutor {
            consumer: global.consumer(),
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global.downcast::<Arc<dyn ScanHandle>>().map_err(|_| {
                Error::Execution("scan source received an invalid global context".into())
            })?;
            global.finish().await
        })
    }
}

struct ScanExecutor {
    consumer: Box<dyn ScanConsumer>,
}

impl SourceExecutor for ScanExecutor {
    fn next_batch<'a>(
        &'a mut self,
        cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let cancelled = cancel.cancelled().fuse();
            let next = self.consumer.next().fuse();
            futures::pin_mut!(cancelled, next);
            futures::select_biased! {
                _ = cancelled => Err(Error::Cancelled),
                batch = next => batch,
            }
        })
    }
}
