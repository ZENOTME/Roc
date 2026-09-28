use super::{GlobalExecContextRef, SourceExec, SourceExecutor};
use crate::CancellationToken;
use crate::{
    Error, Result,
    operator::{ScanConsumer, ScanHandle, ScanOperator, ScanRequest, ScanStorage},
};
use arrow::record_batch::RecordBatch;
use futures::{FutureExt, future::BoxFuture};
use std::sync::Arc;

pub struct ScanExec<StorageTaskDesc> {
    operator: ScanOperator<StorageTaskDesc>,
    storage: Arc<dyn ScanStorage<StorageTaskDesc>>,
}

impl<StorageTaskDesc> ScanExec<StorageTaskDesc> {
    pub fn new(
        operator: ScanOperator<StorageTaskDesc>,
        storage: Arc<dyn ScanStorage<StorageTaskDesc>>,
    ) -> Self {
        Self { operator, storage }
    }
}

struct ScanGlobalContext<StorageTaskDesc> {
    operator: ScanOperator<StorageTaskDesc>,
    handle: Arc<dyn ScanHandle>,
}

impl<StorageTaskDesc> SourceExec for ScanExec<StorageTaskDesc>
where
    StorageTaskDesc: Clone + Send + Sync + 'static,
{
    fn init_global_context(
        &self,
        _batch_rows: usize,
        cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        let projection = self.operator.read_projection()?;
        let operator = self.operator.projected_input(&projection)?;
        let cancel = cancel.child_token();
        let handle = self.storage.start_scan(ScanRequest {
            source: self.operator.source.clone(),
            projection,
            expected_schema: operator.schema.clone(),
            cancel,
        })?;
        Ok(Arc::new(ScanGlobalContext { operator, handle }))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global
            .downcast::<ScanGlobalContext<StorageTaskDesc>>()
            .map_err(|_| {
                Error::Execution("scan source received an invalid global context".into())
            })?;
        Ok(Box::new(ScanExecutor {
            consumer: global.handle.consumer(),
            operator: global.operator.clone(),
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global
                .downcast::<ScanGlobalContext<StorageTaskDesc>>()
                .map_err(|_| {
                    Error::Execution("scan source received an invalid global context".into())
                })?;
            global.handle.finish().await
        })
    }
}

struct ScanExecutor<StorageTaskDesc> {
    consumer: Box<dyn ScanConsumer>,
    operator: ScanOperator<StorageTaskDesc>,
}

impl<StorageTaskDesc> SourceExecutor for ScanExecutor<StorageTaskDesc>
where
    StorageTaskDesc: Send + 'static,
{
    fn next_batch<'a>(
        &'a mut self,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let cancelled = ctx.cancelled().fuse();
            let next = self.consumer.next().fuse();
            futures::pin_mut!(cancelled, next);
            futures::select_biased! {
                _ = cancelled => Err(Error::Cancelled),
                batch = next => match batch? {
                    Some(batch) => Ok(Some(self.operator.apply(batch)?)),
                    None => Ok(None),
                },
            }
        })
    }
}
