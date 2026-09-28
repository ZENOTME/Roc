use crate::Cancel;
use crate::Result;
use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use std::sync::Arc;

/// Everything a storage adapter needs to start one scan.
pub struct ScanRequest<StorageTaskDesc> {
    source: StorageTaskDesc,
    cancel: Cancel,
}

impl<StorageTaskDesc> ScanRequest<StorageTaskDesc> {
    pub fn new(source: StorageTaskDesc, cancel: Cancel) -> Self {
        Self { source, cancel }
    }

    pub fn source(&self) -> &StorageTaskDesc {
        &self.source
    }

    pub fn cancel(&self) -> &Cancel {
        &self.cancel
    }

    pub fn into_parts(self) -> (StorageTaskDesc, Cancel) {
        (self.source, self.cancel)
    }
}

/// Scan storage provides decoding and I/O schedule.
pub trait ScanStorage: Send + Sync + 'static {
    type StorageTaskDesc;

    fn start_scan(
        &self,
        request: ScanRequest<Self::StorageTaskDesc>,
    ) -> Result<Arc<dyn ScanHandle>>;
}

/// Query-scoped scan state shared by all source executors of one pipeline.
pub trait ScanHandle: Send + Sync + 'static {
    fn consumer(&self) -> Box<dyn ScanConsumer>;
    fn finish(&self) -> BoxFuture<'_, Result<()>>;
}

/// Task-local receiver used by one source executor.
pub trait ScanConsumer: Send + 'static {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>>;
}
