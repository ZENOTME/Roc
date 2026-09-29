use crate::error::Result;
use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::Arc;

/// Everything a storage adapter needs to start one scan.
pub struct ScanRequest<StorageTaskDesc> {
    source: StorageTaskDesc,
    shutdown_guard: ShutdownGuard,
}

impl<StorageTaskDesc> ScanRequest<StorageTaskDesc> {
    pub fn new(source: StorageTaskDesc, shutdown_guard: ShutdownGuard) -> Self {
        Self {
            source,
            shutdown_guard,
        }
    }

    pub fn source(&self) -> &StorageTaskDesc {
        &self.source
    }

    /// Observes caller-requested shutdown and keeps shutdown completion pending.
    pub fn shutdown_guard(&self) -> &ShutdownGuard {
        &self.shutdown_guard
    }

    pub fn into_parts(self) -> (StorageTaskDesc, ShutdownGuard) {
        (self.source, self.shutdown_guard)
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
    /// Stops and joins scan work on normal completion, including early stop.
    /// This must not require a shutdown request. Dropping the handle must release
    /// owned resources; background tasks must observe their shutdown guards.
    fn finish(&self) -> BoxFuture<'_, Result<()>>;
}

/// Task-local receiver used by one source executor.
pub trait ScanConsumer: Send + 'static {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>>;
}
