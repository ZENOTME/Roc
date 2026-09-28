use crate::CancellationToken;
use crate::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::future::BoxFuture;
use std::sync::Arc;

/// Everything a storage adapter needs to start one scan.
pub struct ScanRequest<StorageTaskDesc> {
    pub source: StorageTaskDesc,
    pub projection: Vec<usize>,
    pub expected_schema: SchemaRef,
    pub cancel: CancellationToken,
}

/// Host-provided storage dispatcher. It owns I/O, decoding and storage scheduling.
pub trait ScanStorage<StorageTaskDesc>: Send + Sync + 'static {
    fn start_scan(&self, request: ScanRequest<StorageTaskDesc>) -> Result<Arc<dyn ScanHandle>>;
}

/// Query-scoped scan state shared by all source executors of one pipeline.
pub trait ScanHandle: Send + Sync + 'static {
    fn consumer(&self) -> Box<dyn ScanConsumer>;
    fn finish<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
}

/// Task-local receiver used by one source executor.
pub trait ScanConsumer: Send + 'static {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<RecordBatch>>>;
}
