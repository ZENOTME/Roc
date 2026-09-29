use crate::error::Result;
use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::{any::Any, sync::Arc};

mod aggregate;
mod exchange;
mod filter;
mod project;
mod scan;

pub use aggregate::{AggregateSinkExec, AggregateSourceExec};
pub use exchange::{ExchangeSinkExec, ExchangeSourceExec};
pub use filter::FilterExec;
pub use project::{ProjectExec, ProjectionExecutor};
pub use scan::ScanExec;

/// Type-erased global state of executor.
pub type GlobalExecContextRef = Arc<dyn Any + Send + Sync>;

/// Global executor for source operator.
pub trait SourceExec: Send + Sync + 'static {
    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>>;

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Local executor for source operator.
pub trait SourceExecutor: Send + 'static {
    fn next_batch<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>>;
}

/// Global executor for process operator.
pub trait ProcessExec: Send + Sync + 'static {
    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>>;
}

/// Result of a process call.
#[derive(Debug)]
pub enum ProcessResult {
    /// Deliver this output, then accept a new input batch. Empty output is valid.
    NeedMoreInput(RecordBatch),
    /// Deliver this output, then call again with the same input. Empty output is valid.
    MoreResult(RecordBatch),
    /// Deliver this output, then complete the pipeline exeuctor in advanced.
    Finished(RecordBatch),
}

/// Local executor for process operator.
pub trait ProcessExecutor: Send + 'static {
    fn execute(&mut self, input: &RecordBatch) -> Result<ProcessResult>;

    fn finish(&mut self) -> Result<Option<RecordBatch>>;
}

/// Global executor for sink operator.
pub trait SinkExec: Send + Sync + 'static {
    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>>;

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Result of a sink call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkResult {
    /// Continue to sink.
    NeedMoreInput,
    /// Complete the pipeline exeuctor in advanced.
    Finished,
}

/// Local executor for sink operator.
pub trait SinkExecutor: Send + 'static {
    fn sink<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>>;

    fn combine(self: Box<Self>, shutdown_guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>>;
}
