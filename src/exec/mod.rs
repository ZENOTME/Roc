use crate::CancellationToken;
use crate::Result;
use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use std::{any::Any, sync::Arc};

mod aggregate;
mod exchange;
mod filter;
mod project;
mod scan;

pub use aggregate::{AggregateSinkExec, AggregateSourceExec, aggregate_execs};
pub use exchange::{ExchangeSinkExec, ExchangeSourceExec};
pub use filter::FilterExec;
pub use project::ProjectExec;
pub use scan::ScanExec;

/// Type-erased state shared by every task instantiated from one execution role.
pub type GlobalExecContextRef = Arc<dyn Any + Send + Sync>;

/// Logical source role in a pipeline.
pub trait SourceExec: Send + Sync + 'static {
    fn init_global_context(
        &self,
        batch_rows: usize,
        cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>>;

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

pub trait SourceExecutor: Send + 'static {
    fn next_batch<'a>(
        &'a mut self,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>>;
}

/// Logical transform role in a pipeline.
pub trait ProcessExec: Send + Sync + 'static {
    fn init_global_context(
        &self,
        batch_rows: usize,
        cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>>;
}

pub trait ProcessExecutor: Send + 'static {
    fn execute(&mut self, cancel: &CancellationToken, input: RecordBatch) -> Result<RecordBatch>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkStatus {
    NeedMoreInput,
    Finished,
}

/// Logical sink role in a pipeline. Finalize runs after every task has combined.
pub trait SinkExec: Send + Sync + 'static {
    fn init_global_context(
        &self,
        batch_rows: usize,
        cancel: &CancellationToken,
    ) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>>;

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>>;
}

pub trait SinkExecutor: Send + 'static {
    fn sink<'a>(
        &'a mut self,
        ctx: &'a CancellationToken,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkStatus>>;

    fn combine<'a>(self: Box<Self>, ctx: &'a CancellationToken) -> BoxFuture<'a, Result<()>>;
}
