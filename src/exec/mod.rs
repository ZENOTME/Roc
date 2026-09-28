use crate::Cancel;
use crate::Result;
use arrow::record_batch::RecordBatch;
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
pub use project::ProjectExec;
pub use scan::ScanExec;

/// Type-erased global state of executor.
pub type GlobalExecContextRef = Arc<dyn Any + Send + Sync>;

/// Global executor for source operator.
pub trait SourceExec: Send + Sync + 'static {
    fn init_global_context(&self, cancel: &Cancel) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>>;

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Local executor for source operator.
pub trait SourceExecutor: Send + 'static {
    fn next_batch<'a>(
        &'a mut self,
        cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>>;
}

/// Global executor for process operator.
pub trait ProcessExec: Send + Sync + 'static {
    fn init_global_context(&self, cancel: &Cancel) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>>;
}

/// Result of a process call.
#[derive(Debug)]
pub enum ProcessResult {
    /// Deliver this output, then accept a new input batch. Empty output is valid.
    NeedMoreInput(RecordBatch),
    /// Deliver this output, then call again with the same input. Empty output is valid.
    MoreResult(RecordBatch),
    /// Deliver this output, then stop accepting input and finish downstream
    /// processors. This processor's `finish` is not called. Only this worker
    /// stops; global limits require coordination through shared state.
    Finished(RecordBatch),
}

/// Local executor for process operator.
pub trait ProcessExecutor: Send + 'static {
    /// The same input is retained and passed again after `MoreResult`, once the
    /// returned output has been processed downstream. Keep continuation state in
    /// this executor; reset it when returning `NeedMoreInput`.
    fn execute(&mut self, cancel: &Cancel, input: &RecordBatch) -> Result<ProcessResult>;

    /// Called after upstream finishes, once all pending inputs have been processed.
    /// Each output passes through downstream processors before the next call.
    /// Return `None` when drained; no more execute or finish calls follow.
    /// Empty batches are valid. Not called after this processor returns `Finished`,
    /// a downstream processor stops accepting input, or execution fails/cancels.
    fn finish(&mut self, cancel: &Cancel) -> Result<Option<RecordBatch>>;
}

/// Global executor for sink operator.
pub trait SinkExec: Send + Sync + 'static {
    fn init_global_context(&self, cancel: &Cancel) -> Result<GlobalExecContextRef>;

    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>>;

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Result of a sink call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkResult {
    /// Continue to sink.
    NeedMoreInput,
    /// This input already satisfied the sink's goal. Complete the pipeline exeuctor in advanced.
    Finished,
}

/// Local executor for sink operator.
pub trait SinkExecutor: Send + 'static {
    fn sink<'a>(
        &'a mut self,
        cancel: &'a Cancel,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>>;

    fn combine(self: Box<Self>, cancel: &Cancel) -> BoxFuture<'_, Result<()>>;
}
