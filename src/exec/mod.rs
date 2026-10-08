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

use crate::error::Result;
use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::{any::Any, sync::Arc};

mod aggregate;
mod batch;
mod exchange;
mod filter;
mod project;
mod scan;

pub use aggregate::{AggregateSinkExec, AggregateSourceExec};
pub use batch::Batch;
pub use exchange::{ExchangeSinkExec, ExchangeSourceExec};
pub use filter::FilterExec;
pub use project::ProjectExec;
pub use scan::ScanExec;

/// Type-erased global state of executor.
pub type GlobalExecContextRef = Arc<dyn Any + Send + Sync>;

/// Global executor for source operator.
pub trait SourceExec: Send + Sync + 'static {
    fn emit_program(
        &self,
        _global: GlobalExecContextRef,
        _builder: &mut crate::program::ProgramBuilder,
        input: crate::program::ValueId,
    ) -> Result<crate::program::ValueId> {
        Ok(input)
    }

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

    fn emit_program(
        &self,
        global: GlobalExecContextRef,
        builder: &mut crate::program::ProgramBuilder,
        input: crate::program::ValueId,
    ) -> Result<crate::program::ValueId>;
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<crate::program::ProcessProgram> {
        let mut builder = crate::program::ProgramBuilder::default();
        let input = builder.value();
        let output = self.emit_program(global, &mut builder, input)?;
        builder.build_batch(input, Some(output))
    }
}

/// Result of a process call.
#[derive(Debug)]
pub enum ProcessResult {
    /// Input consumed by state updates; no downstream batch.
    Consumed,
    /// Deliver this output, then accept a new input batch. Empty output is valid.
    NeedMoreInput(Batch),
    /// Deliver this output, then call again with the same input. Empty output is valid.
    MoreResult(Batch),
    /// Deliver this output, then complete this pipeline executor early.
    Finished(Batch),
}

/// Global executor for sink operator.
pub trait SinkExec: Send + Sync + 'static {
    fn emit_program(
        &self,
        global: GlobalExecContextRef,
        _builder: &mut crate::program::ProgramBuilder,
        input: crate::program::ValueId,
    ) -> Result<(Box<dyn SinkExecutor>, Option<crate::program::ValueId>)> {
        Ok((self.new_executor(global)?, Some(input)))
    }

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
