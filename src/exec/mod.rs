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

use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;

pub(crate) mod aggregate;
mod exchange;
mod scan;

pub use aggregate::{AggregateExecuteState, AggregateSinkExec, AggregateSourceExec};
pub use exchange::{ExchangeSinkExec, ExchangeSourceExec};
pub use scan::ScanExec;

/// One execution-owned source, including its cursor and concrete runtime state.
pub trait SourceExec: Send + 'static {
    /// Open execution resources; plan construction must not start I/O.
    fn start(&mut self, _guard: &ShutdownGuard) -> Result<()> {
        Ok(())
    }
    fn next_batch<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>>;
    fn finish<'a>(&'a mut self, _guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Execution lifecycle only. Physical operators build suboperators and provide
/// completed local-state bindings before the sink enters a Pipeline.
pub trait SinkExec: Send + 'static {
    fn start(&mut self, _guard: &ShutdownGuard) -> Result<()> {
        Ok(())
    }
    fn flush<'a>(
        &'a mut self,
        _local: &'a mut crate::suboperator::ProgramExecutionContext,
        _guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    /// Consume one completed local context. The sink resolves its ordinary StateIds
    /// in this context and merges those states into its own concrete mutable state.
    fn combine<'a>(
        &'a mut self,
        _local: crate::suboperator::ProgramExecutionContext,
        _guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn finish<'a>(&'a mut self, _guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
