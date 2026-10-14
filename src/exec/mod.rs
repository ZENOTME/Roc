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
use crate::kernel::LocalState;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;

pub(crate) mod aggregate;
mod exchange;
mod scan;

pub use aggregate::{AggregateExecuteState, AggregateSinkExec, AggregateSourceExec};
pub use exchange::{ExchangeSinkExec, ExchangeSourceExec};
pub use scan::ScanExec;

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

/// Whether a sink accepts more output chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkStatus {
    Continue,
    /// No more input is needed; the pipeline stops reading its source.
    Finished,
}

pub trait SinkExec: Send + 'static {
    fn start(&mut self, _guard: &ShutdownGuard) -> Result<()> {
        Ok(())
    }
    /// Consumes one non-empty output chunk of the pipeline program, on the
    /// pipeline coordinator and in completion order.
    fn consume<'a>(
        &'a mut self,
        _chunk: RecordBatch,
        _guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        Box::pin(async { Ok(SinkStatus::Continue) })
    }
    /// Consumes one completed execution instance. The sink takes the states it
    /// declared while building the pipeline and merges them into its own.
    fn combine<'a>(
        &'a mut self,
        _local: LocalState,
        _guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn finish<'a>(&'a mut self, _guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
