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
use crate::error::{Error, Result};
use crate::kernel::LocalState;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::{Arc, Mutex};

pub(crate) mod aggregate;
mod buffer;
mod exchange;
mod scan;
pub mod sort;
mod table;
pub mod window;

pub use aggregate::{AggregateExecuteState, AggregateSinkExec, AggregateSourceExec};
pub use buffer::{Buffer, BufferSinkExec, BufferSourceExec, Finish};
pub use exchange::{ExchangeSinkExec, ExchangeSourceExec};
pub use scan::ScanExec;
pub use table::TableSinkExec;

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
    /// `finish`, with CPU work allowed on `cpu`. Pipelines call this one.
    fn finish_on<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        _cpu: &'a dyn Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        self.finish(guard)
    }
}

/// One unit of CPU work.
pub type CpuJob = Box<dyn FnOnce() -> Result<()> + Send>;

/// Runs CPU work of a finishing sink: on the pipeline's executor, or inline.
pub trait Cpu: Send + Sync {
    /// Runs the jobs, concurrently where it can; fails with a failed job's
    /// error.
    fn run(&self, jobs: Vec<CpuJob>) -> BoxFuture<'_, Result<()>>;
    /// How many jobs run at once.
    fn parallelism(&self) -> usize;
}

/// Runs jobs one after another on the caller's thread.
pub struct Inline;

impl Cpu for Inline {
    fn run(&self, jobs: Vec<CpuJob>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { jobs.into_iter().try_for_each(|job| job()) })
    }
    fn parallelism(&self) -> usize {
        1
    }
}

/// Runs `work` on `cpu`; returns the results in order.
pub async fn run_all<T: Send + 'static>(
    cpu: &dyn Cpu,
    work: Vec<Box<dyn FnOnce() -> Result<T> + Send>>,
) -> Result<Vec<T>> {
    let results = Arc::new(Mutex::new(
        (0..work.len()).map(|_| None).collect::<Vec<Option<T>>>(),
    ));
    let jobs = work
        .into_iter()
        .enumerate()
        .map(|(i, w)| {
            let results = results.clone();
            Box::new(move || {
                let value = w()?;
                results.lock().unwrap()[i] = Some(value);
                Ok(())
            }) as CpuJob
        })
        .collect();
    cpu.run(jobs).await?;
    let results = std::mem::take(&mut *results.lock().unwrap());
    results
        .into_iter()
        .map(|r| r.ok_or_else(|| Error::internal("a CPU job left no result".into())))
        .collect()
}
