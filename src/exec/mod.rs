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
use crate::kernel::{LocalState, StateId};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;

pub(crate) mod aggregate;
mod buffer;
mod exchange;
mod scan;
pub mod sort;
mod table;
pub mod window;

pub use aggregate::{AggregateExecuteState, AggregateSinkExec, AggregateSourceExec};
pub use buffer::{Buffer, BufferSinkExec, BufferSourceExec, Finish, finish};
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

/// The result of local consumption, and the reason for handing off an instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeStatus {
    /// Continue after a batch; as a handoff reason, this instance's input ended.
    End,
    /// Hand off local data, then continue receiving input.
    Flush,
    /// Stop dispatching new input to the entire pipeline.
    Stop,
}

/// Access to sink-owned state used by kernels, without exposing execution scratch space.
pub struct StateContext<'a> {
    local: &'a mut LocalState,
}

impl<'a> StateContext<'a> {
    pub(crate) fn new(local: &'a mut LocalState) -> Self {
        Self { local }
    }

    pub fn get_mut<T: 'static>(&mut self, id: StateId) -> Result<&mut T> {
        self.local.state_mut(id)?.get_mut()
    }

    /// Moves a slot out at End, in local finish or coordinator consume.
    /// No later Program run may use this instance.
    pub fn take<T: 'static>(&mut self, id: StateId) -> Result<T> {
        self.local
            .take_state(id)
            .ok_or_else(|| Error::internal("state is not available".into()))?
            .into_inner()
    }
}

pub struct LocalSinkContext {
    /// Upper bound on the input rows of one Program::run.
    pub batch_rows: usize,
    /// Shared across instances, including a single ordered instance.
    pub partitions: usize,
}

pub fn partition_count(parallelism: usize) -> usize {
    if parallelism <= 1 {
        1
    } else {
        (2 * parallelism).next_power_of_two()
    }
}

pub type CpuJob = Box<dyn FnOnce() -> Result<()> + Send>;

/// CPU work on the same executor as Program jobs. Only final sinks receive this.
pub trait Cpu: Send + Sync {
    /// Submit immediately; polling the returned future only waits for completion.
    fn spawn(&self, job: CpuJob) -> BoxFuture<'static, Result<()>>;
    fn parallelism(&self) -> usize;
}

pub struct Inline;
impl Cpu for Inline {
    fn spawn(&self, job: CpuJob) -> BoxFuture<'static, Result<()>> {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).unwrap_or_else(|_| {
                Err(Error::new(
                    crate::error::ErrorKind::TaskFailed,
                    "CPU task panicked",
                ))
            });
        Box::pin(futures::future::ready(result))
    }
    fn parallelism(&self) -> usize {
        1
    }
}

/// Submit all work before waiting; return outputs in submission order.
pub async fn run_all<T: Send + 'static>(
    cpu: &dyn Cpu,
    work: Vec<Box<dyn FnOnce() -> Result<T> + Send>>,
) -> Result<Vec<T>> {
    use std::sync::{Arc, Mutex};
    let outputs = Arc::new(
        (0..work.len())
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>(),
    );
    let jobs = work
        .into_iter()
        .enumerate()
        .map(|(i, work)| {
            let outputs = outputs.clone();
            cpu.spawn(Box::new(move || {
                *outputs[i].lock().unwrap() = Some(work()?);
                Ok(())
            }))
        })
        .collect::<Vec<_>>();
    futures::future::try_join_all(jobs).await?;
    outputs
        .iter()
        .map(|slot| {
            slot.lock()
                .unwrap()
                .take()
                .ok_or_else(|| Error::internal("a CPU job left no result".into()))
        })
        .collect()
}

/// Exclusively owned by one execution instance; called inside CPU jobs.
pub trait LocalSink: Send + 'static {
    /// Called exactly once after each run, including runs without output.
    /// End promises capacity for another batch_rows input rows; otherwise
    /// return Flush so the coordinator can reset the instance first.
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        states: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus>;

    /// Prepare local data for handoff. Only End may remove kernel slots,
    /// allowing their large allocations to be released on the worker.
    /// Long-running work must check the shutdown guard.
    fn finish(
        &mut self,
        status: ConsumeStatus,
        states: &mut StateContext<'_>,
        guard: &ShutdownGuard,
    ) -> Result<()>;

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub trait SinkExec: Send + 'static {
    fn start(&mut self, _guard: &ShutdownGuard) -> Result<()> {
        Ok(())
    }

    fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>>;

    /// Take prepared data on the coordinator. Flush/Stop must restore local
    /// data and kernel slots for reuse; every instance later hands off End.
    fn consume<'a>(
        &'a mut self,
        status: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        states: StateContext<'a>,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>>;

    /// Cross-instance work and publication, after every End handoff succeeds.
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn Cpu,
    ) -> BoxFuture<'a, Result<()>>;
}

pub(crate) fn downcast_local<T: LocalSink>(local: &mut dyn LocalSink) -> Result<&mut T> {
    local
        .as_any_mut()
        .downcast_mut()
        .ok_or_else(|| Error::internal("local sink type mismatch".into()))
}
