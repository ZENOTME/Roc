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

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
        execution_plan::reset_plan_states,
    },
};
use futures::{FutureExt, future::BoxFuture, task::AtomicWaker};
use roc::{
    error::{Error, Result},
    operator::{ScanConsumer, ScanHandle, ScanRequest, ScanStorage},
};
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

/// A reusable DataFusion physical plan and its execution context.
///
/// Pass an already planned Parquet scan to retain DataFusion's projection,
/// predicate pushdown, row-group pruning and object-store configuration.
/// Other plans are suitable only when their output partitions can be consumed
/// independently. Coalesce cross-partition plans (such as repartitioning) inside
/// DataFusion before adapting them: draining one partition at a time can block
/// an exchange that needs all output partitions to be polled concurrently.
/// Each `start_scan` resets the whole plan with DataFusion's `reset_plan_states`,
/// then creates fresh partition streams. This also resets Parquet's shared
/// file queue, so repeated or concurrent scans do not consume each other's work.
/// Dynamic-filter and recursive-query plans are unsupported because DataFusion
/// cannot reset those plans for independent reuse.
#[derive(Clone, Debug)]
pub struct DataFusionScan {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
}

impl DataFusionScan {
    pub fn new(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Self {
        Self { plan, context }
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub fn partition_count(&self) -> usize {
        self.plan.output_partitioning().partition_count()
    }
}

/// Pulls DataFusion streams directly, without a second producer or batch queue.
///
/// Consumers share an unordered work pool: each DataFusion output partition is
/// assigned once, and a consumer drains its partition before claiming another.
/// There is no promise of global ordering, or of a stable partition-to-worker
/// mapping. Use at least as many Roc workers as scan partitions for full I/O
/// concurrency. DataFusion execution may require an active Tokio runtime.
#[derive(Clone, Copy, Debug, Default)]
pub struct DataFusionStorage;

impl ScanStorage for DataFusionStorage {
    type StorageTaskDesc = DataFusionScan;

    fn start_scan(&self, request: ScanRequest<DataFusionScan>) -> Result<Arc<dyn ScanHandle>> {
        let (mut source, shutdown_guard) = request.into_parts();
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::Cancelled);
        }
        // DataSourceExec itself has a shared, mutable file queue. New execute()
        // calls alone do not restore files consumed by an earlier execution.
        source.plan = reset_plan_states(source.plan)
            .map_err(|error| Error::Execution(format!("DataFusion scan reset: {error}")))?;
        Ok(Arc::new(Handle {
            shared: Arc::new(Shared {
                source,
                shutdown_guard,
                next_partition: AtomicUsize::new(0),
                stopped: AtomicBool::new(false),
                error: Mutex::new(None),
                consumers: Mutex::new(Vec::new()),
            }),
        }))
    }
}

struct Shared {
    source: DataFusionScan,
    shutdown_guard: ShutdownGuard,
    next_partition: AtomicUsize,
    stopped: AtomicBool,
    error: Mutex<Option<Error>>,
    consumers: Mutex<Vec<Weak<ConsumerSlot>>>,
}

impl Shared {
    /// Clear idle as well as pending streams. The registry keeps only weak
    /// references, so dropping a consumer immediately releases its stream.
    fn stop(&self, error: Option<Error>) {
        {
            let mut first_error = self.error.lock().unwrap();
            if first_error.is_none() {
                *first_error = error;
            }
            self.stopped.store(true, Ordering::Release);
        }
        let consumers = self.consumers.lock().unwrap();
        for consumer in consumers.iter().filter_map(Weak::upgrade) {
            consumer.stream.lock().unwrap().take();
            consumer.waker.wake();
        }
    }

    fn terminal_result(&self) -> Result<Option<RecordBatch>> {
        match self.error.lock().unwrap().clone() {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }
}

struct Handle {
    shared: Arc<Shared>,
}

impl ScanHandle for Handle {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        let slot = Arc::new(ConsumerSlot {
            stream: Mutex::new(None),
            waker: AtomicWaker::new(),
        });
        let mut consumers = self.shared.consumers.lock().unwrap();
        consumers.retain(|consumer| consumer.strong_count() > 0);
        consumers.push(Arc::downgrade(&slot));
        Box::new(Consumer {
            shared: self.shared.clone(),
            slot,
        })
    }

    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.shared.stop(None);
            self.shared.terminal_result().map(|_| ())
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shared.stop(None);
    }
}

struct ConsumerSlot {
    // A short synchronous lock permits finish/drop to release streams even
    // when the consumer is alive but no longer polling after an early stop.
    stream: Mutex<Option<(usize, SendableRecordBatchStream)>>,
    waker: AtomicWaker,
}

struct Consumer {
    shared: Arc<Shared>,
    slot: Arc<ConsumerSlot>,
}

impl Consumer {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        self.slot.waker.register(cx.waker());
        loop {
            let mut current = self.slot.stream.lock().unwrap();
            if self.shared.stopped.load(Ordering::Acquire) {
                current.take();
                return Poll::Ready(self.shared.terminal_result());
            }
            if current.is_none() {
                let partition = self.shared.next_partition.fetch_add(1, Ordering::Relaxed);
                if partition >= self.shared.source.partition_count() {
                    return Poll::Ready(Ok(None));
                }
                match self
                    .shared
                    .source
                    .plan
                    .execute(partition, self.shared.source.context.clone())
                {
                    Ok(stream) => *current = Some((partition, stream)),
                    Err(error) => {
                        let error = scan_error(partition, error);
                        drop(current);
                        self.shared.stop(Some(error.clone()));
                        return Poll::Ready(Err(error));
                    }
                }
            }
            let (partition, stream) = current.as_mut().unwrap();
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Ok(Some(batch))),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    current.take();
                }
                Poll::Ready(Some(Err(error))) => {
                    let error = scan_error(*partition, error);
                    drop(current);
                    self.shared.stop(Some(error.clone()));
                    return Poll::Ready(Err(error));
                }
            }
        }
    }
}

impl ScanConsumer for Consumer {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let shared = self.shared.clone();
            let cancelled = shared.shutdown_guard.shutdown_requested().fuse();
            let next = futures::future::poll_fn(|cx| self.poll_next(cx)).fuse();
            futures::pin_mut!(cancelled, next);
            futures::select_biased! {
                _ = cancelled => {
                    shared.stop(Some(Error::Cancelled));
                    Err(Error::Cancelled)
                },
                result = next => result,
            }
        })
    }
}

fn scan_error(partition: usize, error: datafusion::error::DataFusionError) -> Error {
    Error::Execution(format!("DataFusion scan partition {partition}: {error}"))
}
