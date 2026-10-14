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
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
        execution_plan::reset_plan_states,
    },
};
use futures::{future::BoxFuture, task::AtomicWaker};
use roc::{
    error::{Error, ErrorContext, ErrorKind, Result},
    operator::{ScanConsumer, ScanHandle, ScanRequest, ScanStorage},
};
use std::{
    future::Future,
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

/// Reads DataFusion's partition streams for Roc consumers.
///
/// Consumers share an unordered work pool: each DataFusion output partition is
/// assigned once. There is no promise of global ordering, or of a stable
/// partition-to-consumer mapping. Inside a Tokio runtime, the first read of a
/// consumer starts one task per partition that drains partitions from the
/// pool into the consumer's bounded queue, so partitions decode in parallel
/// on the runtime's threads; a Roc pipeline reads its source from one
/// coordinator. Without a runtime, a consumer drains one partition at a time
/// as it is polled. DataFusion execution may require an active Tokio runtime.
#[derive(Clone, Copy, Debug, Default)]
pub struct DataFusionStorage;

impl ScanStorage for DataFusionStorage {
    type StorageTaskDesc = DataFusionScan;

    fn start_scan(&self, request: ScanRequest<DataFusionScan>) -> Result<Arc<dyn ScanHandle>> {
        let (mut source, shutdown_guard) = request.into_parts();
        if shutdown_guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        // DataSourceExec itself has a shared, mutable file queue. New execute()
        // calls alone do not restore files consumed by an earlier execution.
        source.plan = reset_plan_states(source.plan).map_err(|source| {
            Error::new(classify(&source), "DataFusion scan reset failed").with_source(source)
        })?;
        Ok(Arc::new(Handle {
            shared: Arc::new(Shared {
                source,
                shutdown_guard,
                next_partition: AtomicUsize::new(0),
                stopped: AtomicBool::new(false),
                error: Mutex::new(None),
                consumers: Mutex::new(Vec::new()),
                tasks: Mutex::new(Vec::new()),
            }),
        }))
    }
}

struct Shared {
    source: DataFusionScan,
    shutdown_guard: ShutdownGuard,
    next_partition: AtomicUsize,
    stopped: AtomicBool,
    /// The kind and message of the first failure. The consumer that observed
    /// it returns the error itself; the others report a failure of that kind.
    error: Mutex<Option<(ErrorKind, String)>>,
    consumers: Mutex<Vec<Weak<ConsumerSlot>>>,
    /// The decoding tasks of consumers, aborted when the scan stops.
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Shared {
    /// Clear idle as well as pending streams. The registry keeps only weak
    /// references, so dropping a consumer immediately releases its stream.
    fn stop(&self, error: Option<&Error>) {
        {
            let mut first_error = self.error.lock().unwrap();
            if first_error.is_none() {
                *first_error = error.map(|error| (error.kind(), error.message().to_owned()));
            }
            self.stopped.store(true, Ordering::Release);
        }
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        let consumers = self.consumers.lock().unwrap();
        for consumer in consumers.iter().filter_map(Weak::upgrade) {
            consumer.stream.lock().unwrap().take();
            consumer.waker.wake();
        }
    }

    /// Drains partitions from the pool into `queue` until none is left, the
    /// scan stops or the consumer is gone. A failure goes to the queue first,
    /// with its source and context, then stops the scan.
    async fn decode(self: Arc<Self>, queue: tokio::sync::mpsc::Sender<Result<RecordBatch>>) {
        use futures::StreamExt;
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            let partition = self.next_partition.fetch_add(1, Ordering::Relaxed);
            if partition >= self.source.partition_count() {
                return;
            }
            let mut stream = match self.source.plan.execute(partition, self.source.context.clone()) {
                Ok(stream) => stream,
                Err(error) => return self.fail(&queue, scan_error(partition, error)).await,
            };
            while let Some(item) = stream.next().await {
                match item {
                    Ok(batch) => {
                        if queue.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => return self.fail(&queue, scan_error(partition, error)).await,
                }
            }
        }
    }

    async fn fail(&self, queue: &tokio::sync::mpsc::Sender<Result<RecordBatch>>, error: Error) {
        let (kind, message) = (error.kind(), error.message().to_owned());
        let _ = queue.send(Err(error)).await;
        self.stop(Some(&Error::new(kind, message)));
    }

    fn terminal_result(&self) -> Result<Option<RecordBatch>> {
        match self.error.lock().unwrap().clone() {
            Some((kind, message)) => Err(Error::new(kind, message)),
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
            queue: None,
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
    /// The queue of the decoding tasks, once started.
    queue: Option<tokio::sync::mpsc::Receiver<Result<RecordBatch>>>,
}

impl Consumer {
    /// Starts the decoding tasks inside a Tokio runtime.
    fn start_tasks(&mut self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let partitions = self.shared.source.partition_count();
        let (sender, receiver) = tokio::sync::mpsc::channel(2 * partitions.max(1));
        let mut tasks = self.shared.tasks.lock().unwrap();
        if self.shared.stopped.load(Ordering::Acquire) {
            return;
        }
        for _ in 0..partitions {
            tasks.push(runtime.spawn(self.shared.clone().decode(sender.clone())));
        }
        self.queue = Some(receiver);
    }

    fn poll_queue(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        let queue = self.queue.as_mut().unwrap();
        if self.shared.stopped.load(Ordering::Acquire) {
            // A decoding failure is reported with its source.
            while let Ok(item) = queue.try_recv() {
                if let Err(error) = item {
                    return Poll::Ready(Err(error));
                }
            }
            return Poll::Ready(self.shared.terminal_result());
        }
        match queue.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(item.map(Some)),
            Poll::Ready(None) => Poll::Ready(self.shared.terminal_result()),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        self.slot.waker.register(cx.waker());
        if self.queue.is_none() && self.slot.stream.lock().unwrap().is_none() {
            self.start_tasks();
        }
        if self.queue.is_some() {
            return self.poll_queue(cx);
        }
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
                        self.shared.stop(Some(&error));
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
                    self.shared.stop(Some(&error));
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
            let cancelled = shared.shutdown_guard.shutdown_requested();
            futures::pin_mut!(cancelled);
            futures::future::poll_fn(|cx| {
                // Check on every poll so cancellation still wins when data and
                // shutdown both become ready after a previous Pending result.
                if shared.shutdown_guard.is_shutdown_requested() {
                    shared.stop(Some(&Error::cancelled()));
                    return Poll::Ready(Err(Error::cancelled()));
                }
                match self.poll_next(cx) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        // A pending wait registers its waker and rechecks the
                        // latch, so shutdown cannot be lost during registration.
                        match cancelled.as_mut().poll(cx) {
                            Poll::Ready(()) => {
                                shared.stop(Some(&Error::cancelled()));
                                Poll::Ready(Err(Error::cancelled()))
                            }
                            Poll::Pending => Poll::Pending,
                        }
                    }
                }
            })
            .await
        })
    }
}

fn scan_error(partition: usize, source: DataFusionError) -> Error {
    Error::new(classify(&source), "DataFusion scan failed")
        .with_source(source)
        .with_context(ErrorContext::new("datafusion.scan").field("partition", partition))
}

/// DataFusion reports most failures without a stable classification.
fn classify(error: &DataFusionError) -> ErrorKind {
    match error.find_root() {
        DataFusionError::ResourcesExhausted(_) => ErrorKind::ResourceExhausted,
        DataFusionError::NotImplemented(_) => ErrorKind::Unsupported,
        _ => ErrorKind::External,
    }
}
