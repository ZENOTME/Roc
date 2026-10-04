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

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_expr::PhysicalExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream, collect, empty::EmptyExec,
    },
    prelude::{ParquetReadOptions, SessionConfig, SessionContext, col, lit},
};
use futures::{Stream, future::try_join_all, task::ArcWake};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use roc::{
    error::Error,
    operator::{ScanConsumer, ScanRequest, ScanStorage},
};
use roc_datafusion::{DataFusionScan, DataFusionStorage};
use std::{
    fs::File,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

fn parquet_files() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
        Field::new("unused", DataType::Utf8, false),
    ]));
    for part in 0..4_i64 {
        let ids = (part * 64..(part + 1) * 64).collect::<Vec<_>>();
        let values = ids
            .iter()
            .map(|id| (id % 5 != 0).then_some(id * 3))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(vec!["not projected"; 64])),
            ],
        )
        .unwrap();
        let file = File::create(dir.path().join(format!("part-{part}.parquet"))).unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(16))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    dir
}

async fn projected_plan(path: &str, partitions: usize) -> (SessionContext, Arc<dyn ExecutionPlan>) {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(partitions)
            .with_batch_size(7),
    );
    let plan = ctx
        .read_parquet(path, ParquetReadOptions::default())
        .await
        .unwrap()
        .filter(col("id").gt_eq(lit(70_i64)))
        .unwrap()
        .select_columns(&["id", "value"])
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    (ctx, plan)
}

fn sorted_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                ids.value(row),
                (!values.is_null(row)).then(|| values.value(row)),
            )
        }));
    }
    rows.sort_unstable();
    rows
}

async fn drain(mut consumer: Box<dyn ScanConsumer>) -> roc::error::Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = consumer.next().await? {
        batches.push(batch);
    }
    Ok(batches)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parquet_projection_predicate_and_shared_consumers_match_datafusion() {
    let files = parquet_files();
    let (ctx, plan) = projected_plan(files.path().to_str().unwrap(), 4).await;
    let expected = collect(plan.clone(), ctx.task_ctx()).await.unwrap();
    let expected_rows = sorted_rows(&expected);
    assert_eq!(expected_rows.len(), 186);
    let scan = DataFusionScan::new(plan, ctx.task_ctx());
    assert_eq!(scan.schema().fields().len(), 2);
    assert!(scan.partition_count() > 1);

    // Reuse one descriptor with fewer, equal and more consumers than partitions.
    // Each run must receive every row once, without preserving global order.
    for workers in [1, 4, 7] {
        let (shutdown, guard) = asyncband::shutdown::new();
        let handle = DataFusionStorage
            .start_scan(ScanRequest::new(scan.clone(), guard.clone()))
            .unwrap();
        let outputs = try_join_all((0..workers).map(|_| tokio::spawn(drain(handle.consumer()))))
            .await
            .unwrap()
            .into_iter()
            .collect::<roc::error::Result<Vec<_>>>()
            .unwrap();
        let batches = outputs.into_iter().flatten().collect::<Vec<_>>();
        for batch in &batches {
            assert_eq!(batch.schema(), scan.schema());
        }
        assert_eq!(sorted_rows(&batches), expected_rows);
        handle.finish().await.unwrap();
        assert!(!guard.is_shutdown_requested());
        drop((handle, guard));
        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
            .await
            .expect("completed scan leaked a shutdown guard");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_partition_parquet_reexecution_and_concurrent_scans_are_independent() {
    let files = parquet_files();
    let (ctx, plan) = projected_plan(files.path().to_str().unwrap(), 1).await;
    let expected = sorted_rows(&collect(plan.clone(), ctx.task_ctx()).await.unwrap());
    assert_eq!(expected.len(), 186);
    let scan = DataFusionScan::new(plan, ctx.task_ctx());
    assert_eq!(scan.partition_count(), 1);

    // Four files share one DataFusion partition and its mutable file queue.
    // Run twice sequentially, then start two scans before consuming either;
    // every execution must receive all rows from its own fresh queue.
    for executions in [1, 1, 2] {
        let (shutdown, guard) = asyncband::shutdown::new();
        let handles = (0..executions)
            .map(|_| {
                DataFusionStorage
                    .start_scan(ScanRequest::new(scan.clone(), guard.clone()))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let outputs = try_join_all(
            handles
                .iter()
                .map(|handle| tokio::spawn(drain(handle.consumer()))),
        )
        .await
        .unwrap();
        for output in outputs {
            assert_eq!(sorted_rows(&output.unwrap()), expected);
        }
        for handle in &handles {
            handle.finish().await.unwrap();
        }
        drop((handles, guard));
        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
            .await
            .expect("independent scans leaked a shutdown guard");
    }
}

#[tokio::test]
async fn parquet_read_failure_is_not_end_of_stream() {
    let files = parquet_files();
    let (ctx, plan) = projected_plan(files.path().to_str().unwrap(), 4).await;
    // Listing and metadata planning succeeded; fail actual execution-time I/O.
    for file in std::fs::read_dir(files.path()).unwrap() {
        std::fs::remove_file(file.unwrap().path()).unwrap();
    }
    let (_shutdown, guard) = asyncband::shutdown::new();
    let handle = DataFusionStorage
        .start_scan(ScanRequest::new(
            DataFusionScan::new(plan, ctx.task_ctx()),
            guard,
        ))
        .unwrap();
    let error = handle.consumer().next().await.unwrap_err();
    assert!(matches!(error, Error::Execution(_)));
    assert!(error.to_string().contains("DataFusion scan partition"));
    assert!(handle.finish().await.is_err());
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Pending,
    BatchThenPending,
    ResetError,
    ExecuteError,
    StreamError,
}

#[derive(Debug)]
struct TestPlan {
    properties: Arc<PlanProperties>,
    mode: Mode,
    dropped: Arc<AtomicUsize>,
}

impl DisplayAs for TestPlan {
    fn fmt_as(
        &self,
        _kind: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "TestPlan")
    }
}

impl ExecutionPlan for TestPlan {
    fn name(&self) -> &'static str {
        "TestPlan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if matches!(self.mode, Mode::ResetError) {
            Err(DataFusionError::Execution("reset failed".into()))
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if matches!(self.mode, Mode::ExecuteError) {
            return Err(DataFusionError::Execution("execute failed".into()));
        }
        Ok(Box::pin(TestStream {
            schema: self.schema(),
            mode: self.mode,
            dropped: self.dropped.clone(),
        }))
    }
}

struct TestStream {
    schema: SchemaRef,
    mode: Mode,
    dropped: Arc<AtomicUsize>,
}

impl RecordBatchStream for TestStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for TestStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.mode {
            Mode::BatchThenPending => {
                self.mode = Mode::Pending;
                Poll::Ready(Some(Ok(RecordBatch::new_empty(self.schema.clone()))))
            }
            Mode::StreamError => Poll::Ready(Some(Err(DataFusionError::Execution(
                "stream failed".into(),
            )))),
            _ => Poll::Pending,
        }
    }
}

impl Drop for TestStream {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn test_scan(mode: Mode, dropped: Arc<AtomicUsize>) -> DataFusionScan {
    DataFusionScan::new(
        Arc::new(TestPlan {
            properties: EmptyExec::new(Arc::new(Schema::empty()))
                .properties()
                .clone(),
            mode,
            dropped,
        }),
        Arc::new(TaskContext::default()),
    )
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl ArcWake for WakeCounter {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn finish_and_handle_drop_wake_pending_reads_and_release_streams() {
    for drop_handle in [false, true] {
        let (_shutdown, guard) = asyncband::shutdown::new();
        let dropped = Arc::new(AtomicUsize::new(0));
        let handle = DataFusionStorage
            .start_scan(ScanRequest::new(
                test_scan(Mode::Pending, dropped.clone()),
                guard.clone(),
            ))
            .unwrap();
        let mut consumer = handle.consumer();
        let mut next = consumer.next();
        let counter = Arc::new(WakeCounter::default());
        let waker = futures::task::waker(counter.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(next.as_mut().poll(&mut cx).is_pending());
        if drop_handle {
            drop(handle);
        } else {
            futures::executor::block_on(handle.finish()).unwrap();
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(counter.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(next.as_mut().poll(&mut cx), Poll::Ready(Ok(None))));
        assert!(!guard.is_shutdown_requested());
    }
}

#[test]
fn finish_releases_idle_stream_even_if_consumer_stays_alive() {
    let (_shutdown, guard) = asyncband::shutdown::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = DataFusionStorage
        .start_scan(ScanRequest::new(
            test_scan(Mode::BatchThenPending, dropped.clone()),
            guard,
        ))
        .unwrap();
    let mut consumer = handle.consumer();
    assert!(
        futures::executor::block_on(consumer.next())
            .unwrap()
            .is_some()
    );
    futures::executor::block_on(handle.finish()).unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(
        futures::executor::block_on(consumer.next())
            .unwrap()
            .is_none()
    );
}

#[test]
fn dropping_consumer_releases_its_stream_without_finishing_handle() {
    let (_shutdown, guard) = asyncband::shutdown::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = DataFusionStorage
        .start_scan(ScanRequest::new(
            test_scan(Mode::BatchThenPending, dropped.clone()),
            guard,
        ))
        .unwrap();
    let mut consumer = handle.consumer();
    assert!(
        futures::executor::block_on(consumer.next())
            .unwrap()
            .is_some()
    );
    drop(consumer);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    futures::executor::block_on(handle.finish()).unwrap();
}

#[test]
fn caller_shutdown_wakes_reads_and_releases_datafusion_stream() {
    let (shutdown, guard) = asyncband::shutdown::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = DataFusionStorage
        .start_scan(ScanRequest::new(
            test_scan(Mode::Pending, dropped.clone()),
            guard,
        ))
        .unwrap();
    let mut consumer = handle.consumer();
    let mut next = consumer.next();
    let counter = Arc::new(WakeCounter::default());
    let waker = futures::task::waker(counter.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(next.as_mut().poll(&mut cx).is_pending());
    shutdown.request_shutdown();
    assert!(counter.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        next.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::Cancelled))
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    drop(next);
    drop((consumer, handle));
    futures::executor::block_on(shutdown);
}

#[test]
fn execution_and_stream_errors_survive_finalization() {
    for mode in [Mode::ExecuteError, Mode::StreamError] {
        let (_shutdown, guard) = asyncband::shutdown::new();
        let handle = DataFusionStorage
            .start_scan(ScanRequest::new(
                test_scan(mode, Arc::new(AtomicUsize::new(0))),
                guard,
            ))
            .unwrap();
        let error = futures::executor::block_on(handle.consumer().next()).unwrap_err();
        assert!(error.to_string().contains("failed"));
        let final_error = futures::executor::block_on(handle.finish()).unwrap_err();
        assert_eq!(error.to_string(), final_error.to_string());
    }
}

#[test]
fn already_cancelled_request_never_starts_scan() {
    let (shutdown, guard) = asyncband::shutdown::new();
    shutdown.request_shutdown();
    let result = DataFusionStorage.start_scan(ScanRequest::new(
        test_scan(Mode::ResetError, Arc::new(AtomicUsize::new(0))),
        guard,
    ));
    assert!(matches!(result, Err(Error::Cancelled)));
}

#[tokio::test]
async fn reset_failure_is_reported_and_releases_shutdown_guard() {
    let (shutdown, guard) = asyncband::shutdown::new();
    let result = DataFusionStorage.start_scan(ScanRequest::new(
        test_scan(Mode::ResetError, Arc::new(AtomicUsize::new(0))),
        guard.clone(),
    ));
    assert!(matches!(
        result,
        Err(Error::Execution(message)) if message.contains("DataFusion scan reset")
            && message.contains("reset failed")
    ));
    assert!(!guard.is_shutdown_requested());
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
        .await
        .expect("failed scan initialization leaked its shutdown guard");
}
