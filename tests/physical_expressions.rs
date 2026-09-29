use asyncband::shutdown::ShutdownGuard;
use std::sync::{Arc, Mutex};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::future::BoxFuture;
use roc::{
    error::Result,
    exec::{
        FilterExec, GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult, ProjectExec,
        ScanExec, SinkExec, SinkExecutor, SinkResult, SourceExec,
    },
    expr::scalar::BoundScalarExprRef,
    operator::{
        AggregateOperator, FilterOperator, Operator, OperatorTree, OperatorTreeNode,
        ProjectOperator, ScanConsumer, ScanHandle, ScanOperator, ScanRequest, ScanStorage,
    },
    pipeline::{
        Executor, PipelineGraphBuilder, PipelineGraphExecutor, PipelineId, build_pipeline_graph,
        build_pipeline_on_node,
    },
};
use roc::{
    expr::{
        agg::{AggregateFunction, BoundAggregateExpression},
        scalar::{
            BoundConstantExpression, BoundFunctionExpression, BoundReferenceExpression,
            ScalarFunction as ExprOp,
        },
    },
    operator::{Projection, ProjectionExpression as ProjectionExpr},
};

fn column(index: usize) -> BoundScalarExprRef {
    BoundReferenceExpression::new(index).into_ref()
}
fn binary(left: BoundScalarExprRef, op: ExprOp, right: i64) -> BoundScalarExprRef {
    BoundFunctionExpression::new(
        op,
        vec![left, BoundConstantExpression::int64(Some(right)).into_ref()],
    )
    .into_ref()
}
fn filter_executor(predicate: BoundScalarExprRef) -> Box<dyn ProcessExecutor> {
    let exec = FilterExec::new(predicate);
    exec.new_executor(
        exec.init_global_context(&asyncband::shutdown::new().1)
            .unwrap(),
    )
    .unwrap()
}

fn project_executor(projector: Projection) -> Box<dyn ProcessExecutor> {
    let exec = ProjectExec::new(projector);
    let global = exec
        .init_global_context(&asyncband::shutdown::new().1)
        .unwrap();
    exec.new_executor(global).unwrap()
}

fn completed_batch(result: ProcessResult) -> RecordBatch {
    match result {
        ProcessResult::NeedMoreInput(batch) => batch,
        other => panic!("expected a completed input, got {other:?}"),
    }
}

struct MemoryStorage(RecordBatch);
struct MemoryScan(Arc<Mutex<Option<RecordBatch>>>);

impl ScanStorage for MemoryStorage {
    type StorageTaskDesc = Vec<usize>;

    fn start_scan(
        &self,
        request: ScanRequest<Self::StorageTaskDesc>,
    ) -> Result<Arc<dyn ScanHandle>> {
        assert_eq!(request.source(), &[2, 0]);
        let batch = self.0.project(request.source())?;
        Ok(Arc::new(MemoryScan(Arc::new(Mutex::new(Some(batch))))))
    }
}
impl ScanHandle for MemoryScan {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        Box::new(Self(self.0.clone()))
    }
    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl ScanConsumer for MemoryScan {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async { Ok(self.0.lock().unwrap().take()) })
    }
}

#[derive(Clone, Debug, Default)]
struct Collector(Arc<Mutex<Vec<RecordBatch>>>);
impl Operator for Collector {
    fn name(&self) -> &'static str {
        "collector"
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        graph
            .pipeline_mut(current)?
            .set_sink(Box::new(self.clone()))?;
        build_pipeline_on_node(&current_node.children()[0], current, graph)
    }
}
impl SinkExec for Collector {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        Ok(Box::new(self.clone()))
    }
    fn finalize<'a>(
        &'a self,
        _: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl SinkExecutor for Collector {
    fn sink<'a>(
        &'a mut self,
        _shutdown_guard: &'a ShutdownGuard,
        batch: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(batch.clone());
            Ok(SinkResult::NeedMoreInput)
        })
    }
    fn combine(self: Box<Self>, _shutdown_guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
struct TestExecutor;
impl Executor for TestExecutor {
    type JoinError = tokio::task::JoinError;
    type Handle<T>
        = tokio::task::JoinHandle<T>
    where
        T: Send + 'static;
    fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(task)
    }
}

#[tokio::test]
async fn executes_a_fully_bound_tree_with_reordered_scan_and_physical_expressions() {
    let storage_schema = Arc::new(
        Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("unused", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ])
        .with_metadata([("owner".to_owned(), "host".to_owned())].into()),
    );
    let batch = RecordBatch::try_new(
        storage_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"])),
            Arc::new(Int64Array::from(vec![99; 5])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(4),
                Some(5),
            ])),
        ],
    )
    .unwrap();
    let scan_input = Arc::new(storage_schema.project(&[2, 0]).unwrap());
    let scan_output = Arc::new(storage_schema.project(&[0, 2]).unwrap());
    let scan = OperatorTreeNode::new(
        ScanOperator::new(vec![2, 0], Arc::new(MemoryStorage(batch))),
        vec![],
    );
    let filtered = OperatorTreeNode::new(
        FilterOperator::new(binary(column(0), ExprOp::GreaterThan, 1)),
        vec![scan],
    );
    let reordered = OperatorTreeNode::new(
        ProjectOperator::new(Projection::from_indices(scan_input.clone(), &[1, 0]).unwrap()),
        vec![filtered],
    );
    let filter = OperatorTreeNode::new(
        FilterOperator::new(binary(column(1), ExprOp::LessThan, 5)),
        vec![reordered],
    );
    let projector = Projection::new(
        scan_output.clone(),
        vec![
            ProjectionExpr::new(
                BoundConstantExpression::string(Some("constant")).into_ref(),
                "label",
            ),
            ProjectionExpr::new(binary(column(1), ExprOp::Multiply, 2), "doubled"),
        ],
    );
    let projected_schema = projector.output_schema().unwrap();
    let project = OperatorTreeNode::new(ProjectOperator::new(projector), vec![filter]);
    let output_schema = Arc::new(
        Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("total", DataType::Int64, true),
            Field::new("rows", DataType::Int64, false),
            Field::new("null_count", DataType::Int64, false),
        ])
        .with_metadata([("owner".to_owned(), "host".to_owned())].into()),
    );
    let groups = Projection::new(
        projected_schema.clone(),
        vec![ProjectionExpr::new(
            BoundConstantExpression::string(Some("all")).into_ref(),
            "category",
        )],
    );
    let aggregates = vec![
        Arc::new(
            BoundAggregateExpression::new(
                AggregateFunction::Sum,
                vec![binary(column(1), ExprOp::Add, 1)],
            )
            .with_alias("total"),
        ),
        Arc::new(
            BoundAggregateExpression::new(
                AggregateFunction::Count,
                vec![BoundConstantExpression::int64(Some(1)).into_ref()],
            )
            .with_alias("rows"),
        ),
        Arc::new(
            BoundAggregateExpression::new(
                AggregateFunction::Count,
                vec![BoundConstantExpression::int64(None).into_ref()],
            )
            .with_alias("null_count"),
        ),
    ];
    let aggregate = OperatorTreeNode::new(
        AggregateOperator::try_new(groups, aggregates).unwrap(),
        vec![project],
    );
    let collector = Collector::default();
    let tree = OperatorTree::new(OperatorTreeNode::new(collector.clone(), vec![aggregate]));
    let graph = build_pipeline_graph(tree).unwrap();
    PipelineGraphExecutor::new(graph)
        .with_task_executor(TestExecutor)
        .with_parallelism(2)
        .execute()
        .await
        .unwrap();
    let batches = collector.0.lock().unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let result = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(result.schema(), output_schema);
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "all"
    );
    for (index, expected) in [(1, 16), (2, 2), (3, 0)] {
        assert_eq!(
            result
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
}

#[test]
fn scalar_predicates_and_constant_projections_preserve_batch_shape() {
    let input = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        input.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
    )
    .unwrap();
    for (value, rows) in [(Some(true), 3), (Some(false), 0), (None, 0)] {
        let result = filter_executor(BoundConstantExpression::boolean(value).into_ref())
            .execute(&batch)
            .unwrap();
        let result = completed_batch(result);
        assert_eq!(result.num_rows(), rows);
        assert_eq!(result.schema(), input);
    }
    let projector = Projection::new(
        input.clone(),
        vec![
            ProjectionExpr::new(column(0), "renamed"),
            ProjectionExpr::new(
                BoundConstantExpression::int64(Some(7)).into_ref(),
                "constant",
            ),
            ProjectionExpr::new(BoundConstantExpression::int64(None).into_ref(), "null"),
        ],
    );
    let output = Arc::new(Schema::new(vec![
        Field::new("renamed", DataType::Int64, true),
        Field::new("constant", DataType::Int64, false),
        Field::new("null", DataType::Int64, true),
    ]));
    let mut executor = project_executor(projector);
    let result = completed_batch(executor.execute(&batch).unwrap());
    assert_eq!(result.schema(), output);
    assert_eq!(result.column(0).null_count(), 1);
    assert_eq!(
        result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, 7, 7]
    );
    assert_eq!(result.column(2).null_count(), 3);
    assert_eq!(
        completed_batch(executor.execute(&batch.slice(0, 0)).unwrap()).num_rows(),
        0
    );
    let empty = Projection::new(input.clone(), Vec::<ProjectionExpr>::new());
    let zero_columns = completed_batch(project_executor(empty).execute(&batch).unwrap());
    assert_eq!(
        (zero_columns.num_rows(), zero_columns.num_columns()),
        (3, 0)
    );
}

#[test]
fn preserves_arrow_kernel_errors() {
    let input = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
    let batch =
        RecordBatch::try_new(Arc::new(input), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let projector = Projection::new(
        batch.schema().clone(),
        vec![ProjectionExpr::new(
            binary(column(0), ExprOp::Divide, 0),
            "x",
        )],
    );
    let error = project_executor(projector).execute(&batch).unwrap_err();
    assert!(matches!(error, roc::error::Error::Arrow(_)));
}

#[test]
fn filter_rejects_non_boolean_results_without_an_input_schema() {
    let input = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(input, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    for predicate in [
        column(0),
        BoundConstantExpression::int64(Some(1)).into_ref(),
    ] {
        assert!(matches!(
            filter_executor(predicate).execute(&batch),
            Err(roc::error::Error::Execution(_))
        ));
    }
}

#[tokio::test]
async fn scan_forwards_host_task_and_preserves_storage_batches_and_cancellation() {
    let schema = Arc::new(
        Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("unused", DataType::Int64, false),
            Field::new("last", DataType::Int64, false),
        ])
        .with_metadata([("source".to_owned(), "storage".to_owned())].into()),
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![3])),
        ],
    )
    .unwrap();
    let expected = batch.project(&[2, 0]).unwrap();
    let exec = ScanExec::new(ScanOperator::new(
        vec![2, 0],
        Arc::new(MemoryStorage(batch)),
    ));
    let (shutdown, shutdown_guard) = asyncband::shutdown::new();
    let global = exec.init_global_context(&shutdown_guard).unwrap();
    let mut worker = exec.new_executor(global.clone()).unwrap();
    assert_eq!(
        worker.next_batch(&shutdown_guard).await.unwrap().unwrap(),
        expected
    );
    assert!(worker.next_batch(&shutdown_guard).await.unwrap().is_none());
    shutdown.request_shutdown();
    assert!(matches!(
        worker.next_batch(&shutdown_guard).await,
        Err(roc::error::Error::Cancelled)
    ));
    exec.finalize(global, &shutdown_guard).await.unwrap();
}

struct MemoryExchange {
    input: Option<RecordBatch>,
    output: Collector,
    schema: arrow::datatypes::SchemaRef,
    created_sinks: std::sync::atomic::AtomicUsize,
}

impl roc::operator::ExchangeService for MemoryExchange {
    fn start_input(
        &self,
        exchange: usize,
        _shutdown_guard: &ShutdownGuard,
    ) -> Result<Arc<dyn roc::operator::ExchangeHandle>> {
        assert_eq!(exchange, 7);
        Ok(Arc::new(MemoryScan(Arc::new(Mutex::new(
            self.input.clone(),
        )))))
    }

    fn create_sink(
        &self,
        exchange: usize,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Box<dyn roc::operator::ExchangeSink>> {
        assert_eq!(exchange, 8);
        assert_eq!(schema, &self.schema);
        self.created_sinks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(self.output.clone()))
    }
}

impl roc::operator::ExchangeHandle for MemoryScan {
    fn consumer(&self) -> Box<dyn roc::operator::ExchangeConsumer> {
        Box::new(Self(self.0.clone()))
    }
    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl roc::operator::ExchangeConsumer for MemoryScan {
    fn next(&mut self) -> BoxFuture<'_, Option<RecordBatch>> {
        Box::pin(async { self.0.lock().unwrap().take() })
    }
}

impl roc::operator::ExchangeSink for Collector {
    fn send<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(batch.clone());
            Ok(())
        })
    }
}

#[tokio::test]
async fn exchange_relays_batches_and_initializes_sinks_for_empty_input() {
    use roc::operator::{ExchangeSinkOperator, ExchangeSourceOperator};
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    for input in [Some(batch), None] {
        let service = Arc::new(MemoryExchange {
            input: input.clone(),
            output: Collector::default(),
            schema: schema.clone(),
            created_sinks: std::sync::atomic::AtomicUsize::new(0),
        });
        let source = OperatorTreeNode::new(ExchangeSourceOperator::new(7, service.clone()), vec![]);
        let sink = OperatorTreeNode::new(
            ExchangeSinkOperator::new(vec![8], schema.clone(), service.clone()),
            vec![source],
        );
        let graph = build_pipeline_graph(OperatorTree::new(sink)).unwrap();
        PipelineGraphExecutor::new(graph)
            .with_task_executor(TestExecutor)
            .execute()
            .await
            .unwrap();
        assert_eq!(
            service
                .created_sinks
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            *service.output.0.lock().unwrap(),
            input.into_iter().collect::<Vec<_>>()
        );
    }
}
