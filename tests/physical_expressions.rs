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
    exec::{ScanExec, SinkExec, SourceExec},
    expr::scalar::ScalarExprRef,
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
        ExpressionResultType,
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            ConstantExpression, FunctionExpression, FunctionKind as ExprOp, ReferenceExpression,
        },
    },
    operator::{Projection, ProjectionExpression as ProjectionExpr},
};

fn reference(index: usize, data_type: DataType, nullable: bool) -> ScalarExprRef {
    ReferenceExpression::new(index, ExpressionResultType::new(data_type, nullable)).into_ref()
}
/// Input columns in this file are Int64 and nullable unless a test needs an
/// explicitly non-nullable batch.
fn column(index: usize) -> ScalarExprRef {
    reference(index, DataType::Int64, true)
}
/// Comparisons yield Boolean; arithmetic yields the left operand's type. The
/// result is nullable whenever either operand is.
fn binary(left: ScalarExprRef, op: ExprOp, right: i64) -> ScalarExprRef {
    let right = ConstantExpression::int64(Some(right)).into_ref();
    let nullable = left.result_type().is_nullable() || right.result_type().is_nullable();
    let data_type = match op {
        ExprOp::Add | ExprOp::Subtract | ExprOp::Multiply | ExprOp::Divide | ExprOp::Remainder => {
            left.result_type().data_type().clone()
        }
        _ => DataType::Boolean,
    };
    FunctionExpression::binary(op, left, right, data_type, nullable).into_ref()
}
struct BatchProgram {
    program: roc::suboperator::ProgramContext,
    local: roc::suboperator::ProgramExecutionContext,
}
impl BatchProgram {
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        self.program.execute_record_batch(input, &mut self.local)
    }
}
fn filter_program(predicate: ScalarExprRef) -> Result<BatchProgram> {
    let mut b = roc::suboperator::SuboperatorScopeBuilder::new();
    roc::operator::FilterOperator::new(predicate).build_suboperator(&mut b)?;
    let program = b
        .finish(vec![])?
        .compile(roc::suboperator::ExecutionMode::Compiled)?;
    let local = program.new_execution_context();
    Ok(BatchProgram { program, local })
}
fn project_program(projection: Projection) -> BatchProgram {
    let mut b = roc::suboperator::SuboperatorScopeBuilder::new();
    projection.build_suboperator(&mut b).unwrap();
    let program = b
        .finish(vec![])
        .unwrap()
        .compile(roc::suboperator::ExecutionMode::Compiled)
        .unwrap();
    let local = program.new_execution_context();
    BatchProgram { program, local }
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
struct CollectorExec {
    output: Arc<Mutex<Vec<RecordBatch>>>,
    state: roc::suboperator::StateId,
}
impl Operator for Collector {
    fn name(&self) -> &'static str {
        "collector"
    }
    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        build_pipeline_on_node(&node.children()[0], current, graph)?;
        let pipeline = graph.pipeline_mut(current)?;
        let state = pipeline
            .suboperators
            .state(roc::suboperator::StateBuilder::new(|| {
                Ok(Vec::<RecordBatch>::new())
            }));
        pipeline.suboperators.batch_update(state, |state, batch| {
            state.get_mut::<Vec<RecordBatch>>()?.push(batch.clone());
            Ok(roc::suboperator::ProgramControl::Continue)
        })?;
        pipeline.set_sink(Box::new(CollectorExec {
            output: self.0.clone(),
            state,
        }))
    }
}
impl SinkExec for CollectorExec {
    fn combine<'a>(
        &'a mut self,
        mut local: roc::suboperator::ProgramExecutionContext,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.output.lock().unwrap().extend(
                local
                    .take_state(self.state)
                    .unwrap()
                    .into_inner::<Vec<RecordBatch>>()?,
            );
            Ok(())
        })
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
    run_bound_tree(roc::suboperator::ExecutionMode::Compiled).await;
}
async fn run_bound_tree(mode: roc::suboperator::ExecutionMode) {
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
    let projector = Projection::new(vec![
        ProjectionExpr::new(
            ConstantExpression::string(Some("constant")).into_ref(),
            "label",
        ),
        ProjectionExpr::new(binary(column(1), ExprOp::Multiply, 2), "doubled"),
    ])
    .with_metadata(scan_output.metadata().clone());
    let projected_schema = projector.output_schema();
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
    let groups = Projection::new(vec![ProjectionExpr::new(
        ConstantExpression::string(Some("all")).into_ref(),
        "category",
    )])
    .with_metadata(projected_schema.metadata().clone());
    let aggregates = vec![
        Arc::new(
            AggregateExpression::new(
                AggregateFunction::Sum,
                vec![binary(column(1), ExprOp::Add, 1)],
                DataType::Int64,
                true,
            )
            .with_alias("total"),
        ),
        Arc::new(
            AggregateExpression::new(
                AggregateFunction::Count,
                vec![ConstantExpression::int64(Some(1)).into_ref()],
                DataType::Int64,
                false,
            )
            .with_alias("rows"),
        ),
        Arc::new(
            AggregateExpression::new(
                AggregateFunction::Count,
                vec![ConstantExpression::int64(None).into_ref()],
                DataType::Int64,
                false,
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
    let graph = build_pipeline_graph(tree, mode).unwrap();
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
        let result = filter_program(ConstantExpression::boolean(value).into_ref())
            .unwrap()
            .execute(&batch)
            .unwrap();
        assert_eq!(result.num_rows(), rows);
        assert_eq!(result.schema(), input);
    }
    let projector = Projection::new(vec![
        ProjectionExpr::new(column(0), "renamed"),
        ProjectionExpr::new(ConstantExpression::int64(Some(7)).into_ref(), "constant"),
        ProjectionExpr::new(ConstantExpression::int64(None).into_ref(), "null"),
    ]);
    let output = Arc::new(Schema::new(vec![
        Field::new("renamed", DataType::Int64, true),
        Field::new("constant", DataType::Int64, false),
        Field::new("null", DataType::Int64, true),
    ]));
    let mut executor = project_program(projector);
    let result = executor.execute(&batch).unwrap();
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
    assert_eq!(executor.execute(&batch.slice(0, 0)).unwrap().num_rows(), 0);
    let empty = Projection::new(Vec::<ProjectionExpr>::new());
    let zero_columns = project_program(empty).execute(&batch).unwrap();
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
    let projector = Projection::new(vec![ProjectionExpr::new(
        binary(column(0), ExprOp::Divide, 0),
        "x",
    )]);
    let error = project_program(projector).execute(&batch).unwrap_err();
    assert!(matches!(error, roc::error::Error::Arrow(_)));
}

#[test]
fn filter_rejects_non_boolean_results_without_an_input_schema() {
    let input = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let _batch = RecordBatch::try_new(input, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    for predicate in [
        reference(0, DataType::Int64, false),
        ConstantExpression::int64(Some(1)).into_ref(),
    ] {
        assert!(matches!(
            filter_program(predicate),
            Err(roc::error::Error::InvalidPlan(_))
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
    let mut exec = ScanExec::new(ScanOperator::new(
        vec![2, 0],
        Arc::new(MemoryStorage(batch)),
    ));
    let (shutdown, shutdown_guard) = asyncband::shutdown::new();
    exec.start(&shutdown_guard).unwrap();
    assert_eq!(
        exec.next_batch(&shutdown_guard).await.unwrap().unwrap(),
        expected
    );
    assert!(exec.next_batch(&shutdown_guard).await.unwrap().is_none());
    shutdown.request_shutdown();
    assert!(matches!(
        exec.next_batch(&shutdown_guard).await,
        Err(roc::error::Error::Cancelled)
    ));
    exec.finish(&shutdown_guard).await.unwrap();
}

struct MemoryExchange {
    input: Result<Option<RecordBatch>>,
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
        Ok(Arc::new(MemoryExchangeInput(Arc::new(Mutex::new(
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

struct MemoryExchangeInput(Arc<Mutex<Result<Option<RecordBatch>>>>);

impl roc::operator::ExchangeHandle for MemoryExchangeInput {
    fn consumer(&self) -> Box<dyn roc::operator::ExchangeConsumer> {
        Box::new(Self(self.0.clone()))
    }
    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl roc::operator::ExchangeConsumer for MemoryExchangeInput {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async {
            match &mut *self.0.lock().unwrap() {
                Ok(batch) => Ok(batch.take()),
                Err(error) => Err(error.clone()),
            }
        })
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
    for (input, parallelism) in [(Some(batch.clone()), 1), (Some(batch), 3), (None, 3)] {
        let service = Arc::new(MemoryExchange {
            input: Ok(input.clone()),
            output: Collector::default(),
            schema: schema.clone(),
            created_sinks: std::sync::atomic::AtomicUsize::new(0),
        });
        let source = OperatorTreeNode::new(ExchangeSourceOperator::new(7, service.clone()), vec![]);
        let sink = OperatorTreeNode::new(
            ExchangeSinkOperator::new(vec![8], schema.clone(), service.clone()),
            vec![source],
        );
        let graph = build_pipeline_graph(
            OperatorTree::new(sink),
            roc::suboperator::ExecutionMode::Compiled,
        )
        .unwrap();
        assert_eq!(
            service
                .created_sinks
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        PipelineGraphExecutor::new(graph)
            .with_parallelism(parallelism)
            .with_task_executor(TestExecutor)
            .execute()
            .await
            .unwrap();
        assert_eq!(
            service
                .created_sinks
                .load(std::sync::atomic::Ordering::SeqCst),
            parallelism
        );
        assert_eq!(
            *service.output.0.lock().unwrap(),
            input.into_iter().collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn exchange_read_errors_reach_the_caller_without_becoming_end_of_input() {
    let read_error = Arc::new(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "exchange connection reset",
    ));
    let collector = Collector::default();
    let service = Arc::new(MemoryExchange {
        input: Err(roc::error::Error::Io(read_error.clone())),
        output: collector.clone(),
        schema: Arc::new(Schema::empty()),
        created_sinks: std::sync::atomic::AtomicUsize::new(0),
    });
    let source = OperatorTreeNode::new(
        roc::operator::ExchangeSourceOperator::new(7, service),
        vec![],
    );
    let tree = OperatorTree::new(OperatorTreeNode::new(collector.clone(), vec![source]));
    let error = PipelineGraphExecutor::new(
        build_pipeline_graph(tree, roc::suboperator::ExecutionMode::Compiled).unwrap(),
    )
    .with_task_executor(TestExecutor)
    .execute()
    .await
    .unwrap_err();
    assert!(matches!(error, roc::error::Error::Io(actual) if Arc::ptr_eq(&actual, &read_error)));
    assert!(collector.0.lock().unwrap().is_empty());
}
