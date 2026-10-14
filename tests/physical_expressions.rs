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

use asyncband::shutdown::{Shutdown, ShutdownGuard};
use std::sync::{Arc, Mutex};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::future::BoxFuture;
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    error::Result,
    exec::{ScanExec, SinkExec, SinkStatus, SourceExec},
    expr::{
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            ConstantExpression, FunctionExpression, FunctionKind as ExprOp, ReferenceExpression,
            ScalarExprRef,
        },
    },
    kernel::{ExecutionMode, KernelBuilder, LocalState, Program},
    operator::{
        AggregateOperator, FilterOperator, Operator, OperatorTree, OperatorTreeNode,
        ProjectOperator, ScanConsumer, ScanHandle, ScanOperator, ScanRequest, ScanStorage,
    },
    pipeline::{
        Executor, PipelineGraphBuilder, PipelineGraphExecutor, PipelineId, build_pipeline_graph,
        build_pipeline_graph_with_mode, build_pipeline_on_node,
    },
};

fn column(
    columns: &mut ColumnContext,
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> ColumnId {
    columns.allocate_named(ColumnType::new(data_type, nullable), name)
}
fn reference(column: ColumnId) -> ScalarExprRef {
    ReferenceExpression::new(column).into_ref()
}
fn int(columns: &mut ColumnContext, value: Option<i64>) -> ScalarExprRef {
    let output = columns.allocate(ColumnType::new(DataType::Int64, value.is_none()));
    ConstantExpression::int64(value, output).into_ref()
}
/// Comparisons yield Boolean; arithmetic yields the left operand's type. The
/// result is nullable whenever the left operand is (the constant is not).
fn binary(
    columns: &mut ColumnContext,
    left: ScalarExprRef,
    op: ExprOp,
    right: i64,
) -> ScalarExprRef {
    let right = int(columns, Some(right));
    let left_type = columns.column_type(left.output()).unwrap().clone();
    let data_type = match op {
        ExprOp::Add | ExprOp::Subtract | ExprOp::Multiply | ExprOp::Divide | ExprOp::Remainder => {
            left_type.data_type
        }
        _ => DataType::Boolean,
    };
    let output = columns.allocate(ColumnType::new(data_type, left_type.nullable));
    FunctionExpression::binary(op, left, right, output).into_ref()
}
/// A standalone program over `inputs`, producing `outputs`.
struct BatchProgram {
    program: Program,
    local: LocalState,
    _shutdown: Shutdown,
    guard: ShutdownGuard,
}
impl BatchProgram {
    fn new(
        columns: &ColumnContext,
        inputs: &[ColumnId],
        build: impl FnOnce(&mut KernelBuilder<'_>) -> Result<Vec<ColumnId>>,
    ) -> Result<Self> {
        let mut builder = KernelBuilder::new(columns);
        builder.bind_inputs(inputs)?;
        let outputs = build(&mut builder)?;
        let program = Program::compile(builder.finish(Some(&outputs))?)?;
        let local = program.new_local()?;
        let (shutdown, guard) = asyncband::shutdown::new();
        Ok(Self {
            program,
            local,
            _shutdown: shutdown,
            guard,
        })
    }
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        Ok(self
            .program
            .run(input, &mut self.local, &self.guard)?
            .unwrap())
    }
}
fn filter_program(
    columns: &ColumnContext,
    inputs: &[ColumnId],
    predicate: ScalarExprRef,
) -> Result<BatchProgram> {
    BatchProgram::new(columns, inputs, |builder| {
        FilterOperator::new(predicate, inputs.to_vec()).build_kernels(builder)?;
        Ok(inputs.to_vec())
    })
}
fn project_program(
    columns: &ColumnContext,
    inputs: &[ColumnId],
    expressions: Vec<ScalarExprRef>,
) -> Result<BatchProgram> {
    BatchProgram::new(columns, inputs, |builder| {
        let project = ProjectOperator::new(expressions);
        project.build_kernels(builder)?;
        Ok(project.output_columns())
    })
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

/// Root operator collecting the output chunks of its child's pipeline.
#[derive(Clone, Debug, Default)]
struct Collector(Arc<Mutex<Vec<RecordBatch>>>);
struct CollectorExec {
    output: Arc<Mutex<Vec<RecordBatch>>>,
}
impl Operator for Collector {
    fn name(&self) -> &'static str {
        "collector"
    }
    fn output_columns(&self) -> Vec<ColumnId> {
        vec![]
    }
    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let child = &node.children()[0];
        build_pipeline_on_node(child, current, graph)?;
        graph.pipeline_mut(current)?.set_sink(
            Box::new(CollectorExec {
                output: self.0.clone(),
            }),
            Some(child.operator().output_columns()),
        )
    }
}
impl SinkExec for CollectorExec {
    fn consume<'a>(
        &'a mut self,
        chunk: RecordBatch,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        Box::pin(async move {
            self.output.lock().unwrap().push(chunk);
            Ok(SinkStatus::Continue)
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
    for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
        execute_fully_bound_tree(mode).await;
    }
}

async fn execute_fully_bound_tree(mode: ExecutionMode) {
    let storage_schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Utf8, false),
        Field::new("unused", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        storage_schema,
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
    let mut columns = ColumnContext::new();
    let group = column(&mut columns, "group", DataType::Utf8, false);
    let value = column(&mut columns, "value", DataType::Int64, true);
    // The storage task reads columns 2 and 0, in that order.
    let scan = OperatorTreeNode::new(
        ScanOperator::new(
            vec![2, 0],
            Arc::new(MemoryStorage(batch)),
            vec![value, group],
        ),
        vec![],
    );
    let filtered = OperatorTreeNode::new(
        FilterOperator::new(
            binary(&mut columns, reference(value), ExprOp::GreaterThan, 1),
            vec![value, group],
        ),
        vec![scan],
    );
    let reordered = OperatorTreeNode::new(
        ProjectOperator::new(vec![reference(group), reference(value)]),
        vec![filtered],
    );
    let filter = OperatorTreeNode::new(
        FilterOperator::new(
            binary(&mut columns, reference(value), ExprOp::LessThan, 5),
            vec![group, value],
        ),
        vec![reordered],
    );
    let label = column(&mut columns, "label", DataType::Utf8, false);
    let doubled = binary(&mut columns, reference(value), ExprOp::Multiply, 2);
    let project = OperatorTreeNode::new(
        ProjectOperator::new(vec![
            ConstantExpression::string(Some("constant"), label).into_ref(),
            doubled.clone(),
        ]),
        vec![filter],
    );
    let category = column(&mut columns, "category", DataType::Utf8, false);
    let total = column(&mut columns, "total", DataType::Int64, true);
    let rows = column(&mut columns, "rows", DataType::Int64, false);
    let null_count = column(&mut columns, "null_count", DataType::Int64, false);
    let incremented = binary(&mut columns, doubled, ExprOp::Add, 1);
    let one = int(&mut columns, Some(1));
    let null = int(&mut columns, None);
    let aggregates = vec![
        Arc::new(AggregateExpression::new(
            AggregateFunction::Sum,
            vec![incremented],
            total,
        )),
        Arc::new(AggregateExpression::new(
            AggregateFunction::Count,
            vec![one],
            rows,
        )),
        Arc::new(AggregateExpression::new(
            AggregateFunction::Count,
            vec![null],
            null_count,
        )),
    ];
    let aggregate = OperatorTreeNode::new(
        AggregateOperator::try_new(
            vec![ConstantExpression::string(Some("all"), category).into_ref()],
            aggregates,
        )
        .unwrap(),
        vec![project],
    );
    let collector = Collector::default();
    let tree = OperatorTree::new(
        columns,
        OperatorTreeNode::new(collector.clone(), vec![aggregate]),
    );
    let graph = build_pipeline_graph_with_mode(&tree, mode).unwrap();
    PipelineGraphExecutor::new(graph)
        .with_task_executor(TestExecutor)
        .with_parallelism(2)
        .execute()
        .await
        .unwrap();
    let batches = collector.0.lock().unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let result = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("total", DataType::Int64, true),
        Field::new("rows", DataType::Int64, false),
        Field::new("null_count", DataType::Int64, false),
    ]));
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
    let mut columns = ColumnContext::new();
    let a = column(&mut columns, "a", DataType::Int64, true);
    let input = columns.schema(&[a]).unwrap();
    let batch = RecordBatch::try_new(
        input.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
    )
    .unwrap();
    for (value, rows) in [(Some(true), 3), (Some(false), 0), (None, 0)] {
        let predicate = columns.allocate(ColumnType::new(DataType::Boolean, value.is_none()));
        let predicate = ConstantExpression::boolean(value, predicate).into_ref();
        let result = filter_program(&columns, &[a], predicate)
            .unwrap()
            .execute(&batch)
            .unwrap();
        assert_eq!(result.num_rows(), rows);
        assert_eq!(result.schema(), input);
    }
    let constant = column(&mut columns, "constant", DataType::Int64, false);
    let null = column(&mut columns, "null", DataType::Int64, true);
    let expressions = vec![
        reference(a),
        ConstantExpression::int64(Some(7), constant).into_ref(),
        ConstantExpression::int64(None, null).into_ref(),
    ];
    let output = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("constant", DataType::Int64, false),
        Field::new("null", DataType::Int64, true),
    ]));
    let mut executor = project_program(&columns, &[a], expressions).unwrap();
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
    let zero_columns = project_program(&columns, &[a], vec![])
        .unwrap()
        .execute(&batch)
        .unwrap();
    assert_eq!(
        (zero_columns.num_rows(), zero_columns.num_columns()),
        (3, 0)
    );
}

#[test]
fn preserves_arrow_kernel_errors() {
    let mut columns = ColumnContext::new();
    let a = column(&mut columns, "a", DataType::Int64, true);
    let batch = RecordBatch::try_new(
        columns.schema(&[a]).unwrap(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let division = binary(&mut columns, reference(a), ExprOp::Divide, 0);
    let error = project_program(&columns, &[a], vec![division])
        .unwrap()
        .execute(&batch)
        .unwrap_err();
    assert!(matches!(error, roc::error::Error::Arrow(_)));
}

#[test]
fn filter_rejects_non_boolean_results() {
    let mut columns = ColumnContext::new();
    let v = column(&mut columns, "v", DataType::Int64, false);
    for predicate in [reference(v), int(&mut columns, Some(1))] {
        assert!(matches!(
            filter_program(&columns, &[v], predicate),
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
        vec![],
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
async fn exchange_relays_batches_and_opens_sinks_once_even_for_empty_input() {
    use roc::operator::{ExchangeSinkOperator, ExchangeSourceOperator};
    let mut columns = ColumnContext::new();
    let v = column(&mut columns, "v", DataType::Int64, false);
    let schema = columns.schema(&[v]).unwrap();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    for (input, parallelism) in [(Some(batch.clone()), 1), (Some(batch), 3), (None, 3)] {
        let service = Arc::new(MemoryExchange {
            input: Ok(input.clone()),
            output: Collector::default(),
            schema: schema.clone(),
            created_sinks: std::sync::atomic::AtomicUsize::new(0),
        });
        let source = OperatorTreeNode::new(
            ExchangeSourceOperator::new(7, service.clone(), vec![v]),
            vec![],
        );
        let sink = OperatorTreeNode::new(
            ExchangeSinkOperator::new(vec![8], service.clone()),
            vec![source],
        );
        let graph = build_pipeline_graph(&OperatorTree::new(columns.clone(), sink)).unwrap();
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
            1
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
        roc::operator::ExchangeSourceOperator::new(7, service, vec![]),
        vec![],
    );
    let tree = OperatorTree::new(
        ColumnContext::new(),
        OperatorTreeNode::new(collector.clone(), vec![source]),
    );
    let error = PipelineGraphExecutor::new(build_pipeline_graph(&tree).unwrap())
        .with_task_executor(TestExecutor)
        .execute()
        .await
        .unwrap_err();
    assert!(matches!(error, roc::error::Error::Io(actual) if Arc::ptr_eq(&actual, &read_error)));
    assert!(collector.0.lock().unwrap().is_empty());
}
