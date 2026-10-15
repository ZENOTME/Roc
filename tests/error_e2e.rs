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

//! Error kinds, sources and diagnostic frames observed by a host executing
//! complete plans. Binding errors surface while the pipeline graph is built;
//! row errors surface from the pipeline that ran the failing program.

use arrow::{
    array::{ArrayRef, AsArray, Int64Array, StringArray},
    datatypes::{DataType, Field, Int64Type, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    error::{Error, ErrorKind, Result},
    exec::{ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, StateContext},
    expr::{
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            CaseExpression, CastExpression, CastMode, CoalesceExpression, ConstantExpression,
            FunctionExpression, FunctionKind, ReferenceExpression, ScalarExprRef,
        },
    },
    kernel::ExecutionMode,
    operator::{
        AggregateOperator, Operator, OperatorTree, OperatorTreeNode, ProjectOperator, ScanConsumer,
        ScanHandle, ScanOperator, ScanRequest, ScanStorage,
    },
    pipeline::{
        Executor, PipelineGraphBuilder, PipelineGraphExecutor, PipelineId,
        build_pipeline_graph_with_mode, build_pipeline_on_node,
    },
};
use std::{
    error::Error as _,
    sync::{Arc, Mutex},
};

const MODES: [ExecutionMode; 2] = [ExecutionMode::Compile, ExecutionMode::ZeroCompile];

// Only the host adapters are test doubles. Plans use Roc's scan, expression,
// aggregation and pipeline implementations and run through spawned Tokio tasks.
struct MemoryStorage(RecordBatch);
#[derive(Clone)]
struct MemoryScan(Arc<Mutex<Option<RecordBatch>>>);
impl ScanStorage for MemoryStorage {
    type StorageTaskDesc = ();
    fn start_scan(&self, _: ScanRequest<()>) -> Result<Arc<dyn ScanHandle>> {
        Ok(Arc::new(MemoryScan(Arc::new(Mutex::new(Some(
            self.0.clone(),
        ))))))
    }
}
impl ScanHandle for MemoryScan {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        Box::new(self.clone())
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
            Box::new(self.clone()),
            Some(child.operator().output_columns()),
        )
    }
}
impl LocalSink for Collector {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            self.0.lock().unwrap().push(chunk);
        }
        Ok(ConsumeStatus::End)
    }
    fn finish(
        &mut self,
        _: ConsumeStatus,
        _: &mut StateContext<'_>,
        _: &ShutdownGuard,
    ) -> Result<()> {
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
impl SinkExec for Collector {
    fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        Ok(Box::new(self.clone()))
    }
    fn consume<'a>(
        &'a mut self,
        _: ConsumeStatus,
        _: &'a mut dyn LocalSink,
        _: StateContext<'a>,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn finish<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        _cpu: &'a dyn roc::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
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

/// Columns of a plan reading one input batch.
struct Plan {
    columns: ColumnContext,
    batch: RecordBatch,
    inputs: Vec<ColumnId>,
}
impl Plan {
    fn new(batch: RecordBatch) -> Self {
        let mut columns = ColumnContext::new();
        let inputs = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                columns.allocate_named(
                    ColumnType::new(f.data_type().clone(), f.is_nullable()),
                    f.name(),
                )
            })
            .collect();
        Self {
            columns,
            batch,
            inputs,
        }
    }
    fn column(&mut self, data_type: DataType, nullable: bool) -> ColumnId {
        self.columns.allocate(ColumnType::new(data_type, nullable))
    }
    fn input(&self, i: usize) -> ScalarExprRef {
        ReferenceExpression::new(self.inputs[i]).into_ref()
    }
    fn int(&mut self, value: Option<i64>) -> ScalarExprRef {
        let output = self.column(DataType::Int64, value.is_none());
        ConstantExpression::int64(value, output).into_ref()
    }
    fn boolean(&mut self, value: bool) -> ScalarExprRef {
        let output = self.column(DataType::Boolean, false);
        ConstantExpression::boolean(Some(value), output).into_ref()
    }
    fn binary(
        &mut self,
        function: FunctionKind,
        left: ScalarExprRef,
        right: ScalarExprRef,
        data_type: DataType,
    ) -> ScalarExprRef {
        let output = self.column(data_type, true);
        FunctionExpression::binary(function, left, right, output).into_ref()
    }
    fn divide_by_zero(&mut self) -> ScalarExprRef {
        let (one, zero) = (self.int(Some(1)), self.int(Some(0)));
        self.binary(FunctionKind::Divide, one, zero, DataType::Int64)
    }
    fn scan(&self) -> OperatorTreeNode {
        OperatorTreeNode::new(
            ScanOperator::new(
                (),
                Arc::new(MemoryStorage(self.batch.clone())),
                self.inputs.clone(),
            ),
            vec![],
        )
    }
}

/// Builds and runs `plan` under a collector. A failed query must not have
/// published output.
async fn execute(
    plan: &Plan,
    root: OperatorTreeNode,
    mode: ExecutionMode,
) -> Result<Vec<RecordBatch>> {
    let collector = Collector::default();
    let tree = OperatorTree::new(
        plan.columns.clone(),
        OperatorTreeNode::new(collector.clone(), vec![root]),
    );
    let graph = build_pipeline_graph_with_mode(&tree, mode)?;
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(TestExecutor)
        .with_parallelism(2);
    let shutdown = executor.shutdown();
    let result = executor.execute().await;
    if result.is_err() {
        // The host owns cancellation and must join any remaining work before
        // inspecting the sink.
        shutdown.request_shutdown();
    }
    shutdown.await;
    let output = std::mem::take(&mut *collector.0.lock().unwrap());
    if result.is_err() {
        assert!(
            output.is_empty(),
            "failed query unexpectedly produced output"
        );
    }
    result?;
    Ok(output)
}
async fn project(
    plan: &Plan,
    expression: ScalarExprRef,
    mode: ExecutionMode,
) -> Result<Vec<RecordBatch>> {
    let root = OperatorTreeNode::new(ProjectOperator::new(vec![expression]), vec![plan.scan()]);
    execute(plan, root, mode).await
}
fn values(output: &[RecordBatch]) -> Vec<Option<i64>> {
    output
        .iter()
        .flat_map(|batch| batch.column(0).as_primitive::<Int64Type>().iter())
        .collect()
}

fn frame<'a>(error: &'a Error, operation: &str) -> &'a [(&'static str, String)] {
    error
        .frames()
        .iter()
        .filter_map(|frame| frame.context.as_ref())
        .find(|context| context.operation == operation)
        .unwrap_or_else(|| panic!("missing {operation}: {error:?}"))
        .fields
        .as_slice()
}
fn has_frame(error: &Error, operation: &str) -> bool {
    error
        .frames()
        .iter()
        .any(|f| f.context.as_ref().is_some_and(|c| c.operation == operation))
}
fn field(name: &'static str, value: impl ToString) -> (&'static str, String) {
    (name, value.to_string())
}
fn arrow_source(error: &Error) -> &ArrowError {
    error
        .source()
        .and_then(|source| source.downcast_ref::<ArrowError>())
        .unwrap_or_else(|| panic!("missing Arrow source: {error:?}"))
}
/// A row error raised by the program of pipeline 0, at `row` of its chunk.
fn assert_row_error(error: &Error, kind: ErrorKind, row: usize) {
    assert_eq!(error.kind(), kind, "{error:?}");
    assert_eq!(
        frame(error, "kernel.raise"),
        [field("row", row)],
        "{error:?}"
    );
    assert_eq!(
        frame(error, "pipeline.execute"),
        [field("pipeline", 0)],
        "{error:?}"
    );
}

fn text_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["bad"]))],
    )
    .unwrap()
}
fn int_batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
    )
    .unwrap()
}

#[tokio::test]
async fn malformed_functions_are_plan_errors_with_their_binding_path() {
    for mode in MODES {
        let mut plan = Plan::new(text_batch());
        let one = plan.int(Some(1));
        let output = plan.column(DataType::Int64, false);
        let malformed = FunctionExpression::unary(FunctionKind::Add, one, output).into_ref();
        let output = plan.column(DataType::Int64, false);
        let cast = CastExpression::new(malformed, CastMode::Strict, output).into_ref();
        let error = project(&plan, cast, mode).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidPlan, "{error:?}");
        assert_eq!(frame(&error, "function.bind"), [field("function", "Add")]);
        assert_eq!(frame(&error, "projection.bind"), [field("column", 0)]);
        // The cast propagates its input's failure with a location only.
        let cast = error
            .frames()
            .iter()
            .find(|f| f.context.is_none() && f.location.file().ends_with("src/expr/scalar/cast.rs"))
            .unwrap_or_else(|| panic!("missing cast location: {error:?}"));
        assert!(format!("{error:?}").contains(&format!("  at {}", cast.location)));
    }
}

#[tokio::test]
async fn unsupported_casts_fail_while_binding_even_without_rows() {
    for mode in MODES {
        for cast_mode in [CastMode::Strict, CastMode::Try] {
            for batch in [text_batch(), text_batch().slice(0, 0)] {
                let mut plan = Plan::new(batch);
                let one = plan.int(Some(1));
                let to = DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into());
                let output = plan.column(to.clone(), true);
                let cast = CastExpression::new(one, cast_mode, output).into_ref();
                let error = project(&plan, cast, mode).await.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Unsupported, "{error:?}");
                assert!(error.source().is_none());
                assert_eq!(
                    frame(&error, "cast.bind"),
                    [field("from", DataType::Int64), field("to", to)]
                );
                assert_eq!(frame(&error, "projection.bind"), [field("column", 0)]);
                assert!(!has_frame(&error, "pipeline.execute"));
            }
        }
    }
}

#[tokio::test]
async fn strict_casts_report_invalid_input_and_try_casts_produce_null() {
    for mode in MODES {
        for constant in [true, false] {
            let mut plan = Plan::new(text_batch());
            let input = if constant {
                let output = plan.column(DataType::Utf8, false);
                ConstantExpression::string(Some("bad"), output).into_ref()
            } else {
                plan.input(0)
            };
            let output = plan.column(DataType::Int64, false);
            let strict = CastExpression::new(input.clone(), CastMode::Strict, output).into_ref();
            let error = project(&plan, strict, mode).await.unwrap_err();
            assert_row_error(&error, ErrorKind::InvalidInput, 0);
            assert!(
                matches!(arrow_source(&error), ArrowError::CastError(message) if message.contains("'bad'")),
                "{error:?}"
            );

            let output = plan.column(DataType::Int64, true);
            let try_cast = CastExpression::new(input, CastMode::Try, output).into_ref();
            let output = project(&plan, try_cast, mode).await.unwrap();
            assert_eq!(values(&output), [None]);
        }
    }
}

#[tokio::test]
async fn checked_arithmetic_reports_the_failure_kind_and_the_first_failing_row() {
    for mode in MODES {
        for constant in [true, false] {
            // The input overflows from row 1; every row divides by zero.
            for (function, rhs, kind, row) in [
                (FunctionKind::Add, 1, ErrorKind::ArithmeticOverflow, 1),
                (FunctionKind::Divide, 0, ErrorKind::DivisionByZero, 0),
            ] {
                let mut plan = Plan::new(int_batch(vec![1, i64::MAX, i64::MAX]));
                let (left, row) = if constant {
                    (plan.int(Some(i64::MAX)), 0)
                } else {
                    (plan.input(0), row)
                };
                let rhs = plan.int(Some(rhs));
                let expression = plan.binary(function, left, rhs, DataType::Int64);
                let error = project(&plan, expression, mode).await.unwrap_err();
                assert_row_error(&error, kind, row);
                assert!(
                    matches!(
                        (kind, arrow_source(&error)),
                        (
                            ErrorKind::ArithmeticOverflow,
                            ArrowError::ArithmeticOverflow(_)
                        ) | (ErrorKind::DivisionByZero, ArrowError::DivideByZero)
                    ),
                    "{error:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn case_raises_only_the_errors_of_branches_it_evaluates() {
    for mode in MODES {
        for broken in ["condition", "result", "else"] {
            let mut plan = Plan::new(text_batch());
            let (yes, no) = (plan.boolean(true), plan.boolean(false));
            let one = plan.int(Some(1));
            let failing = plan.divide_by_zero();
            let condition = if broken == "condition" {
                let one = plan.int(Some(1));
                plan.binary(FunctionKind::Equal, failing.clone(), one, DataType::Boolean)
            } else {
                yes.clone()
            };
            let (branches, otherwise) = match broken {
                "else" => (vec![(no.clone(), one.clone())], failing.clone()),
                "result" => (vec![(no.clone(), one.clone()), (yes, failing.clone())], one),
                _ => (
                    vec![(no.clone(), one.clone()), (condition, one.clone())],
                    one,
                ),
            };
            let output = plan.column(DataType::Int64, true);
            let case = CaseExpression::new(branches, otherwise, output).into_ref();
            let error = project(&plan, case, mode).await.unwrap_err();
            assert_row_error(&error, ErrorKind::DivisionByZero, 0);
            assert!(matches!(arrow_source(&error), ArrowError::DivideByZero));
        }
        // A branch whose condition is FALSE is never evaluated.
        let mut plan = Plan::new(text_batch());
        let no = plan.boolean(false);
        let two = plan.int(Some(2));
        let failing = plan.divide_by_zero();
        let output = plan.column(DataType::Int64, true);
        let case = CaseExpression::new(vec![(no, failing)], two, output).into_ref();
        let output = project(&plan, case, mode).await.unwrap();
        assert_eq!(values(&output), [Some(2)]);
    }
}

#[tokio::test]
async fn coalesce_raises_only_the_errors_of_arguments_it_evaluates() {
    for mode in MODES {
        let mut plan = Plan::new(text_batch());
        let null = plan.int(None);
        let failing = plan.divide_by_zero();
        let output = plan.column(DataType::Int64, true);
        let coalesce = CoalesceExpression::new(vec![null, failing], output).into_ref();
        let error = project(&plan, coalesce, mode).await.unwrap_err();
        assert_row_error(&error, ErrorKind::DivisionByZero, 0);

        let mut plan = Plan::new(text_batch());
        let one = plan.int(Some(1));
        let failing = plan.divide_by_zero();
        let output = plan.column(DataType::Int64, true);
        let coalesce = CoalesceExpression::new(vec![one, failing], output).into_ref();
        let output = project(&plan, coalesce, mode).await.unwrap();
        assert_eq!(values(&output), [Some(1)]);
    }
}

#[tokio::test]
async fn aggregate_binding_distinguishes_unsupported_distinct_from_invalid_plans() {
    for mode in MODES {
        for (distinct, arguments, expected) in [
            (true, 1, ErrorKind::Unsupported),
            (false, 0, ErrorKind::InvalidPlan),
        ] {
            let mut plan = Plan::new(text_batch());
            let arguments = (0..arguments).map(|_| plan.int(Some(1))).collect();
            let output = plan.column(DataType::Int64, true);
            let mut sum = AggregateExpression::new(AggregateFunction::Sum, arguments, output);
            if distinct {
                sum = sum.with_distinct();
            }
            let aggregate = AggregateOperator::try_new(vec![], vec![Arc::new(sum)]).unwrap();
            let root = OperatorTreeNode::new(aggregate, vec![plan.scan()]);
            let error = execute(&plan, root, mode).await.unwrap_err();
            assert_eq!(error.kind(), expected, "{error:?}");
            assert_eq!(frame(&error, "aggregate.bind"), [field("aggregate", 0)]);
        }
    }
}
