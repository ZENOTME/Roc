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
    array::{Array, StringArray},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use roc::{
    error::{Error, ErrorKind, Result},
    exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult},
    expr::{
        ExpressionResultType,
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            CaseExpression, CastExpression, CastMode, CoalesceExpression, ConstantExpression,
            FunctionExpression, FunctionKind, ReferenceExpression, ScalarExprRef,
        },
    },
    operator::{
        AggregateOperator, Operator, OperatorTree, OperatorTreeNode, ProjectOperator, Projection,
        ProjectionExpression, ScanConsumer, ScanHandle, ScanOperator, ScanRequest, ScanStorage,
    },
    pipeline::{
        Executor, PipelineGraphBuilder, PipelineGraphExecutor, PipelineId, build_pipeline_graph,
        build_pipeline_on_node,
    },
};
use std::{
    error::Error as _,
    sync::{Arc, Mutex},
};

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
    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        id: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        graph.pipeline_mut(id)?.set_sink(Box::new(self.clone()))?;
        build_pipeline_on_node(&node.children()[0], id, graph)
    }
}
impl SinkExec for Collector {
    fn init_global_context(&self, _: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        Ok(Box::new(self.clone()))
    }
    fn finalize<'a>(
        &'a self,
        _: GlobalExecContextRef,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl SinkExecutor for Collector {
    fn sink<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        batch: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(batch.clone());
            Ok(SinkResult::NeedMoreInput)
        })
    }
    fn combine(self: Box<Self>, _: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
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

fn scan(batch: &RecordBatch) -> OperatorTreeNode {
    OperatorTreeNode::new(
        ScanOperator::new((), Arc::new(MemoryStorage(batch.clone()))),
        vec![],
    )
}
async fn execute(plan: OperatorTreeNode) -> Result<Vec<RecordBatch>> {
    let collector = Collector::default();
    let tree = OperatorTree::new(OperatorTreeNode::new(collector.clone(), vec![plan]));
    let graph = build_pipeline_graph(tree)?;
    let executor = PipelineGraphExecutor::new(graph).with_task_executor(TestExecutor);
    let shutdown = executor.shutdown();
    let result = executor.execute().await;
    if result.is_err() {
        // The host owns cancellation and must join any remaining work before
        // inspecting the sink. These fixtures fail before producing a batch.
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
async fn project(expression: ScalarExprRef, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
    let projection = Projection::new(vec![ProjectionExpression::new(expression, "value")]);
    execute(OperatorTreeNode::new(
        ProjectOperator::new(projection),
        vec![scan(batch)],
    ))
    .await
}
fn assert_projection_execution_path(error: &Error) {
    assert_frame(error, "projection.evaluate", Some(("column", "0")));
    assert_frame(error, "processor.execute", Some(("processor", "0")));
    assert_frame(error, "worker.execute", Some(("worker", "0")));
    assert_frame(error, "pipeline.execute", Some(("pipeline", "0")));
}

#[tokio::test]
async fn projection_binding_preserves_plan_classification() {
    let malformed = FunctionExpression::unary(
        FunctionKind::Add,
        ConstantExpression::int64(Some(1)).into_ref(),
        DataType::Int64,
        false,
    )
    .into_ref();
    let error = project(malformed, &text_batch()).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidPlan);
    assert_frame(&error, "projection.bind", Some(("column", "0")));
    assert_frame(
        &error,
        "processor.create_executor",
        Some(("processor", "0")),
    );
    assert_frame(&error, "pipeline.execute", Some(("pipeline", "0")));
}

#[tokio::test]
async fn binding_retains_location_only_frames_alongside_context() {
    let malformed = FunctionExpression::unary(
        FunctionKind::Add,
        ConstantExpression::int64(Some(1)).into_ref(),
        DataType::Int64,
        false,
    )
    .into_ref();
    let case = CaseExpression::new(vec![], malformed, DataType::Int64, false).into_ref();
    let cast = CastExpression::new(case, DataType::Int64, CastMode::Strict, false).into_ref();
    let error = project(cast, &text_batch()).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidPlan);
    let locations: Vec<_> = error
        .frames()
        .iter()
        .filter(|frame| frame.context.is_none())
        .map(|frame| frame.location)
        .collect();
    assert_eq!(locations.len(), 2, "{error:?}");
    assert!(locations[0].file().ends_with("src/expr/scalar/case.rs"));
    assert!(locations[1].file().ends_with("src/expr/scalar/cast.rs"));
    let report = format!("{error:?}");
    for location in locations {
        assert!(report.contains(&format!("  at {location}")), "{report}");
    }
    assert_frame(&error, "projection.bind", Some(("column", "0")));
    assert_frame(&error, "pipeline.execute", Some(("pipeline", "0")));
}

fn text_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["bad"]))],
    )
    .unwrap()
}
fn assert_location_frame(error: &Error, file: &str) {
    let frame = error
        .frames()
        .iter()
        .find(|frame| frame.context.is_none() && frame.location.file().ends_with(file))
        .unwrap_or_else(|| panic!("missing location in {file}: {error:?}"));
    assert!(format!("{error:?}").contains(&format!("  at {}", frame.location)));
}

fn assert_frame(error: &Error, operation: &str, field: Option<(&str, &str)>) {
    let frame = error
        .frames()
        .iter()
        .find(|frame| {
            frame
                .context
                .as_ref()
                .is_some_and(|context| context.operation == operation)
        })
        .unwrap_or_else(|| panic!("missing {operation}: {error:?}"));
    if let Some((name, value)) = field {
        assert!(
            frame
                .context
                .as_ref()
                .unwrap()
                .fields
                .iter()
                .any(|(key, actual)| *key == name && actual == value),
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn projection_reports_cast_input_failure_and_preserves_try_cast_nulls() {
    let batch = text_batch();
    for input in [
        ConstantExpression::string(Some("bad")).into_ref(),
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Utf8, false)).into_ref(),
    ] {
        let error = project(
            CastExpression::new(input.clone(), DataType::Int64, CastMode::Strict, false).into_ref(),
            &batch,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(matches!(
            error.source().unwrap().downcast_ref::<ArrowError>(),
            Some(ArrowError::CastError(_))
        ));
        assert_frame(&error, "cast.evaluate", Some(("from", "Utf8")));
        assert_frame(&error, "cast.evaluate", Some(("to", "Int64")));
        assert_projection_execution_path(&error);
        let output = project(
            CastExpression::new(input, DataType::Int64, CastMode::Try, true).into_ref(),
            &batch,
        )
        .await
        .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let batch = output.iter().find(|batch| batch.num_rows() > 0).unwrap();
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        assert!(batch.column(0).is_null(0));
    }
}

#[tokio::test]
async fn unsupported_cast_fails_binding_in_both_modes_even_without_rows() {
    for mode in [CastMode::Strict, CastMode::Try] {
        for batch in [text_batch(), text_batch().slice(0, 0)] {
            let error = project(
                CastExpression::new(
                    ConstantExpression::int64(Some(1)).into_ref(),
                    DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into()),
                    mode,
                    true,
                )
                .into_ref(),
                &batch,
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert!(error.source().is_none());
            assert_frame(&error, "cast.bind", Some(("from", "Int64")));
            assert_frame(&error, "projection.bind", Some(("column", "0")));
            assert_frame(
                &error,
                "processor.create_executor",
                Some(("processor", "0")),
            );
            assert_frame(&error, "pipeline.execute", Some(("pipeline", "0")));
            assert!(!error.frames().iter().any(|frame| {
                frame
                    .context
                    .as_ref()
                    .is_some_and(|context| context.operation == "cast.evaluate")
            }));
        }
    }
}

#[tokio::test]
async fn checked_arithmetic_preserves_failure_kinds_for_scalar_and_array_inputs() {
    use arrow::array::Int64Array;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
    )
    .unwrap();
    for input in [
        ConstantExpression::int64(Some(i64::MAX)).into_ref(),
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, false)).into_ref(),
    ] {
        for (function, rhs, kind) in [
            (FunctionKind::Add, 1, ErrorKind::ArithmeticOverflow),
            (FunctionKind::Divide, 0, ErrorKind::DivisionByZero),
        ] {
            let error = project(
                FunctionExpression::binary(
                    function,
                    input.clone(),
                    ConstantExpression::int64(Some(rhs)).into_ref(),
                    DataType::Int64,
                    false,
                )
                .into_ref(),
                &batch,
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), kind);
            let source = error
                .source()
                .unwrap()
                .downcast_ref::<ArrowError>()
                .unwrap();
            assert!(matches!(
                (kind, source),
                (
                    ErrorKind::ArithmeticOverflow,
                    ArrowError::ArithmeticOverflow(_)
                ) | (ErrorKind::DivisionByZero, ArrowError::DivideByZero)
            ));
            assert_projection_execution_path(&error);
        }
    }
}

fn divide_by_zero() -> ScalarExprRef {
    FunctionExpression::binary(
        FunctionKind::Divide,
        ConstantExpression::int64(Some(1)).into_ref(),
        ConstantExpression::int64(Some(0)).into_ref(),
        DataType::Int64,
        false,
    )
    .into_ref()
}
#[tokio::test]
async fn projection_retains_case_condition_result_and_else_paths() {
    let yes = ConstantExpression::boolean(Some(true)).into_ref();
    let no = ConstantExpression::boolean(Some(false)).into_ref();
    let one = ConstantExpression::int64(Some(1)).into_ref();
    let broken_condition = FunctionExpression::binary(
        FunctionKind::Equal,
        divide_by_zero(),
        one.clone(),
        DataType::Boolean,
        false,
    )
    .into_ref();
    for (branches, otherwise, operation, branch) in [
        (
            vec![(no.clone(), one.clone()), (broken_condition, one.clone())],
            one.clone(),
            "case.condition",
            Some(("branch", "1")),
        ),
        (
            vec![(no.clone(), one.clone()), (yes, divide_by_zero())],
            one.clone(),
            "case.result",
            Some(("branch", "1")),
        ),
        (vec![(no, one)], divide_by_zero(), "case.else", None),
    ] {
        let error = project(
            CaseExpression::new(branches, otherwise, DataType::Int64, false).into_ref(),
            &text_batch(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DivisionByZero);
        assert!(matches!(
            error.source().unwrap().downcast_ref::<ArrowError>(),
            Some(ArrowError::DivideByZero)
        ));
        if branch.is_some() {
            assert_frame(&error, operation, branch);
        } else {
            assert_location_frame(&error, "src/expr/scalar/case.rs");
        }
        assert_frame(&error, "function.evaluate", Some(("function", "Divide")));
        assert_projection_execution_path(&error);
    }
}

#[tokio::test]
async fn coalesce_reports_only_the_evaluated_argument() {
    let null = ConstantExpression::int64(None).into_ref();
    let error = project(
        CoalesceExpression::new(vec![null, divide_by_zero()], DataType::Int64, true).into_ref(),
        &text_batch(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DivisionByZero);
    assert_frame(&error, "coalesce.argument", Some(("argument", "1")));
    assert_projection_execution_path(&error);
    let one = ConstantExpression::int64(Some(1)).into_ref();
    let output = project(
        CoalesceExpression::new(vec![one, divide_by_zero()], DataType::Int64, true).into_ref(),
        &text_batch(),
    )
    .await
    .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = output.iter().find(|batch| batch.num_rows() > 0).unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(1)]);
}

#[tokio::test]
async fn aggregate_binding_distinguishes_unsupported_distinct_from_invalid_plan() {
    for (expression, expected) in [
        (
            AggregateExpression::new(
                AggregateFunction::Sum,
                Some(ConstantExpression::int64(Some(1)).into_ref()),
                DataType::Int64,
                true,
            )
            .with_distinct(),
            ErrorKind::Unsupported,
        ),
        (
            AggregateExpression::new(AggregateFunction::Sum, None, DataType::Int64, true),
            ErrorKind::InvalidPlan,
        ),
    ] {
        let aggregate =
            AggregateOperator::try_new(Projection::new(vec![]), vec![Arc::new(expression)])
                .unwrap();
        let plan = OperatorTreeNode::new(aggregate, vec![scan(&text_batch())]);
        let error = execute(plan).await.unwrap_err();
        assert_eq!(error.kind(), expected);
        assert_frame(&error, "aggregate.bind", Some(("aggregate", "0")));
        assert_location_frame(&error, "src/pipeline/execution.rs");
        assert_frame(&error, "pipeline.execute", Some(("pipeline", "0")));
    }
}
