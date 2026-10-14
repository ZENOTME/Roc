//! Identical resident-input workloads for main and generated execution.
//! The only engine-specific code is in adapter below.
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use roc::{
    error::Result,
    operator::{OperatorTreeNode, ScanConsumer, ScanHandle, ScanOperator, ScanRequest, ScanStorage},
    pipeline::{Executor, PipelineExecutionConfig, PipelineGraphExecutor},
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

// Every engine variant uses the allocator of the DataFusion and DuckDB
// reference builds' class, so allocation policy does not decide comparisons.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BATCH: usize = 8192;
const CASES: [&str; 6] = [
    "scan",
    "filter_project",
    "arithmetic_chain",
    "integer_division",
    "global_aggregate",
    "grouped_aggregate",
];
struct Storage(Arc<Vec<RecordBatch>>);
struct Handle {
    batches: Arc<Vec<RecordBatch>>,
    cursor: Arc<AtomicUsize>,
}
struct Consumer {
    batches: Arc<Vec<RecordBatch>>,
    cursor: Arc<AtomicUsize>,
}
impl ScanStorage for Storage {
    type StorageTaskDesc = ();
    fn start_scan(&self, _: ScanRequest<()>) -> Result<Arc<dyn ScanHandle>> {
        Ok(Arc::new(Handle {
            batches: self.0.clone(),
            cursor: Arc::new(AtomicUsize::new(0)),
        }))
    }
}
impl ScanHandle for Handle {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        Box::new(Consumer {
            batches: self.batches.clone(),
            cursor: self.cursor.clone(),
        })
    }
    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl ScanConsumer for Consumer {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        let batch = self
            .batches
            .get(self.cursor.fetch_add(1, Ordering::Relaxed))
            .cloned();
        Box::pin(async { Ok(batch) })
    }
}
struct Tasks(tokio::runtime::Handle);
impl Executor for Tasks {
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
        self.0.spawn(task)
    }
}
type Output = Arc<Mutex<Vec<RecordBatch>>>;
#[cfg(feature = "main-engine")]
mod adapter {
    use super::*;
    use roc::exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult};
    use roc::{
        expr::{
            ExpressionResultType,
            agg::{AggregateExpression, AggregateFunction},
            scalar::{
                ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
                ScalarExprRef,
            },
        },
        operator::{AggregateOperator, FilterOperator, ProjectOperator, Projection, ProjectionExpression},
        pipeline::{PipelineGraphBuilder, build_pipeline_on_node},
    };
    fn aggregate_input(expr: ScalarExprRef) -> Option<ScalarExprRef> {
        Some(expr)
    }
    struct Sink(Output);
    struct Local {
        output: Output,
        batches: Vec<RecordBatch>,
    }
    impl SinkExec for Sink {
        fn init_global_context(&self, _: &ShutdownGuard) -> Result<GlobalExecContextRef> {
            Ok(Arc::new(()))
        }
        fn new_executor(&self, _: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
            Ok(Box::new(Local {
                output: self.0.clone(),
                batches: vec![],
            }))
        }
        fn finalize<'a>(
            &'a self,
            _: GlobalExecContextRef,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl SinkExecutor for Local {
        fn sink<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            input: &'a RecordBatch,
        ) -> BoxFuture<'a, Result<SinkResult>> {
            Box::pin(async move {
                self.batches.push(input.clone());
                Ok(SinkResult::NeedMoreInput)
            })
        }
        fn combine(self: Box<Self>, _: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.output.lock().unwrap().extend(self.batches);
                Ok(())
            })
        }
    }
    pub const MODES: [&str; 1] = ["main"];
    pub fn prewarm(_: &str) -> Option<f64> {
        None
    }
    pub fn build(
        case: &str,
        _mode: &str,
        batches: Arc<Vec<RecordBatch>>,
        output: Output,
    ) -> Result<(roc::pipeline::PipelineGraph, SchemaRef)> {
        let (tree, schema) = plan(case, batches);
        let mut graph = PipelineGraphBuilder::new();
        build_pipeline_on_node(&tree, 0, &mut graph)?;
        graph.pipeline_mut(0)?.set_sink(Box::new(Sink(output)))?;
        Ok((graph.finish()?, schema))
    }
    fn col(i: usize) -> ScalarExprRef {
        let s = schema();
        let f = s.field(i);
        ReferenceExpression::new(
            i,
            ExpressionResultType::new(f.data_type().clone(), f.is_nullable()),
        )
        .into_ref()
    }
    fn binary(
        kind: FunctionKind,
        a: ScalarExprRef,
        b: i64,
        typ: DataType,
        nullable: bool,
    ) -> ScalarExprRef {
        FunctionExpression::binary(
            kind,
            a,
            ConstantExpression::int64(Some(b)).into_ref(),
            typ,
            nullable,
        )
        .into_ref()
    }
    fn plan(case: &str, batches: Arc<Vec<RecordBatch>>) -> (OperatorTreeNode, SchemaRef) {
        let mut source =
            OperatorTreeNode::new(ScanOperator::new((), Arc::new(Storage(batches))), vec![]);
        if case == "scan" {
            return (source, schema());
        }
        if case.ends_with("aggregate") {
            let groups = if case == "grouped_aggregate" {
                vec![ProjectionExpression::new(col(1), "group_key")]
            } else {
                vec![]
            };
            let aggs = vec![
                Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Sum,
                        aggregate_input(col(2)),
                        DataType::Int64,
                        true,
                    )
                    .with_alias("total"),
                ),
                Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Count,
                        aggregate_input(col(2)),
                        DataType::Int64,
                        false,
                    )
                    .with_alias("count"),
                ),
            ];
            let op = AggregateOperator::try_new(Projection::new(groups), aggs).unwrap();
            let output = op.output_schema();
            return (OperatorTreeNode::new(op, vec![source]), output);
        }
        let mut expr = col(2);
        if case == "filter_project" {
            source = OperatorTreeNode::new(
                FilterOperator::new(binary(
                    FunctionKind::GreaterThan,
                    col(3),
                    50,
                    DataType::Boolean,
                    false,
                )),
                vec![source],
            );
            expr = binary(FunctionKind::Add, expr, 1, DataType::Int64, true);
        } else {
            for i in 1..=16 {
                expr = binary(FunctionKind::Add, expr, i, DataType::Int64, true);
                if case == "integer_division" && i == 8 {
                    expr = binary(FunctionKind::Divide, expr, 3, DataType::Int64, true);
                }
            }
        }
        let projection = Projection::new(vec![
            ProjectionExpression::new(col(0), "id"),
            ProjectionExpression::new(expr, "result"),
        ]);
        let output = projection.output_schema();
        (
            OperatorTreeNode::new(ProjectOperator::new(projection), vec![source]),
            output,
        )
    }
}

#[cfg(not(feature = "main-engine"))]
mod adapter {
    use super::*;
    use roc::{
        column::{ColumnContext, ColumnId, ColumnType},
        exec::{SinkExec, SinkStatus},
        expr::{
            agg::{AggregateExpression, AggregateFunction},
            scalar::{ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression, ScalarExprRef},
        },
        kernel::ExecutionMode,
        operator::{AggregateOperator, FilterOperator, Operator, OperatorTree, ProjectOperator},
        pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_graph_with_mode, build_pipeline_on_node},
    };
    pub const MODES: [&str; 2] = ["compile", "zero-compile"];
    fn mode(name: &str) -> ExecutionMode {
        match name {
            "compile" => ExecutionMode::Compile,
            "zero-compile" => ExecutionMode::ZeroCompile,
            _ => panic!("unknown mode {name}"),
        }
    }
    /// Zero-compile kernels are built into the binary: nothing to prewarm.
    pub fn prewarm(_: &str) -> Option<f64> {
        None
    }
    /// Root operator passing its child's output chunks to `Sink`.
    #[derive(Debug)]
    struct Collect(Output);
    struct Sink(Output);
    impl Operator for Collect {
        fn name(&self) -> &'static str {
            "collect"
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
                Box::new(Sink(self.0.clone())),
                Some(child.operator().output_columns()),
            )
        }
    }
    impl SinkExec for Sink {
        fn consume<'a>(
            &'a mut self,
            chunk: RecordBatch,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<SinkStatus>> {
            Box::pin(async move {
                self.0.lock().unwrap().push(chunk);
                Ok(SinkStatus::Continue)
            })
        }
    }
    struct Plan {
        columns: ColumnContext,
        inputs: Vec<ColumnId>,
    }
    impl Plan {
        fn column(&mut self, name: &str, data_type: DataType, nullable: bool) -> ColumnId {
            self.columns.allocate_named(ColumnType::new(data_type, nullable), name)
        }
        fn col(&self, i: usize) -> ScalarExprRef {
            ReferenceExpression::new(self.inputs[i]).into_ref()
        }
        fn binary(&mut self, kind: FunctionKind, a: ScalarExprRef, b: i64, typ: DataType, nullable: bool, name: Option<&str>) -> ScalarExprRef {
            let constant = self.columns.allocate(ColumnType::new(DataType::Int64, false));
            let typ = ColumnType::new(typ, nullable);
            let output = match name {
                Some(name) => self.columns.allocate_named(typ, name),
                None => self.columns.allocate(typ),
            };
            FunctionExpression::binary(kind, a, ConstantExpression::int64(Some(b), constant).into_ref(), output).into_ref()
        }
    }
    fn plan(case: &str, batches: Arc<Vec<RecordBatch>>) -> (Plan, OperatorTreeNode, Vec<ColumnId>) {
        let mut p = Plan { columns: ColumnContext::new(), inputs: vec![] };
        let schema = schema();
        p.inputs = schema
            .fields()
            .iter()
            .map(|f| p.column(f.name(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let inputs = p.inputs.clone();
        let mut source = OperatorTreeNode::new(
            ScanOperator::new((), Arc::new(Storage(batches)), inputs.clone()),
            vec![],
        );
        if case == "scan" {
            return (p, source, inputs);
        }
        if case.ends_with("aggregate") {
            let groups = if case == "grouped_aggregate" { vec![p.col(1)] } else { vec![] };
            let total = p.column("total", DataType::Int64, true);
            let count = p.column("count", DataType::Int64, false);
            let aggs = vec![
                Arc::new(AggregateExpression::new(AggregateFunction::Sum, vec![p.col(2)], total)),
                Arc::new(AggregateExpression::new(AggregateFunction::Count, vec![p.col(2)], count)),
            ];
            let op = AggregateOperator::try_new(groups, aggs).unwrap();
            let outputs = op.output_columns();
            return (p, OperatorTreeNode::new(op, vec![source]), outputs);
        }
        let mut expr = p.col(2);
        if case == "filter_project" {
            let col = p.col(3);
            let predicate = p.binary(FunctionKind::GreaterThan, col, 50, DataType::Boolean, false, None);
            source = OperatorTreeNode::new(FilterOperator::new(predicate, inputs.clone()), vec![source]);
            expr = p.binary(FunctionKind::Add, expr, 1, DataType::Int64, true, Some("result"));
        } else {
            for i in 1..=16 {
                let name = (i == 16).then_some("result");
                expr = p.binary(FunctionKind::Add, expr, i, DataType::Int64, true, name);
                if case == "integer_division" && i == 8 {
                    expr = p.binary(FunctionKind::Divide, expr, 3, DataType::Int64, true, None);
                }
            }
        }
        let project = ProjectOperator::new(vec![p.col(0), expr]);
        let outputs = project.output_columns();
        (p, OperatorTreeNode::new(project, vec![source]), outputs)
    }
    pub fn build(
        case: &str,
        name: &str,
        batches: Arc<Vec<RecordBatch>>,
        output: Output,
    ) -> Result<(roc::pipeline::PipelineGraph, SchemaRef)> {
        let (p, tree, outputs) = plan(case, batches);
        let schema = p.columns.schema(&outputs)?;
        let tree = OperatorTree::new(p.columns, OperatorTreeNode::new(Collect(output), vec![tree]));
        Ok((build_pipeline_graph_with_mode(&tree, mode(name))?, schema))
    }
}
fn value(id: usize) -> Option<i64> {
    (id % 17 != 0).then_some((id % 1000) as i64 - 500)
}
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
        Field::new("selector", DataType::Int64, false),
    ]))
}
fn input(rows: usize) -> Arc<Vec<RecordBatch>> {
    let schema = schema();
    Arc::new(
        (0..rows)
            .step_by(BATCH)
            .map(|start| {
                let end = rows.min(start + BATCH);
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values((start..end).map(|i| i as i64)))
                            as ArrayRef,
                        Arc::new(Int64Array::from_iter_values(
                            (start..end).map(|i| (i % 256) as i64),
                        )),
                        Arc::new(Int64Array::from_iter((start..end).map(value))),
                        Arc::new(Int64Array::from_iter_values(
                            (start..end).map(|i| (i % 100) as i64),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect(),
    )
}
async fn run(
    case: &str,
    mode: &str,
    batches: Arc<Vec<RecordBatch>>,
    threads: usize,
) -> (f64, f64, Vec<RecordBatch>, SchemaRef) {
    let start = Instant::now();
    let output: Output = Arc::new(Mutex::new(vec![]));
    let (graph, schema) = adapter::build(case, mode, batches, output.clone()).unwrap();
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(Tasks(tokio::runtime::Handle::current()))
        .with_parallelism(threads)
        .with_config(PipelineExecutionConfig {
            batch_rows: BATCH,
            yield_batches: 16,
        });
    let shutdown = executor.shutdown();
    let prepare_ms = start.elapsed().as_secs_f64() * 1000.;
    let start = Instant::now();
    executor.execute().await.unwrap();
    shutdown.await;
    let batches = std::mem::take(&mut *output.lock().unwrap());
    (
        prepare_ms,
        start.elapsed().as_secs_f64() * 1000.,
        batches,
        schema,
    )
}
fn validate(case: &str, rows: usize, batches: &[RecordBatch], schema: &SchemaRef) -> usize {
    let mut seen = vec![
        false;
        if case == "global_aggregate" {
            1
        } else if case == "grouped_aggregate" {
            256
        } else {
            rows
        }
    ];
    let mut sums = vec![0i64; 256];
    let mut counts = vec![0i64; 256];
    if case.ends_with("aggregate") {
        for id in 0..rows {
            if let Some(v) = value(id) {
                let group = if case == "global_aggregate" {
                    0
                } else {
                    id % 256
                };
                sums[group] += v;
                counts[group] += 1;
            }
        }
    }
    let mut total = 0;
    for batch in batches {
        assert_eq!(&batch.schema(), schema);
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect();
        for r in 0..batch.num_rows() {
            let cell = |i: usize| (!columns[i].is_null(r)).then(|| columns[i].value(r));
            let id = if case == "global_aggregate" {
                0
            } else {
                cell(0).unwrap() as usize
            };
            assert!(!seen[id], "duplicate output row");
            seen[id] = true;
            if case.ends_with("aggregate") {
                let offset = usize::from(case == "grouped_aggregate");
                assert_eq!(cell(offset), (counts[id] != 0).then_some(sums[id]));
                assert_eq!(cell(offset + 1), Some(counts[id]));
            } else if case == "scan" {
                assert_eq!(cell(1), Some((id % 256) as i64));
                assert_eq!(cell(2), value(id));
                assert_eq!(cell(3), Some((id % 100) as i64));
            } else {
                if case == "filter_project" {
                    assert!(id % 100 > 50);
                }
                let expected = value(id).map(|v| match case {
                    "filter_project" => v + 1,
                    "arithmetic_chain" => v + 136,
                    _ => (v + 36) / 3 + 100,
                });
                assert_eq!(cell(1), expected);
            }
            total += 1;
        }
    }
    for (id, present) in seen.iter().enumerate() {
        assert_eq!(
            *present,
            case != "filter_project" || id % 100 > 50,
            "missing output row {id}"
        );
    }
    total
}
fn median(x: &[f64]) -> f64 {
    let mut v = x.to_vec();
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 0 {
        (v[n / 2 - 1] + v[n / 2]) / 2.
    } else {
        v[n / 2]
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let rows: usize = args[1].parse().unwrap();
    let samples: usize = args[2].parse().unwrap();
    let threads: usize = args[3].parse().unwrap();
    let mode = args.get(4).map_or(adapter::MODES[0], String::as_str);
    assert!(adapter::MODES.contains(&mode), "mode must be one of {:?}", adapter::MODES);
    let prewarm_ms = adapter::prewarm(mode);
    let batches = input(rows);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();
    let mut results = vec![];
    runtime.block_on(async {
        // ROC_BENCH_CASES limits a run to some cases, e.g. for profiling.
        let only = std::env::var("ROC_BENCH_CASES").ok();
        for case in CASES
            .into_iter()
            .filter(|c| only.as_ref().is_none_or(|o| o.split(',').any(|x| x == *c)))
        {
            let (_, _, output, schema) = run(case, mode, batches.clone(), threads).await;
            let expected_rows = validate(case, rows, &output, &schema); drop(output);
            for _ in 0..2 { let (_, _, out, _) = run(case, mode, batches.clone(), threads).await; std::hint::black_box(out); }
            let mut prep = vec![]; let mut execution = vec![];
            for _ in 0..samples {
                let (p, e, out, _) = run(case, mode, batches.clone(), threads).await;
                assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), expected_rows);
                std::hint::black_box(&out); prep.push(p); execution.push(e);
            }
            eprintln!("{case} threads={threads} prepare={:.3}ms execute={:.3}ms", median(&prep), median(&execution));
            results.push(json!({"case":case,"prepare_ms":prep,"execute_ms":execution,"prepare_median_ms":median(&prep),"execute_median_ms":median(&execution),"output_rows":expected_rows}));
        }
    });
    println!(
        "{}",
        json!({"engine":if cfg!(feature="main-engine") {"main"} else {"current"},"mode":mode,"prewarm_ms":prewarm_ms,"rows":rows,"batch_rows":BATCH,"threads":threads,"samples":samples,"warmups":2,"correctness":"every output row checked against independent Rust calculation, including nulls and duplicate/missing IDs; timed runs row count checked","cases":results})
    );
}
