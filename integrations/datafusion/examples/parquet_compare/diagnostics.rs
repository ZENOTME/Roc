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

//! Separate diagnostics; the original benchmark and its saved results are unchanged.

use super::*;
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    datasource::MemTable,
    error::Result as DfResult,
    physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
        metrics::MetricValue,
    },
};
use futures::Stream;
use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    task::{Context, Poll},
};

const WARMUPS: usize = 2;
const SAMPLES: usize = 10;
pub(super) const WORKLOADS: [Workload; 4] = [
    Workload::Scan,
    Workload::FilterProject,
    Workload::GlobalAggregate,
    Workload::GroupedAggregate,
];

#[derive(Debug, Default, Clone)]
pub(super) struct BatchShape {
    rows: usize,
    batches: usize,
    min_rows: Option<usize>,
    max_rows: usize,
    histogram: BTreeMap<usize, usize>,
}

impl BatchShape {
    fn add(&mut self, batch: &RecordBatch) {
        let rows = batch.num_rows();
        self.rows += rows;
        self.batches += 1;
        self.min_rows = Some(self.min_rows.map_or(rows, |minimum| minimum.min(rows)));
        self.max_rows = self.max_rows.max(rows);
        *self.histogram.entry(rows).or_default() += 1;
    }

    pub(super) fn from_batches(batches: &[RecordBatch]) -> Self {
        let mut shape = Self::default();
        for batch in batches {
            shape.add(batch);
        }
        shape
    }

    pub(super) fn json(&self) -> Value {
        json!({
            "rows": self.rows, "batches": self.batches,
            "min_rows": self.min_rows, "max_rows": self.max_rows,
            "histogram_rows_to_batches": self.histogram,
        })
    }
}

/// Used only in the untimed scan audit, symmetrically for both engines.
/// The observer records the scan's actual output after DataFusion batch splitting.
#[derive(Debug)]
struct ObservedScan {
    input: Arc<dyn ExecutionPlan>,
    shape: Arc<Mutex<BatchShape>>,
}

impl DisplayAs for ObservedScan {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiagnosticObservedScan")
    }
}

impl ExecutionPlan for ObservedScan {
    fn name(&self) -> &'static str {
        "DiagnosticObservedScan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(
                "diagnostic scan observer requires one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            shape: self.shape.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        Ok(Box::pin(ObservedStream {
            input: self.input.execute(partition, context)?,
            shape: self.shape.clone(),
        }))
    }
}

struct ObservedStream {
    input: SendableRecordBatchStream,
    shape: Arc<Mutex<BatchShape>>,
}

impl Stream for ObservedStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.input.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(batch))) = &result {
            self.shape.lock().unwrap().add(batch);
        }
        result
    }
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Materialize ordinary numbers now: DataFusion metric handles are shared, live
/// counters. In particular resetting DataSourceExec does not reset its metrics.
fn scan_counters(scan: &Arc<dyn ExecutionPlan>) -> BTreeMap<String, usize> {
    let mut result = BTreeMap::new();
    if let Some(metrics) = scan.metrics() {
        for metric in metrics.iter() {
            let value = metric.value();
            match value {
                MetricValue::PruningMetrics {
                    name,
                    pruning_metrics,
                } => {
                    for (suffix, number) in [
                        ("pruned", pruning_metrics.pruned()),
                        ("matched", pruning_metrics.matched()),
                        ("fully_matched", pruning_metrics.fully_matched()),
                    ] {
                        *result.entry(format!("{name}.{suffix}")).or_default() += number;
                    }
                }
                MetricValue::OutputRows(_)
                | MetricValue::OutputBatches(_)
                | MetricValue::OutputBytes(_)
                | MetricValue::Count { .. }
                | MetricValue::Gauge { .. } => {
                    *result.entry(value.name().to_string()).or_default() += value.as_usize();
                }
                _ => {}
            }
        }
    }
    result
}

pub(super) fn session(threads: usize) -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(threads)
            .with_batch_size(BATCH_ROWS),
    )
}

pub(super) fn session_flags(ctx: &SessionContext) -> Value {
    let state = ctx.state();
    let options = state.config_options();
    json!({
        "parquet_pushdown_filters": options.execution.parquet.pushdown_filters,
        "parquet_pruning": options.execution.parquet.pruning,
        "parquet_enable_page_index": options.execution.parquet.enable_page_index,
        "parquet_reorder_filters": options.execution.parquet.reorder_filters,
        "enable_dynamic_filter_pushdown": options.optimizer.enable_dynamic_filter_pushdown,
        "enable_aggregate_dynamic_filter_pushdown": options.optimizer.enable_aggregate_dynamic_filter_pushdown,
        "batch_size": state.config().batch_size(),
        "target_partitions": state.config().target_partitions(),
    })
}

pub(super) async fn parquet_scan(
    ctx: &SessionContext,
    data_dir: &Path,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(ctx
        .read_parquet(
            data_dir.to_str().ok_or("non-UTF8 data path")?,
            ParquetReadOptions::default(),
        )
        .await?
        .create_physical_plan()
        .await?)
}

fn assert_scan_counters_equal(
    df: &BTreeMap<String, usize>,
    roc: &BTreeMap<String, usize>,
) -> Result<Vec<String>> {
    let mut keys = BTreeSet::from([
        "output_rows".to_owned(),
        "output_batches".to_owned(),
        "bytes_scanned".to_owned(),
    ]);
    keys.extend(
        df.keys()
            .chain(roc.keys())
            .filter(|key| {
                key.contains("row_group")
                    || key.contains("page_index")
                    || key.contains("pushdown_rows")
                    || key.contains("files_ranges")
            })
            .cloned(),
    );
    for key in &keys {
        let left = df.get(key).copied();
        let right = roc.get(key).copied();
        if left != right {
            return Err(format!(
                "scan work mismatch for {key}: DataFusion={left:?}, Roc={right:?}"
            )
            .into());
        }
        if matches!(
            key.as_str(),
            "output_rows" | "output_batches" | "bytes_scanned"
        ) && left.is_none()
        {
            return Err(format!("required scan metric is absent: {key}").into());
        }
    }
    Ok(keys.into_iter().collect())
}

async fn audit_scan_work(data_dir: &Path, threads: usize) -> Result<Vec<Value>> {
    let mut results = Vec::new();
    for workload in WORKLOADS {
        let mut engine_reports = Vec::new();
        let mut expected = None;
        let mut expected_schema = None;
        let mut snapshots = Vec::new();
        let mut input_shapes = Vec::new();
        for engine in ["datafusion", "roc"] {
            // Each engine gets an independently planned ParquetSource and a
            // fresh metrics registry, rather than reusing a cumulative blueprint.
            let ctx = session(threads);
            let scan = parquet_scan(&ctx, data_dir).await?;
            if scan_counters(&scan)
                .get("output_rows")
                .copied()
                .unwrap_or(0)
                != 0
            {
                return Err("freshly planned source has execution metrics".into());
            }
            let shape = Arc::new(Mutex::new(BatchShape::default()));
            let observed: Arc<dyn ExecutionPlan> = Arc::new(ObservedScan {
                input: scan.clone(),
                shape: shape.clone(),
            });
            let (schema, output) = if engine == "datafusion" {
                let schema = df_plan(workload, observed.clone())?.schema();
                let (_, output) = run_df(workload, &observed, ctx.task_ctx()).await?;
                (schema, output)
            } else {
                let (tree, schema) = roc_plan(workload, observed, ctx.task_ctx())?;
                let (_, output) = run_roc(&tree, threads).await?;
                (schema, output)
            };
            let rows = sorted_rows(&schema, &output)?;
            if let Some(expected) = &expected {
                if expected != &rows || expected_schema.as_ref() != Some(&schema) {
                    return Err(format!("scan audit result mismatch: {}", workload.name()).into());
                }
            } else {
                expected = Some(rows);
                expected_schema = Some(schema.clone());
            }
            let input_shape = shape.lock().unwrap().clone();
            let counters = scan_counters(&scan);
            if counters.get("output_rows").copied() != Some(input_shape.rows) {
                return Err("scan metric rows differ from observed scan output".into());
            }
            engine_reports.push(json!({
                "engine": engine,
                "scan_plan": format!("{}", displayable(scan.as_ref()).indent(true)),
                "scan_partitions": scan.output_partitioning().partition_count(),
                "session_flags": session_flags(&ctx),
                "input_batch_shape": input_shape.json(),
                "output_batch_shape": BatchShape::from_batches(&output).json(),
                "scan_counters": counters,
                "output_schema": format!("{schema:?}"),
            }));
            snapshots.push(counters);
            input_shapes.push(input_shape);
        }
        let checked = assert_scan_counters_equal(&snapshots[0], &snapshots[1])?;
        if engine_reports[0]["scan_partitions"] != engine_reports[1]["scan_partitions"] {
            return Err("scan partition counts differ between engines".into());
        }
        if input_shapes[0].json() != input_shapes[1].json() {
            return Err(format!("scan output batch shape mismatch: {}", workload.name()).into());
        }
        println!(
            "scan audit {} threads={threads}: equal rows, batches, bytes and pruning",
            workload.name()
        );
        results.push(json!({
            "workload": workload.name(), "threads": threads,
            "engines": engine_reports, "asserted_equal_scan_metrics": checked,
            "correctness": "exact full schema and sorted row multiset equal",
            "actual_input_batch_shapes_equal": true,
        }));
    }
    Ok(results)
}

async fn default_sql_plans(data_dir: &Path, threads: usize) -> Result<Vec<Value>> {
    let ctx = session(threads);
    ctx.register_parquet(
        "t",
        data_dir.to_str().ok_or("non-UTF8 data path")?,
        ParquetReadOptions::default(),
    )
    .await?;
    let mut plans = Vec::new();
    for workload in &WORKLOADS[1..] {
        let plan = ctx
            .sql(workload.sql())
            .await?
            .create_physical_plan()
            .await?;
        plans.push(json!({
            "threads": threads, "sql": workload.sql(),
            "physical_plan": format!("{}", displayable(plan.as_ref()).indent(true)),
            "session_flags": session_flags(&ctx),
            "timed": false,
        }));
    }
    Ok(plans)
}

#[derive(Clone, Copy)]
pub(super) enum Variant {
    DfDefault,
    DfChecked,
    DfCheckedUncoalesced,
    RocDefault,
    RocNoYield,
}

impl Variant {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::DfDefault => "datafusion_default",
            Self::DfChecked => "datafusion_checked_add",
            Self::DfCheckedUncoalesced => "datafusion_checked_add_filter_batch1",
            Self::RocDefault => "roc_yield16",
            Self::RocNoYield => "roc_yield_max",
        }
    }
}

pub(super) fn diagnostic_df_plan(
    variant: Variant,
    workload: Workload,
    scan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !matches!(workload, Workload::FilterProject) || matches!(variant, Variant::DfDefault) {
        return df_plan(workload, scan);
    }
    let predicate = Arc::new(BinaryExpr::new(
        df_column("selector", 3),
        DfOperator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(50)))),
    ));
    let mut filter = DfFilterExec::try_new(predicate, scan)?;
    if matches!(variant, Variant::DfCheckedUncoalesced) {
        filter = filter.with_batch_size(1)?;
    }
    let adjusted: Arc<dyn PhysicalExpr> = Arc::new(
        BinaryExpr::new(
            df_column("value", 2),
            DfOperator::Plus,
            Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
        )
        .with_fail_on_overflow(true),
    );
    Ok(Arc::new(ProjectionExec::try_new(
        vec![
            (df_column("id", 0), "id".to_owned()),
            (adjusted, "adjusted".to_owned()),
        ],
        Arc::new(filter),
    )?))
}

async fn run_roc_with_yield(
    tree: &OperatorTreeNode,
    threads: usize,
    yield_batches: usize,
) -> Result<(f64, Vec<RecordBatch>)> {
    let output = Arc::new(Mutex::new(vec![]));
    let mut graph = PipelineGraphBuilder::new();
    graph
        .pipeline_mut(0)?
        .set_sink(Box::new(CollectSink(output.clone())))?;
    build_pipeline_on_node(tree, 0, &mut graph)?;
    let executor = PipelineGraphExecutor::new(graph.finish()?)
        .with_task_executor(TokioExecutor(tokio::runtime::Handle::current()))
        .with_parallelism(threads)
        .with_config(PipelineExecutionConfig {
            batch_rows: BATCH_ROWS,
            yield_batches,
        });
    let shutdown = executor.shutdown();
    let started = Instant::now();
    let result = executor.execute().await;
    if result.is_err() {
        shutdown.request_shutdown();
    }
    shutdown.await;
    result?;
    let batches = std::mem::take(&mut *output.lock().unwrap());
    Ok((started.elapsed().as_secs_f64() * 1000.0, batches))
}

pub(super) async fn run_variant(
    variant: Variant,
    workload: Workload,
    scan: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    threads: usize,
) -> Result<(f64, Vec<RecordBatch>)> {
    match variant {
        Variant::RocDefault | Variant::RocNoYield => {
            let (tree, _) = roc_plan(workload, scan.clone(), context)?;
            let yield_batches = if matches!(variant, Variant::RocNoYield) {
                usize::MAX
            } else {
                16
            };
            run_roc_with_yield(&tree, threads, yield_batches).await
        }
        _ => {
            let plan = diagnostic_df_plan(variant, workload, scan.clone())?;
            let started = Instant::now();
            let plan = reset_plan_states(plan)?;
            let batches = collect(plan, context).await?;
            Ok((started.elapsed().as_secs_f64() * 1000.0, batches))
        }
    }
}

async fn resident_ablation(
    data_dir: &Path,
    threads: usize,
    csv: &mut String,
) -> Result<Vec<Value>> {
    let ctx = session(threads);
    let parquet = parquet_scan(&ctx, data_dir).await?;
    let schema = parquet.schema();
    // Decode once, outside every measured interval. The exact same immutable
    // Arrow buffers and round-robin partition layout feed all variants.
    let batches = collect(parquet, ctx.task_ctx()).await?;
    let input_shape = BatchShape::from_batches(&batches);
    let mut partitions = vec![Vec::new(); threads];
    for (index, batch) in batches.into_iter().enumerate() {
        partitions[index % threads].push(batch);
    }
    let partition_shapes = partitions
        .iter()
        .map(|batches| BatchShape::from_batches(batches).json())
        .collect::<Vec<_>>();
    let table = Arc::new(MemTable::try_new(schema, partitions)?);
    let scan = ctx.read_table(table)?.create_physical_plan().await?;
    if scan.output_partitioning().partition_count() != threads {
        return Err("resident scan partition count differs from requested layout".into());
    }

    let mut results = Vec::new();
    for workload in WORKLOADS {
        let variants = if matches!(workload, Workload::FilterProject) {
            vec![
                Variant::DfDefault,
                Variant::DfChecked,
                Variant::DfCheckedUncoalesced,
                Variant::RocDefault,
                Variant::RocNoYield,
            ]
        } else {
            vec![Variant::DfDefault, Variant::RocDefault, Variant::RocNoYield]
        };
        let expected_schema = df_plan(workload, scan.clone())?.schema();
        let mut expected = None;
        let mut output_rows = 0;
        let mut variant_reports = Vec::new();
        for &variant in &variants {
            let (_, output) =
                run_variant(variant, workload, &scan, ctx.task_ctx(), threads).await?;
            let rows = sorted_rows(&expected_schema, &output)?;
            if let Some(expected) = &expected {
                if expected != &rows {
                    return Err(format!(
                        "resident result mismatch: {} {}",
                        workload.name(),
                        variant.name()
                    )
                    .into());
                }
            } else {
                output_rows = rows.len();
                expected = Some(rows);
            }
            let df_plan_text = if matches!(variant, Variant::RocDefault | Variant::RocNoYield) {
                Value::Null
            } else {
                json!(format!(
                    "{}",
                    displayable(diagnostic_df_plan(variant, workload, scan.clone())?.as_ref())
                        .indent(true)
                ))
            };
            variant_reports.push(json!({
                "variant": variant.name(),
                "output_batch_shape": BatchShape::from_batches(&output).json(),
                "datafusion_plan": df_plan_text,
            }));
        }
        drop(expected);
        for warmup in 0..WARMUPS {
            for position in 0..variants.len() {
                let index = (warmup + position) % variants.len();
                let (_, output) =
                    run_variant(variants[index], workload, &scan, ctx.task_ctx(), threads).await?;
                check_row_count(&output, output_rows)?;
                drop(output);
            }
        }
        let mut samples = vec![Vec::new(); variants.len()];
        let mut sample_order = Vec::new();
        for sample in 0..SAMPLES {
            let mut order = Vec::new();
            for position in 0..variants.len() {
                let index = (sample + position) % variants.len();
                let variant = variants[index];
                let (ms, output) =
                    run_variant(variant, workload, &scan, ctx.task_ctx(), threads).await?;
                check_row_count(&output, output_rows)?;
                std::hint::black_box(&output);
                drop(output);
                samples[index].push(ms);
                order.push(variant.name());
                csv.push_str(&format!(
                    "{},{},{},{},{},{:.6}\n",
                    workload.name(),
                    threads,
                    variant.name(),
                    sample + 1,
                    position + 1,
                    ms
                ));
            }
            sample_order.push(order);
        }
        for ((report, variant), timings) in variant_reports.iter_mut().zip(&variants).zip(&samples)
        {
            let midpoint = median(timings);
            report["samples_ms"] = json!(timings);
            report["median_ms"] = json!(midpoint);
            println!(
                "resident {} threads={threads} {}: {midpoint:.3} ms",
                workload.name(),
                variant.name()
            );
        }
        results.push(json!({
            "workload": workload.name(), "threads": threads,
            "input_batch_shape": input_shape.json(), "partition_input_shapes": partition_shapes,
            "scan_plan": format!("{}", displayable(scan.as_ref()).indent(true)),
            "output_rows": output_rows, "variants": variant_reports,
            "sample_order": sample_order,
            "correctness": "each variant's full schema and sorted row multiset equal before timing",
        }));
    }
    Ok(results)
}

pub(super) fn run() -> Result<()> {
    let args = std::env::args().skip(2).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: parquet_compare --diagnose DATA_DIR OUTPUT_DIR".into());
    }
    let data_dir = Path::new(&args[0]);
    let output_dir = Path::new(&args[1]);
    if !data_dir.is_dir() {
        return Err("diagnostic DATA_DIR must be an existing Parquet directory".into());
    }
    fs::create_dir_all(output_dir)?;
    let mut audit = Vec::new();
    let mut sql_plans = Vec::new();
    let mut resident = Vec::new();
    let mut csv = String::from("workload,threads,variant,sample,order_position,elapsed_ms\n");
    for threads in [1, 4] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .max_blocking_threads(threads)
            .enable_all()
            .build()?;
        audit.extend(runtime.block_on(audit_scan_work(data_dir, threads))?);
        sql_plans.extend(runtime.block_on(default_sql_plans(data_dir, threads))?);
        resident.extend(runtime.block_on(resident_ablation(data_dir, threads, &mut csv))?);
    }
    let report = json!({
        "methodology": {
            "scan_audit": "Untimed correctness/work audit. Each engine/workload independently plans a fresh bare Parquet source. Both use the same transparent observer at the scan output. The observer is absent from resident timings.",
            "source_metrics": "Fresh source metric registry per engine execution; pruning counters are read as pruned/matched/fully_matched, never flattened with as_usize. Input batch histograms observe actual scan output after DataFusion batch splitting.",
            "default_sql": "Default SessionContext SQL physical plans only; no SQL execution or timing comparison. Batch size and target partitions match the audit. Recorded session flags distinguish statistics/page pruning from row-level decoder filtering: a predicate in the scan plan does not itself prove decoder filtering is enabled. The generated random selector generally spans 0..99 per row group, so effective min/max pruning must not be inferred from the plan text.",
            "resident_input": "Parquet decoded once outside timers, then identical Arrow batches assigned round-robin to MemTable partitions. No Parquet I/O or decoding inside resident timings.",
            "timed": "Fresh physical operators and Roc graphs built outside timers; scan/plan state reset, execution-state initialization, operator/driver work, full output materialization and Roc task cleanup inside timers; output deallocation outside.",
            "overflow_mismatch_in_original_benchmark": "Original DataFusion BinaryExpr value+1 uses wrapping addition; Roc uses checked addition. Non-overflowing generated values yield equal results but different checks. Historical baseline data is unchanged. FilterProject compares original DataFusion, checked DataFusion, and checked DataFusion with filter batch size 1 separately.",
            "filter_batch1": "DataFusion checked variant sets FilterExec batch_size=1 to avoid combining normal nonempty filtered batches. Batch histograms verify its output shape; this does not disable other operator behavior.",
            "yield_ablation": "Roc changes only PipelineExecutionConfig.yield_batches from 16 to usize::MAX. Explicit yields in other code paths, including aggregate final merge, remain. This is a diagnostic variant, not a production scheduling recommendation.",
            "limits": "Resident experiments still combine operator and driver costs. They do not establish a push-versus-pull execution-model advantage or isolate all kernel differences.",
            "samples": SAMPLES, "warmups": WARMUPS,
            "sample_order": "Rotate first variant each sample and warmup; run each variant once per round.",
        },
        "metadata": {
            "data_directory": fs::canonicalize(data_dir)?, "batch_rows": BATCH_ROWS,
            "threads": [1, 4], "datafusion_version": locked_package_version("datafusion"),
            "arrow_version": locked_package_version("arrow"), "release_build": !cfg!(debug_assertions),
        },
        "scan_audit": audit, "default_sql_plans": sql_plans,
        "resident_ablation": resident,
    });
    fs::write(
        output_dir.join("diagnostics.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    fs::write(output_dir.join("diagnostic_samples.csv"), csv)?;
    println!(
        "Saved diagnostics.json and diagnostic_samples.csv in {}",
        output_dir.display()
    );
    Ok(())
}
