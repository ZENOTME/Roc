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

//! Compare native DataFusion operators and native Roc operators on the exact
//! same DataFusion Parquet scan blueprint, with fresh execution state for every
//! run. This measures execution above a shared reader implementation,
//! not two different storage engines or an automatically translated SQL plan.
//!
//! Run from the repository root:
//! cargo run --release -p roc-datafusion --example parquet_compare -- \
//!   --rows 4000000 --samples 7 --warmups 2 --partitions 1,4 \
//!   --output-dir target/parquet-comparison
//!
//! Audit actual scan work and run resident-input execution diagnostics:
//! cargo run --release -p roc-datafusion --example parquet_compare -- \
//!   --diagnose PATH_TO_EXISTING_PARQUET_DIRECTORY OUTPUT_DIRECTORY

use std::{
    error::Error,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use asyncband::shutdown::ShutdownGuard;
use datafusion::{
    common::ScalarValue,
    execution::TaskContext,
    execution::context::{SessionConfig, SessionContext},
    logical_expr::Operator as DfOperator,
    physical_expr::{
        PhysicalExpr,
        aggregate::AggregateExprBuilder,
        expressions::{BinaryExpr, Column, Literal},
    },
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, Partitioning,
        aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
        coalesce_partitions::CoalescePartitionsExec,
        collect, displayable,
        execution_plan::reset_plan_states,
        filter::FilterExec as DfFilterExec,
        projection::ProjectionExec,
        repartition::RepartitionExec,
    },
    prelude::ParquetReadOptions,
};
use futures::future::BoxFuture;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::WriterProperties,
};
use roc::{
    error::Result as RocResult,
    exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult},
    expr::{
        ExpressionResultType,
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
            ScalarExprRef,
        },
    },
    operator::{
        AggregateOperator, FilterOperator, OperatorTreeNode, ProjectOperator, Projection,
        ProjectionExpression, ScanOperator,
    },
    pipeline::{
        Executor, PipelineExecutionConfig, PipelineGraphBuilder, PipelineGraphExecutor,
        build_pipeline_on_node,
    },
};
use roc_datafusion::{DataFusionScan, DataFusionStorage};
use serde_json::{Value, json};

#[path = "parquet_compare/ablation.rs"]
mod ablation;
#[path = "parquet_compare/diagnostics.rs"]
mod diagnostics;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const BATCH_ROWS: usize = 8192;
const ROW_GROUP_ROWS: usize = 65_536;
const GROUPS: usize = 256;

struct Options {
    rows: usize,
    files: usize,
    samples: usize,
    warmups: usize,
    compression: Compression,
    partitions: Vec<usize>,
    output_dir: PathBuf,
}

impl Options {
    fn parse() -> Result<Self> {
        let mut options = Self {
            rows: 4_000_000,
            files: 8,
            samples: 7,
            warmups: 2,
            compression: Compression::SNAPPY,
            partitions: vec![1, 4],
            output_dir: "target/parquet-comparison".into(),
        };
        let mut args = std::env::args().skip(1);
        while let Some(name) = args.next() {
            if name == "--help" || name == "-h" {
                println!(
                    "parquet_compare [--rows N] [--files N] [--samples N] [--warmups N] [--compression snappy|uncompressed] [--partitions 1,4] [--output-dir PATH]\nparquet_compare --diagnose EXISTING_PARQUET_DIRECTORY OUTPUT_DIRECTORY"
                );
                std::process::exit(0);
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {name}"))?;
            match name.as_str() {
                "--rows" => options.rows = value.parse()?,
                "--files" => options.files = value.parse()?,
                "--samples" => options.samples = value.parse()?,
                "--warmups" => options.warmups = value.parse()?,
                "--compression" => {
                    options.compression = match value.as_str() {
                        "snappy" => Compression::SNAPPY,
                        "uncompressed" => Compression::UNCOMPRESSED,
                        _ => return Err("compression must be snappy or uncompressed".into()),
                    }
                }
                "--partitions" => {
                    options.partitions = value
                        .split(',')
                        .map(str::parse)
                        .collect::<std::result::Result<_, _>>()?
                }
                "--output-dir" => options.output_dir = value.into(),
                _ => return Err(format!("unknown option {name}").into()),
            }
        }
        if options.rows == 0
            || options.files == 0
            || options.samples == 0
            || options.partitions.is_empty()
            || options.partitions.contains(&0)
        {
            return Err("rows, files, samples, and partition counts must be positive".into());
        }
        options.files = options.files.min(options.rows);
        Ok(options)
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Scan,
    FilterProject,
    GlobalAggregate,
    GroupedAggregate,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Scan => "scan_only",
            Self::FilterProject => "filter_project",
            Self::GlobalAggregate => "global_aggregate",
            Self::GroupedAggregate => "grouped_aggregate",
        }
    }
    fn sql(self) -> &'static str {
        match self {
            Self::Scan => "SELECT id, group_key, value, selector FROM t",
            Self::FilterProject => "SELECT id, value + 1 AS adjusted FROM t WHERE selector > 50",
            Self::GlobalAggregate => "SELECT SUM(value) AS total, COUNT(value) AS count FROM t",
            Self::GroupedAggregate => {
                "SELECT group_key, SUM(value) AS total, COUNT(value) AS count FROM t GROUP BY group_key"
            }
        }
    }
}

fn mix(mut n: u64) -> u64 {
    n = n.wrapping_add(0x9e3779b97f4a7c15);
    n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
    n ^ (n >> 31)
}

fn generate_data(options: &Options, data_dir: &Path) -> Result<u64> {
    // Use a fresh directory so an earlier run with more files cannot silently
    // add input rows. The benchmark never deletes an existing directory.
    fs::create_dir(data_dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
        Field::new("selector", DataType::Int64, true),
    ]));
    let mut bytes = 0;
    for file_index in 0..options.files {
        let begin = options.rows * file_index / options.files;
        let end = options.rows * (file_index + 1) / options.files;
        let path = data_dir.join(format!("part-{file_index:03}.parquet"));
        let properties = WriterProperties::builder()
            .set_compression(options.compression)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path)?, schema.clone(), Some(properties))?;
        for start in (begin..end).step_by(BATCH_ROWS) {
            let finish = (start + BATCH_ROWS).min(end);
            let ids = Int64Array::from_iter_values((start..finish).map(|i| i as i64));
            let keys = Int64Array::from_iter_values(
                (start..finish).map(|i| (mix(i as u64) % GROUPS as u64) as i64),
            );
            let values =
                Int64Array::from_iter((start..finish).map(|i| {
                    (i % 17 != 0).then(|| (mix(i as u64 + 1) % 200_001) as i64 - 100_000)
                }));
            let selectors = Int64Array::from_iter(
                (start..finish).map(|i| (i % 23 != 0).then(|| (mix(i as u64 + 2) % 100) as i64)),
            );
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(ids),
                Arc::new(keys),
                Arc::new(values),
                Arc::new(selectors),
            ];
            writer.write(&RecordBatch::try_new(schema.clone(), arrays)?)?;
        }
        writer.close()?;
        bytes += fs::metadata(path)?.len();
    }
    Ok(bytes)
}

fn df_column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

fn df_plan(workload: Workload, scan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    match workload {
        Workload::Scan => Ok(scan),
        Workload::FilterProject => {
            let predicate = Arc::new(BinaryExpr::new(
                df_column("selector", 3),
                DfOperator::Gt,
                Arc::new(Literal::new(ScalarValue::Int64(Some(50)))),
            ));
            let filter = Arc::new(DfFilterExec::try_new(predicate, scan)?);
            let adjusted: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                df_column("value", 2),
                DfOperator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            ));
            Ok(Arc::new(ProjectionExec::try_new(
                vec![
                    (df_column("id", 0), "id".to_string()),
                    (adjusted, "adjusted".to_string()),
                ],
                filter,
            )?))
        }
        Workload::GlobalAggregate | Workload::GroupedAggregate => {
            let input_schema = scan.schema();
            let aggregates = vec![
                Arc::new(
                    AggregateExprBuilder::new(
                        datafusion::functions_aggregate::sum::sum_udaf(),
                        vec![df_column("value", 2)],
                    )
                    .schema(input_schema.clone())
                    .alias("total")
                    .build()?,
                ),
                Arc::new(
                    AggregateExprBuilder::new(
                        datafusion::functions_aggregate::count::count_udaf(),
                        vec![df_column("value", 2)],
                    )
                    .schema(input_schema.clone())
                    .alias("count")
                    .build()?,
                ),
            ];
            let partitions = scan.output_partitioning().partition_count();
            let groups =
                PhysicalGroupBy::new_single(if matches!(workload, Workload::GroupedAggregate) {
                    vec![(df_column("group_key", 1), "group_key".to_string())]
                } else {
                    vec![]
                });
            if partitions == 1 {
                return Ok(Arc::new(AggregateExec::try_new(
                    AggregateMode::Single,
                    groups,
                    aggregates,
                    vec![None; 2],
                    scan,
                    input_schema,
                )?));
            }
            let final_groups = groups.as_final();
            let partial: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                groups,
                aggregates.clone(),
                vec![None; 2],
                scan,
                input_schema.clone(),
            )?);
            let (mode, merged): (_, Arc<dyn ExecutionPlan>) =
                if matches!(workload, Workload::GroupedAggregate) {
                    (
                        AggregateMode::FinalPartitioned,
                        Arc::new(RepartitionExec::try_new(
                            partial,
                            Partitioning::Hash(vec![df_column("group_key", 0)], partitions),
                        )?),
                    )
                } else {
                    (
                        AggregateMode::Final,
                        Arc::new(CoalescePartitionsExec::new(partial)),
                    )
                };
            Ok(Arc::new(AggregateExec::try_new(
                mode,
                final_groups,
                aggregates,
                vec![None; 2],
                merged,
                input_schema,
            )?))
        }
    }
}

fn roc_column(index: usize, schema: &Schema) -> ScalarExprRef {
    let field = schema.field(index);
    ReferenceExpression::new(
        index,
        ExpressionResultType::new(field.data_type().clone(), field.is_nullable()),
    )
    .into_ref()
}

fn roc_plan(
    workload: Workload,
    scan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<(OperatorTreeNode, SchemaRef)> {
    let schema = scan.schema();
    let source = OperatorTreeNode::new(
        ScanOperator::new(
            DataFusionScan::new(scan, context),
            Arc::new(DataFusionStorage),
        ),
        vec![],
    );
    match workload {
        Workload::Scan => Ok((source, schema)),
        Workload::FilterProject => {
            let predicate = FunctionExpression::binary(
                FunctionKind::GreaterThan,
                roc_column(3, &schema),
                ConstantExpression::int64(Some(50)).into_ref(),
                DataType::Boolean,
                schema.field(3).is_nullable(),
            )
            .into_ref();
            let filter = OperatorTreeNode::new(FilterOperator::new(predicate), vec![source]);
            let adjusted = FunctionExpression::binary(
                FunctionKind::Add,
                roc_column(2, &schema),
                ConstantExpression::int64(Some(1)).into_ref(),
                DataType::Int64,
                schema.field(2).is_nullable(),
            )
            .into_ref();
            let projection = Projection::new(vec![
                ProjectionExpression::new(roc_column(0, &schema), "id"),
                ProjectionExpression::new(adjusted, "adjusted"),
            ]);
            let output = projection.output_schema();
            Ok((
                OperatorTreeNode::new(ProjectOperator::new(projection), vec![filter]),
                output,
            ))
        }
        Workload::GlobalAggregate | Workload::GroupedAggregate => {
            let groups = if matches!(workload, Workload::GroupedAggregate) {
                vec![ProjectionExpression::new(
                    roc_column(1, &schema),
                    "group_key",
                )]
            } else {
                vec![]
            };
            let aggregates = vec![
                Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Sum,
                        vec![roc_column(2, &schema)],
                        DataType::Int64,
                        true,
                    )
                    .with_alias("total"),
                ),
                Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Count,
                        vec![roc_column(2, &schema)],
                        DataType::Int64,
                        false,
                    )
                    .with_alias("count"),
                ),
            ];
            let aggregate = AggregateOperator::try_new(Projection::new(groups), aggregates)?;
            let output = aggregate.output_schema();
            Ok((OperatorTreeNode::new(aggregate, vec![source]), output))
        }
    }
}

struct TokioExecutor(tokio::runtime::Handle);
impl Executor for TokioExecutor {
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

struct CollectSink(Arc<Mutex<Vec<RecordBatch>>>);
struct CollectSinkExecutor {
    output: Arc<Mutex<Vec<RecordBatch>>>,
    local: Vec<RecordBatch>,
}

impl SinkExec for CollectSink {
    fn init_global_context(&self, _: &ShutdownGuard) -> RocResult<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _: GlobalExecContextRef) -> RocResult<Box<dyn SinkExecutor>> {
        Ok(Box::new(CollectSinkExecutor {
            output: self.0.clone(),
            local: vec![],
        }))
    }
    fn finalize<'a>(
        &'a self,
        _: GlobalExecContextRef,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, RocResult<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl SinkExecutor for CollectSinkExecutor {
    fn sink<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        batch: &'a RecordBatch,
    ) -> BoxFuture<'a, RocResult<SinkResult>> {
        Box::pin(async move {
            self.local.push(batch.clone());
            Ok(SinkResult::NeedMoreInput)
        })
    }
    fn combine(self: Box<Self>, _: &ShutdownGuard) -> BoxFuture<'_, RocResult<()>> {
        Box::pin(async move {
            self.output.lock().unwrap().extend(self.local);
            Ok(())
        })
    }
}

async fn run_roc(tree: &OperatorTreeNode, parallelism: usize) -> Result<(f64, Vec<RecordBatch>)> {
    let output = Arc::new(Mutex::new(vec![]));
    let mut graph = PipelineGraphBuilder::new();
    graph
        .pipeline_mut(0)?
        .set_sink(Box::new(CollectSink(output.clone())))?;
    build_pipeline_on_node(tree, 0, &mut graph)?;
    let executor = PipelineGraphExecutor::new(graph.finish()?)
        .with_task_executor(TokioExecutor(tokio::runtime::Handle::current()))
        .with_parallelism(parallelism)
        .with_config(PipelineExecutionConfig {
            batch_rows: BATCH_ROWS,
            ..Default::default()
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

async fn run_df(
    workload: Workload,
    scan: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<(f64, Vec<RecordBatch>)> {
    // Build operators outside the timer, just as Roc builds its graph. Scan
    // plans also own state (including the queue of unopened Parquet files), so
    // reset the entire plan inside the timer, matching DataFusionStorage.
    let plan = df_plan(workload, scan.clone())?;
    let started = Instant::now();
    let plan = reset_plan_states(plan)?;
    let batches = collect(plan, context).await?;
    Ok((started.elapsed().as_secs_f64() * 1000.0, batches))
}

fn sorted_rows(schema: &Schema, batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>> {
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect(),
    )?;
    let mut result = Vec::new();
    for batch in batches {
        if batch.schema().as_ref() != schema {
            return Err("batch schema does not match declared output schema".into());
        }
        let rows = converter.convert_columns(batch.columns())?;
        result.extend(rows.iter().map(|row| row.as_ref().to_vec()));
    }
    result.sort_unstable();
    Ok(result)
}

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn check_row_count(batches: &[RecordBatch], expected: usize) -> Result<()> {
    let actual: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if actual != expected {
        return Err(format!(
            "execution returned {actual} rows; validated execution returned {expected}"
        )
        .into());
    }
    Ok(())
}

async fn compare(options: &Options, data_dir: &Path, parallelism: usize) -> Result<Vec<Value>> {
    let config = SessionConfig::new()
        .with_target_partitions(parallelism)
        .with_batch_size(BATCH_ROWS);
    let context = SessionContext::new_with_config(config);
    let scan = context
        .read_parquet(
            data_dir.to_str().ok_or("non-UTF8 data path")?,
            ParquetReadOptions::default(),
        )
        .await?
        .create_physical_plan()
        .await?;
    let scan_partitions = scan.output_partitioning().partition_count();
    let scan_description = format!("{}", displayable(scan.as_ref()).indent(true));
    let mut results = Vec::new();
    for workload in [
        Workload::Scan,
        Workload::FilterProject,
        Workload::GlobalAggregate,
        Workload::GroupedAggregate,
    ] {
        let df = df_plan(workload, scan.clone())?;
        let (roc, roc_schema) = roc_plan(workload, scan.clone(), context.task_ctx())?;
        if df.schema() != roc_schema {
            return Err(format!(
                "{} schema mismatch: DataFusion {:?}; Roc {:?}",
                workload.name(),
                df.schema(),
                roc_schema
            )
            .into());
        }
        let (_, df_output) = run_df(workload, &scan, context.task_ctx()).await?;
        let (_, roc_output) = run_roc(&roc, parallelism).await?;
        let output_rows: usize = df_output.iter().map(RecordBatch::num_rows).sum();
        if sorted_rows(&df.schema(), &df_output)? != sorted_rows(&roc_schema, &roc_output)? {
            return Err(format!("{} full result comparison failed", workload.name()).into());
        }
        drop(df_output);
        drop(roc_output);
        for _ in 0..options.warmups {
            drop(run_df(workload, &scan, context.task_ctx()).await?);
            drop(run_roc(&roc, parallelism).await?);
        }
        let mut df_samples = Vec::with_capacity(options.samples);
        let mut roc_samples = Vec::with_capacity(options.samples);
        let mut run_order = Vec::with_capacity(options.samples);
        for sample in 0..options.samples {
            // Each result is released before starting the other engine, keeping
            // live materialized output memory comparable in the timed pairs.
            let (df_ms, roc_ms) = if sample % 2 == 0 {
                let (df_ms, output) = run_df(workload, &scan, context.task_ctx()).await?;
                check_row_count(&output, output_rows)?;
                std::hint::black_box(&output);
                drop(output);
                let (roc_ms, output) = run_roc(&roc, parallelism).await?;
                check_row_count(&output, output_rows)?;
                std::hint::black_box(&output);
                drop(output);
                run_order.push("datafusion,roc");
                (df_ms, roc_ms)
            } else {
                let (roc_ms, output) = run_roc(&roc, parallelism).await?;
                check_row_count(&output, output_rows)?;
                std::hint::black_box(&output);
                drop(output);
                let (df_ms, output) = run_df(workload, &scan, context.task_ctx()).await?;
                check_row_count(&output, output_rows)?;
                std::hint::black_box(&output);
                drop(output);
                run_order.push("roc,datafusion");
                (df_ms, roc_ms)
            };
            df_samples.push(df_ms);
            roc_samples.push(roc_ms);
        }
        let df_median = median(&df_samples);
        let roc_median = median(&roc_samples);
        println!(
            "{:<18} threads={parallelism} scan_partitions={scan_partitions} rows={output_rows:>9} DataFusion={df_median:>9.3}ms Roc={roc_median:>9.3}ms DF/Roc={:.3}x",
            workload.name(),
            df_median / roc_median
        );
        results.push(json!({
            "workload": workload.name(), "sql_equivalent": workload.sql(),
            "runtime_threads": parallelism, "roc_workers": parallelism,
            "target_partitions": parallelism, "scan_partitions": scan_partitions,
            "output_rows": output_rows, "correctness": "exact full schema and unordered row multiset equal",
            "datafusion_ms": df_samples, "roc_ms": roc_samples,
            "datafusion_median_ms": df_median, "roc_median_ms": roc_median,
            "datafusion_over_roc": df_median / roc_median, "run_order": run_order,
            "shared_scan_plan": scan_description,
            "datafusion_plan": format!("{}", displayable(df.as_ref()).indent(true)),
        }));
    }
    Ok(results)
}

fn command_output(command: &str, args: &[&str]) -> String {
    std::process::Command::new(command)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unavailable".into())
}

fn locked_package_version(name: &str) -> &'static str {
    let package_name = format!("name = \"{name}\"");
    include_str!("../../../Cargo.lock")
        .split("[[package]]")
        .find(|package| package.lines().any(|line| line == package_name))
        .and_then(|package| {
            package.lines().find_map(|line| {
                line.strip_prefix("version = \"")
                    .and_then(|version| version.strip_suffix('"'))
            })
        })
        .unwrap_or("unknown")
}

fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--ablate") {
        return ablation::run();
    }
    if std::env::args().nth(1).as_deref() == Some("--diagnose") {
        return diagnostics::run();
    }
    let options = Options::parse()?;
    fs::create_dir_all(&options.output_dir)?;
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let data_dir = options.output_dir.join(format!("data-{timestamp}"));
    println!(
        "Generating {} deterministic rows in {} Parquet files...",
        options.rows, options.files
    );
    let parquet_bytes = generate_data(&options, &data_dir)?;
    println!(
        "Parquet size: {:.2} MiB; full result validation precedes warmups and timings.",
        parquet_bytes as f64 / (1024.0 * 1024.0)
    );
    let mut results = vec![];
    for &parallelism in &options.partitions {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(parallelism)
            .max_blocking_threads(parallelism)
            .enable_all()
            .build()?;
        results.extend(runtime.block_on(compare(&options, &data_dir, parallelism))?);
    }
    let report = json!({
        "methodology": {
            "comparison": "native DataFusion versus native Roc operators over the identical DataFusion Parquet scan blueprint and reader configuration; both reset execution state for every run",
            "planning": "manual equivalent physical operators; graph and plan construction outside timers; no downstream optimization or scan pushdown",
            "scan_projection": "all four input columns in every workload for both engines",
            "aggregation": "DataFusion uses Single for one scan partition; otherwise Partial then global Final or hash Repartition/FinalPartitioned for groups. Roc uses worker-local partials and one global final merge.",
            "timed": "full DataFusion scan/plan state reset, execution state initialization, Parquet I/O/decoding, native operators, complete output materialization and Roc task cleanup",
            "excluded": "data generation, metadata discovery, plan/graph construction, validation, warmups, result deallocation",
            "cache": "warm OS page cache; cache is not flushed; engines alternate first position",
            "limitations": "synthetic local Parquet; controlled physical-operator comparison over a shared reader, not default SQL optimization; no TPC-H claim, SQL translator, spill, or memory-limit comparison",
        },
        "metadata": {
            "timestamp_unix_ns": timestamp.to_string(), "rows": options.rows, "files": options.files,
            "parquet_bytes": parquet_bytes, "data_directory": fs::canonicalize(&data_dir)?,
            "compression": format!("{:?}", options.compression), "dictionary": false, "encoding": "PLAIN",
            "batch_rows": BATCH_ROWS, "row_group_rows": ROW_GROUP_ROWS, "group_cardinality": GROUPS,
            "nulls": "value: every 17th row; selector: every 23rd row",
            "generator": "deterministic SplitMix64 transform of row ID; no random seed",
            "samples": options.samples, "warmups": options.warmups,
            "datafusion_version": locked_package_version("datafusion"), "arrow_version": locked_package_version("arrow"), "parquet_version": locked_package_version("parquet"),
            "os": std::env::consts::OS, "architecture": std::env::consts::ARCH,
            "available_parallelism": std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1),
            "release_build": !cfg!(debug_assertions), "rustc": command_output("rustc", &["--version"]),
            "git_commit": command_output("git", &["rev-parse", "HEAD"]),
            "git_dirty": !command_output("git", &["status", "--porcelain"]).is_empty(),
            "cpu": command_output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        },
        "results": results,
    });
    fs::write(
        options.output_dir.join("results.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    let mut csv =
        String::from("workload,threads,scan_partitions,sample,first_engine,datafusion_ms,roc_ms\n");
    for result in report["results"].as_array().unwrap() {
        for sample in 0..options.samples {
            csv.push_str(&format!(
                "{},{},{},{},{},{:.6},{:.6}\n",
                result["workload"].as_str().unwrap(),
                result["runtime_threads"],
                result["scan_partitions"],
                sample + 1,
                if sample % 2 == 0 { "datafusion" } else { "roc" },
                result["datafusion_ms"][sample].as_f64().unwrap(),
                result["roc_ms"][sample].as_f64().unwrap()
            ));
        }
    }
    fs::write(options.output_dir.join("samples.csv"), csv)?;
    println!(
        "Saved results.json and samples.csv in {}",
        options.output_dir.display()
    );
    Ok(())
}
