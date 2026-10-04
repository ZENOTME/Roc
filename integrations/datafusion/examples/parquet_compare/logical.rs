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

//! SQL frontend comparison through the real LogicalPlan -> Roc converter.
//! parquet_compare --logical DATA_DIR OUTPUT_DIR LABEL [samples]
use super::*;
use roc_datafusion::LogicalPlanConverter;

async fn execute_df(
    plan: &Arc<dyn ExecutionPlan>,
    state: Arc<TaskContext>,
) -> Result<(f64, Vec<RecordBatch>)> {
    let started = Instant::now();
    let output = collect(reset_plan_states(plan.clone())?, state).await?;
    Ok((started.elapsed().as_secs_f64() * 1000.0, output))
}
async fn measure(data_dir: &Path, threads: usize, samples: usize) -> Result<Vec<Value>> {
    let ctx = diagnostics::session(threads);
    ctx.register_parquet(
        "t",
        data_dir.to_str().ok_or("non UTF-8 path")?,
        ParquetReadOptions::default(),
    )
    .await?;
    let mut results = Vec::new();
    for workload in diagnostics::WORKLOADS {
        let (state, logical) = ctx.sql(workload.sql()).await?.into_parts();
        let logical = state.optimize(&logical)?;
        let started = Instant::now();
        let df = state.create_physical_plan(&logical).await?;
        let df_planning_ms = started.elapsed().as_secs_f64() * 1000.0;
        let started = Instant::now();
        let roc = LogicalPlanConverter::new(state.clone())
            .convert(&logical)
            .await?;
        let roc_planning_ms = started.elapsed().as_secs_f64() * 1000.0;
        let schema = logical.schema().as_arrow();
        let (_, expected) = execute_df(&df, state.task_ctx()).await?;
        let (_, actual) = run_roc(roc.root(), threads).await?;
        if df.schema().as_ref() != schema
            || sorted_rows(schema, &expected)? != sorted_rows(schema, &actual)?
        {
            return Err(format!(
                "{} full SQL result/schema comparison failed",
                workload.name()
            )
            .into());
        }
        let output_rows = expected.iter().map(RecordBatch::num_rows).sum();
        drop(expected);
        drop(actual);
        for _ in 0..2 {
            drop(execute_df(&df, state.task_ctx()).await?);
            drop(run_roc(roc.root(), threads).await?);
        }
        let mut df_ms = Vec::new();
        let mut roc_ms = Vec::new();
        for sample in 0..samples {
            if sample % 2 == 0 {
                let (ms, output) = execute_df(&df, state.task_ctx()).await?;
                check_row_count(&output, output_rows)?;
                df_ms.push(ms);
                drop(output);
                let (ms, output) = run_roc(roc.root(), threads).await?;
                check_row_count(&output, output_rows)?;
                roc_ms.push(ms);
                drop(output);
            } else {
                let (ms, output) = run_roc(roc.root(), threads).await?;
                check_row_count(&output, output_rows)?;
                roc_ms.push(ms);
                drop(output);
                let (ms, output) = execute_df(&df, state.task_ctx()).await?;
                check_row_count(&output, output_rows)?;
                df_ms.push(ms);
                drop(output);
            }
        }
        println!(
            "{} threads={threads} DF={:.3}ms Roc={:.3}ms DF/Roc={:.3}x",
            workload.name(),
            median(&df_ms),
            median(&roc_ms),
            median(&df_ms) / median(&roc_ms)
        );
        results.push(json!({
            "workload": workload.name(), "sql": workload.sql(), "threads": threads,
            "output_rows": output_rows, "correctness": "exact full schema and unordered row multiset equal before timing; row count checked in every sample",
            "datafusion_median_ms": median(&df_ms), "roc_median_ms": median(&roc_ms), "datafusion_over_roc": median(&df_ms)/median(&roc_ms),
            "datafusion_ms": df_ms, "roc_ms": roc_ms,
            "datafusion_planning_ms_single_observation": df_planning_ms, "roc_conversion_ms_single_observation": roc_planning_ms,
            "logical_plan": format!("{}", logical.display_indent_schema()),
            "datafusion_plan": format!("{}", displayable(df.as_ref()).indent(true)),
            "roc_plan": format!("{roc:#?}"), "session": diagnostics::session_flags(&ctx),
        }));
    }
    Ok(results)
}
pub(super) fn run() -> Result<()> {
    let args = std::env::args().skip(2).collect::<Vec<_>>();
    if args.len() < 3 || args.len() > 4 {
        return Err("--logical DATA_DIR OUTPUT_DIR LABEL [samples]".into());
    }
    let data_dir = fs::canonicalize(&args[0])?;
    let output_dir = PathBuf::from(&args[1]);
    let samples = args
        .get(3)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10usize);
    if samples == 0 {
        return Err("samples must be positive".into());
    }
    fs::create_dir_all(&output_dir)?;
    let mut results = Vec::new();
    for threads in [1, 4] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .max_blocking_threads(threads)
            .enable_all()
            .build()?;
        results.extend(runtime.block_on(measure(&data_dir, threads, samples))?);
    }
    let report = json!({
        "methodology": {
            "entry": "same DataFusion SQL, analyzed optimized logical plan; native DataFusion physical planning versus LogicalPlanConverter -> Roc pipeline",
            "storage": "DataFusion TableProvider::scan with logical projection, filters and fetch for Roc; native DataFusion scan planning for DataFusion; session settings identical",
            "timed": "scan/plan state reset, execution state initialization, complete output collection, Roc task cleanup",
            "excluded": "SQL planning, logical optimization, physical planning/conversion, Roc graph construction, validation, warmups, output deallocation",
            "samples": samples, "warmups": 2, "order": "DataFusion first on even samples; Roc first on odd samples",
            "semantics": "Roc retains checked integer arithmetic and SUM; generated inputs do not overflow",
            "limitations": "synthetic local Parquet, warm OS cache, 256 groups; not TPC-H; planning observations are not a planning benchmark",
        },
        "metadata": ablation::build_metadata(&args[2])?, "input_files": ablation::input_files(&data_dir)?, "results": results,
    });
    fs::write(
        output_dir.join("results.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    let mut csv = String::from("workload,threads,sample,first_engine,datafusion_ms,roc_ms\n");
    for result in report["results"].as_array().unwrap() {
        for sample in 0..samples {
            csv.push_str(&format!(
                "{},{},{},{},{:.6},{:.6}\n",
                result["workload"].as_str().unwrap(),
                result["threads"],
                sample + 1,
                if sample % 2 == 0 { "datafusion" } else { "roc" },
                result["datafusion_ms"][sample].as_f64().unwrap(),
                result["roc_ms"][sample].as_f64().unwrap()
            ));
        }
    }
    fs::write(output_dir.join("samples.csv"), csv)?;
    Ok(())
}
