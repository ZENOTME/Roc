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

//! Repeated measurements for externally built source ablations.
//! `parquet_compare --ablate DATA_DIR OUTPUT_DIR LABEL [samples]`
//! The harness never changes source code or chooses compiler settings.

use super::diagnostics::{
    BatchShape, Variant, WORKLOADS, diagnostic_df_plan, parquet_scan, run_variant, session,
    session_flags,
};
use super::*;
use datafusion::datasource::MemTable;
use std::{io::Write, process::Command};

const WARMUPS: usize = 2;
const DEFAULT_SAMPLES: usize = 10;

/// Hashing is outside every measured interval. Requiring an actual SHA-256
/// avoids silently replacing an input identity with an unavailable placeholder.
fn sha256(path: &Path) -> Result<String> {
    for (program, args) in [("shasum", &["-a", "256"][..]), ("sha256sum", &[][..])] {
        let Ok(output) = Command::new(program).args(args).arg(path).output() else {
            continue;
        };
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            if let Some(hash) = text.split_whitespace().next()
                && hash.len() == 64
                && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Ok(hash.to_ascii_lowercase());
            }
        }
    }
    Err(format!(
        "cannot hash {}: install shasum or sha256sum",
        path.display()
    )
    .into())
}

pub(super) fn input_files(data_dir: &Path) -> Result<Vec<Value>> {
    let mut pending = vec![data_dir.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else if entry.file_type()?.is_file()
                && entry.path().extension().is_some_and(|ext| ext == "parquet")
            {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    if files.is_empty() {
        return Err("DATA_DIR contains no Parquet files".into());
    }
    files
        .into_iter()
        .map(|path| {
            Ok(json!({
                "path": path.strip_prefix(data_dir)?,
                "bytes": fs::metadata(&path)?.len(),
                "sha256": sha256(&path)?,
            }))
        })
        .collect()
}

pub(super) fn build_metadata(label: &str) -> Result<Value> {
    let executable = fs::canonicalize(std::env::current_exe()?)?;
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let git = |args: &[&str]| {
        Command::new("git")
            .current_dir(&repository)
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .unwrap_or_else(|| "unavailable".to_owned())
    };
    Ok(json!({
        "label": label,
        "executable": executable,
        "executable_sha256": sha256(&executable)?,
        "compiled_source_sha256": option_env!("ROC_ABLATION_SOURCE_SHA256"),
        "compiled_build_label": option_env!("ROC_ABLATION_BUILD_LABEL"),
        "compiled_rustflags": option_env!("RUSTFLAGS"),
        "release_build": !cfg!(debug_assertions),
        "datafusion_version": locked_package_version("datafusion"),
        "arrow_version": locked_package_version("arrow"),
        "operating_system": std::env::consts::OS,
        "architecture": std::env::consts::ARCH,
        "available_parallelism": std::thread::available_parallelism()?.get(),
        "runtime_rustc": command_output("rustc", &["-Vv"]),
        "runtime_uname": command_output("uname", &["-a"]),
        "runtime_checkout": {
            "directory": fs::canonicalize(&repository)?,
            "git_head": git(&["rev-parse", "HEAD"]),
            "git_status": git(&["status", "--short"]),
            "cargo_lock_sha256": sha256(&repository.join("Cargo.lock"))?,
            "note": "Checkout and rustc metadata describe runtime surroundings, not necessarily this binary's source. The executable hash identifies the measured binary; compile-time source hash and label are optional build inputs.",
        },
    }))
}

fn shape(batches: &[RecordBatch]) -> Value {
    BatchShape::from_batches(batches).json()
}

async fn measure_case(
    scenario: &str,
    workload: Workload,
    scan: &Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
    threads: usize,
    samples: usize,
    input_shape: &Value,
    csv: &mut String,
) -> Result<Value> {
    let variants = if matches!(workload, Workload::FilterProject) {
        vec![
            Variant::DfChecked,
            Variant::DfCheckedUncoalesced,
            Variant::RocDefault,
        ]
    } else {
        vec![Variant::DfDefault, Variant::RocDefault]
    };
    let expected_schema = diagnostic_df_plan(variants[0], workload, scan.clone())?.schema();
    let mut expected_rows = None;
    let mut output_rows = 0;
    let mut reports = Vec::new();
    for &variant in &variants {
        let (schema, plan) = if matches!(variant, Variant::RocDefault) {
            let (tree, schema) = roc_plan(workload, scan.clone(), ctx.task_ctx())?;
            (schema, format!("{tree:#?}"))
        } else {
            let plan = diagnostic_df_plan(variant, workload, scan.clone())?;
            (
                plan.schema(),
                format!("{}", displayable(plan.as_ref()).indent(true)),
            )
        };
        if schema != expected_schema {
            return Err(format!("{} output plan schema differs", variant.name()).into());
        }
        let (_, output) = run_variant(variant, workload, scan, ctx.task_ctx(), threads).await?;
        let rows = sorted_rows(&expected_schema, &output)?;
        if let Some(expected_rows) = &expected_rows {
            if expected_rows != &rows {
                return Err(format!(
                    "{scenario} {} {} result mismatch",
                    workload.name(),
                    variant.name()
                )
                .into());
            }
        } else {
            output_rows = rows.len();
            expected_rows = Some(rows);
        }
        reports.push(json!({
            "variant": variant.name(), "physical_plan": plan,
            "output_schema": format!("{schema:?}"),
            "output_batch_shape": shape(&output),
        }));
    }
    drop(expected_rows);
    if matches!(workload, Workload::FilterProject)
        && reports[1]["output_batch_shape"] != reports[2]["output_batch_shape"]
    {
        return Err(
            format!("{scenario} checked DataFusion batch1 and Roc output shapes differ").into(),
        );
    }
    let mut timings = vec![Vec::with_capacity(samples); variants.len()];
    let mut run_order = Vec::with_capacity(samples);
    for round in 0..WARMUPS + samples {
        let is_sample = round >= WARMUPS;
        let ordinal = if is_sample { round - WARMUPS } else { round };
        let mut order = Vec::new();
        for position in 0..variants.len() {
            let index = (ordinal + position) % variants.len();
            let variant = variants[index];
            let (ms, output) =
                run_variant(variant, workload, scan, ctx.task_ctx(), threads).await?;
            check_row_count(&output, output_rows)?;
            std::hint::black_box(&output);
            drop(output);
            if is_sample {
                timings[index].push(ms);
                order.push(variant.name());
                csv.push_str(&format!(
                    "{},{},{},{},{},{},{:.6}\n",
                    scenario,
                    workload.name(),
                    threads,
                    variant.name(),
                    ordinal + 1,
                    position + 1,
                    ms
                ));
            }
        }
        if is_sample {
            run_order.push(order);
        }
    }
    for ((report, variant), times) in reports.iter_mut().zip(&variants).zip(&timings) {
        let midpoint = median(times);
        report["samples_ms"] = json!(times);
        report["median_ms"] = json!(midpoint);
        report["min_ms"] = json!(times.iter().copied().reduce(f64::min).unwrap());
        report["max_ms"] = json!(times.iter().copied().reduce(f64::max).unwrap());
        println!(
            "{scenario} {} threads={threads} {}: {midpoint:.3} ms",
            workload.name(),
            variant.name()
        );
    }
    Ok(json!({
        "scenario": scenario, "workload": workload.name(), "sql_equivalent": workload.sql(),
        "runtime_threads": threads, "runtime_max_blocking_threads": threads,
        "roc_workers": threads, "roc_yield_batches": 16,
        "scan_partitions": scan.output_partitioning().partition_count(),
        "scan_plan": format!("{}", displayable(scan.as_ref()).indent(true)),
        "input_batch_shape": input_shape, "input_rows": input_shape["rows"],
        "output_rows": output_rows, "variants": reports, "sample_order": run_order,
        "session_flags": session_flags(ctx),
        "correctness": "exact declared schema, every materialized batch schema, and sorted row multiset equal before timing; row count checked after every warmup/sample",
        "filter_batch1_roc_shapes_equal": matches!(workload, Workload::FilterProject),
    }))
}

async fn measure_threads(
    data_dir: &Path,
    threads: usize,
    samples: usize,
    csv: &mut String,
) -> Result<Vec<Value>> {
    let ctx = session(threads);
    let parquet = parquet_scan(&ctx, data_dir).await?;
    // The sole resident decode and its allocation/partitioning happen before
    // any timed interval. Parquet cases independently execute the file scan.
    let batches = collect(reset_plan_states(parquet.clone())?, ctx.task_ctx()).await?;
    let input_shape = shape(&batches);
    // Parallel collection order is scheduler-dependent. The generated dataset
    // has unique, non-null Int64 ids and contiguous rows within each batch.
    // Sorting batch handles by their first id stabilizes the resident layout
    // across binaries without copying any Arrow buffers.
    let mut batches = batches
        .into_iter()
        .map(|batch| {
            let first_id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|ids| ids.iter().next().flatten())
                .ok_or(
                    "resident ablation requires nonempty batches with an Int64 id in column 0",
                )?;
            Ok((first_id, batch))
        })
        .collect::<Result<Vec<_>>>()?;
    batches.sort_unstable_by_key(|(first_id, _)| *first_id);
    let mut partitions = vec![Vec::new(); threads];
    for (index, (_, batch)) in batches.into_iter().enumerate() {
        partitions[index % threads].push(batch);
    }
    let partition_shapes = partitions
        .iter()
        .map(|batches| shape(batches))
        .collect::<Vec<_>>();
    let table = Arc::new(MemTable::try_new(parquet.schema(), partitions)?);
    let resident = ctx.read_table(table)?.create_physical_plan().await?;
    if resident.output_partitioning().partition_count() != threads {
        return Err("resident scan changed the requested partition layout".into());
    }
    let mut results = Vec::new();
    for (scenario, scan) in [("parquet", parquet), ("resident", resident)] {
        for workload in WORKLOADS {
            let mut report = measure_case(
                scenario,
                workload,
                &scan,
                &ctx,
                threads,
                samples,
                &input_shape,
                csv,
            )
            .await?;
            if scenario == "resident" {
                report["partition_input_shapes"] = json!(partition_shapes);
            }
            results.push(report);
        }
    }
    Ok(results)
}

pub(super) fn run() -> Result<()> {
    let args = std::env::args().skip(2).collect::<Vec<_>>();
    if !(3..=4).contains(&args.len()) {
        return Err("usage: parquet_compare --ablate DATA_DIR OUTPUT_DIR LABEL [samples]".into());
    }
    let samples = args
        .get(3)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(DEFAULT_SAMPLES);
    if samples == 0 || args[2].trim().is_empty() {
        return Err("samples must be positive and LABEL nonempty".into());
    }
    let data_dir = fs::canonicalize(&args[0])?;
    if !data_dir.is_dir() {
        return Err("DATA_DIR must be an existing Parquet directory".into());
    }
    let output_dir = Path::new(&args[1]);
    fs::create_dir_all(output_dir)?;
    if output_dir.join("ablation.json").exists() || output_dir.join("ablation_samples.csv").exists()
    {
        return Err(
            "OUTPUT_DIR already contains ablation results; use a fresh output directory".into(),
        );
    }
    let started_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let files = input_files(&data_dir)?;
    let build = build_metadata(&args[2])?;
    let mut cases = Vec::new();
    let mut csv =
        String::from("scenario,workload,threads,variant,sample,order_position,elapsed_ms\n");
    for threads in [1, 4] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .max_blocking_threads(threads)
            .enable_all()
            .build()?;
        cases.extend(runtime.block_on(measure_threads(&data_dir, threads, samples, &mut csv))?);
    }
    let report = json!({
        "format_version": 1,
        "label": args[2], "started_at_unix_seconds": started_at,
        "completed_at_unix_seconds": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        "metadata": build, "input_directory": data_dir, "input_files": files,
        "batch_rows": BATCH_ROWS, "samples": samples, "warmups": WARMUPS,
        "methodology": {
            "comparison": "Externally compiled source variants are labeled and measured by the same harness. This executable never patches source or changes compilation flags.",
            "datafusion_reference": "FilterProject uses checked value+1, matching Roc overflow semantics; DataFusion default coalescing and checked filter batch_size=1 are distinct references. Original wrapping-add baseline data is not used as the checked reference.",
            "timing": "Graph/physical operator construction outside timers. Plan reset, execution-state initialization, operator/driver work, full output collection and Roc task cleanup/output take inside timers. Full comparison, row-count checks, and output deallocation outside timers.",
            "resident": "Decode once per thread setting, sort batch handles by their first unique Int64 id to remove parallel collection-order variation, then round-robin immutable Arrow batches among MemTable partitions. The dataset must be generated by this benchmark. Reuse identical buffers/layout across every variant. No Parquet I/O/decode within resident timings.",
            "parquet": "Both engines use the same bare Parquet scan blueprint with all four columns and no predicate pushdown, reset per execution. Input hashing, decoding for resident preparation, correctness checks and warmups warm the file cache; these are not cold-cache measurements.",
            "rotation": "Within every workload/scenario, each variant runs once per round; first variant rotates each warmup/sample. Two warmups per variant. Workloads/scenarios use fixed order, and each thread count has a separately created and destroyed Tokio runtime.",
            "batch_shape": "Input shape comes from an untimed collection of the shared source. Output shape comes from each variant's full correctness precheck; checked DataFusion filter batch1 and Roc output histograms must match. Timed executions verify total output rows only.",
            "interpretation": "Resident timings still include operators, allocation, collection and scheduling. They do not by themselves establish an advantage of a push/pull execution model. Cross-build attribution requires changing one source factor and matching input fingerprints/build settings.",
            "identity": "Input files and executable use SHA-256. Optional ROC_ABLATION_SOURCE_SHA256 and ROC_ABLATION_BUILD_LABEL are captured at compilation, so an external build runner can bind each binary to its exact source snapshot.",
        },
        "cases": cases,
    });
    let mut json_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_dir.join("ablation.json"))?;
    json_file.write_all(&serde_json::to_vec_pretty(&report)?)?;
    let mut csv_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_dir.join("ablation_samples.csv"))?;
    csv_file.write_all(csv.as_bytes())?;
    println!(
        "Saved ablation.json and ablation_samples.csv in {}",
        output_dir.display()
    );
    Ok(())
}
