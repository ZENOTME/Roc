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

//! Runs the TPC-DS corpus through Roc and DataFusion and reports, per
//! statement, the stage it reached and both engines' times.
//!
//!     cargo run --release -p roc-datafusion --example tpcds -- \
//!         <data-dir> [--mode compile|zero] [--workers N] [--timeout SECONDS] [--verbose]
//!         [--repeat N: best of N runs of each engine] [--profile: with --repeat, Roc alone]
//!         [--explain: print pipelines] [--threads N: runtime threads, default --workers]
//!         [--set datafusion.option=value] [--sql STATEMENT: run it instead]
//!         [--report FILE] [query...]
use roc::kernel::ExecutionMode;
use roc_datafusion::{
    run::{Stage, run_repeated},
    tpcds,
};
use std::fmt::Write;

/// Both engines run on a runtime of `--workers` threads, and DataFusion
/// plans as many partitions, so they compare on the same cores.
fn main() -> datafusion::error::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let value = |flag: &str| {
        args.iter()
            .position(|a| a == flag)
            .map(|i| args[i + 1].parse::<usize>().unwrap())
    };
    let workers = value("--workers").unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(value("--threads").unwrap_or(workers))
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_all())
}

async fn run_all() -> datafusion::error::Result<()> {
    let mut args = std::env::args().skip(1);
    let data = std::path::PathBuf::from(args.next().expect("a data directory"));
    let mut mode = ExecutionMode::Compile;
    let mut workers = 4;
    let mut report = None;
    let mut timeout = 120;
    let mut verbose = false;
    let mut repeat = 0;
    let mut profile = false;
    let mut explain = false;
    let mut sql = None;
    let mut only = vec![];
    let mut settings = vec![];
    while let Some(a) = args.next() {
        match a.as_str() {
            "--mode" => {
                mode = match args.next().as_deref() {
                    Some("zero") => ExecutionMode::ZeroCompile,
                    _ => ExecutionMode::Compile,
                }
            }
            "--workers" => workers = args.next().unwrap().parse().unwrap(),
            "--threads" => drop(args.next()),
            "--report" => report = args.next(),
            "--timeout" => timeout = args.next().unwrap().parse().unwrap(),
            "--verbose" => verbose = true,
            "--explain" => explain = true,
            "--sql" => sql = args.next(),
            "--repeat" => repeat = args.next().unwrap().parse().unwrap(),
            "--profile" => profile = true,
            "--set" => settings.push(args.next().unwrap()),
            q => only.push(q.to_string()),
        }
    }
    let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(workers);
    for setting in &settings {
        let (key, value) = setting.split_once('=').expect("--set key=value");
        config = config.set_str(key, value);
    }
    let ctx = datafusion::prelude::SessionContext::new_with_config(config);
    tpcds::register_tables(&ctx, &data).await?;
    let mut table = String::new();
    writeln!(
        table,
        "| statement | stage | rows | Roc ms | DataFusion ms | translate / build / execute ms | sink finish ms | detail |"
    )
    .unwrap();
    writeln!(
        table,
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |"
    )
    .unwrap();
    let mut passed = 0;
    let mut statements = tpcds::statements(&tpcds::query_dir())?;
    if let Some(sql) = sql {
        statements = vec![tpcds::Statement {
            name: "sql".into(),
            sql,
        }];
    }
    let mut total = 0;
    for s in &statements {
        if !only.is_empty() && !only.contains(&s.name) {
            continue;
        }
        total += 1;
        if explain {
            let plan = roc_datafusion::run::plan(&ctx, &s.sql).await?;
            match roc_datafusion::run::explain(&ctx, &plan, mode).await {
                Ok(text) => println!("== {}\n{text}", s.name),
                Err(stage) => println!("== {}: {stage:?}", s.name),
            }
            continue;
        }
        if repeat > 0 && profile {
            // Profiling: run Roc alone, again and again.
            let plan = roc_datafusion::run::plan(&ctx, &s.sql).await?;
            let mut best = std::time::Duration::MAX;
            for _ in 0..repeat {
                let start = std::time::Instant::now();
                let mut phases = Default::default();
                roc_datafusion::run::run_roc(&ctx, &plan, mode, workers, &mut phases)
                    .await
                    .unwrap();
                best = best.min(start.elapsed());
            }
            println!(
                "{}: best of {repeat}: {:.1} ms",
                s.name,
                best.as_secs_f64() * 1e3
            );
            continue;
        }
        let limit = std::time::Duration::from_secs(timeout);
        let runs = repeat.max(1);
        let outcome = match tokio::time::timeout(
            limit,
            run_repeated(&ctx, &s.sql, mode, workers, runs),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => roc_datafusion::run::Outcome {
                stage: Stage::Execute(format!("timed out after {timeout} s")),
                rows: 0,
                roc: None,
                datafusion: None,
                phases: Default::default(),
            },
        };
        let (stage, detail) = match &outcome.stage {
            Stage::Plan(d) => ("plan", d.clone()),
            Stage::Translate(d) => ("translate", d.clone()),
            Stage::Execute(d) => ("execute", d.clone()),
            Stage::Validate(d) => ("validate", d.clone()),
            Stage::Passed { ordered } => {
                passed += 1;
                (
                    "passed",
                    if *ordered { "" } else { "unordered" }.to_string(),
                )
            }
        };
        if verbose && !detail.is_empty() {
            eprintln!("{}: {detail}", s.name);
        }
        // The first line of a diagnostic names the failure.
        let detail = detail.lines().next().unwrap_or("").replace('|', "\\|");
        let ms = |d: Option<std::time::Duration>| {
            d.map_or("".into(), |d| format!("{:.1}", d.as_secs_f64() * 1e3))
        };
        let p = outcome.phases;
        let line = format!(
            "| {} | {stage} | {} | {} | {} | {} / {} / {} | {} | {} |",
            s.name,
            outcome.rows,
            ms(outcome.roc),
            ms(outcome.datafusion),
            ms(Some(p.translate)),
            ms(Some(p.build)),
            ms(Some(p.execute)),
            ms(Some(p.sink_finish)),
            detail.chars().take(300).collect::<String>()
        );
        println!("{line}");
        writeln!(table, "{line}").unwrap();
    }
    println!("passed {passed} / {total}");
    if let Some(path) = report {
        let header =
            format!("TPC-DS SF1, mode {mode:?}, {workers} workers: passed {passed} / {total}\n\n");
        std::fs::write(path, header + &table).unwrap();
    }
    Ok(())
}
