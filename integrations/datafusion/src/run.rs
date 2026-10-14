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

//! Runs SQL statements through Roc and through DataFusion, and compares the
//! results. DataFusion plans both; only downstream execution differs.
use crate::plan::translate;
use datafusion::{
    arrow::{
        array::{Array, AsArray},
        datatypes::{DataType, Float32Type, Float64Type},
        record_batch::RecordBatch,
        util::display::ArrayFormatter,
    },
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};
use roc::{
    kernel::ExecutionMode,
    pipeline::{Executor, PipelineGraphExecutor, build_pipeline_graph_with_mode},
};
use std::time::{Duration, Instant};

/// Runs Roc tasks on a Tokio runtime.
#[derive(Clone)]
pub struct TokioExecutor(pub tokio::runtime::Handle);

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

/// How far one statement got.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stage {
    /// DataFusion could not plan it.
    Plan(String),
    /// Roc has no translation of some node, expression or type.
    Translate(String),
    /// Roc failed while building pipelines or executing.
    Execute(String),
    /// Roc's result differs from DataFusion's.
    Validate(String),
    /// Equal results; `ordered` when the row sequence is equal too.
    Passed { ordered: bool },
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub stage: Stage,
    pub rows: usize,
    pub roc: Option<Duration>,
    pub datafusion: Option<Duration>,
}

/// The optimized plan of `sql`.
pub async fn plan(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<LogicalPlan> {
    let plan = ctx.state().create_logical_plan(sql).await?;
    ctx.state().optimize(&plan)
}

/// Executes `plan` through Roc with `parallelism` workers.
pub async fn run_roc(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    mode: ExecutionMode,
    parallelism: usize,
) -> Result<Vec<RecordBatch>, Stage> {
    let state = ctx.state();
    let query = translate(plan, &state)
        .await
        .map_err(|e| Stage::Translate(format!("{e:?}")))?;
    let graph = build_pipeline_graph_with_mode(&query.tree, mode)
        .map_err(|e| Stage::Execute(format!("{e:?}")))?;
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(TokioExecutor(tokio::runtime::Handle::current()))
        .with_parallelism(parallelism);
    let shutdown = executor.shutdown();
    let result = executor.execute().await;
    if result.is_err() {
        shutdown.request_shutdown();
    }
    shutdown.await;
    result.map_err(|e| Stage::Execute(format!("{e:?}")))?;
    Ok(query.collect.take())
}

/// One value of a result row, comparable across engines.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Value {
    Null,
    Float(f64),
    Text(String),
}

fn rows(batches: &[RecordBatch]) -> Vec<Vec<Value>> {
    let mut out = vec![];
    for batch in batches {
        let columns = batch.columns();
        let formatters = columns
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &Default::default()).unwrap())
            .collect::<Vec<_>>();
        for r in 0..batch.num_rows() {
            out.push(
                columns
                    .iter()
                    .zip(&formatters)
                    .map(|(c, f)| {
                        if c.is_null(r) {
                            Value::Null
                        } else {
                            match c.data_type() {
                                DataType::Float64 => {
                                    Value::Float(c.as_primitive::<Float64Type>().value(r))
                                }
                                DataType::Float32 => {
                                    Value::Float(c.as_primitive::<Float32Type>().value(r) as f64)
                                }
                                _ => Value::Text(f.value(r).to_string()),
                            }
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn close(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => {
            x == y
                || (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
                || (x.is_nan() && y.is_nan())
        }
        (a, b) => a == b,
    }
}

fn same(a: &[Vec<Value>], b: &[Vec<Value>]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.len() == y.len() && x.iter().zip(y).all(|(x, y)| close(x, y)))
}

fn key(row: &[Value]) -> Vec<String> {
    // Floats sort by a coarse rendering so near-equal values pair up.
    row.iter()
        .map(|v| match v {
            Value::Null => "\u{0}".to_string(),
            Value::Float(f) => format!("{f:.6e}"),
            Value::Text(t) => t.clone(),
        })
        .collect()
}

/// Compares Roc's result with DataFusion's: column types, then rows as an
/// ordered sequence or, failing that, as a multiset.
pub fn compare(roc: &[RecordBatch], expected: &[RecordBatch]) -> Stage {
    let types = |b: &[RecordBatch]| {
        b.first().map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>()
        })
    };
    if let (Some(a), Some(b)) = (types(roc), types(expected))
        && a != b
    {
        return Stage::Validate(format!("column types {a:?} differ from {b:?}"));
    }
    let (mut a, mut b) = (rows(roc), rows(expected));
    if same(&a, &b) {
        return Stage::Passed { ordered: true };
    }
    if a.len() != b.len() {
        return Stage::Validate(format!("{} rows, expected {}", a.len(), b.len()));
    }
    a.sort_by_key(|r| key(r));
    b.sort_by_key(|r| key(r));
    if same(&a, &b) {
        return Stage::Passed { ordered: false };
    }
    let first = a
        .iter()
        .zip(&b)
        .position(|(x, y)| !(x.len() == y.len() && x.iter().zip(y).all(|(x, y)| close(x, y))))
        .unwrap();
    Stage::Validate(format!(
        "row {first} of the sorted rows: {:?}, expected {:?}",
        a[first], b[first]
    ))
}

/// Plans, runs and validates one statement.
pub async fn run(
    ctx: &SessionContext,
    sql: &str,
    mode: ExecutionMode,
    parallelism: usize,
) -> Outcome {
    let mut outcome = Outcome {
        stage: Stage::Plan(String::new()),
        rows: 0,
        roc: None,
        datafusion: None,
    };
    let plan = match plan(ctx, sql).await {
        Ok(plan) => plan,
        Err(e) => {
            outcome.stage = Stage::Plan(e.to_string());
            return outcome;
        }
    };
    let start = Instant::now();
    let roc = run_roc(ctx, &plan, mode, parallelism).await;
    outcome.roc = Some(start.elapsed());
    let roc = match roc {
        Ok(batches) => batches,
        Err(stage) => {
            outcome.stage = stage;
            return outcome;
        }
    };
    let start = Instant::now();
    let expected = match ctx.execute_logical_plan(plan).await {
        Ok(frame) => frame.collect().await,
        Err(e) => Err(e),
    };
    outcome.datafusion = Some(start.elapsed());
    let expected = match expected {
        Ok(batches) => batches,
        Err(e) => {
            outcome.stage = Stage::Validate(format!("DataFusion failed: {e}"));
            return outcome;
        }
    };
    outcome.rows = expected.iter().map(RecordBatch::num_rows).sum();
    outcome.stage = compare(&roc, &expected);
    outcome
}
