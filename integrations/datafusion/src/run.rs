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
use datafusion::arrow::compute::{SortColumn, SortOptions, concat, lexsort_to_indices, take};
use datafusion::{
    arrow::{
        array::{Array, AsArray},
        datatypes::{DataType, Float32Type, Float64Type},
        record_batch::RecordBatch,
        util::display::ArrayFormatter,
    },
    logical_expr::{Expr, LogicalPlan},
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
    /// Roc's time translating the plan, building pipelines (which compiles
    /// programs) and executing them.
    pub phases: Phases,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Phases {
    pub translate: Duration,
    pub build: Duration,
    pub execute: Duration,
}

/// The optimized plan of `sql`.
pub async fn plan(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<LogicalPlan> {
    let plan = ctx.state().create_logical_plan(sql).await?;
    ctx.state().optimize(&plan)
}

/// The pipelines Roc would run for `plan`: their kernels and steps.
pub async fn explain(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    mode: ExecutionMode,
) -> Result<String, Stage> {
    let query = translate(plan, &ctx.state())
        .await
        .map_err(|e| Stage::Translate(format!("{e:?}")))?;
    let graph = build_pipeline_graph_with_mode(&query.tree, mode)
        .map_err(|e| Stage::Execute(format!("{e:?}")))?;
    Ok(graph.explain())
}

/// Executes `plan` through Roc with `parallelism` workers.
pub async fn run_roc(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    mode: ExecutionMode,
    parallelism: usize,
    phases: &mut Phases,
) -> Result<Vec<RecordBatch>, Stage> {
    let state = ctx.state();
    let start = Instant::now();
    let query = translate(plan, &state)
        .await
        .map_err(|e| Stage::Translate(format!("{e:?}")))?;
    phases.translate = start.elapsed();
    let start = Instant::now();
    let graph = build_pipeline_graph_with_mode(&query.tree, mode)
        .map_err(|e| Stage::Execute(format!("{e:?}")))?;
    phases.build = start.elapsed();
    let start = Instant::now();
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
    phases.execute = start.elapsed();
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

/// The output columns the result of `plan` is ordered by, when its order
/// is a sort by output columns; None when it has no order or one this
/// cannot check.
pub fn output_order(plan: &LogicalPlan) -> Option<Vec<(usize, SortOptions)>> {
    match plan {
        LogicalPlan::Sort(s) => s
            .expr
            .iter()
            .map(|k| match &k.expr {
                Expr::Column(c) => Some((
                    s.input.schema().index_of_column(c).ok()?,
                    SortOptions {
                        descending: !k.asc,
                        nulls_first: k.nulls_first,
                    },
                )),
                _ => None,
            })
            .collect(),
        LogicalPlan::Limit(l) => output_order(&l.input),
        LogicalPlan::SubqueryAlias(a) => output_order(&a.input),
        LogicalPlan::Projection(p) => {
            let input = output_order(&p.input)?;
            let schema = p.input.schema();
            input
                .into_iter()
                .map(|(i, options)| {
                    let position = p.expr.iter().position(|e| {
                        let mut e = e;
                        while let Expr::Alias(a) = e {
                            e = &a.expr;
                        }
                        matches!(e, Expr::Column(c) if schema.index_of_column(c).ok() == Some(i))
                    })?;
                    Some((position, options))
                })
                .collect()
        }
        _ => None,
    }
}

/// Whether the rows are ordered by `order`: sorting them by those keys
/// leaves the key columns unchanged.
fn ordered(batches: &[RecordBatch], order: &[(usize, SortOptions)]) -> bool {
    if batches.is_empty() {
        return true;
    }
    let columns = order
        .iter()
        .map(|&(i, _)| {
            let parts = batches
                .iter()
                .map(|b| b.column(i).as_ref())
                .collect::<Vec<_>>();
            concat(&parts).unwrap()
        })
        .collect::<Vec<_>>();
    let keys = columns
        .iter()
        .zip(order)
        .map(|(values, (_, options))| SortColumn {
            values: values.clone(),
            options: Some(*options),
        })
        .collect::<Vec<_>>();
    let indices = lexsort_to_indices(&keys, None).unwrap();
    columns
        .iter()
        .all(|c| take(c.as_ref(), &indices, None).unwrap().to_data() == c.to_data())
}

/// Compares Roc's result with DataFusion's: column types, then rows as an
/// ordered sequence or, failing that, as a multiset ordered by `order`.
pub fn compare(
    roc: &[RecordBatch],
    expected: &[RecordBatch],
    order: Option<&[(usize, SortOptions)]>,
) -> Stage {
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
        // Rows with equal sort keys may come in any order.
        if let Some(order) = order
            && !ordered(roc, order)
        {
            return Stage::Validate(format!("rows are not ordered by {order:?}"));
        }
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
    run_repeated(ctx, sql, mode, parallelism, 1).await
}

/// Like `run`, with each engine running `runs` times: times are the best
/// run's, and the last results are validated.
pub async fn run_repeated(
    ctx: &SessionContext,
    sql: &str,
    mode: ExecutionMode,
    parallelism: usize,
    runs: usize,
) -> Outcome {
    let mut outcome = Outcome {
        stage: Stage::Plan(String::new()),
        rows: 0,
        roc: None,
        datafusion: None,
        phases: Phases::default(),
    };
    let plan = match plan(ctx, sql).await {
        Ok(plan) => plan,
        Err(e) => {
            outcome.stage = Stage::Plan(e.to_string());
            return outcome;
        }
    };
    let best = |best: &mut Option<Duration>, start: Instant| {
        let t = start.elapsed();
        *best = Some(best.map_or(t, |b| b.min(t)));
    };
    let mut roc = Ok(vec![]);
    for _ in 0..runs.max(1) {
        let start = Instant::now();
        let mut phases = Phases::default();
        roc = run_roc(ctx, &plan, mode, parallelism, &mut phases).await;
        if outcome.roc.is_none_or(|b| start.elapsed() < b) {
            outcome.phases = phases;
        }
        best(&mut outcome.roc, start);
        if roc.is_err() {
            break;
        }
    }
    let roc = match roc {
        Ok(batches) => batches,
        Err(stage) => {
            outcome.stage = stage;
            return outcome;
        }
    };
    let checked = plan.clone();
    let mut expected = Ok(vec![]);
    for _ in 0..runs.max(1) {
        let start = Instant::now();
        expected = match ctx.execute_logical_plan(plan.clone()).await {
            Ok(frame) => frame.collect().await,
            Err(e) => Err(e),
        };
        best(&mut outcome.datafusion, start);
    }
    let order = output_order(&checked);
    let expected = match expected {
        Ok(batches) => batches,
        Err(e) => {
            outcome.stage = Stage::Validate(format!("DataFusion failed: {e}"));
            return outcome;
        }
    };
    outcome.rows = expected.iter().map(RecordBatch::num_rows).sum();
    outcome.stage = compare(&roc, &expected, order.as_deref());
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;

    fn batch(keys: &[i64], values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ];
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn rows_must_follow_the_sort_keys_but_ties_may_come_in_any_order() {
        let order = [(0, SortOptions::default())];
        let expected = batch(&[1, 1, 2], &["a", "b", "c"]);
        let tie_swapped = batch(&[1, 1, 2], &["b", "a", "c"]);
        let misordered = batch(&[2, 1, 1], &["c", "a", "b"]);
        assert_eq!(
            compare(&[expected.clone()], &[expected.clone()], Some(&order)),
            Stage::Passed { ordered: true }
        );
        assert_eq!(
            compare(&[tie_swapped], &[expected.clone()], Some(&order)),
            Stage::Passed { ordered: false }
        );
        assert!(matches!(
            compare(&[misordered.clone()], &[expected.clone()], Some(&order)),
            Stage::Validate(_)
        ));
        // Without an order to check, equal multisets pass.
        assert_eq!(
            compare(&[misordered], &[expected], None),
            Stage::Passed { ordered: false }
        );
    }
}
