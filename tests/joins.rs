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

//! Joins as expansions of the probe rows into their matches, compared with
//! nested loops over the same rows: duplicates, NULL keys, residual filters,
//! fan-out across windows, nested joins and work after the join, in every
//! execution mode with one and four workers.
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    error::ErrorKind,
    expr::{
        agg::{AggregateExpression, AggregateFunction},
        scalar::{FunctionExpression, FunctionKind, ReferenceExpression, ScalarExprRef},
    },
    kernel::ExecutionMode,
    operator::{AggregateOperator, JoinOperator, JoinType, OperatorTreeNode, ProjectOperator},
};
use std::sync::Arc;

mod support;
use support::exec::{execute, execute_all, scan};

/// `(key, value)` rows: keys repeat, some are NULL.
fn probe_rows(n: i64) -> Vec<(Option<i64>, i64)> {
    (0..n)
        .map(|i| ((i % 13 != 0).then_some(i % 37), i))
        .collect()
}
fn build_rows() -> Vec<(Option<i64>, String)> {
    let mut rows = vec![];
    for k in 0..30i64 {
        // Keys below 10 match three times, others once; 30..37 never.
        for copy in 0..(if k < 10 { 3 } else { 1 }) {
            rows.push((Some(k), format!("b{k}-{copy}")));
        }
    }
    rows.push((None, "null key".into()));
    rows
}

fn batches(columns: Vec<(&str, ArrayRef)>, chunk: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let rows = columns[0].1.len();
    let batch = RecordBatch::try_new(schema, columns.into_iter().map(|c| c.1).collect()).unwrap();
    (0..rows)
        .step_by(chunk)
        .map(|o| batch.slice(o, chunk.min(rows - o)))
        .collect()
}

struct Plan {
    columns: ColumnContext,
    pk: ColumnId,
    pv: ColumnId,
    bk: ColumnId,
    bw: ColumnId,
    probe: Vec<(Option<i64>, i64)>,
    build: Vec<(Option<i64>, String)>,
}

impl Plan {
    fn new(probe: Vec<(Option<i64>, i64)>, build: Vec<(Option<i64>, String)>) -> Self {
        let mut columns = ColumnContext::new();
        let int = ColumnType::new(DataType::Int64, true);
        let pk = columns.allocate_named(int.clone(), "pk");
        let pv = columns.allocate_named(int.clone(), "pv");
        let bk = columns.allocate_named(int, "bk");
        let bw = columns.allocate_named(ColumnType::new(DataType::Utf8, true), "bw");
        Self {
            columns,
            pk,
            pv,
            bk,
            bw,
            probe,
            build,
        }
    }
    fn probe_scan(&self) -> OperatorTreeNode {
        let k = Int64Array::from(self.probe.iter().map(|r| r.0).collect::<Vec<_>>());
        let v = Int64Array::from(self.probe.iter().map(|r| r.1).collect::<Vec<_>>());
        scan(
            batches(vec![("pk", Arc::new(k)), ("pv", Arc::new(v))], 700),
            vec![self.pk, self.pv],
        )
    }
    fn build_scan(&self) -> OperatorTreeNode {
        let k = Int64Array::from(self.build.iter().map(|r| r.0).collect::<Vec<_>>());
        let w = StringArray::from(self.build.iter().map(|r| r.1.as_str()).collect::<Vec<_>>());
        scan(
            batches(vec![("bk", Arc::new(k)), ("bw", Arc::new(w))], 16),
            vec![self.bk, self.bw],
        )
    }
    fn column(&mut self, name: &str, t: DataType) -> ColumnId {
        self.columns.allocate_named(ColumnType::new(t, true), name)
    }
    /// A join of the probe and build scans; returns it and its join columns.
    fn join(
        &mut self,
        join_type: JoinType,
        filter: impl FnOnce(&mut Self, [ColumnId; 4]) -> Option<ScalarExprRef>,
    ) -> (OperatorTreeNode, [ColumnId; 4], Option<ColumnId>) {
        let out = [
            self.column("k", DataType::Int64),
            self.column("v", DataType::Int64),
            self.column("bk2", DataType::Int64),
            self.column("w", DataType::Utf8),
        ];
        let mark = (join_type == JoinType::Mark).then(|| self.column("mark", DataType::Boolean));
        let filter = filter(self, out);
        let join = JoinOperator::try_new(
            join_type,
            vec![(self.pk, self.bk)],
            false,
            vec![(self.pk, out[0]), (self.pv, out[1])],
            vec![(self.bk, out[2]), (self.bw, out[3])],
            filter,
            mark,
        )
        .unwrap();
        let node = OperatorTreeNode::new(join, vec![self.probe_scan(), self.build_scan()]);
        (node, out, mark)
    }
}

fn reference(c: ColumnId) -> ScalarExprRef {
    ReferenceExpression::new(c).into_ref()
}

fn show(v: Option<i64>) -> String {
    v.map_or("NULL".into(), |v| v.to_string())
}

/// Expected rows by nested loops; `filter` sees probe and build rows.
fn expected(
    plan: &Plan,
    join_type: JoinType,
    filter: impl Fn(&(Option<i64>, i64), &(Option<i64>, String)) -> bool,
) -> Vec<Vec<String>> {
    let mut rows = vec![];
    for p in &plan.probe {
        let matches = plan
            .build
            .iter()
            .filter(|b| p.0.is_some() && p.0 == b.0 && filter(p, b))
            .collect::<Vec<_>>();
        let probe = vec![show(p.0), p.1.to_string()];
        match join_type {
            JoinType::Inner | JoinType::Left => {
                for b in &matches {
                    rows.push([probe.clone(), vec![show(b.0), b.1.clone()]].concat());
                }
                if matches.is_empty() && join_type == JoinType::Left {
                    rows.push([probe.clone(), vec!["NULL".into(), "NULL".into()]].concat());
                }
            }
            JoinType::Semi if !matches.is_empty() => rows.push(probe),
            JoinType::Anti if matches.is_empty() => rows.push(probe),
            JoinType::Mark => rows.push([probe, vec![(!matches.is_empty()).to_string()]].concat()),
            _ => {}
        }
    }
    rows.sort();
    rows
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

#[test]
fn every_join_type_matches_nested_loops() {
    for join_type in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Semi,
        JoinType::Anti,
        JoinType::Mark,
    ] {
        let mut plan = Plan::new(probe_rows(3000), build_rows());
        let (join, _, _) = plan.join(join_type, |_, _| None);
        let rows = execute_all(&plan.columns, &join).unwrap();
        assert_eq!(
            sorted(rows),
            expected(&plan, join_type, |_, _| true),
            "{join_type:?}"
        );
    }
}

#[test]
fn residual_filters_select_matches() {
    for join_type in [
        JoinType::Inner,
        JoinType::Semi,
        JoinType::Anti,
        JoinType::Mark,
    ] {
        let mut plan = Plan::new(probe_rows(2500), build_rows());
        // v % 3 != 0 AND w > 'b2'
        let (join, _, _) = plan.join(join_type, |plan, out| {
            let three = plan.column("three", DataType::Int64);
            let three = roc::expr::scalar::ConstantExpression::int64(Some(3), three).into_ref();
            let rem = plan.column("rem", DataType::Int64);
            let rem =
                FunctionExpression::binary(FunctionKind::Remainder, reference(out[1]), three, rem)
                    .into_ref();
            let zero = plan.column("zero", DataType::Int64);
            let zero = roc::expr::scalar::ConstantExpression::int64(Some(0), zero).into_ref();
            let a = plan.column("a", DataType::Boolean);
            let a = FunctionExpression::binary(FunctionKind::NotEqual, rem, zero, a).into_ref();
            let b2 = plan.column("b2", DataType::Utf8);
            let b2 = roc::expr::scalar::ConstantExpression::string(Some("b2"), b2).into_ref();
            let b = plan.column("b", DataType::Boolean);
            let b = FunctionExpression::binary(FunctionKind::GreaterThan, reference(out[3]), b2, b)
                .into_ref();
            let both = plan.column("both", DataType::Boolean);
            Some(
                roc::expr::scalar::ConjunctionExpression::new(
                    roc::expr::scalar::Conjunction::And,
                    vec![a, b],
                    both,
                )
                .into_ref(),
            )
        });
        let rows = execute_all(&plan.columns, &join).unwrap();
        let filter =
            |p: &(Option<i64>, i64), b: &(Option<i64>, String)| p.1 % 3 != 0 && b.1.as_str() > "b2";
        assert_eq!(
            sorted(rows),
            expected(&plan, join_type, filter),
            "{join_type:?}"
        );
    }
}

#[test]
fn products_pair_every_row_and_empty_builds_match_nothing() {
    let mut plan = Plan::new(probe_rows(40), build_rows());
    let out = [
        plan.column("k", DataType::Int64),
        plan.column("v", DataType::Int64),
        plan.column("bk2", DataType::Int64),
        plan.column("w", DataType::Utf8),
    ];
    let product = JoinOperator::try_new(
        JoinType::Inner,
        vec![],
        false,
        vec![(plan.pk, out[0]), (plan.pv, out[1])],
        vec![(plan.bk, out[2]), (plan.bw, out[3])],
        None,
        None,
    )
    .unwrap();
    let node = OperatorTreeNode::new(product, vec![plan.probe_scan(), plan.build_scan()]);
    let rows = execute_all(&plan.columns, &node).unwrap();
    assert_eq!(rows.len(), plan.probe.len() * plan.build.len());

    for join_type in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Anti,
        JoinType::Mark,
    ] {
        let mut plan = Plan::new(probe_rows(100), vec![]);
        let (join, _, _) = plan.join(join_type, |_, _| None);
        let rows = execute_all(&plan.columns, &join).unwrap();
        assert_eq!(
            sorted(rows),
            expected(&plan, join_type, |_, _| true),
            "{join_type:?}"
        );
    }
}

#[test]
fn work_after_a_join_reads_both_sides_and_aggregates_matches() {
    let mut plan = Plan::new(probe_rows(5000), build_rows());
    let (join, out, _) = plan.join(JoinType::Inner, |_, _| None);
    // SUM(v + bk) GROUP BY w: the projection reads a probe and a build column.
    let sum = plan.column("s", DataType::Int64);
    let add =
        FunctionExpression::binary(FunctionKind::Add, reference(out[1]), reference(out[2]), sum)
            .into_ref();
    let total = plan.column("total", DataType::Int64);
    let count = plan.column("count", DataType::Int64);
    let w = plan.column("group", DataType::Utf8);
    let aggregate = AggregateOperator::try_new(
        vec![ReferenceExpression::new(out[3]).into_ref()],
        vec![
            Arc::new(AggregateExpression::new(
                AggregateFunction::Sum,
                vec![add],
                total,
            )),
            Arc::new(AggregateExpression::new(
                AggregateFunction::Count,
                vec![],
                count,
            )),
        ],
    )
    .unwrap();
    let _ = w;
    let node = OperatorTreeNode::new(aggregate, vec![join]);
    let rows = execute_all(&plan.columns, &node).unwrap();
    let mut expected = std::collections::BTreeMap::<String, (i64, i64)>::new();
    for p in &plan.probe {
        for b in &plan.build {
            if p.0.is_some() && p.0 == b.0 {
                let e = expected.entry(b.1.clone()).or_default();
                e.0 += p.1 + b.0.unwrap();
                e.1 += 1;
            }
        }
    }
    let expected = expected
        .into_iter()
        .map(|(w, (s, c))| vec![w, s.to_string(), c.to_string()])
        .collect::<Vec<_>>();
    assert_eq!(sorted(rows), expected);
}

#[test]
fn nested_joins_import_through_every_level() {
    let mut plan = Plan::new(probe_rows(2000), build_rows());
    let (inner, out, _) = plan.join(JoinType::Inner, |_, _| None);
    // Join the result again on the build key, against the same build rows.
    let k2 = plan.column("bk3", DataType::Int64);
    let w2 = plan.column("w2", DataType::Utf8);
    let b_k = plan.column("bk_second", DataType::Int64);
    let b_w = plan.column("bw_second", DataType::Utf8);
    let keep = [
        plan.column("v_out", DataType::Int64),
        plan.column("w_out", DataType::Utf8),
    ];
    let second = JoinOperator::try_new(
        JoinType::Inner,
        vec![(out[2], b_k)],
        false,
        vec![(out[1], keep[0]), (out[3], keep[1])],
        vec![(b_k, k2), (b_w, w2)],
        None,
        None,
    )
    .unwrap();
    let build2 = {
        let k = Int64Array::from(plan.build.iter().map(|r| r.0).collect::<Vec<_>>());
        let w = StringArray::from(plan.build.iter().map(|r| r.1.as_str()).collect::<Vec<_>>());
        scan(
            batches(vec![("bk", Arc::new(k)), ("bw", Arc::new(w))], 5),
            vec![b_k, b_w],
        )
    };
    let node = OperatorTreeNode::new(second, vec![inner, build2]);
    let rows = execute_all(&plan.columns, &node).unwrap();
    let mut expected = vec![];
    for p in &plan.probe {
        for b in &plan.build {
            if p.0.is_some() && p.0 == b.0 {
                for c in &plan.build {
                    if c.0 == b.0 {
                        expected.push(vec![p.1.to_string(), b.1.clone(), show(c.0), c.1.clone()]);
                    }
                }
            }
        }
    }
    assert_eq!(sorted(rows), sorted(expected));
}

#[test]
fn errors_after_an_expansion_report_the_probe_row() {
    // 1 / (bk - 5) fails on the first probe row whose key is 5.
    let mut plan = Plan::new(probe_rows(3000), build_rows());
    let (join, out, _) = plan.join(JoinType::Inner, |_, _| None);
    let five = plan.column("five", DataType::Int64);
    let five = roc::expr::scalar::ConstantExpression::int64(Some(5), five).into_ref();
    let diff = plan.column("diff", DataType::Int64);
    let diff = FunctionExpression::binary(FunctionKind::Subtract, reference(out[2]), five, diff)
        .into_ref();
    let one = plan.column("one", DataType::Int64);
    let one = roc::expr::scalar::ConstantExpression::int64(Some(1), one).into_ref();
    let q = plan.column("q", DataType::Int64);
    let q = FunctionExpression::binary(FunctionKind::Divide, one, diff, q).into_ref();
    let node = OperatorTreeNode::new(ProjectOperator::new(vec![q]), vec![join]);
    let first = plan.probe.iter().position(|p| p.0 == Some(5)).unwrap();
    for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
        let error = execute(&plan.columns, &node, mode, 1).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DivisionByZero, "{error:?}");
        let context = error.frames()[0].context.as_ref().unwrap();
        // Probe chunks have 700 rows.
        assert_eq!(
            context.fields,
            [("row", (first % 700).to_string())],
            "{error:?}"
        );
    }
}

/// Gathered build decimals live in window slots and die inside one fused
/// run: `(b1 * 3) + 1` and `(b2 * 5) + 1`. Each sum may take a slot pair its
/// product's input just freed, so each 128-bit output must be moved to a
/// slot pair of its own.
#[test]
fn fused_runs_over_gathered_decimals_keep_their_slots() {
    use arrow::array::Decimal128Array;
    use roc::expr::scalar::ConstantExpression;
    let t = DataType::Decimal128(18, 2);
    let mut columns = ColumnContext::new();
    let pk = columns.allocate_named(ColumnType::new(DataType::Int64, true), "pk");
    let bk = columns.allocate_named(ColumnType::new(DataType::Int64, true), "bk");
    let b1 = columns.allocate_named(ColumnType::new(t.clone(), true), "b1");
    let b2 = columns.allocate_named(ColumnType::new(t.clone(), true), "b2");
    let probe_keys = (0..2500i64).map(|i| i % 50).collect::<Vec<_>>();
    let probe = scan(
        batches(
            vec![("pk", Arc::new(Int64Array::from(probe_keys.clone())))],
            1000,
        ),
        vec![pk],
    );
    let v1 = (0..50i128).map(|k| k * 1_000_003 - 77).collect::<Vec<_>>();
    let v2 = (0..50i128)
        .map(|k| 9_999_999_999 - k * 13)
        .collect::<Vec<_>>();
    let decimal = |v: &[i128]| -> ArrayRef {
        Arc::new(
            Decimal128Array::from_iter_values(v.iter().copied())
                .with_precision_and_scale(18, 2)
                .unwrap(),
        )
    };
    let build = scan(
        batches(
            vec![
                ("bk", Arc::new(Int64Array::from_iter_values(0..50))),
                ("b1", decimal(&v1)),
                ("b2", decimal(&v2)),
            ],
            50,
        ),
        vec![bk, b1, b2],
    );
    let out = [b1, b2].map(|_| columns.allocate(ColumnType::new(t.clone(), true)));
    let k_out = columns.allocate(ColumnType::new(DataType::Int64, true));
    let bk_out = columns.allocate(ColumnType::new(DataType::Int64, true));
    let join = JoinOperator::try_new(
        JoinType::Inner,
        vec![(pk, bk)],
        false,
        vec![(pk, k_out)],
        vec![(bk, bk_out), (b1, out[0]), (b2, out[1])],
        None,
        None,
    )
    .unwrap();
    let join = OperatorTreeNode::new(join, vec![probe, build]);
    let mut exprs = vec![];
    for (c, factor) in [(out[0], 300), (out[1], 500)] {
        let f = columns.allocate(ColumnType::new(t.clone(), false));
        let product = columns.allocate(ColumnType::new(DataType::Decimal128(37, 4), true));
        let product = FunctionExpression::binary(
            FunctionKind::Multiply,
            reference(c),
            ConstantExpression::try_new(decimal(&[factor]), f)
                .unwrap()
                .into_ref(),
            product,
        )
        .into_ref();
        let one = columns.allocate(ColumnType::new(DataType::Decimal128(37, 4), false));
        let one = ConstantExpression::try_new(
            Arc::new(
                Decimal128Array::from_iter_values([10_000i128])
                    .with_precision_and_scale(37, 4)
                    .unwrap(),
            ),
            one,
        )
        .unwrap()
        .into_ref();
        let sum = columns.allocate(ColumnType::new(DataType::Decimal128(38, 4), true));
        exprs.push(FunctionExpression::binary(FunctionKind::Add, product, one, sum).into_ref());
    }
    let node = OperatorTreeNode::new(ProjectOperator::new(exprs), vec![join]);
    let mut rows = execute_all(&columns, &node).unwrap();
    let show = |v: i128| {
        let sign = if v < 0 { "-" } else { "" };
        format!("{sign}{}.{:04}", v.abs() / 10_000, v.abs() % 10_000)
    };
    let mut expected = probe_keys
        .iter()
        .map(|&k| {
            let k = k as usize;
            vec![show(v1[k] * 300 + 10_000), show(v2[k] * 500 + 10_000)]
        })
        .collect::<Vec<_>>();
    rows.sort();
    expected.sort();
    assert_eq!(rows, expected);
}
