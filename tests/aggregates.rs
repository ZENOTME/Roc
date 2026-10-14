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

//! Decimal sums and averages, sample standard deviations and grouping sets,
//! merged across workers, in every execution mode.
use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    expr::{
        agg::{AggregateExpression, AggregateFunction},
        scalar::ReferenceExpression,
    },
    operator::{AggregateOperator, OperatorTreeNode},
};
use std::collections::BTreeMap;
use std::sync::Arc;

mod support;
use support::exec::{execute_all, scan};

struct Data {
    group: Vec<Option<&'static str>>,
    amount: Vec<Option<i128>>,
    x: Vec<Option<f64>>,
}

fn data(rows: usize) -> Data {
    let groups = [Some("a"), Some("b"), None, Some("c")];
    Data {
        group: (0..rows).map(|i| groups[i % 4]).collect(),
        amount: (0..rows)
            .map(|i| (i % 7 != 3).then_some(i as i128 * 37 - 5_000))
            .collect(),
        x: (0..rows)
            .map(|i| (i % 5 != 0).then_some(i as f64 * 0.5))
            .collect(),
    }
}

struct Plan {
    columns: ColumnContext,
    group: ColumnId,
    amount: ColumnId,
    x: ColumnId,
    node: OperatorTreeNode,
}

fn plan(d: &Data) -> Plan {
    let mut columns = ColumnContext::new();
    let group = columns.allocate_named(ColumnType::new(DataType::Utf8, true), "g");
    let amount =
        columns.allocate_named(ColumnType::new(DataType::Decimal128(7, 2), true), "amount");
    let x = columns.allocate_named(ColumnType::new(DataType::Float64, true), "x");
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(7, 2), true),
        Field::new("x", DataType::Float64, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(d.group.clone())),
        Arc::new(
            Decimal128Array::from(d.amount.clone())
                .with_precision_and_scale(7, 2)
                .unwrap(),
        ),
        Arc::new(Float64Array::from(d.x.clone())),
    ];
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    let batches = (0..batch.num_rows())
        .step_by(1500)
        .map(|o| batch.slice(o, 1500.min(batch.num_rows() - o)))
        .collect();
    Plan {
        node: scan(batches, vec![group, amount, x]),
        columns,
        group,
        amount,
        x,
    }
}

fn decimal(v: i128, scale: u32) -> String {
    let neg = v < 0;
    let a = v.unsigned_abs();
    let p = 10u128.pow(scale);
    format!(
        "{}{}.{:0w$}",
        if neg { "-" } else { "" },
        a / p,
        a % p,
        w = scale as usize
    )
}

#[test]
fn decimal_aggregates_and_stddev_merge_exactly() {
    let d = data(10_000);
    let mut p = plan(&d);
    let sum = p
        .columns
        .allocate_named(ColumnType::new(DataType::Decimal128(17, 2), true), "sum");
    let avg = p
        .columns
        .allocate_named(ColumnType::new(DataType::Decimal128(11, 6), true), "avg");
    let sd = p
        .columns
        .allocate_named(ColumnType::new(DataType::Float64, true), "sd");
    let reference = |c| ReferenceExpression::new(c).into_ref();
    let aggregate = AggregateOperator::try_new(
        vec![reference(p.group)],
        vec![
            Arc::new(AggregateExpression::new(
                AggregateFunction::Sum,
                vec![reference(p.amount)],
                sum,
            )),
            Arc::new(AggregateExpression::new(
                AggregateFunction::Avg,
                vec![reference(p.amount)],
                avg,
            )),
            Arc::new(AggregateExpression::new(
                AggregateFunction::StddevSamp,
                vec![reference(p.x)],
                sd,
            )),
        ],
    )
    .unwrap();
    let node = OperatorTreeNode::new(aggregate, vec![p.node]);
    let rows = execute_all(&p.columns, &node).unwrap();
    let mut groups: BTreeMap<Option<&str>, (i128, i64, Vec<f64>)> = BTreeMap::new();
    for i in 0..d.group.len() {
        let g = groups.entry(d.group[i]).or_default();
        if let Some(a) = d.amount[i] {
            g.0 += a;
            g.1 += 1;
        }
        if let Some(x) = d.x[i] {
            g.2.push(x);
        }
    }
    let mut actual = rows;
    actual.sort();
    for row in &actual {
        let key = (row[0] != "NULL").then_some(row[0].as_str());
        let (sum, count, xs) = &groups[&key];
        assert_eq!(row[1], decimal(*sum, 2), "{key:?}");
        // AVG of decimal(7, 2) is decimal(11, 6): sum * 10^4 / count, truncated.
        assert_eq!(row[2], decimal(sum * 10_000 / *count as i128, 6), "{key:?}");
        let mean = xs.iter().sum::<f64>() / xs.len() as f64;
        let variance = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (xs.len() - 1) as f64;
        let sd: f64 = row[3].parse().unwrap();
        assert!(
            (sd - variance.sqrt()).abs() < 1e-9 * sd.abs().max(1.0),
            "{key:?}"
        );
    }
    assert_eq!(actual.len(), groups.len());
}

#[test]
fn rollup_distinguishes_subtotals_from_null_keys() {
    let d = data(6_000);
    let mut p = plan(&d);
    // ROLLUP(g, parity): sets (g, parity), (g), ().
    let parity = p
        .columns
        .allocate_named(ColumnType::new(DataType::Int64, true), "parity");
    let id = p
        .columns
        .allocate_named(ColumnType::new(DataType::UInt8, false), "__grouping_id");
    let count = p
        .columns
        .allocate_named(ColumnType::new(DataType::Int64, false), "count");
    let reference = |c| ReferenceExpression::new(c).into_ref();
    let two = p.columns.allocate(ColumnType::new(DataType::Int64, false));
    let parity_expr = roc::expr::scalar::FunctionExpression::binary(
        roc::expr::scalar::FunctionKind::Remainder,
        roc::expr::scalar::CastExpression::new(
            reference(p.amount),
            roc::expr::scalar::CastMode::Strict,
            p.columns.allocate(ColumnType::new(DataType::Int64, true)),
        )
        .into_ref(),
        roc::expr::scalar::ConstantExpression::int64(Some(2), two).into_ref(),
        parity,
    )
    .into_ref();
    let aggregate = AggregateOperator::try_new(
        vec![reference(p.group), parity_expr],
        vec![Arc::new(AggregateExpression::new(
            AggregateFunction::Count,
            vec![],
            count,
        ))],
    )
    .unwrap()
    .with_grouping_sets(vec![vec![0, 1], vec![0], vec![]], id)
    .unwrap();
    let node = OperatorTreeNode::new(aggregate, vec![p.node]);
    let rows = execute_all(&p.columns, &node).unwrap();
    let mut expected: BTreeMap<Vec<String>, i64> = BTreeMap::new();
    for i in 0..d.group.len() {
        let g = d.group[i].map_or("NULL".to_string(), str::to_string);
        // CAST(decimal AS BIGINT) truncates; NULL amounts have a NULL parity.
        let parity = d.amount[i].map_or("NULL".to_string(), |a| ((a / 100) % 2).to_string());
        for key in [
            vec![g.clone(), parity, "0".into()],
            vec![g, "NULL".into(), "1".into()],
            vec!["NULL".into(), "NULL".into(), "3".into()],
        ] {
            *expected.entry(key).or_default() += 1;
        }
    }
    let mut actual = rows
        .into_iter()
        .map(|mut r| {
            let c = r.pop().unwrap();
            (r, c.parse::<i64>().unwrap())
        })
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
}
