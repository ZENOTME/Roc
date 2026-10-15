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

//! Sorts, limits, unions, scalar subquery rows, shared subplans and window
//! functions, compared with direct computations in every mode.
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    error::ErrorKind,
    exec::window::{WindowFrame, WindowFunction},
    kernel::ExecutionMode,
    operator::{
        LimitOperator, OperatorTreeNode, SingleRowOperator, SortColumn, SortOperator,
        SpoolOperator, UnionOperator, WindowExpression, WindowOperator,
    },
};
use std::sync::Arc;

mod support;
use support::exec::{execute, execute_all, rows, scan};

struct Table {
    columns: ColumnContext,
    k: ColumnId,
    s: ColumnId,
    d: ColumnId,
    data: Vec<(Option<i64>, Option<String>, Option<i128>)>,
}

fn table(n: usize) -> Table {
    let mut columns = ColumnContext::new();
    let k = columns.allocate_named(ColumnType::new(DataType::Int64, true), "k");
    let s = columns.allocate_named(ColumnType::new(DataType::Utf8, true), "s");
    let d = columns.allocate_named(ColumnType::new(DataType::Decimal128(7, 2), true), "d");
    let data = (0..n)
        .map(|i| {
            (
                (i % 11 != 0).then_some((i * 7 % 13) as i64),
                (i % 5 != 0).then(|| format!("s{}", i % 9)),
                (i % 6 != 0).then_some(i as i128 * 3 - 200),
            )
        })
        .collect();
    Table {
        columns,
        k,
        s,
        d,
        data,
    }
}

impl Table {
    fn scan(&self) -> OperatorTreeNode {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Decimal128(7, 2), true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                self.data.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                self.data.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(self.data.iter().map(|r| r.2).collect::<Vec<_>>())
                    .with_precision_and_scale(7, 2)
                    .unwrap(),
            ),
        ];
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        let batches = (0..batch.num_rows())
            .step_by(333)
            .map(|o| batch.slice(o, 333.min(batch.num_rows() - o)))
            .collect();
        scan(batches, vec![self.k, self.s, self.d])
    }
    fn all(&self) -> Vec<ColumnId> {
        vec![self.k, self.s, self.d]
    }
}

fn sorted_rows(columns: &ColumnContext, node: &OperatorTreeNode) -> Vec<Vec<String>> {
    // Ordered results must agree in sequence, not only as multisets.
    let first = rows(&execute(columns, node, ExecutionMode::Compile, 4).unwrap());
    for (mode, workers) in [
        (ExecutionMode::ZeroCompile, 1),
        (ExecutionMode::ZeroCompile, 4),
    ] {
        assert_eq!(rows(&execute(columns, node, mode, workers).unwrap()), first);
    }
    first
}

#[test]
fn sorts_order_by_every_key_and_keep_the_first_rows() {
    let t = table(2000);
    let keys = vec![
        SortColumn {
            column: t.s,
            descending: true,
            nulls_first: false,
        },
        SortColumn {
            column: t.k,
            descending: false,
            nulls_first: true,
        },
        SortColumn {
            column: t.d,
            descending: true,
            nulls_first: true,
        },
    ];
    let mut expected = t.data.clone();
    expected.sort_by(|a, b| {
        let s = match (&a.1, &b.1) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, _) => std::cmp::Ordering::Greater,
            (_, None) => std::cmp::Ordering::Less,
            (x, y) => y.cmp(x),
        };
        s.then(a.0.cmp(&b.0)).then(match (a.2, b.2) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, _) => std::cmp::Ordering::Less,
            (_, None) => std::cmp::Ordering::Greater,
            (x, y) => y.cmp(&x),
        })
    });
    for fetch in [None, Some(0), Some(10), Some(5000)] {
        let sort = SortOperator::try_new(keys.clone(), t.all(), fetch).unwrap();
        let node = OperatorTreeNode::new(sort, vec![t.scan()]);
        let actual = sorted_rows(&t.columns, &node);
        let n = fetch.unwrap_or(usize::MAX).min(expected.len());
        let k = |r: &Vec<String>| (r[0].clone(), r[1].clone());
        let e = |r: &(Option<i64>, Option<String>, Option<i128>)| {
            (
                r.0.map_or("NULL".into(), |v| v.to_string()),
                r.1.clone().unwrap_or("NULL".into()),
            )
        };
        assert_eq!(
            actual.iter().map(k).collect::<Vec<_>>(),
            expected[..n].iter().map(e).collect::<Vec<_>>(),
            "{fetch:?}"
        );
        // A limit after the sort skips and keeps rows in sort order.
        let sort = SortOperator::try_new(keys.clone(), t.all(), fetch).unwrap();
        let limit = LimitOperator::new(3, Some(4), t.all());
        let node = OperatorTreeNode::new(limit, vec![OperatorTreeNode::new(sort, vec![t.scan()])]);
        let limited = sorted_rows(&t.columns, &node);
        assert_eq!(limited, actual[3.min(n)..7.min(n)].to_vec());
    }
}

#[test]
fn unions_spools_and_single_rows() {
    let mut t = table(500);
    let outputs = vec![
        t.columns
            .allocate_named(ColumnType::new(DataType::Int64, true), "uk"),
        t.columns
            .allocate_named(ColumnType::new(DataType::Utf8, true), "us"),
        t.columns
            .allocate_named(ColumnType::new(DataType::Decimal128(7, 2), true), "ud"),
    ];
    // The same subplan read twice through one spool, plus a third copy.
    let key = SpoolOperator::new_key();
    let spool = |t: &mut Table| {
        let out = vec![
            t.columns.allocate(ColumnType::new(DataType::Int64, true)),
            t.columns.allocate(ColumnType::new(DataType::Utf8, true)),
            t.columns
                .allocate(ColumnType::new(DataType::Decimal128(7, 2), true)),
        ];
        (
            OperatorTreeNode::new(
                SpoolOperator::new(key, t.all(), out.clone()),
                vec![t.scan()],
            ),
            out,
        )
    };
    let (a, ac) = spool(&mut t);
    let (b, bc) = spool(&mut t);
    let union = UnionOperator::try_new(vec![ac, bc, t.all()], outputs).unwrap();
    let node = OperatorTreeNode::new(union, vec![a, b, t.scan()]);
    let actual = execute_all(&t.columns, &node).unwrap();
    assert_eq!(actual.len(), 1500);

    for (rows, expected) in [(0, Some(1)), (1, Some(1)), (2, None)] {
        let t = table(rows);
        let one = OperatorTreeNode::new(SingleRowOperator::new(t.all()), vec![t.scan()]);
        match expected {
            Some(n) => {
                let r = execute_all(&t.columns, &one).unwrap();
                assert_eq!(r.len(), n);
                if rows == 0 {
                    assert!(r[0].iter().all(|v| v == "NULL"));
                }
            }
            None => {
                let e = execute(&t.columns, &one, ExecutionMode::Compile, 1).unwrap_err();
                assert_eq!(e.kind(), ErrorKind::InvalidInput);
            }
        }
    }
}

#[test]
fn windows_rank_and_aggregate_over_their_frames() {
    let mut t = table(1500);
    let rank = t
        .columns
        .allocate_named(ColumnType::new(DataType::UInt64, false), "rank");
    let total = t
        .columns
        .allocate_named(ColumnType::new(DataType::Decimal128(17, 2), true), "total");
    let running = t.columns.allocate_named(
        ColumnType::new(DataType::Decimal128(17, 2), true),
        "running",
    );
    let max = t
        .columns
        .allocate_named(ColumnType::new(DataType::Decimal128(7, 2), true), "max");
    let avg = t
        .columns
        .allocate_named(ColumnType::new(DataType::Decimal128(11, 6), true), "avg");
    let by_k = SortColumn {
        column: t.k,
        descending: false,
        nulls_first: false,
    };
    let window = |function, argument, frame, output| WindowExpression {
        function,
        argument,
        partition: vec![t.s],
        order: if frame == WindowFrame::Partition {
            vec![]
        } else {
            vec![by_k]
        },
        frame,
        output,
    };
    let windows = vec![
        window(
            WindowFunction::Rank,
            None,
            WindowFrame::RangeToCurrent,
            rank,
        ),
        window(
            WindowFunction::Sum,
            Some(t.d),
            WindowFrame::Partition,
            total,
        ),
        window(
            WindowFunction::Sum,
            Some(t.d),
            WindowFrame::RangeToCurrent,
            running,
        ),
        window(
            WindowFunction::Max,
            Some(t.d),
            WindowFrame::RangeToCurrent,
            max,
        ),
        window(WindowFunction::Avg, Some(t.d), WindowFrame::Partition, avg),
    ];
    let op = WindowOperator::try_new(t.all(), windows).unwrap();
    let node = OperatorTreeNode::new(op, vec![t.scan()]);
    let actual = execute_all(&t.columns, &node).unwrap();
    let dec = |v: i128, s: u32| {
        let p = 10u128.pow(s);
        let a = v.unsigned_abs();
        format!(
            "{}{}.{:0w$}",
            if v < 0 { "-" } else { "" },
            a / p,
            a % p,
            w = s as usize
        )
    };
    let mut expected = vec![];
    for row in &t.data {
        let part = t.data.iter().filter(|r| r.1 == row.1).collect::<Vec<_>>();
        // NULL keys order last.
        let before = |r: &&(Option<i64>, Option<String>, Option<i128>)| match (r.0, row.0) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(b)) => a <= b,
        };
        let earlier = part.iter().filter(|r| match (r.0, row.0) {
            (Some(a), Some(b)) => a < b,
            (Some(_), None) => true,
            _ => false,
        });
        let sum = |rows: &[&&(Option<i64>, Option<String>, Option<i128>)]| {
            let values = rows.iter().filter_map(|r| r.2).collect::<Vec<_>>();
            (!values.is_empty()).then(|| values.iter().sum::<i128>())
        };
        let all = part.iter().collect::<Vec<_>>();
        let peers = part.iter().filter(|r| before(r)).collect::<Vec<_>>();
        let values = part.iter().filter_map(|r| r.2).collect::<Vec<_>>();
        let opt = |v: Option<String>| v.unwrap_or("NULL".into());
        expected.push(vec![
            row.0.map_or("NULL".into(), |v| v.to_string()),
            opt(row.1.clone()),
            opt(row.2.map(|v| dec(v, 2))),
            (earlier.count() + 1).to_string(),
            opt(sum(&all).map(|v| dec(v, 2))),
            opt(sum(&peers).map(|v| dec(v, 2))),
            opt(peers.iter().filter_map(|r| r.2).max().map(|v| dec(v, 2))),
            opt((!values.is_empty()).then(|| {
                dec(
                    values.iter().sum::<i128>() * 10_000 / values.len() as i128,
                    6,
                )
            })),
        ]);
    }
    let (mut actual, mut expected) = (actual, expected);
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}
