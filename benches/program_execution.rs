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

use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use roc::{
    exec::{FilterExec, ProcessExec, ProcessResult, SinkExec},
    expr::{
        ExpressionResultType,
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            CaseExpression, CastExpression, CastMode, CoalesceExpression, Conjunction,
            ConjunctionExpression, ConstantExpression, FunctionExpression, FunctionKind,
            ReferenceExpression, ScalarExprRef,
        },
    },
    operator::{AggregateOperator, Projection, ProjectionExpression},
};
use std::{hint::black_box, sync::Arc, time::Duration};
fn col(i: usize) -> ScalarExprRef {
    ReferenceExpression::new(i, ExpressionResultType::new(DataType::Int64, true)).into_ref()
}
fn int(i: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(i)).into_ref()
}
fn binary(op: FunctionKind, a: ScalarExprRef, b: ScalarExprRef) -> ScalarExprRef {
    let ty = match op {
        FunctionKind::Add
        | FunctionKind::Subtract
        | FunctionKind::Multiply
        | FunctionKind::Divide
        | FunctionKind::Remainder => DataType::Int64,
        _ => DataType::Boolean,
    };
    FunctionExpression::binary(op, a, b, ty, true).into_ref()
}
fn projection(exprs: Vec<ScalarExprRef>) -> Projection {
    Projection::new(
        exprs
            .into_iter()
            .enumerate()
            .map(|(i, e)| ProjectionExpression::new(e, format!("c{i}")))
            .collect(),
    )
}
fn input(rows: usize) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|i| ((i * 7919) % 1000) as i64 - 500),
        )),
        Arc::new(Int64Array::from(
            (0..rows)
                .map(|i| (i % 7 != 0).then_some((i % 100) as i64))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|i| (i % 32) as i64),
        )),
    ];
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
            Field::new("key", DataType::Int64, false),
        ])),
        columns,
    )
    .unwrap()
}
fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("program_migration");
    group
        .sample_size(40)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_millis(700));
    for rows in [2048, 16384] {
        let input = input(rows);
        let prepared: roc::exec::Batch = input.clone().into();
        let arithmetic = binary(
            FunctionKind::Add,
            binary(FunctionKind::Multiply, col(0), int(3)),
            col(1),
        );
        let predicate = binary(FunctionKind::LessThan, col(0), int(0));
        let complex = CaseExpression::new(
            vec![(
                predicate.clone(),
                CoalesceExpression::new(vec![col(1), int(5)], DataType::Int64, false).into_ref(),
            )],
            binary(FunctionKind::Add, col(0), int(1)),
            DataType::Int64,
            true,
        )
        .into_ref();
        let boolean = ConjunctionExpression::new(
            Conjunction::And,
            (0..8)
                .map(|i| binary(FunctionKind::GreaterThan, col(0), int(-400 + i * 20)))
                .collect(),
            true,
        )
        .into_ref();
        for (name, expr) in [
            ("arithmetic", arithmetic.clone()),
            ("case_coalesce", complex.clone()),
            ("and8", boolean),
            (
                "cast",
                CastExpression::new(col(0), DataType::Float64, CastMode::Strict, true).into_ref(),
            ),
        ] {
            let mut projector = projection_program(projection(vec![expr])).unwrap();
            group.bench_function(BenchmarkId::new(name, rows), |b| {
                b.iter(|| black_box(projector.run_batch(black_box(&prepared)).unwrap()))
            });
        }
        let batch = input.clone().into();
        let mut filter = FilterExec::new(predicate.clone())
            .new_executor(Arc::new(()))
            .unwrap();
        let mut projector = projection_program(projection(vec![arithmetic.clone()])).unwrap();
        group.bench_function(BenchmarkId::new("filter_project", rows), |b| {
            b.iter(|| {
                let ProcessResult::NeedMoreInput(filtered) =
                    filter.execute(black_box(&batch)).unwrap()
                else {
                    panic!()
                };
                black_box(projector.run_batch(&filtered).unwrap())
            })
        });
        for grouped in [false, true] {
            let groups = projection(if grouped { vec![col(2)] } else { vec![] });
            let aggregates = if grouped {
                vec![
                    Arc::new(
                        AggregateExpression::new(
                            AggregateFunction::Sum,
                            Some(arithmetic.clone()),
                            DataType::Int64,
                            true,
                        )
                        .with_filter(predicate.clone()),
                    ),
                    Arc::new(
                        AggregateExpression::new(
                            AggregateFunction::Count,
                            Some(col(1)),
                            DataType::Int64,
                            false,
                        )
                        .with_distinct(),
                    ),
                    Arc::new(AggregateExpression::new(
                        AggregateFunction::Avg,
                        Some(col(1)),
                        DataType::Float64,
                        true,
                    )),
                    Arc::new(AggregateExpression::new(
                        AggregateFunction::Min,
                        Some(complex.clone()),
                        DataType::Int64,
                        true,
                    )),
                    Arc::new(AggregateExpression::new(
                        AggregateFunction::Max,
                        Some(col(0)),
                        DataType::Int64,
                        true,
                    )),
                ]
            } else {
                vec![Arc::new(AggregateExpression::new(
                    AggregateFunction::Sum,
                    Some(arithmetic.clone()),
                    DataType::Int64,
                    true,
                ))]
            };
            let (sink, _) = AggregateOperator::try_new(groups, aggregates)
                .unwrap()
                .into_execs();
            let (_shutdown, guard) = asyncband::shutdown::new();
            let global = sink.init_global_context(&guard).unwrap();
            let mut exec = sink.new_executor(global).unwrap();
            group.bench_function(
                BenchmarkId::new(if grouped { "grouped_all" } else { "sum" }, rows),
                |b| {
                    b.iter(|| {
                        black_box(
                            futures::executor::block_on(exec.sink(&guard, black_box(&input)))
                                .unwrap(),
                        )
                    })
                },
            );
        }
    }
    group.finish();
}
criterion_group!(benches, bench, fusion_bench);
criterion_main!(benches);

#[path = "../tests/support/program.rs"]
mod program_support;
use program_support::*;

#[cfg(feature = "jit")]
fn fusion_bench(c: &mut Criterion) {
    use roc::program::ProgramBuilder;
    let mut group = c.benchmark_group("program_fusion");
    group
        .sample_size(40)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_millis(700));
    for rows in [64usize, 2048, 16384] {
        for percent in [0, 50, 100] {
            let data: roc::exec::Batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Int64, false),
                    Field::new("y", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|i| i as i64 % 97),
                    )),
                ],
            )
            .unwrap()
            .into();
            let col = |i| {
                ReferenceExpression::new(i, ExpressionResultType::new(DataType::Int64, false))
                    .into_ref()
            };
            let pred = FunctionExpression::binary(
                FunctionKind::LessThan,
                col(0),
                int((rows * percent / 100) as i64),
                DataType::Boolean,
                false,
            )
            .into_ref();
            let mul = FunctionExpression::binary(
                FunctionKind::Multiply,
                col(0),
                int(3),
                DataType::Int64,
                false,
            )
            .into_ref();
            let expr =
                FunctionExpression::binary(FunctionKind::Add, mul, col(1), DataType::Int64, false)
                    .into_ref();
            let proj = projection(vec![expr]);
            let make = |fuse: bool| {
                let mut b = ProgramBuilder::default();
                let input = b.value();
                let selected = b.emit_filter(input, &pred, None).unwrap();
                let output = b.emit_project(selected, &proj).unwrap();
                if fuse {
                    assert_eq!(b.fuse().unwrap().regions, 1);
                }
                b.build_batch(input, Some(output)).unwrap()
            };
            let mut direct = make(false);
            let mut fused = make(true);
            let a = direct.run_batch(&data).unwrap();
            let b = fused.run_batch(&data).unwrap();
            assert_eq!(a.num_rows(), b.num_rows());
            if a.num_rows() > 0 {
                assert_eq!(a, b);
            }
            for (name, p) in [("direct", &mut direct), ("jit", &mut fused)] {
                group.bench_function(BenchmarkId::new(name, format!("{rows}_{percent}")), |b| {
                    b.iter(|| black_box(p.execute(black_box(&data)).unwrap()))
                });
            }
        }
    }
    group.finish();
}
#[cfg(not(feature = "jit"))]
fn fusion_bench(_: &mut Criterion) {}
