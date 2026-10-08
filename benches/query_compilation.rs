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
    array::{ArrayRef, AsArray, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use roc::{
    exec::{Batch, FilterExec, ProcessExec, ProcessResult},
    expr::{
        ExpressionResultType,
        scalar::{
            ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
            ScalarExprRef,
        },
    },
    jit::NativeBatchKernel,
    operator::{Projection, ProjectionExpression},
};
use std::{hint::black_box, sync::Arc, time::Duration};

#[path = "../tests/support/bitmap_aot.rs"]
mod bitmap_aot;

fn plan(threshold: i64) -> (ScalarExprRef, Projection) {
    let col = |index| {
        ReferenceExpression::new(index, ExpressionResultType::new(DataType::Int64, false))
            .into_ref()
    };
    let int = |n| ConstantExpression::int64(Some(n)).into_ref();
    let predicate = FunctionExpression::binary(
        FunctionKind::LessThan,
        col(0),
        int(threshold),
        DataType::Boolean,
        false,
    )
    .into_ref();
    let multiply = FunctionExpression::binary(
        FunctionKind::Multiply,
        col(0),
        int(3),
        DataType::Int64,
        false,
    )
    .into_ref();
    let result =
        FunctionExpression::binary(FunctionKind::Add, multiply, col(1), DataType::Int64, false)
            .into_ref();
    (
        predicate,
        Projection::new(vec![ProjectionExpression::new(result, "value")]),
    )
}

fn input(rows: usize) -> RecordBatch {
    let mut seed = 7_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((seed >> 32) % 1000) as i64 - 500
    };
    let columns: Vec<ArrayRef> = (0..2)
        .map(|_| {
            Arc::new(Int64Array::from(
                (0..rows).map(|_| next()).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ])),
        columns,
    )
    .unwrap()
}

// Dense Arrow reference using emitted scalar programs: eagerly filter columns, then evaluate each
// expression node with the existing dense Arrow primitives.
struct ArrowReference {
    predicate: roc::program::ProcessProgram,
    projector: roc::program::ProcessProgram,
}
impl ArrowReference {
    fn new(predicate: &ScalarExprRef, projection: &Projection) -> Self {
        Self {
            predicate: predicate.program().unwrap(),
            projector: projection_program(projection.clone()).unwrap(),
        }
    }
    fn execute(&mut self, input: &RecordBatch) -> RecordBatch {
        let executor = Value::input(input.columns(), input.num_rows());
        let mask = self
            .predicate
            .run_value(&executor)
            .unwrap()
            .into_array(input.num_rows())
            .unwrap();
        let filtered = arrow::compute::filter_record_batch(input, mask.as_boolean()).unwrap();
        self.projector
            .run_batch(&(&filtered).clone().into())
            .unwrap()
    }
}

fn benchmark(c: &mut Criterion) {
    if std::env::var_os("ROC_BITMAP_DIAGNOSTICS").is_some() {
        diagnose(c);
        return;
    }
    // AOT remains the same-algorithm backend control. Vector has the same query
    // and selection pushdown, but deliberately retains expression intermediates.
    let mut group = c.benchmark_group("selection_comparison");
    group
        .sample_size(50)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1));
    for rows in [64, 2048, 16384] {
        let input = input(rows);
        let batch = Batch::from(input.clone());
        for (selectivity, threshold) in [(0, -500), (50, 0), (100, 500)] {
            let (predicate, projection) = plan(threshold);
            let compiled = NativeBatchKernel::compile(&predicate, &projection)
                .unwrap()
                .unwrap();
            let aot = bitmap_aot::aot(threshold, projection.output_schema());
            let mut arrow = ArrowReference::new(&predicate, &projection);
            let mut filter = FilterExec::new(predicate.clone())
                .new_executor(Arc::new(()))
                .unwrap();
            let mut projector = projection_program(projection.clone()).unwrap();
            let mut vector = |input: &Batch| {
                let ProcessResult::NeedMoreInput(filtered) = filter.execute(input).unwrap() else {
                    unreachable!()
                };
                projector.run_batch(&filtered).unwrap()
            };
            let expected = arrow.execute(&input);
            assert_eq!(vector(&batch), expected);
            assert_eq!(compiled.execute_batch(&input).unwrap(), expected);
            assert_eq!(aot.execute_batch(&input).unwrap(), expected);
            let case = format!("rows={rows}/select={selectivity}");
            let mut backends = ["arrow", "vector_selection", "aot", "cranelift"];
            if std::env::var_os("ROC_BITMAP_BENCH_REVERSE").is_some() {
                backends.reverse();
            }
            for name in backends {
                group.bench_function(BenchmarkId::new(name, &case), |b| match name {
                    "arrow" => b.iter(|| black_box(arrow.execute(black_box(&input)))),
                    "vector_selection" => b.iter(|| black_box(vector(black_box(&batch)))),
                    "aot" => b.iter(|| {
                        black_box(black_box(&aot).execute_batch(black_box(&input)).unwrap())
                    }),
                    _ => b.iter(|| {
                        black_box(
                            black_box(&compiled)
                                .execute_batch(black_box(&input))
                                .unwrap(),
                        )
                    }),
                });
            }
        }
    }
    group.finish();

    let mut group = c.benchmark_group("jit_bitmap_startup");
    group
        .sample_size(30)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_millis(500));
    let (predicate, projection) = plan(0);
    group.bench_function("compile_and_drop", |b| {
        b.iter(|| {
            black_box(
                NativeBatchKernel::compile(black_box(&predicate), black_box(&projection))
                    .unwrap()
                    .unwrap(),
            );
        })
    });
    let input = input(2048);
    group.bench_function("compile_execute_2048_and_drop", |b| {
        b.iter(|| {
            let compiled =
                NativeBatchKernel::compile(black_box(&predicate), black_box(&projection))
                    .unwrap()
                    .unwrap();
            black_box(compiled.execute_batch(black_box(&input)).unwrap());
        })
    });
    group.finish();
}
fn diagnose(c: &mut Criterion) {
    let input = input(16384);
    let mut group = c.benchmark_group("bitmap_diagnosis");
    group
        .sample_size(50)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1));
    for (case, threshold) in [("reject", -500), ("constant_false_control", i64::MIN)] {
        let (predicate, projection) = plan(threshold);
        let jit = NativeBatchKernel::compile_with_disassembly(&predicate, &projection)
            .unwrap()
            .unwrap();
        let aot = bitmap_aot::aot(threshold, projection.output_schema());
        let dir = std::path::PathBuf::from(std::env::var_os("ROC_BITMAP_DIAGNOSTICS").unwrap());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{case}.clif")), jit.clif()).unwrap();
        std::fs::write(dir.join(format!("{case}.asm")), jit.disassembly().unwrap()).unwrap();
        let mut programs = [("aot", &aot), ("cranelift", &jit)];
        if std::env::var_os("ROC_BITMAP_BENCH_REVERSE").is_some() {
            programs.reverse();
        }
        for (name, program) in programs {
            let mut kernel = program.prepare_predicate_benchmark(&input).unwrap();
            let mut masks = vec![u64::MAX; input.num_rows().div_ceil(64)];
            assert_eq!(kernel(&mut masks), 0);
            assert!(masks.iter().all(|&m| m == 0));
            assert_eq!(program.execute_batch(&input).unwrap().num_rows(), 0);
            group.bench_function(format!("{case}/{name}/predicate_only"), |b| {
                b.iter(|| black_box(kernel(black_box(&mut masks))));
            });
            group.bench_function(format!("{case}/{name}/whole_batch"), |b| {
                b.iter(|| black_box(black_box(program).execute_batch(black_box(&input)).unwrap()));
            });
        }
        if case == "reject" {
            let mut evaluation = predicate.program().unwrap();
            let executor = Value::input(input.columns(), input.num_rows());
            group.bench_function("reject/arrow/predicate_materialized", |b| {
                b.iter(|| black_box(evaluation.run_value(black_box(&executor)).unwrap()));
            });
            let mut arrow = ArrowReference::new(&predicate, &projection);
            group.bench_function("reject/arrow/whole_batch", |b| {
                b.iter(|| black_box(arrow.execute(black_box(&input))));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);

#[path = "../tests/support/program.rs"]
mod program_support;
use program_support::*;
