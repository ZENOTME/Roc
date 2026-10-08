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

#![cfg(feature = "jit")]

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use roc::{
    error::Result,
    exec::{FilterExec, ProcessExec, ProcessResult},
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
use std::{collections::HashMap, sync::Arc};

fn col(index: usize) -> ScalarExprRef {
    ReferenceExpression::new(index, ExpressionResultType::new(DataType::Int64, false)).into_ref()
}
fn int(n: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(n)).into_ref()
}
fn boolean(v: bool) -> ScalarExprRef {
    ConstantExpression::boolean(Some(v)).into_ref()
}
fn bin(kind: FunctionKind, left: ScalarExprRef, right: ScalarExprRef) -> ScalarExprRef {
    let ty = match kind {
        FunctionKind::Add
        | FunctionKind::Subtract
        | FunctionKind::Multiply
        | FunctionKind::Divide => DataType::Int64,
        _ => DataType::Boolean,
    };
    FunctionExpression::binary(kind, left, right, ty, false).into_ref()
}
fn projection(exprs: Vec<ScalarExprRef>) -> Projection {
    Projection::new(
        exprs
            .into_iter()
            .enumerate()
            .map(|(i, e)| ProjectionExpression::new(e, format!("out{i}")))
            .collect(),
    )
    .with_metadata(HashMap::from([("test".into(), "jit".into())]))
}
fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(values.clone())),
            Arc::new(Int64Array::from(
                values.into_iter().map(|v| -v).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
fn reference(
    input: &RecordBatch,
    predicate: &ScalarExprRef,
    projection: &Projection,
) -> Result<RecordBatch> {
    let mut filter = FilterExec::new(predicate.clone()).new_executor(Arc::new(()))?;
    let ProcessResult::NeedMoreInput(filtered) = filter.execute(&input.clone().into())? else {
        unreachable!()
    };
    projection_program(projection.clone())?
        .run_batch(&(&filtered.into_record_batch()?).clone().into())
}

#[test]
fn differential_comparisons_arithmetic_slices_and_reuse() {
    use FunctionKind::*;
    let mut seed = 7_u64;
    let data = batch(
        (0..4096)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                ((seed >> 32) % 2001) as i64 - 1000
            })
            .collect(),
    );
    let outputs = projection(vec![
        bin(Add, bin(Multiply, col(0), int(3)), col(1)),
        bin(Subtract, int(12), col(0)),
        col(0),
        int(-17),
        col(0),
    ]);
    for cmp in [
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
    ] {
        let predicate = bin(cmp, bin(Add, col(0), int(7)), int(7));
        let kernel = NativeBatchKernel::compile(&predicate, &outputs)
            .unwrap()
            .unwrap();
        assert!(kernel.clif().contains("load.i64"));
        for (offset, len) in [(0, 0), (1, 1), (3, 63), (7, 2048), (0, 4096)] {
            let input = data.slice(offset, len);
            assert_eq!(
                kernel.execute_batch(&input).unwrap(),
                reference(&input, &predicate, &outputs).unwrap()
            );
        }
    }
}

#[test]
fn empty_projection_constant_predicates_and_no_input_columns() {
    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(11)),
    )
    .unwrap();
    for predicate in [boolean(true), boolean(false)] {
        for outputs in [projection(vec![]), projection(vec![int(42)])] {
            let kernel = NativeBatchKernel::compile(&predicate, &outputs)
                .unwrap()
                .unwrap();
            for data in [&input, &input.slice(3, 0)] {
                assert_eq!(
                    kernel.execute_batch(data).unwrap(),
                    reference(data, &predicate, &outputs).unwrap()
                );
            }
        }
    }
}

#[test]
fn checked_overflow_and_filtered_out_errors() {
    use FunctionKind::*;
    // Projection errors on rejected rows must not be evaluated. Both execution
    // modes still report errors in a predicate evaluated before row selection.
    for (kind, left, right) in [
        (Add, i64::MAX, 1),
        (Subtract, i64::MIN, 1),
        (Multiply, i64::MIN, -1),
        (Multiply, i64::MAX, 2),
    ] {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![0, left]))],
        )
        .unwrap();
        let outputs = projection(vec![bin(kind, col(0), int(right))]);
        for predicate in [boolean(true), boolean(false), bin(Equal, col(0), int(0))] {
            let kernel = NativeBatchKernel::compile(&predicate, &outputs)
                .unwrap()
                .unwrap();
            let actual = kernel.execute_batch(&input);
            let expected = reference(&input, &predicate, &outputs);
            assert_eq!(actual.is_err(), expected.is_err());
            if let Ok(expected) = expected {
                assert_eq!(actual.unwrap(), expected);
            }
            // A failure doesn't poison later batches or return partial columns.
            assert_eq!(
                kernel.execute_batch(&input.slice(0, 1)).unwrap(),
                reference(&input.slice(0, 1), &predicate, &outputs).unwrap()
            );
        }
        let predicate = bin(Equal, bin(kind, col(0), int(right)), int(0));
        let outputs = projection(vec![]);
        let kernel = NativeBatchKernel::compile(&predicate, &outputs)
            .unwrap()
            .unwrap();
        assert!(kernel.execute_batch(&input).is_err());
        assert!(reference(&input, &predicate, &outputs).is_err());
        assert_eq!(
            kernel.execute_batch(&input.slice(0, 0)).unwrap().num_rows(),
            0
        );
    }
}

#[test]
fn signed_comparisons_cover_integer_boundaries() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![
            i64::MIN,
            -1,
            0,
            1,
            i64::MAX,
        ]))],
    )
    .unwrap();
    use FunctionKind::*;
    for cmp in [
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
    ] {
        for bound in [i64::MIN, -1, 0, 1, i64::MAX] {
            let predicate = bin(cmp, col(0), int(bound));
            let outputs = projection(vec![col(0)]);
            let kernel = NativeBatchKernel::compile(&predicate, &outputs)
                .unwrap()
                .unwrap();
            assert_eq!(
                kernel.execute_batch(&input).unwrap(),
                reference(&input, &predicate, &outputs).unwrap()
            );
        }
    }
}

#[test]
fn unsupported_ir_declines_compilation() {
    let nullable =
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true)).into_ref();
    for expr in [
        nullable,
        ConstantExpression::int64(None).into_ref(),
        ConstantExpression::float64(Some(1.0)).into_ref(),
        bin(FunctionKind::Divide, col(0), int(2)),
        FunctionExpression::binary(FunctionKind::Equal, col(0), int(2), DataType::Int64, false)
            .into_ref(),
    ] {
        assert!(
            NativeBatchKernel::compile(&boolean(true), &projection(vec![expr]))
                .unwrap()
                .is_none()
        );
    }
    for predicate in [
        int(1),
        ConstantExpression::boolean(None).into_ref(),
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Boolean, false)).into_ref(),
    ] {
        assert!(
            NativeBatchKernel::compile(&predicate, &projection(vec![col(0)]))
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn invalid_runtime_columns_are_rejected_before_native_loads() {
    let kernel = NativeBatchKernel::compile(&boolean(true), &projection(vec![col(0)]))
        .unwrap()
        .unwrap();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
    )
    .unwrap();
    assert!(kernel.execute_batch(&input).is_err());
    // Nulls outside the logical slice do not invalidate it.
    assert_eq!(
        kernel.execute_batch(&input.slice(0, 1)).unwrap().num_rows(),
        1
    );
    let strings = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["wrong type"]))],
    )
    .unwrap();
    assert!(kernel.execute_batch(&strings).is_err());
    let missing = NativeBatchKernel::compile(&boolean(true), &projection(vec![col(usize::MAX)]))
        .unwrap()
        .unwrap();
    assert!(missing.execute_batch(&input).is_err());
}

#[test]
fn compiled_fragment_can_move_to_a_worker() {
    let kernel = NativeBatchKernel::compile(&boolean(true), &projection(vec![col(0)]))
        .unwrap()
        .unwrap();
    std::thread::spawn(move || {
        let input = batch(vec![1, 2, 3]);
        assert_eq!(
            kernel.execute_batch(&input).unwrap().column(0),
            input.column(0)
        );
    })
    .join()
    .unwrap();
}

#[path = "support/bitmap_aot.rs"]
mod bitmap_aot;

#[test]
fn bitmap_aot_and_jit_match_arrow_at_chunk_and_tail_boundaries() {
    // Consecutive empty, full and mixed chunks, including bit 63. Slices put
    // each pattern at different logical offsets and exercise every tail width.
    let values: Vec<i64> = (0..320)
        .map(|i| match i / 64 {
            0 => 10,
            1 => -10,
            2 => {
                if i % 2 == 0 {
                    -10
                } else {
                    10
                }
            }
            3 => {
                if i % 64 == 63 {
                    -10
                } else {
                    10
                }
            }
            _ => -10,
        })
        .collect();
    let data = batch(values);
    for threshold in [-500, 0, 500] {
        let predicate = bin(FunctionKind::LessThan, col(0), int(threshold));
        let outputs = projection(vec![bin(
            FunctionKind::Add,
            bin(FunctionKind::Multiply, col(0), int(3)),
            col(1),
        )]);
        let jit = NativeBatchKernel::compile(&predicate, &outputs)
            .unwrap()
            .unwrap();
        let aot = bitmap_aot::aot(threshold, outputs.output_schema());
        for offset in [0, 1, 7, 63, 64] {
            for len in (0..=129).chain([191, 192, 193, 255, 256]) {
                let input = data.slice(offset, len);
                let expected = reference(&input, &predicate, &outputs).unwrap();
                assert_eq!(
                    jit.execute_batch(&input).unwrap(),
                    expected,
                    "JIT threshold={threshold}, offset={offset}, len={len}"
                );
                assert_eq!(
                    aot.execute_batch(&input).unwrap(),
                    expected,
                    "AOT threshold={threshold}, offset={offset}, len={len}"
                );
            }
        }
    }
}

#[test]
fn bitmap_overflow_at_last_bit_next_chunk_and_tail_matches_aot() {
    let predicate = bin(FunctionKind::LessThan, col(0), int(0));
    let outputs = projection(vec![bin(
        FunctionKind::Add,
        bin(FunctionKind::Multiply, col(0), int(3)),
        col(1),
    )]);
    let jit = NativeBatchKernel::compile(&predicate, &outputs)
        .unwrap()
        .unwrap();
    let aot = bitmap_aot::aot(0, outputs.output_schema());
    for dense in [false, true] {
        for position in [0, 1, 2, 3, 60, 61, 62, 63, 64, 127, 128] {
            for (x, y, fails) in [
                (i64::MIN, 0, true),
                (-1, i64::MIN, true),
                (i64::MAX, i64::MAX, false),
            ] {
                let mut xs = vec![if dense { -1 } else { 1 }; 129];
                let mut ys = vec![0; 129];
                xs[position] = x;
                ys[position] = y;
                let input = RecordBatch::try_new(
                    batch(vec![]).schema(),
                    vec![
                        Arc::new(Int64Array::from(xs)),
                        Arc::new(Int64Array::from(ys)),
                    ],
                )
                .unwrap();
                assert_eq!(reference(&input, &predicate, &outputs).is_err(), fails);
                for program in [&jit, &aot] {
                    let actual = program.execute_batch(&input);
                    assert_eq!(actual.is_err(), fails);
                    if !fails {
                        assert_eq!(
                            actual.unwrap(),
                            reference(&input, &predicate, &outputs).unwrap()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn diagnostic_predicate_matches_logical_bitmap_and_keeps_tail_bits_zero() {
    let data = batch((0..200).map(|i| i - 100).collect());
    let predicate = bin(FunctionKind::LessThan, col(0), int(0));
    let outputs = projection(vec![col(0)]);
    let jit = NativeBatchKernel::compile_with_disassembly(&predicate, &outputs)
        .unwrap()
        .unwrap();
    assert!(!jit.disassembly().unwrap().is_empty());
    assert!(
        NativeBatchKernel::compile(&predicate, &outputs)
            .unwrap()
            .unwrap()
            .disassembly()
            .is_none()
    );
    for len in [0, 1, 63, 64, 65, 129] {
        let input = data.slice(3, len);
        let mut call = jit.prepare_predicate_benchmark(&input).unwrap();
        let mut masks = vec![u64::MAX; len.div_ceil(64)];
        let count = call(&mut masks);
        let mut expected = vec![0_u64; len.div_ceil(64)];
        for row in 0..len {
            if row < 97 {
                expected[row / 64] |= 1 << (row % 64);
            }
        }
        assert_eq!(masks, expected);
        assert_eq!(count, len.min(97));
    }
    let input = data.slice(0, 64);
    let mut call = jit.prepare_predicate_benchmark(&input).unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call(&mut []))).is_err());
    // A bad caller buffer cannot reach the unchecked native stores.
    assert_eq!(call(&mut [0]), 64);
    let bad = NativeBatchKernel::compile(&boolean(true), &projection(vec![col(99)]))
        .unwrap()
        .unwrap();
    assert!(bad.prepare_predicate_benchmark(&input).is_err());
}

#[test]
fn simd_comparisons_preserve_every_bit_signed_boundaries_and_operand_order() {
    use FunctionKind::*;
    let limits = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    let mut seed = 29_u64;
    let xs: Vec<i64> = (0..384)
        .map(|i| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            if i % 3 == 0 {
                limits[i % limits.len()]
            } else {
                seed as i64
            }
        })
        .collect();
    let ys: Vec<i64> = (0..384).map(|i| xs[(i * 13 + 5) % xs.len()]).collect();
    let data = RecordBatch::try_new(
        batch(vec![]).schema(),
        vec![
            Arc::new(Int64Array::from(xs)),
            Arc::new(Int64Array::from(ys)),
        ],
    )
    .unwrap();
    let outputs = projection(vec![col(0), col(1)]);
    for kind in [
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
    ] {
        let mut predicates = vec![bin(kind, col(0), col(1))];
        for bound in limits {
            predicates.push(bin(kind, col(0), int(bound)));
            predicates.push(bin(kind, int(bound), col(0)));
        }
        for predicate in predicates {
            let jit = NativeBatchKernel::compile(&predicate, &outputs)
                .unwrap()
                .unwrap();
            for offset in [0, 1, 7, 63] {
                for len in [0, 1, 63, 64, 65, 127, 128, 129, 191, 257] {
                    let input = data.slice(offset, len);
                    assert_eq!(
                        jit.execute_batch(&input).unwrap(),
                        reference(&input, &predicate, &outputs).unwrap(),
                        "{kind:?}, offset={offset}, len={len}"
                    );
                }
            }
        }
    }
    let predicate = bin(LessThan, col(0), int(0));
    let jit = NativeBatchKernel::compile(&predicate, &outputs)
        .unwrap()
        .unwrap();
    for bit in 0..64 {
        for inverse in [false, true] {
            let mut values = vec![if inverse { -1 } else { 1 }; 64];
            values[bit] = -values[bit];
            let input = batch(values);
            let mut masks = [0_u64];
            let count = jit.prepare_predicate_benchmark(&input).unwrap()(&mut masks);
            let expected = if inverse {
                !(1_u64 << bit)
            } else {
                1_u64 << bit
            };
            assert_eq!(masks, [expected]);
            assert_eq!(count, expected.count_ones() as usize);
        }
    }
}

#[test]
fn checked_multiply_matches_rust_at_signed_product_boundaries() {
    let values = [
        i64::MIN,
        i64::MIN + 1,
        i64::MIN / 3 - 1,
        i64::MIN / 3,
        -3037000500,
        -3037000499,
        -3,
        -1,
        0,
        1,
        3,
        3037000499,
        3037000500,
        i64::MAX / 3,
        i64::MAX / 3 + 1,
        i64::MAX - 1,
        i64::MAX,
    ];
    let jit = NativeBatchKernel::compile(
        &boolean(true),
        &projection(vec![bin(FunctionKind::Multiply, col(0), col(1))]),
    )
    .unwrap()
    .unwrap();
    for left in values {
        for right in values {
            for len in [1, 65] {
                let input = RecordBatch::try_new(
                    batch(vec![]).schema(),
                    vec![
                        Arc::new(Int64Array::from(vec![left; len])),
                        Arc::new(Int64Array::from(vec![right; len])),
                    ],
                )
                .unwrap();
                let actual = jit.execute_batch(&input);
                if let Some(value) = left.checked_mul(right) {
                    let output = actual.unwrap();
                    let column = output
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(column.values().as_ref(), vec![value; len]);
                } else {
                    assert!(actual.is_err(), "{left} * {right}");
                }
            }
        }
    }
}

#[path = "support/program.rs"]
mod program_support;
use program_support::*;
