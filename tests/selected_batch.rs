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
    array::{AsArray, BooleanArray, Int64Array},
    buffer::BooleanBuffer,
    datatypes::{DataType, Field, Int64Type, Schema},
    record_batch::RecordBatch,
};
use roc::{
    exec::{Batch, FilterExec, ProcessExec, ProcessResult},
    expr::{
        ExpressionResultType,
        scalar::{
            ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
            ScalarExprRef,
        },
    },
    operator::{Projection, ProjectionExpression},
};
use std::sync::Arc;
fn input(x: Vec<Option<i64>>, y: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ])),
        vec![Arc::new(Int64Array::from(x)), Arc::new(Int64Array::from(y))],
    )
    .unwrap()
}
fn col(i: usize) -> ScalarExprRef {
    ReferenceExpression::new(i, ExpressionResultType::new(DataType::Int64, true)).into_ref()
}
fn int(v: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(v)).into_ref()
}
fn bin(kind: FunctionKind, a: ScalarExprRef, b: ScalarExprRef) -> ScalarExprRef {
    let ty = match kind {
        FunctionKind::Add
        | FunctionKind::Subtract
        | FunctionKind::Multiply
        | FunctionKind::Divide => DataType::Int64,
        _ => DataType::Boolean,
    };
    FunctionExpression::binary(kind, a, b, ty, true).into_ref()
}
fn projector(expressions: Vec<ScalarExprRef>) -> roc::program::ProcessProgram {
    projection_program(Projection::new(
        expressions
            .into_iter()
            .enumerate()
            .map(|(i, e)| ProjectionExpression::new(e, format!("c{i}")))
            .collect(),
    ))
    .unwrap()
}
fn filter(batch: &Batch, expr: ScalarExprRef) -> Batch {
    let mut executor = FilterExec::new(expr).new_executor(Arc::new(())).unwrap();
    let ProcessResult::NeedMoreInput(output) = executor.execute(batch).unwrap() else {
        panic!()
    };
    output
}
#[test]
fn selection_composition_slices_and_projection_share_original_columns() {
    let data = input(
        (0..140).map(Some).collect(),
        (0..140).map(|i| Some(-i)).collect(),
    )
    .slice(3, 130);
    let batch = Batch::from(data.clone());
    let first = filter(&batch, bin(FunctionKind::LessThan, col(0), int(100)));
    let second = filter(&first, bin(FunctionKind::GreaterThan, col(0), int(62)));
    assert_eq!(second.num_rows(), 37);
    assert_eq!(second.physical().num_rows(), 130);
    assert!(Arc::ptr_eq(second.physical().column(0), data.column(0)));
    let projected = second.project(&[1, 0, 1]).unwrap();
    assert!(Arc::ptr_eq(projected.physical().column(0), data.column(1)));
    for (offset, len) in [(0, 37), (1, 35), (20, 0), (36, 1)] {
        assert_eq!(
            second.slice(offset, len).into_record_batch().unwrap(),
            data.slice(60 + offset, len)
        );
    }
    assert!(Batch::try_new(data.clone(), BooleanBuffer::from(vec![true])).is_err());
    assert_eq!(batch.physical(), &data);
    assert!(batch.selection().is_none());
    let all = Batch::try_new(data, BooleanBuffer::new_set(130)).unwrap();
    assert!(all.selection().is_none());
    let zero_columns = second.project(&[]).unwrap().into_record_batch().unwrap();
    assert_eq!(zero_columns.num_rows(), 37);
    assert_eq!(zero_columns.num_columns(), 0);
}
#[test]
fn selected_primitives_match_dense_arrow_for_nullable_slices_and_operand_layouts() {
    use FunctionKind::*;
    let data = input(
        (0..140).map(|i| (i % 7 != 0).then_some(i - 70)).collect(),
        (0..140).map(|i| (i % 11 != 0).then_some(i + 1)).collect(),
    )
    .slice(5, 129);
    for pattern in [0, 1, 2, 3] {
        // Sliced bitmaps deliberately have a non-byte-aligned offset.
        let bits = BooleanBuffer::from(
            (0..140)
                .map(|i| match pattern {
                    0 => false,
                    1 => true,
                    2 => i % 2 == 0,
                    _ => i % 13 == 0,
                })
                .collect::<Vec<_>>(),
        )
        .slice(5, 129);
        let batch = Batch::try_new(data.clone(), bits).unwrap();
        let dense = batch.materialize().unwrap();
        for offset in [0, batch.num_rows() / 2, batch.num_rows()] {
            let len = batch.num_rows() - offset;
            assert_eq!(
                batch.slice(offset, len).into_record_batch().unwrap(),
                dense.slice(offset, len)
            );
        }
        for op in [
            Add,
            Subtract,
            Multiply,
            Equal,
            NotEqual,
            LessThan,
            LessThanOrEqual,
            GreaterThan,
            GreaterThanOrEqual,
        ] {
            let operands = [
                col(0),
                int(3),
                bin(Add, col(0), int(1)),
                ConstantExpression::int64(None).into_ref(),
            ];
            for a in &operands {
                for b in &operands {
                    let mut p = projector(vec![bin(op, a.clone(), b.clone()), col(1), int(5)]);
                    assert_eq!(
                        p.run_batch(&batch).unwrap(),
                        p.run_batch(&dense.as_ref().clone().into()).unwrap(),
                        "{op:?}, pattern={pattern}"
                    );
                }
            }
        }
    }
}
#[test]
fn inactive_overflow_and_division_by_zero_are_not_evaluated() {
    let data = input(
        vec![Some(i64::MAX), Some(2), None, Some(-3)],
        vec![Some(0), Some(2), Some(0), Some(3)],
    );
    let batch = Batch::try_new(
        data.clone(),
        BooleanBuffer::from(vec![false, true, false, true]),
    )
    .unwrap();
    let mut p = projector(vec![bin(
        FunctionKind::Add,
        bin(FunctionKind::Multiply, col(0), int(3)),
        col(1),
    )]);
    assert_eq!(
        p.run_batch(&batch)
            .unwrap()
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .as_ref(),
        &[8, -6]
    );
    assert!(p.run_batch(&(&data).clone().into()).is_err());
    let mut fallback = projector(vec![bin(FunctionKind::Divide, int(12), col(1))]);
    assert_eq!(
        fallback.run_batch(&batch).unwrap(),
        fallback
            .run_batch(&batch.materialize().unwrap().as_ref().clone().into())
            .unwrap()
    );
    assert!(fallback.run_batch(&(&data).clone().into()).is_err());
    let empty = Batch::try_new(data, BooleanBuffer::new_unset(4)).unwrap();
    let mut constants = projector(vec![bin(FunctionKind::Add, int(i64::MAX), int(1))]);
    assert_eq!(constants.run_batch(&empty).unwrap().num_rows(), 0);
    assert!(constants.run_batch(&batch).is_err());
}
#[test]
fn nullable_filter_compacts_only_at_explicit_boundary() {
    let data = input(vec![Some(1), None, Some(3), Some(4)], vec![Some(1); 4]);
    let batch = Batch::try_new(
        data.clone(),
        BooleanBuffer::from(vec![true, false, true, true]),
    )
    .unwrap();
    let output = batch
        .filter(&BooleanArray::from(vec![Some(true), None, Some(false)]))
        .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert!(Arc::ptr_eq(output.physical().column(0), data.column(0)));
    assert_eq!(output.into_record_batch().unwrap(), data.slice(0, 1));
    assert!(batch.filter(&BooleanArray::from(vec![true])).is_err());
    // A reference-only projection materializes only its output column.
    let out = projector(vec![col(1)]).run_batch(&batch).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.num_columns(), 1);
}

#[path = "support/program.rs"]
mod program_support;
use program_support::*;
