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

//! Decimal arithmetic, comparisons and casts agree with Arrow's kernels in
//! every execution mode, including NULLs and boundary values.
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array},
    compute::{cast_with_options, kernels::cmp, kernels::numeric},
    datatypes::DataType,
    error::ArrowError,
};
use roc::{
    error::ErrorKind,
    expr::scalar::{CastExpression, CastMode, FunctionExpression, FunctionKind, ScalarExprRef},
};
use std::sync::Arc;

mod support;
use support::*;

fn decimals(values: &[Option<i128>], p: u8, s: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(p, s)
            .unwrap(),
    )
}

fn column(i: usize, t: &DataType) -> ScalarExprRef {
    roc::expr::scalar::ReferenceExpression::new(input_column(i, t.clone(), true)).into_ref()
}

fn evaluate(e: ScalarExprRef, inputs: &[ArrayRef]) -> roc::error::Result<ArrayRef> {
    let rows = inputs[0].len();
    TestProgram::try_new(e)?.evaluate(&TestInput::new(inputs, rows))
}

fn values() -> Vec<Option<i128>> {
    vec![
        Some(0),
        Some(1),
        Some(-1),
        Some(12345),
        Some(-9_999_999),
        Some(9_999_999),
        None,
        Some(50),
        Some(-50),
        Some(149),
        Some(-150),
    ]
}

#[test]
fn arithmetic_matches_arrow_result_types_and_values() {
    let a = decimals(&values(), 7, 2);
    let mut b_values = values();
    b_values.reverse();
    // Avoid division by zero on non-null rows: Arrow fails the whole batch.
    for v in b_values.iter_mut() {
        if *v == Some(0) {
            *v = Some(3);
        }
    }
    let b = decimals(&b_values, 5, 1);
    type Kernel =
        fn(&dyn arrow::array::Datum, &dyn arrow::array::Datum) -> Result<ArrayRef, ArrowError>;
    let cases: [(FunctionKind, Kernel); 4] = [
        (FunctionKind::Add, numeric::add),
        (FunctionKind::Subtract, numeric::sub),
        (FunctionKind::Multiply, numeric::mul),
        (FunctionKind::Divide, numeric::div),
    ];
    for (function, kernel) in cases {
        let expected = kernel(&a, &b).unwrap();
        let output = output_column(expected.data_type().clone(), true);
        let e = FunctionExpression::binary(
            function,
            column(0, a.data_type()),
            column(1, b.data_type()),
            output,
        )
        .into_ref();
        let actual = evaluate(e, &[a.clone(), b.clone()]).unwrap();
        assert_eq!(actual.to_data(), expected.to_data(), "{function:?}");
    }
}

#[test]
fn division_by_zero_and_overflow_fail() {
    let a = decimals(&[Some(1), Some(i128::MAX / 10)], 38, 0);
    let b = decimals(&[Some(0), Some(1)], 38, 0);
    let output = output_column(DataType::Decimal128(38, 4), true);
    let e = FunctionExpression::binary(
        FunctionKind::Divide,
        column(0, a.data_type()),
        column(1, b.data_type()),
        output,
    )
    .into_ref();
    let error = evaluate(e, &[a.clone(), b.clone()]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DivisionByZero, "{error:?}");
    let output = output_column(DataType::Decimal128(38, 0), true);
    let e = FunctionExpression::binary(
        FunctionKind::Multiply,
        column(0, a.data_type()),
        column(0, a.data_type()),
        output,
    )
    .into_ref();
    let error = evaluate(e, &[a]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ArithmeticOverflow, "{error:?}");
}

#[test]
fn comparisons_see_a_common_scale() {
    let a = decimals(&values(), 7, 2);
    let b = decimals(
        &[
            Some(0),
            Some(0),
            Some(0),
            Some(1234),
            Some(-100000),
            Some(1),
            None,
            Some(5),
            Some(-5),
            Some(15),
            Some(-15),
        ],
        9,
        1,
    );
    for (function, kernel) in [
        (
            FunctionKind::Equal,
            cmp::eq
                as fn(
                    &dyn arrow::array::Datum,
                    &dyn arrow::array::Datum,
                ) -> Result<BooleanArray, ArrowError>,
        ),
        (FunctionKind::LessThan, cmp::lt),
        (FunctionKind::GreaterThanOrEqual, cmp::gt_eq),
    ] {
        let common = DataType::Decimal128(10, 2);
        let options = Default::default();
        let expected = kernel(
            &cast_with_options(&a, &common, &options).unwrap(),
            &cast_with_options(&b, &common, &options).unwrap(),
        )
        .unwrap();
        let output = output_column(DataType::Boolean, true);
        let e = FunctionExpression::binary(
            function,
            column(0, a.data_type()),
            column(1, b.data_type()),
            output,
        )
        .into_ref();
        let actual = evaluate(e, &[a.clone(), b.clone()]).unwrap();
        assert_eq!(actual.to_data(), expected.to_data(), "{function:?}");
    }
}

#[test]
fn casts_round_and_check_precision_like_arrow() {
    let strict = arrow::compute::CastOptions {
        safe: false,
        ..Default::default()
    };
    let safe = arrow::compute::CastOptions::default();
    let a = decimals(&values(), 7, 2);
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(0),
        Some(-7),
        None,
        Some(123456),
    ]));
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.125),
        Some(-2.5),
        Some(1e30),
        None,
        Some(f64::NAN),
        Some(3.14159),
    ]));
    let cases: Vec<(ArrayRef, DataType)> = vec![
        (a.clone(), DataType::Decimal128(9, 4)),
        (a.clone(), DataType::Decimal128(6, 1)),
        (a.clone(), DataType::Decimal128(5, 0)),
        (a.clone(), DataType::Decimal128(3, 0)),
        (a.clone(), DataType::Float64),
        (a.clone(), DataType::Int64),
        (a.clone(), DataType::Int32),
        (ints.clone(), DataType::Decimal128(12, 2)),
        (ints.clone(), DataType::Decimal128(5, 2)),
        (floats.clone(), DataType::Decimal128(15, 2)),
    ];
    for (input, to) in cases {
        for (mode, options) in [(CastMode::Try, &safe), (CastMode::Strict, &strict)] {
            let expected = cast_with_options(&input, &to, options);
            let output = output_column(to.clone(), true);
            let e = CastExpression::new(column(0, input.data_type()), mode, output).into_ref();
            let actual = evaluate(e, &[input.clone()]);
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(
                    actual.to_data(),
                    expected.to_data(),
                    "{} to {to} {mode:?}",
                    input.data_type()
                ),
                (Err(actual), Err(_)) => {
                    assert_eq!(actual.kind(), ErrorKind::InvalidInput, "{actual:?}")
                }
                (actual, expected) => panic!(
                    "{} to {to} {mode:?}: {actual:?} vs {expected:?}",
                    input.data_type()
                ),
            }
        }
    }
    // NULL decimal rows stay NULL.
    assert!(a.is_null(6));
}

#[test]
fn filtered_decimal_rows_keep_both_words() {
    use arrow::{
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use roc::{
        column::{ColumnContext, ColumnType},
        expr::scalar::{ConstantExpression, ReferenceExpression},
        operator::{FilterOperator, OperatorTreeNode},
    };
    let mut columns = ColumnContext::new();
    let d = columns.allocate_named(ColumnType::new(DataType::Decimal128(20, 2), true), "d");
    let k = columns.allocate_named(ColumnType::new(DataType::Int64, true), "k");
    let values = (0..3000i128)
        .map(|i| (i % 7 != 0).then_some(i * 1_000_000_000_000_007 - 50))
        .collect::<Vec<_>>();
    let keys = (0..3000i64).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Decimal128(20, 2), true),
        Field::new("k", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            decimals(&values, 20, 2),
            Arc::new(Int64Array::from(keys.clone())),
        ],
    )
    .unwrap();
    // k % 3 = 1 keeps a third of the rows: outputs are compacted.
    let three = columns.allocate(ColumnType::new(DataType::Int64, false));
    let rem = columns.allocate(ColumnType::new(DataType::Int64, true));
    let one = columns.allocate(ColumnType::new(DataType::Int64, false));
    let keep = columns.allocate(ColumnType::new(DataType::Boolean, true));
    let predicate = FunctionExpression::binary(
        FunctionKind::Equal,
        FunctionExpression::binary(
            FunctionKind::Remainder,
            ReferenceExpression::new(k).into_ref(),
            ConstantExpression::int64(Some(3), three).into_ref(),
            rem,
        )
        .into_ref(),
        ConstantExpression::int64(Some(1), one).into_ref(),
        keep,
    )
    .into_ref();
    let filter = OperatorTreeNode::new(
        FilterOperator::new(predicate, vec![d]),
        vec![support::exec::scan(vec![batch], vec![d, k])],
    );
    let mut actual = support::exec::execute_all(&columns, &filter).unwrap();
    let mut expected = values
        .iter()
        .zip(&keys)
        .filter(|(_, k)| *k % 3 == 1)
        .map(|(v, _)| {
            vec![v.map_or("NULL".to_string(), |v| {
                let sign = if v < 0 { "-" } else { "" };
                format!("{sign}{}.{:02}", v.abs() / 100, v.abs() % 100)
            })]
        })
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}
