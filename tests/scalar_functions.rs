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

//! String, rounding and bitwise functions, with NULLs, in every mode.
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
        StringArray, StringViewArray, UInt8Array,
    },
    datatypes::{DataType, Decimal128Type, Float64Type, Int64Type, UInt8Type},
};
use roc::{
    error::ErrorKind,
    expr::scalar::{
        ConstantExpression, FunctionExpression, FunctionKind, LikeExpression, ReferenceExpression,
        ScalarExprRef,
    },
};
use std::sync::Arc;

mod support;
use support::*;

fn column(i: usize, t: DataType) -> ScalarExprRef {
    ReferenceExpression::new(input_column(i, t, true)).into_ref()
}
fn int(v: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(v), output_column(DataType::Int64, false)).into_ref()
}
fn evaluate(e: ScalarExprRef, inputs: &[ArrayRef]) -> roc::error::Result<ArrayRef> {
    let rows = inputs[0].len();
    TestProgram::try_new(e)?.evaluate(&TestInput::new(inputs, rows))
}
fn call(f: FunctionKind, args: Vec<ScalarExprRef>, t: DataType) -> ScalarExprRef {
    FunctionExpression::new(f, args, output_column(t, true)).into_ref()
}

fn texts() -> ArrayRef {
    Arc::new(StringArray::from(vec![
        Some("hello"),
        None,
        Some("héllo wörld"),
        Some(""),
        Some("0-500 dollars"),
        Some("unknown"),
    ]))
}

#[test]
fn substrings_upper_and_concatenation() {
    let input = texts();
    let s = input.as_string::<i32>();
    for (start, count) in [(1, 5), (0, 2), (-2, 4), (3, 100), (2, 3)] {
        let e = call(
            FunctionKind::Substring,
            vec![column(0, DataType::Utf8), int(start), int(count)],
            DataType::Utf8,
        );
        let actual = evaluate(e, std::slice::from_ref(&input)).unwrap();
        let expected = s
            .iter()
            .map(|v| {
                v.map(|v| {
                    let first = (start - 1).max(0) as usize;
                    let end = (start - 1 + count).max(0) as usize;
                    v.chars()
                        .skip(first)
                        .take(end.saturating_sub(first))
                        .collect::<String>()
                })
            })
            .collect::<StringArray>();
        assert_eq!(
            actual.as_string::<i32>(),
            &expected,
            "substr({start}, {count})"
        );
    }
    let negative = call(
        FunctionKind::Substring,
        vec![column(0, DataType::Utf8), int(1), int(-1)],
        DataType::Utf8,
    );
    let error = evaluate(negative, std::slice::from_ref(&input)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput, "{error:?}");

    let upper = call(
        FunctionKind::Upper,
        vec![column(0, DataType::Utf8)],
        DataType::Utf8,
    );
    let actual = evaluate(upper, std::slice::from_ref(&input)).unwrap();
    let expected = s
        .iter()
        .map(|v| v.map(str::to_uppercase))
        .collect::<StringArray>();
    assert_eq!(actual.as_string::<i32>(), &expected);

    let views: ArrayRef = Arc::new(StringViewArray::from_iter(s.iter()));
    let concat = call(
        FunctionKind::Concat,
        vec![column(0, DataType::Utf8View), column(1, DataType::Utf8View)],
        DataType::Utf8View,
    );
    let actual = evaluate(concat, &[views.clone(), views]).unwrap();
    let expected = s
        .iter()
        .map(|v| v.map(|v| format!("{v}{v}")))
        .collect::<StringViewArray>();
    assert_eq!(actual.as_string_view(), &expected);
}

#[test]
fn like_matches_sql_patterns() {
    let input = texts();
    for (pattern, negated) in [
        ("0-500%", false),
        ("%ll%", false),
        ("_nknown", true),
        ("", false),
    ] {
        let p = constant(Arc::new(StringArray::from(vec![pattern])))
            .unwrap()
            .into_ref();
        let e = LikeExpression::new(
            column(0, DataType::Utf8),
            p,
            negated,
            None,
            output_column(DataType::Boolean, true),
        )
        .into_ref();
        let actual = evaluate(e, std::slice::from_ref(&input)).unwrap();
        let matches = |v: &str| match pattern {
            "0-500%" => v.starts_with("0-500"),
            "%ll%" => v.contains("ll"),
            "_nknown" => {
                v.chars().count() == 7 && v.chars().skip(1).collect::<String>() == "nknown"
            }
            _ => v.is_empty(),
        };
        let expected = input
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|v| matches(v) != negated))
            .collect::<BooleanArray>();
        assert_eq!(actual.as_boolean(), &expected, "{pattern}");
    }
}

#[test]
fn abs_and_round_follow_their_types() {
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(-3), None, Some(4), Some(0)]));
    let abs = call(
        FunctionKind::Abs,
        vec![column(0, DataType::Int64)],
        DataType::Int64,
    );
    let actual = evaluate(abs, std::slice::from_ref(&ints)).unwrap();
    assert_eq!(
        actual.as_primitive::<Int64Type>(),
        &Int64Array::from(vec![Some(3), None, Some(4), Some(0)])
    );
    let min: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
    let abs = call(
        FunctionKind::Abs,
        vec![column(0, DataType::Int64)],
        DataType::Int64,
    );
    assert_eq!(
        evaluate(abs, &[min]).unwrap_err().kind(),
        ErrorKind::ArithmeticOverflow
    );

    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(-123_456_789),
            Some(150),
            Some(-250),
            None,
            Some(4_999),
        ])
        .with_precision_and_scale(23, 6)
        .unwrap(),
    );
    let t = DataType::Decimal128(23, 6);
    let abs = call(FunctionKind::Abs, vec![column(0, t.clone())], t.clone());
    let actual = evaluate(abs, std::slice::from_ref(&decimals)).unwrap();
    let expected = decimals
        .as_primitive::<Decimal128Type>()
        .iter()
        .map(|v| v.map(i128::abs))
        .collect::<Decimal128Array>()
        .with_precision_and_scale(23, 6)
        .unwrap();
    assert_eq!(actual.as_primitive::<Decimal128Type>(), &expected);
    // round(x, 2) of decimal(23, 6) is decimal(23, 2), halves away from zero.
    let round = call(
        FunctionKind::Round,
        vec![column(0, t), int(2)],
        DataType::Decimal128(23, 2),
    );
    let actual = evaluate(round, std::slice::from_ref(&decimals)).unwrap();
    let expected = Decimal128Array::from(vec![Some(-12_346), Some(0), Some(0), None, Some(0)])
        .with_precision_and_scale(23, 2)
        .unwrap();
    assert_eq!(actual.as_primitive::<Decimal128Type>(), &expected);

    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.005),
        Some(-2.345),
        None,
        Some(2.5),
    ]));
    let round = call(
        FunctionKind::Round,
        vec![column(0, DataType::Float64), int(2)],
        DataType::Float64,
    );
    let actual = evaluate(round, std::slice::from_ref(&floats)).unwrap();
    let expected = floats
        .as_primitive::<Float64Type>()
        .iter()
        .map(|v| v.map(|v| (v * 100.0).round() / 100.0))
        .collect::<Float64Array>();
    assert_eq!(actual.as_primitive::<Float64Type>(), &expected);
}

#[test]
fn grouping_bits_shift_and_mask() {
    let ids: ArrayRef = Arc::new(UInt8Array::from(vec![0, 1, 2, 3]));
    let mask = ConstantExpression::try_new(
        Arc::new(UInt8Array::from(vec![2])),
        output_column(DataType::UInt8, false),
    )
    .unwrap()
    .into_ref();
    let one = ConstantExpression::try_new(
        Arc::new(UInt8Array::from(vec![1])),
        output_column(DataType::UInt8, false),
    )
    .unwrap()
    .into_ref();
    let and = call(
        FunctionKind::BitwiseAnd,
        vec![column(0, DataType::UInt8), mask],
        DataType::UInt8,
    );
    let shifted = call(FunctionKind::ShiftRight, vec![and, one], DataType::UInt8);
    let actual = evaluate(shifted, &[ids]).unwrap();
    assert_eq!(
        actual.as_primitive::<UInt8Type>(),
        &UInt8Array::from(vec![0, 0, 1, 1])
    );
}
