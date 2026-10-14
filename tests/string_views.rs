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

//! View strings are byte values like any other string type: inline and
//! out-of-line values, NULLs and sliced arrays read and write correctly.
use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, StringArray, StringViewArray},
    datatypes::DataType,
};
use roc::expr::scalar::{
    CaseExpression, CastExpression, CastMode, FunctionExpression, FunctionKind,
    ReferenceExpression, ScalarExprRef,
};
use std::sync::Arc;

mod support;
use support::*;

fn strings() -> ArrayRef {
    let values = [
        Some("short"),
        None,
        Some("a value longer than twelve bytes"),
        Some(""),
        Some("twelve bytes"),
        Some("a value longer than twelve bytes"),
        Some("short"),
    ];
    // Sliced: the first view is not at offset zero.
    let a =
        StringViewArray::from_iter([Some("skipped value, also long")].into_iter().chain(values));
    Arc::new(a.slice(1, values.len()))
}

fn column(t: DataType) -> ScalarExprRef {
    ReferenceExpression::new(input_column(0, t, true)).into_ref()
}

fn view_literal(value: &str) -> ScalarExprRef {
    constant(Arc::new(StringViewArray::from(vec![value])))
        .unwrap()
        .into_ref()
}

fn evaluate(e: ScalarExprRef, input: &ArrayRef) -> ArrayRef {
    TestProgram::try_new(e)
        .unwrap()
        .evaluate(&TestInput::new(std::slice::from_ref(input), input.len()))
        .unwrap()
}

#[test]
fn comparisons_read_inline_and_buffered_views() {
    let input = strings();
    for literal in ["short", "a value longer than twelve bytes", ""] {
        let output = output_column(DataType::Boolean, true);
        let e = FunctionExpression::binary(
            FunctionKind::Equal,
            column(DataType::Utf8View),
            view_literal(literal),
            output,
        )
        .into_ref();
        let actual = evaluate(e, &input);
        let expected = input
            .as_string_view()
            .iter()
            .map(|v| v.map(|v| v == literal))
            .collect::<BooleanArray>();
        assert_eq!(actual.as_boolean(), &expected, "{literal}");
    }
}

#[test]
fn computed_views_are_written_as_views() {
    let input = strings();
    // CASE WHEN s = 'short' THEN 'a value longer than twelve bytes' ELSE s END
    let condition = FunctionExpression::binary(
        FunctionKind::Equal,
        column(DataType::Utf8View),
        view_literal("short"),
        output_column(DataType::Boolean, true),
    )
    .into_ref();
    let e = CaseExpression::new(
        vec![(condition, view_literal("a value longer than twelve bytes"))],
        column(DataType::Utf8View),
        output_column(DataType::Utf8View, true),
    )
    .into_ref();
    let actual = evaluate(e, &input);
    let expected = input
        .as_string_view()
        .iter()
        .map(|v| {
            v.map(|v| {
                if v == "short" {
                    "a value longer than twelve bytes"
                } else {
                    v
                }
            })
        })
        .collect::<StringViewArray>();
    assert_eq!(actual.as_string_view(), &expected);
}

#[test]
fn views_cast_to_and_from_offset_strings() {
    let input = strings();
    let to_utf8 = CastExpression::new(
        column(DataType::Utf8View),
        CastMode::Strict,
        output_column(DataType::Utf8, true),
    )
    .into_ref();
    let utf8 = evaluate(to_utf8, &input);
    let expected = input.as_string_view().iter().collect::<StringArray>();
    assert_eq!(utf8.as_string::<i32>(), &expected);
    let back = CastExpression::new(
        column(DataType::Utf8),
        CastMode::Strict,
        output_column(DataType::Utf8View, true),
    )
    .into_ref();
    let views = evaluate(back, &utf8);
    assert_eq!(views.as_string_view(), input.as_string_view());
    assert_eq!(views.null_count(), 1);
}
