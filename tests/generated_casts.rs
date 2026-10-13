use arrow::{
    array::*,
    compute::{CastOptions, cast_with_options},
    datatypes::*,
};
use roc::expr::scalar::*;
use std::sync::Arc;

mod support;
use support::*;

#[test]
fn generated_numeric_casts_match_arrow_across_widths_nulls_and_boundaries() {
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(i8::MAX),
            Some(-1),
            Some(0),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            None,
            Some(i16::MAX),
            Some(-1),
            Some(0),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
            Some(-1),
            Some(0),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            Some(-1),
            Some(0),
        ])),
        Arc::new(UInt8Array::from(vec![
            Some(u8::MAX),
            None,
            Some(1),
            Some(0),
        ])),
        Arc::new(UInt16Array::from(vec![
            Some(u16::MAX),
            None,
            Some(1),
            Some(0),
        ])),
        Arc::new(UInt32Array::from(vec![
            Some(u32::MAX),
            None,
            Some(1),
            Some(0),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(u64::MAX),
            None,
            Some(1),
            Some(0),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(f32::MIN),
            None,
            Some(f32::MAX),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(-0.5),
            Some(-128.5),
            Some(127.75),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(f64::MIN),
            None,
            Some(f64::MAX),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(-0.5),
            Some(-128.5),
            Some(127.75),
        ])),
    ];
    for input in &inputs {
        for target in inputs.iter().map(|a| a.data_type()) {
            for mode in [CastMode::Strict, CastMode::Try] {
                let expr = CastExpression::new(
                    ReferenceExpression::new(input_column(0, input.data_type().clone(), true))
                        .into_ref(),
                    mode,
                    output_column(target.clone(), true),
                )
                .into_ref();
                let p = TestProgram::try_new(expr).unwrap();
                let actual =
                    p.evaluate_all(&TestInput::new(std::slice::from_ref(input), input.len()));
                let expected = cast_with_options(
                    input.as_ref(),
                    target,
                    &CastOptions {
                        safe: mode == CastMode::Try,
                        ..Default::default()
                    },
                );
                match (actual, expected) {
                    (Ok(a), Ok(b)) => assert_eq!(
                        a[0].as_ref(),
                        b.as_ref(),
                        "{} -> {target}, {mode:?}",
                        input.data_type()
                    ),
                    (Err(_), Err(_)) => {}
                    (a, b) => panic!(
                        "{} -> {target}, {mode:?}: generated={a:?}, Arrow={b:?}",
                        input.data_type()
                    ),
                }
            }
        }
    }
}
#[test]
fn strings_are_typed_scalar_calls_and_survive_the_invocation_arena() {
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        Some("127"),
        Some("128"),
        Some("bad"),
        None,
        Some("-128"),
    ]));
    let parsed = CastExpression::new(
        ReferenceExpression::new(input_column(0, DataType::Utf8, true)).into_ref(),
        CastMode::Try,
        output_column(DataType::Int8, true),
    )
    .into_ref();
    let text = CastExpression::new(
        parsed,
        CastMode::Strict,
        output_column(DataType::Utf8, true),
    )
    .into_ref();
    let p = TestProgram::try_new(text).unwrap();
    let out = p.evaluate_all(&TestInput::new(&[input], 5)).unwrap();
    p.evaluate_all(&TestInput::new(
        &[Arc::new(StringArray::from(vec!["0"]))],
        1,
    ))
    .unwrap();
    assert_eq!(
        out[0].as_ref(),
        &StringArray::from(vec![Some("127"), None, None, None, Some("-128")])
    );
}

#[test]
fn typed_float_branches_merge_and_generated_integer_cast_is_visible_in_ir() {
    let yes =
        ConstantExpression::boolean(Some(true), output_column(DataType::Boolean, false)).into_ref();
    let value = constant(Arc::new(Float32Array::from(vec![1.25])))
        .unwrap()
        .into_ref();
    let no = ConstantExpression::null(&DataType::Float32, output_column(DataType::Float32, true))
        .into_ref();
    let case = CaseExpression::new(
        vec![(yes, value)],
        no,
        output_column(DataType::Float32, true),
    )
    .into_ref();
    let p = TestProgram::try_new(case).unwrap();
    let out = p.evaluate_all(&TestInput::new(&[], 3)).unwrap();
    assert_eq!(out[0].as_ref(), &Float32Array::from(vec![1.25; 3]));

    let cast = CastExpression::new(
        ReferenceExpression::new(input_column(0, DataType::Int32, true)).into_ref(),
        CastMode::Strict,
        output_column(DataType::Int64, true),
    )
    .into_ref();
    let p = TestProgram::try_new(cast).unwrap();
    assert!(
        p.program().generated_ir().contains("sext i32"),
        "{}",
        p.program().generated_ir()
    );
}

#[test]
fn boolean_text_casts_bind_the_boolean_parser() {
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        Some("TRUE"),
        Some(" yes "),
        Some("of"),
        Some("0"),
        Some("invalid"),
        None,
    ]));
    let cast = CastExpression::new(
        ReferenceExpression::new(input_column(0, DataType::Utf8, true)).into_ref(),
        CastMode::Try,
        output_column(DataType::Boolean, true),
    )
    .into_ref();
    let p = TestProgram::try_new(cast).unwrap();
    let out = p
        .evaluate_all(&TestInput::new(std::slice::from_ref(&input), input.len()))
        .unwrap();
    let expected =
        cast_with_options(input.as_ref(), &DataType::Boolean, &CastOptions::default()).unwrap();
    assert_eq!(out[0].as_ref(), expected.as_ref());
}

#[test]
fn binary_to_text_validates_utf8_inside_the_generated_cast() {
    let input: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(b"valid".as_slice()),
        Some(&[0xff]),
        None,
    ]));
    for mode in [CastMode::Strict, CastMode::Try] {
        let cast = CastExpression::new(
            ReferenceExpression::new(input_column(0, DataType::Binary, true)).into_ref(),
            mode,
            output_column(DataType::Utf8, true),
        )
        .into_ref();
        let p = TestProgram::try_new(cast).unwrap();
        let out = p.evaluate_all(&TestInput::new(std::slice::from_ref(&input), 3));
        if mode == CastMode::Strict {
            assert!(out.is_err());
        } else {
            assert_eq!(
                out.unwrap()[0].as_ref(),
                &StringArray::from(vec![Some("valid"), None, None])
            );
        }
    }
}
