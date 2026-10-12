use arrow::{
    array::*,
    compute::{CastOptions, cast_with_options},
    datatypes::*,
};
use roc::{
    expr::{ExpressionResultType, scalar::*},
    suboperator::{ExecutionMode, ProgramContext},
};
use std::sync::Arc;

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
                    ReferenceExpression::new(
                        0,
                        ExpressionResultType::new(input.data_type().clone(), true),
                    )
                    .into_ref(),
                    target.clone(),
                    mode,
                    true,
                )
                .into_ref();
                let p = ProgramContext::from_expressions(&[expr], ExecutionMode::Compiled).unwrap();
                let actual = p.execute(
                    std::slice::from_ref(input),
                    input.len(),
                    &mut p.new_execution_context(),
                );
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
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Utf8, true)).into_ref(),
        DataType::Int8,
        CastMode::Try,
        true,
    )
    .into_ref();
    let text = CastExpression::new(parsed, DataType::Utf8, CastMode::Strict, true).into_ref();
    let p = ProgramContext::from_expressions(&[text], ExecutionMode::Compiled).unwrap();
    let mut local = p.new_execution_context();
    let out = p.execute(&[input], 5, &mut local).unwrap();
    p.execute(&[Arc::new(StringArray::from(vec!["0"]))], 1, &mut local)
        .unwrap();
    assert_eq!(
        out[0].as_ref(),
        &StringArray::from(vec![Some("127"), None, None, None, Some("-128")])
    );
    assert_eq!(p.compiled_program_count(), 1);
}

#[test]
fn typed_float_branches_merge_and_generated_integer_cast_is_visible_in_ir() {
    let yes = ConstantExpression::boolean(Some(true)).into_ref();
    let value = ConstantExpression::try_new(Arc::new(Float32Array::from(vec![1.25])))
        .unwrap()
        .into_ref();
    let no = ConstantExpression::null(&DataType::Float32).into_ref();
    let case = CaseExpression::new(vec![(yes, value)], no, DataType::Float32, true).into_ref();
    let p = ProgramContext::from_expressions(&[case], ExecutionMode::Compiled).unwrap();
    let out = p.execute(&[], 3, &mut p.new_execution_context()).unwrap();
    assert_eq!(out[0].as_ref(), &Float32Array::from(vec![1.25; 3]));

    let cast = CastExpression::new(
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int32, true)).into_ref(),
        DataType::Int64,
        CastMode::Strict,
        true,
    )
    .into_ref();
    let p = ProgramContext::from_expressions(&[cast], ExecutionMode::Compiled).unwrap();
    assert!(
        p.generated_ir().contains("sextend.i64"),
        "{}",
        p.generated_ir()
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
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Utf8, true)).into_ref(),
        DataType::Boolean,
        CastMode::Try,
        true,
    )
    .into_ref();
    let p = ProgramContext::from_expressions(&[cast], ExecutionMode::Compiled).unwrap();
    let out = p
        .execute(
            std::slice::from_ref(&input),
            input.len(),
            &mut p.new_execution_context(),
        )
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
            ReferenceExpression::new(0, ExpressionResultType::new(DataType::Binary, true))
                .into_ref(),
            DataType::Utf8,
            mode,
            true,
        )
        .into_ref();
        let p = ProgramContext::from_expressions(&[cast], ExecutionMode::Compiled).unwrap();
        let out = p.execute(
            std::slice::from_ref(&input),
            3,
            &mut p.new_execution_context(),
        );
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
