use arrow::{
    array::{Array, ArrayRef, AsArray, Int64Array, StringArray},
    datatypes::DataType,
};
#[cfg(feature = "jit")]
use arrow::{
    array::{Float64Array, Int32Array},
    buffer::{NullBuffer, ScalarBuffer},
    datatypes::Int64Type,
};
use roc::{
    error::Error,
    expr::{ExpressionResultType, scalar::*},
    suboperator::*,
};
use std::sync::Arc;
fn reference(index: usize, ty: DataType) -> ScalarExprRef {
    ReferenceExpression::new(index, ExpressionResultType::new(ty, true)).into_ref()
}
fn binary(kind: FunctionKind, a: ScalarExprRef, b: ScalarExprRef, ty: DataType) -> ScalarExprRef {
    FunctionExpression::binary(kind, a, b, ty, true).into_ref()
}
#[test]
fn dense_ids_reject_foreign_variables_and_share_common_dependencies() {
    let mut a = SuboperatorBuilder::new();
    let x = a.input(0, ExpressionResultType::new(DataType::Int64, true));
    let mut b = SuboperatorBuilder::new();
    let y = b.input(0, ExpressionResultType::new(DataType::Int64, true));
    assert_eq!(x.index(), y.index());
    assert!(matches!(
        a.binary(FunctionKind::Add, x, y),
        Err(Error::InvalidPlan(_))
    ));
    let sum = binary(
        FunctionKind::Add,
        reference(0, DataType::Int64),
        ConstantExpression::int64(Some(2)).into_ref(),
        DataType::Int64,
    );
    let p = ProgramContext::from_expressions(
        &[sum.clone(), sum],
        roc::suboperator::ExecutionMode::Native,
    )
    .unwrap();
    // Constant is a bound state parameter, not a broadcast temporary.
    assert_eq!(p.programs().len(), 3);
    assert_eq!(p.programs()[1].states().len(), 1);
    let mut local = p.new_execution_context();
    for rows in [5, 0, 1, 9] {
        let input: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))];
        let out = p.execute(&input, rows, &mut local).unwrap();
        assert_eq!(out[0].as_ref(), out[1].as_ref());
        assert_eq!(out[0].len(), rows);
    }
}
#[test]
fn empty_expression_list_exports_no_columns() {
    let program = ProgramContext::from_expressions(&[], ExecutionMode::Native).unwrap();
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    assert!(
        program
            .execute(&columns, 3, &mut program.new_execution_context())
            .unwrap()
            .is_empty()
    );
}
#[test]
fn arrow_buffers_keep_slices_children_and_owners() {
    let a: ArrayRef = Arc::new(StringArray::from(vec![Some("before"), None, Some("after")]));
    let buffer = ColumnBuffer::from_arrow(a.clone()).slice(1, 2).unwrap();
    drop(a);
    assert_eq!(
        buffer
            .as_arrow()
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some("after")]
    );
    assert!(buffer.slice(2, 1).is_err());
}
#[cfg(feature = "jit")]
fn compare_compiled(expr: ScalarExprRef, columns: &[ArrayRef], rows: usize, compiled: bool) {
    let native = ProgramContext::from_expressions(
        std::slice::from_ref(&expr),
        roc::suboperator::ExecutionMode::Native,
    )
    .unwrap();
    let expected = native.execute(columns, rows, &mut native.new_execution_context());
    let jit = ProgramContext::from_expressions(&[expr], ExecutionMode::Compiled).unwrap();
    assert_eq!(jit.is_compiled(), compiled);
    let actual = jit.execute(columns, rows, &mut jit.new_execution_context());
    match (expected, actual) {
        (Ok(a), Ok(b)) => assert_eq!(a, b),
        (Err(Error::Arrow(_)), Err(Error::Arrow(_))) => {}
        (a, b) => panic!("native={a:?}, compiled={b:?}"),
    }
}
#[cfg(feature = "jit")]
#[test]
fn compiled_loop_matches_native_with_nulls_offsets_overflow_and_multiple_outputs() {
    let values = Int64Array::new(
        ScalarBuffer::from(vec![99, i64::MAX, 7, -3, 0, 123]),
        Some(NullBuffer::from(vec![true, false, true, true, true, true])),
    )
    .slice(1, 4);
    let columns: Vec<ArrayRef> = vec![Arc::new(values)];
    let x = reference(0, DataType::Int64);
    let two = ConstantExpression::int64(Some(2)).into_ref();
    for kind in [
        FunctionKind::Add,
        FunctionKind::Subtract,
        FunctionKind::Multiply,
        FunctionKind::Equal,
        FunctionKind::NotEqual,
        FunctionKind::LessThan,
        FunctionKind::LessThanOrEqual,
        FunctionKind::GreaterThan,
        FunctionKind::GreaterThanOrEqual,
    ] {
        let ty = if matches!(
            kind,
            FunctionKind::Add | FunctionKind::Subtract | FunctionKind::Multiply
        ) {
            DataType::Int64
        } else {
            DataType::Boolean
        };
        compare_compiled(binary(kind, x.clone(), two.clone(), ty), &columns, 4, true);
    }
    for kind in [
        FunctionKind::Negate,
        FunctionKind::IsNull,
        FunctionKind::IsNotNull,
    ] {
        let ty = if kind == FunctionKind::Negate {
            DataType::Int64
        } else {
            DataType::Boolean
        };
        compare_compiled(
            FunctionExpression::unary(kind, x.clone(), ty, true).into_ref(),
            &columns,
            4,
            true,
        );
    }
    let expr = binary(FunctionKind::Add, x.clone(), two.clone(), DataType::Int64);
    compare_compiled(
        expr.clone(),
        &[Arc::new(Int64Array::from(vec![i64::MAX]))],
        1,
        true,
    );
    compare_compiled(
        expr.clone(),
        &[Arc::new(Int64Array::from(Vec::<i64>::new()))],
        0,
        true,
    );
    let p = ProgramContext::from_expressions(&[expr, x.clone()], ExecutionMode::Compiled).unwrap();
    let out = p
        .execute(&columns, 4, &mut p.new_execution_context())
        .unwrap();
    assert_eq!(out[1].as_ref(), columns[0].as_ref());
    assert_eq!(
        out[0]
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(9), Some(-1), Some(2)]
    );
    // A failed batch does not leave stale output pointers in a reusable instance.
    let mut local = p.new_execution_context();
    assert!(
        p.execute(&[Arc::new(Int64Array::from(vec![i64::MAX]))], 1, &mut local)
            .is_err()
    );
    assert!(p.execute(&columns, 4, &mut local).is_ok());
    // The immutable code owner can be shared; each call owns separate frames.
    let p = Arc::new(p);
    std::thread::scope(|s| {
        for _ in 0..4 {
            let p = p.clone();
            let columns = &columns;
            s.spawn(move || {
                for _ in 0..10 {
                    assert!(
                        p.execute(columns, 4, &mut p.new_execution_context())
                            .is_ok()
                    );
                }
            });
        }
    });
}
#[cfg(feature = "jit")]
#[test]
fn compiled_float_and_explicit_native_boundaries() {
    let cols: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
        Some(1.5),
        None,
        Some(-2.5),
    ]))];
    for kind in [
        FunctionKind::Add,
        FunctionKind::Subtract,
        FunctionKind::Multiply,
        FunctionKind::Divide,
    ] {
        compare_compiled(
            binary(
                kind,
                reference(0, DataType::Float64),
                ConstantExpression::float64(Some(2.0)).into_ref(),
                DataType::Float64,
            ),
            &cols,
            3,
            true,
        );
    }
    let ints: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2, 3]))];
    compare_compiled(
        binary(
            FunctionKind::Add,
            reference(0, DataType::Int32),
            reference(0, DataType::Int32),
            DataType::Int32,
        ),
        &ints,
        3,
        true,
    );
    let divide = binary(
        FunctionKind::Divide,
        ConstantExpression::int64(Some(1)).into_ref(),
        ConstantExpression::int64(Some(0)).into_ref(),
        DataType::Int64,
    );
    let expr = CaseExpression::new(
        vec![(
            ConstantExpression::boolean(Some(true)).into_ref(),
            ConstantExpression::int64(Some(7)).into_ref(),
        )],
        divide,
        DataType::Int64,
        false,
    )
    .into_ref();
    compare_compiled(expr, &[], 5, true);
    compare_compiled(ConstantExpression::boolean(None).into_ref(), &[], 3, true);
}

#[cfg(feature = "jit")]
#[test]
fn compiled_signed_boundaries_and_float_total_comparison() {
    for (kind, left, right) in [
        (FunctionKind::Add, i64::MAX, 1),
        (FunctionKind::Subtract, i64::MIN, 1),
        (FunctionKind::Multiply, i64::MIN, -1),
        (FunctionKind::Multiply, i64::MAX, 2),
    ] {
        let expr = binary(
            kind,
            reference(0, DataType::Int64),
            ConstantExpression::int64(Some(right)).into_ref(),
            DataType::Int64,
        );
        compare_compiled(
            expr.clone(),
            &[Arc::new(Int64Array::from(vec![left]))],
            1,
            true,
        );
        let masked = Int64Array::new(
            ScalarBuffer::from(vec![left]),
            Some(NullBuffer::from(vec![false])),
        );
        compare_compiled(expr, &[Arc::new(masked)], 1, true);
    }
    compare_compiled(
        FunctionExpression::unary(
            FunctionKind::Negate,
            reference(0, DataType::Int64),
            DataType::Int64,
            true,
        )
        .into_ref(),
        &[Arc::new(Int64Array::from(vec![i64::MIN]))],
        1,
        true,
    );
    let floats: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
        f64::NAN,
        -0.0,
        0.0,
        f64::INFINITY,
    ]))];
    compare_compiled(
        binary(
            FunctionKind::Equal,
            reference(0, DataType::Float64),
            ConstantExpression::float64(Some(f64::NAN)).into_ref(),
            DataType::Boolean,
        ),
        &floats,
        4,
        true,
    );
}

#[cfg(feature = "jit")]
#[test]
fn compiled_primitive_widths_overflow_unsigned_order_and_null_safe_comparisons() {
    use arrow::array::*;
    fn verify(left: ArrayRef, right: ArrayRef) {
        let ty = left.data_type().clone();
        let rows = left.len();
        let columns = vec![left, right];
        for kind in [
            FunctionKind::Add,
            FunctionKind::Subtract,
            FunctionKind::Multiply,
            FunctionKind::Equal,
            FunctionKind::NotEqual,
            FunctionKind::LessThan,
            FunctionKind::LessThanOrEqual,
            FunctionKind::GreaterThan,
            FunctionKind::GreaterThanOrEqual,
            FunctionKind::IsDistinctFrom,
            FunctionKind::IsNotDistinctFrom,
        ] {
            let result = if matches!(
                kind,
                FunctionKind::Add | FunctionKind::Subtract | FunctionKind::Multiply
            ) {
                ty.clone()
            } else {
                DataType::Boolean
            };
            compare_compiled(
                binary(
                    kind,
                    reference(0, ty.clone()),
                    reference(1, ty.clone()),
                    result,
                ),
                &columns,
                rows,
                true,
            );
        }
    }
    macro_rules! integers {
        ($array:ty,$native:ty) => {{
            for (l, r) in [(1, 2), (<$native>::MAX, 1), (<$native>::MIN, 2)] {
                let a = <$array>::from(vec![Some(9), Some(l), None, Some(l), None]).slice(1, 4);
                let b = <$array>::from(vec![Some(9), Some(r), Some(r), None, None]).slice(1, 4);
                verify(Arc::new(a), Arc::new(b));
            }
        }};
    }
    integers!(Int8Array, i8);
    integers!(Int16Array, i16);
    integers!(Int32Array, i32);
    integers!(Int64Array, i64);
    integers!(UInt8Array, u8);
    integers!(UInt16Array, u16);
    integers!(UInt32Array, u32);
    integers!(UInt64Array, u64);
    verify(
        Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            None,
            Some(-0.0),
            Some(f32::INFINITY),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            Some(1.0),
            Some(0.0),
            Some(f32::NEG_INFINITY),
        ])),
    );
}

#[cfg(feature = "jit")]
#[test]
fn compiled_boolean_kleene_tables_and_sliced_bitmaps() {
    use arrow::array::BooleanArray;
    let pairs = [Some(false), Some(true), None]
        .into_iter()
        .flat_map(|a| {
            [Some(false), Some(true), None]
                .into_iter()
                .map(move |b| (a, b))
        })
        .collect::<Vec<_>>();
    let left = BooleanArray::from(pairs.iter().map(|p| p.0).collect::<Vec<_>>()).slice(1, 8);
    let right = BooleanArray::from(pairs.iter().map(|p| p.1).collect::<Vec<_>>()).slice(1, 8);
    let columns: Vec<ArrayRef> = vec![Arc::new(left), Arc::new(right)];
    for and in [Conjunction::And, Conjunction::Or] {
        compare_compiled(
            ConjunctionExpression::new(
                and,
                vec![
                    reference(0, DataType::Boolean),
                    reference(1, DataType::Boolean),
                ],
                true,
            )
            .into_ref(),
            &columns,
            8,
            true,
        );
    }
    compare_compiled(
        NotExpression::new(reference(0, DataType::Boolean), true).into_ref(),
        &columns,
        8,
        true,
    );
    for kind in [
        FunctionKind::Equal,
        FunctionKind::LessThan,
        FunctionKind::IsDistinctFrom,
        FunctionKind::IsNotDistinctFrom,
    ] {
        compare_compiled(
            binary(
                kind,
                reference(0, DataType::Boolean),
                reference(1, DataType::Boolean),
                DataType::Boolean,
            ),
            &columns,
            8,
            true,
        );
    }
}

#[cfg(feature = "jit")]
#[test]
fn staged_partition_and_native_barriers_preserve_values() {
    let mut expr = reference(0, DataType::Int64);
    for _ in 0..10 {
        expr = binary(
            FunctionKind::Add,
            expr,
            ConstantExpression::int64(Some(1)).into_ref(),
            DataType::Int64,
        );
    }
    // Integer division is a fallible native kernel; the planner must still
    // compile both compute phases around it.
    expr = binary(
        FunctionKind::Divide,
        expr,
        ConstantExpression::int64(Some(2)).into_ref(),
        DataType::Int64,
    );
    for _ in 0..6 {
        expr = binary(
            FunctionKind::Multiply,
            expr,
            ConstantExpression::int64(Some(2)).into_ref(),
            DataType::Int64,
        );
    }
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(-10)]))];
    let program = ProgramContext::from_expressions(
        std::slice::from_ref(&expr),
        roc::suboperator::ExecutionMode::Native,
    )
    .unwrap();
    let mut local = program.new_execution_context();
    let expected = program.execute(&columns, 3, &mut local).unwrap();
    for (mode, count) in [
        (ExecutionMode::Compiled, 2),
        (ExecutionMode::Staged, 5),
        (ExecutionMode::Native, 0),
    ] {
        let program = ProgramContext::from_expressions(std::slice::from_ref(&expr), mode).unwrap();
        assert_eq!(
            program.compiled_program_count(),
            count,
            "{}",
            program.explain()
        );
        assert_eq!(program.execute(&columns, 3, &mut local).unwrap(), expected);
    }
}

#[cfg(feature = "jit")]
#[test]
fn independent_source_columns_do_not_split_fusion() {
    let left = binary(
        FunctionKind::Add,
        reference(0, DataType::Int64),
        ConstantExpression::int64(Some(1)).into_ref(),
        DataType::Int64,
    );
    let right = binary(
        FunctionKind::Add,
        reference(1, DataType::Int64),
        ConstantExpression::int64(Some(2)).into_ref(),
        DataType::Int64,
    );
    let expr = binary(FunctionKind::Multiply, left, right, DataType::Int64);
    let program =
        ProgramContext::from_expressions(std::slice::from_ref(&expr), ExecutionMode::Native)
            .unwrap();
    let input: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(3), None])),
        Arc::new(Int64Array::from(vec![Some(4), Some(5)])),
    ];
    let expected = program
        .execute(&input, 2, &mut program.new_execution_context())
        .unwrap();
    let program = ProgramContext::from_expressions(&[expr], ExecutionMode::Compiled).unwrap();
    assert_eq!(program.compiled_program_count(), 1, "{}", program.explain());
    assert_eq!(
        program
            .execute(&input, 2, &mut program.new_execution_context())
            .unwrap(),
        expected
    );
}
