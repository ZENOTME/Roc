use arrow::{
    array::{Array, ArrayRef, BooleanArray, Datum, Int32Array, Int64Array, Scalar, StringArray},
    compute::kernels::{cmp, numeric},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use roc::{
    error::Error,
    expr::scalar::ScalarExprRef,
    expr::scalar::executor::{ExpressionExecutor, ExpressionInput, ScalarExpressionExecutor},
    expr::scalar::*,
};
use std::sync::Arc;

fn reference(i: usize) -> ScalarExprRef {
    ReferenceExpression::new(i).into_ref()
}
fn int(i: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(i)).into_ref()
}
fn call(f: ScalarFunction, args: Vec<ScalarExprRef>) -> ScalarExprRef {
    FunctionExpression::new(f, args).into_ref()
}
fn integers(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}
fn evaluate(expr: ScalarExprRef, batch: &RecordBatch) -> ArrayRef {
    ExpressionExecutor::try_new(vec![expr], batch.schema())
        .unwrap()
        .evaluate_arrays(&ExpressionInput::new(batch.columns(), batch.num_rows()))
        .unwrap()
        .remove(0)
}
fn ints(array: &ArrayRef) -> Vec<Option<i64>> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn bools(array: &ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

// Compare bound kernels with Arrow's dynamic entry points, including broadcast
// direction, scalar NULLs, and an array of length one that must remain an array.
fn compare_bound_binary_kernels(left: ArrayRef, right: ArrayRef) {
    use ScalarFunction::*;
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", left.data_type().clone(), true),
        Field::new("right", right.data_type().clone(), true),
    ]));
    let rows = left.len();
    let columns = [left, right];
    for left_scalar in [false, true] {
        for right_scalar in [false, true] {
            for scalar_index in [0, 2].into_iter().filter(|&index| index < rows) {
                let l = if left_scalar {
                    columns[0].slice(scalar_index, 1)
                } else {
                    columns[0].clone()
                };
                let r = if right_scalar {
                    columns[1].slice(scalar_index, 1)
                } else {
                    columns[1].clone()
                };
                let ls = Scalar::new(l.slice(0, 1));
                let rs = Scalar::new(r.slice(0, 1));
                let ld: &dyn Datum = if left_scalar { &ls } else { &l };
                let rd: &dyn Datum = if right_scalar { &rs } else { &r };
                for function in [
                    Add,
                    Subtract,
                    Multiply,
                    Divide,
                    Remainder,
                    Equal,
                    NotEqual,
                    LessThan,
                    LessThanOrEqual,
                    GreaterThan,
                    GreaterThanOrEqual,
                    IsDistinctFrom,
                    IsNotDistinctFrom,
                ] {
                    let expected = match function {
                        Add => numeric::add(ld, rd),
                        Subtract => numeric::sub(ld, rd),
                        Multiply => numeric::mul(ld, rd),
                        Divide => numeric::div(ld, rd),
                        Remainder => numeric::rem(ld, rd),
                        Equal => cmp::eq(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        NotEqual => cmp::neq(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        LessThan => cmp::lt(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        LessThanOrEqual => cmp::lt_eq(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        GreaterThan => cmp::gt(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        GreaterThanOrEqual => cmp::gt_eq(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        IsDistinctFrom => cmp::distinct(ld, rd).map(|a| Arc::new(a) as ArrayRef),
                        IsNotDistinctFrom => {
                            cmp::not_distinct(ld, rd).map(|a| Arc::new(a) as ArrayRef)
                        }
                        _ => unreachable!(),
                    };
                    let argument = |value: &ArrayRef, scalar, index| {
                        if scalar {
                            ConstantExpression::try_new(value.clone())
                                .unwrap()
                                .into_ref()
                        } else {
                            reference(index)
                        }
                    };
                    let expr = call(
                        function,
                        vec![argument(&l, left_scalar, 0), argument(&r, right_scalar, 1)],
                    );
                    let mut executor =
                        ScalarExpressionExecutor::try_new(expr, schema.clone()).unwrap();
                    assert_eq!(executor.is_scalar(), left_scalar && right_scalar);
                    let actual = executor.evaluate(&ExpressionInput::new(&columns, rows));
                    let context = format!(
                        "{:?} {function:?}, scalar=({left_scalar},{right_scalar}), index={scalar_index}",
                        l.data_type()
                    );
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => {
                            assert_eq!(actual.to_data(), expected.to_data(), "{context}");
                        }
                        (Err(_), Err(_)) => {}
                        (actual, expected) => {
                            panic!("{context}: actual={actual:?}, expected={expected:?}")
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn all_bound_numeric_signatures_match_arrow() {
    macro_rules! check {
        ($array:ty, $native:ty) => {{
            let left = [
                Some(6 as $native),
                Some(3 as $native),
                None,
                Some(12 as $native),
            ];
            let right = [Some(2 as $native), None, None, Some(4 as $native)];
            compare_bound_binary_kernels(
                Arc::new(<$array>::from(left.to_vec())),
                Arc::new(<$array>::from(right.to_vec())),
            );
            compare_bound_binary_kernels(
                Arc::new(<$array>::from(vec![Some(6 as $native)])),
                Arc::new(<$array>::from(vec![Some(2 as $native)])),
            );
        }};
    }
    use arrow::array::*;
    check!(Int8Array, i8);
    check!(Int16Array, i16);
    check!(Int32Array, i32);
    check!(Int64Array, i64);
    check!(UInt8Array, u8);
    check!(UInt16Array, u16);
    check!(UInt32Array, u32);
    check!(UInt64Array, u64);
    check!(Float32Array, f32);
    check!(Float64Array, f64);
    // Signed zeros and NaNs must preserve Arrow's total-order comparisons,
    // while division by zero retains IEEE floating-point results.
    compare_bound_binary_kernels(
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::NAN),
            None,
            Some(1.0),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            None,
            Some(0.0),
        ])),
    );
}

#[test]
fn nested_branches_and_predicates_preserve_selection_order_and_duplicates() {
    use ScalarFunction::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2)]);
    let rows = [3, 0, 2, 1, 3];
    let input = ExpressionInput::new(batch.columns(), batch.num_rows()).with_selection(&rows);
    let guarded = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            call(NotEqual, vec![reference(0), int(0)]),
            call(
                GreaterThan,
                vec![call(Divide, vec![int(100), reference(0)]), int(30)],
            ),
        ],
    )
    .into_ref();
    let predicate = ConjunctionExpression::new(
        Conjunction::Or,
        vec![call(Equal, vec![reference(0), int(0)]), guarded.clone()],
    )
    .into_ref();
    let mut filter = ScalarExpressionExecutor::try_new(predicate, batch.schema()).unwrap();
    assert_eq!(filter.select(&input).unwrap(), vec![3, 0, 3]);

    let expr = CaseExpression::new(vec![(guarded, int(1))], int(0)).into_ref();
    let mut executor = ScalarExpressionExecutor::try_new(expr, batch.schema()).unwrap();
    assert!(!executor.is_scalar());
    assert_eq!(
        ints(&executor.evaluate(&input).unwrap()),
        vec![Some(1), Some(0), Some(0), Some(0), Some(1)]
    );

    let coalesce = CoalesceExpression::new(vec![reference(0), int(-1)]).into_ref();
    let case = CaseExpression::new(
        vec![
            (call(Equal, vec![reference(0), int(0)]), int(0)),
            (
                call(IsNotNull, vec![reference(0)]),
                call(Divide, vec![int(100), reference(0)]),
            ),
        ],
        coalesce,
    )
    .into_ref();
    let mut executor =
        ScalarExpressionExecutor::try_new(call(Add, vec![case, int(3)]), batch.schema()).unwrap();
    let first = executor.evaluate(&input).unwrap();
    assert_eq!(
        ints(&first),
        vec![Some(53), Some(3), Some(2), Some(28), Some(53)]
    );
    let one = [1];
    assert_eq!(
        ints(&executor.evaluate(&input.with_selection(&one)).unwrap()),
        vec![Some(28)]
    );
    assert!(!executor.is_scalar());
    assert_eq!(ints(&executor.evaluate(&input).unwrap()), ints(&first));
}

#[test]
fn coalesce_reuses_branch_state_after_errors_with_selected_inputs() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("divisor", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(7), None, Some(9), None, None])),
        Arc::new(Int64Array::from(vec![0, 2, 0, 4, 0])),
    ];
    let expr = CoalesceExpression::new(vec![
        reference(0),
        call(ScalarFunction::Divide, vec![int(100), reference(1)]),
    ])
    .into_ref();
    let mut executor = ScalarExpressionExecutor::try_new(expr, schema).unwrap();
    let rows = [3, 0, 1, 2, 1];
    let input = ExpressionInput::new(&columns, 5).with_selection(&rows);
    let result = executor.evaluate(&input).unwrap();
    assert_eq!(
        ints(&result),
        vec![Some(25), Some(7), Some(50), Some(9), Some(50)]
    );
    assert!(executor.evaluate(&input.with_selection(&[3, 4])).is_err());
    assert_eq!(ints(&executor.evaluate(&input).unwrap()), ints(&result));
    assert_eq!(
        ints(&result),
        vec![Some(25), Some(7), Some(50), Some(9), Some(50)]
    );
}

#[test]
fn typed_kernels_downcast_used_columns_without_validating_the_input_layout() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let mut executor = ScalarExpressionExecutor::try_new(int(3), schema.clone()).unwrap();
    let wrong_type: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2]))];
    // Constants do not inspect unrelated columns or retain the bound schema.
    let input = ExpressionInput::new(&wrong_type, 2);
    assert_eq!(ints(&executor.evaluate(&input).unwrap()), vec![Some(3)]);
    assert_eq!(
        ints(&executor.evaluate_array(&input).unwrap()),
        vec![Some(3); 2]
    );
    assert_eq!(
        executor.evaluate(&input.with_selection(&[])).unwrap().len(),
        0
    );
    let mut addition = ScalarExpressionExecutor::try_new(
        call(ScalarFunction::Add, vec![reference(0), int(1)]),
        schema,
    )
    .unwrap();
    assert!(
        matches!(addition.evaluate(&input), Err(Error::Execution(message)) if message == "expected Int64 array")
    );
}

#[test]
fn concrete_executors_work_independently_and_convert_to_dispatch() {
    let batch = integers(vec![Some(2), None, Some(4)]);
    let schema = batch.schema();
    let input = ExpressionInput::new(batch.columns(), batch.num_rows());
    let mut column = ReferenceExpression::new(0)
        .create_executor(schema.clone())
        .unwrap();
    assert_eq!(
        ints(&column.evaluate(&input).unwrap()),
        vec![Some(2), None, Some(4)]
    );
    let mut constant = ConstantExpression::int64(Some(3))
        .create_executor(schema.clone())
        .unwrap();
    assert_eq!(ints(&constant.evaluate(&input).unwrap()), vec![Some(3)]);
    assert_eq!(
        ints(&constant.evaluate_array(&input).unwrap()),
        vec![Some(3); 3]
    );

    let add = FunctionExpression::new(ScalarFunction::Add, vec![reference(0), int(3)]);
    let mut binary = FunctionExpressionExecutor::try_new(&add, schema.clone()).unwrap();
    assert_eq!(
        ints(&binary.evaluate(&input).unwrap()),
        vec![Some(5), None, Some(7)]
    );
    let mut wrapped: ScalarExpressionExecutor = binary.into();
    assert_eq!(
        ints(&wrapped.evaluate(&input).unwrap()),
        vec![Some(5), None, Some(7)]
    );

    let negate = FunctionExpression::new(ScalarFunction::Negate, vec![reference(0)]);
    let mut unary = negate.create_executor(schema.clone()).unwrap();
    assert_eq!(
        ints(&unary.evaluate(&input).unwrap()),
        vec![Some(-2), None, Some(-4)]
    );

    let mut cast = CastExpression::new(reference(0), DataType::Int32, CastMode::Strict)
        .create_executor(schema.clone())
        .unwrap();
    let cast_output = cast.evaluate(&input).unwrap();
    assert_eq!(
        cast_output
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2), None, Some(4)]
    );

    let is_null = call(ScalarFunction::IsNull, vec![reference(0)]);
    let mut not = NotExpression::new(is_null.clone())
        .create_executor(schema.clone())
        .unwrap();
    assert_eq!(
        bools(&not.evaluate(&input).unwrap()),
        vec![Some(true), Some(false), Some(true)]
    );
    let mut conjunction = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            NotExpression::new(is_null.clone()).into_ref(),
            call(ScalarFunction::GreaterThan, vec![reference(0), int(2)]),
        ],
    )
    .create_executor(schema.clone())
    .unwrap();
    assert_eq!(conjunction.select(&input).unwrap(), vec![2]);

    let mut case = CaseExpression::new(vec![(is_null, int(7))], reference(0))
        .create_executor(schema.clone())
        .unwrap();
    assert_eq!(
        ints(&case.evaluate(&input).unwrap()),
        vec![Some(2), Some(7), Some(4)]
    );
    let mut coalesce = CoalesceExpression::new(vec![reference(0), int(7)])
        .create_executor(schema.clone())
        .unwrap();
    assert_eq!(
        ints(&coalesce.evaluate(&input).unwrap()),
        vec![Some(2), Some(7), Some(4)]
    );

    // Standalone executors must retain the empty-input contract and skip kernels.
    let error = call(ScalarFunction::Divide, vec![int(1), int(0)]);
    let mut case = CaseExpression::new(vec![], error.clone())
        .create_executor(schema.clone())
        .unwrap();
    let mut coalesce = CoalesceExpression::new(vec![error])
        .create_executor(schema)
        .unwrap();
    let empty = input.with_selection(&[]);
    assert_eq!(case.evaluate(&empty).unwrap().len(), 0);
    assert_eq!(coalesce.evaluate(&empty).unwrap().len(), 0);
}

#[test]
fn checked_integer_errors_and_null_rows_match_arrow() {
    compare_bound_binary_kernels(
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(1),
            None,
            Some(i64::MAX),
        ])),
        Arc::new(Int64Array::from(vec![Some(-1), Some(0), None, Some(1)])),
    );
    let batch = integers(vec![None]);
    // Dividing a NULL row by zero must not invoke the checked operation.
    assert_eq!(
        ints(&evaluate(
            call(ScalarFunction::Divide, vec![reference(0), int(0)]),
            &batch
        )),
        vec![None]
    );
    assert_eq!(
        ints(&evaluate(
            call(ScalarFunction::Remainder, vec![int(i64::MIN), int(-1)]),
            &batch
        )),
        vec![Some(0)]
    );
    let unsigned = ConstantExpression::try_new(Arc::new(arrow::array::UInt64Array::from(vec![1])))
        .unwrap()
        .into_ref();
    assert!(
        ScalarExpressionExecutor::try_new(
            call(ScalarFunction::Negate, vec![unsigned]),
            batch.schema()
        )
        .is_err()
    );
}

#[test]
fn numeric_kernels_broadcast_scalars_and_preserve_nulls() {
    use ScalarFunction::*;
    let batch = integers(vec![Some(6), None, Some(-3)]);
    for (function, expected) in [
        (Add, vec![Some(8), None, Some(-1)]),
        (Subtract, vec![Some(4), None, Some(-5)]),
        (Multiply, vec![Some(12), None, Some(-6)]),
        (Divide, vec![Some(3), None, Some(-1)]),
        (Remainder, vec![Some(0), None, Some(-1)]),
    ] {
        assert_eq!(
            ints(&evaluate(
                call(function, vec![reference(0), int(2)]),
                &batch
            )),
            expected
        );
    }
    assert_eq!(
        ints(&evaluate(call(Negate, vec![reference(0)]), &batch)),
        vec![Some(-6), None, Some(3)]
    );
    let mut executor =
        ExpressionExecutor::try_new(vec![call(Add, vec![int(2), int(3)])], batch.schema()).unwrap();
    assert!(executor.executors()[0].is_scalar());
    assert_eq!(
        ints(
            &executor
                .evaluate(&ExpressionInput::new(batch.columns(), batch.num_rows()))
                .unwrap()[0]
        ),
        vec![Some(5)]
    );
    let overflow = call(Add, vec![int(i64::MAX), int(1)]);
    assert!(
        ExpressionExecutor::try_new(vec![overflow], batch.schema())
            .unwrap()
            .evaluate_arrays(&ExpressionInput::new(batch.columns(), batch.num_rows()))
            .is_err()
    );
}

#[test]
fn comparisons_and_null_safe_comparisons() {
    use ScalarFunction::*;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None, None])),
            Arc::new(Int64Array::from(vec![Some(1), Some(3), None, Some(4)])),
        ],
    )
    .unwrap();
    for (function, expected) in [
        (Equal, vec![Some(true), Some(false), None, None]),
        (NotEqual, vec![Some(false), Some(true), None, None]),
        (LessThan, vec![Some(false), Some(true), None, None]),
        (LessThanOrEqual, vec![Some(true), Some(true), None, None]),
        (GreaterThan, vec![Some(false), Some(false), None, None]),
        (
            GreaterThanOrEqual,
            vec![Some(true), Some(false), None, None],
        ),
        (
            IsDistinctFrom,
            vec![Some(false), Some(true), Some(false), Some(true)],
        ),
        (
            IsNotDistinctFrom,
            vec![Some(true), Some(false), Some(true), Some(false)],
        ),
    ] {
        assert_eq!(
            bools(&evaluate(
                call(function, vec![reference(0), reference(1)]),
                &batch
            )),
            expected
        );
    }
    assert_eq!(
        bools(&evaluate(call(IsNull, vec![reference(0)]), &batch)),
        vec![Some(false), Some(false), Some(true), Some(true)]
    );
    assert_eq!(
        bools(&evaluate(call(IsNotNull, vec![reference(0)]), &batch)),
        vec![Some(true), Some(true), Some(false), Some(false)]
    );
}

#[test]
fn boolean_value_execution_preserves_three_valued_logic() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Boolean, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let values = [Some(false), Some(true), None];
    let a = values.into_iter().flat_map(|v| [v; 3]).collect::<Vec<_>>();
    let b = values.repeat(3);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(a.clone())),
            Arc::new(BooleanArray::from(b.clone())),
        ],
    )
    .unwrap();
    let and =
        ConjunctionExpression::new(Conjunction::And, vec![reference(0), reference(1)]).into_ref();
    let or =
        ConjunctionExpression::new(Conjunction::Or, vec![reference(0), reference(1)]).into_ref();
    let expected_and = a
        .iter()
        .zip(&b)
        .map(|(a, b)| match (a, b) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_or = a
        .iter()
        .zip(&b)
        .map(|(a, b)| match (a, b) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(bools(&evaluate(and.clone(), &batch)), expected_and);
    assert_eq!(bools(&evaluate(or.clone(), &batch)), expected_or);
    assert_eq!(
        bools(&evaluate(
            NotExpression::new(reference(0)).into_ref(),
            &batch
        )),
        a.iter().map(|v| v.map(|v| !v)).collect::<Vec<_>>()
    );
    for (expr, expected) in [(and, expected_and), (or, expected_or)] {
        let mask = ExpressionExecutor::try_new(vec![expr], batch.schema())
            .unwrap()
            .select(&ExpressionInput::new(batch.columns(), batch.num_rows()))
            .unwrap();
        assert_eq!(
            mask,
            expected
                .iter()
                .enumerate()
                .filter_map(|(i, value)| (*value == Some(true)).then_some(i))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn case_executes_only_matching_rows_in_original_order() {
    use ScalarFunction::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2), Some(0)]);
    let expr = CaseExpression::new(
        vec![
            (call(IsNull, vec![reference(0)]), int(-1)),
            (
                call(NotEqual, vec![reference(0), int(0)]),
                call(Divide, vec![int(100), reference(0)]),
            ),
        ],
        int(0),
    )
    .into_ref();
    let mut executor = ExpressionExecutor::try_new(vec![expr.clone()], batch.schema()).unwrap();
    let result = executor
        .evaluate_arrays(&ExpressionInput::new(batch.columns(), batch.num_rows()))
        .unwrap()
        .remove(0);
    assert_eq!(
        ints(&result),
        vec![Some(0), Some(25), Some(-1), Some(50), Some(0)]
    );
    // A second worker and subsequent evaluations must not invalidate old results.
    assert_eq!(ints(&evaluate(expr, &batch)), ints(&result));
    let next = integers(vec![Some(5), Some(0)]);
    assert_eq!(
        ints(
            &executor
                .evaluate_arrays(&ExpressionInput::new(next.columns(), next.num_rows()))
                .unwrap()
                .remove(0)
        ),
        vec![Some(20), Some(0)]
    );
    assert_eq!(
        ints(&result),
        vec![Some(0), Some(25), Some(-1), Some(50), Some(0)]
    );
    // Unselected scalar errors must also stay unevaluated.
    let expr = CaseExpression::new(
        vec![(
            ConstantExpression::boolean(Some(false)).into_ref(),
            call(Divide, vec![int(1), int(0)]),
        )],
        int(7),
    )
    .into_ref();
    assert_eq!(ints(&evaluate(expr, &batch)), vec![Some(7); 5]);
}

#[test]
fn coalesce_selects_remaining_rows_and_skips_unused_errors() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("divisor", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None, Some(9), None])),
            Arc::new(Int64Array::from(vec![0, 2, 0, 4])),
        ],
    )
    .unwrap();
    let expr = CoalesceExpression::new(vec![
        reference(0),
        call(ScalarFunction::Divide, vec![int(100), reference(1)]),
        call(ScalarFunction::Divide, vec![int(1), int(0)]),
    ])
    .into_ref();
    assert_eq!(
        ints(&evaluate(expr, &batch)),
        vec![Some(7), Some(50), Some(9), Some(25)]
    );
    let all_null = CoalesceExpression::new(vec![
        ConstantExpression::int64(None).into_ref(),
        ConstantExpression::int64(None).into_ref(),
    ])
    .into_ref();
    assert_eq!(ints(&evaluate(all_null, &batch)), vec![None; 4]);
}

#[test]
fn filter_selection_short_circuits_but_value_evaluation_keeps_its_contract() {
    use ScalarFunction::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2)]);
    let expr = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            call(NotEqual, vec![reference(0), int(0)]),
            call(
                GreaterThan,
                vec![call(Divide, vec![int(100), reference(0)]), int(30)],
            ),
        ],
    )
    .into_ref();
    let mut executor = ExpressionExecutor::try_new(vec![expr], batch.schema()).unwrap();
    assert_eq!(
        executor
            .select(&ExpressionInput::new(batch.columns(), batch.num_rows()))
            .unwrap(),
        vec![3]
    );
    assert!(
        executor
            .evaluate_arrays(&ExpressionInput::new(batch.columns(), batch.num_rows()))
            .is_err()
    );
    assert_eq!(
        executor
            .select(&ExpressionInput::new(batch.columns(), batch.num_rows()))
            .unwrap(),
        vec![3]
    );
}

#[test]
fn cast_modes_result_metadata_and_empty_inputs() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["42", "bad"]))]).unwrap();
    let strict = CastExpression::new(reference(0), DataType::Int64, CastMode::Strict).into_ref();
    let try_cast = CastExpression::new(reference(0), DataType::Int64, CastMode::Try).into_ref();
    let mut executor = ExpressionExecutor::try_new(vec![try_cast], batch.schema()).unwrap();
    assert!(executor.results().next().unwrap().nullable);
    assert_eq!(
        ints(
            &executor
                .evaluate_arrays(&ExpressionInput::new(batch.columns(), batch.num_rows()))
                .unwrap()
                .remove(0)
        ),
        vec![Some(42), None]
    );
    assert!(
        ExpressionExecutor::try_new(vec![strict], batch.schema())
            .unwrap()
            .evaluate_arrays(&ExpressionInput::new(batch.columns(), batch.num_rows()))
            .is_err()
    );
    let error_expr = call(ScalarFunction::Divide, vec![int(1), int(0)]);
    let empty = RecordBatch::new_empty(batch.schema());
    assert_eq!(evaluate(error_expr, &empty).len(), 0);
    // Zero-column batches still have an explicit cardinality.
    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    assert_eq!(ints(&evaluate(int(5), &input)), vec![Some(5); 3]);
}

#[test]
fn invalid_descriptions_are_rejected_without_rebinding() {
    let batch = integers(vec![Some(1)]);
    let i32_constant = ConstantExpression::try_new(Arc::new(Int32Array::from(vec![1])))
        .unwrap()
        .into_ref();
    for expr in [
        reference(1),
        call(ScalarFunction::Add, vec![reference(0)]),
        call(ScalarFunction::Add, vec![reference(0), i32_constant]),
        NotExpression::new(reference(0)).into_ref(),
        CoalesceExpression::new(vec![]).into_ref(),
        CaseExpression::new(vec![(int(1), int(2))], int(3)).into_ref(),
        CaseExpression::new(
            vec![(ConstantExpression::boolean(Some(true)).into_ref(), int(2))],
            ConstantExpression::string(Some("bad")).into_ref(),
        )
        .into_ref(),
    ] {
        assert!(matches!(
            ExpressionExecutor::try_new(vec![expr], batch.schema()),
            Err(Error::InvalidPlan(_))
        ));
    }
    assert!(ConstantExpression::try_new(Arc::new(Int64Array::from(vec![1, 2]))).is_err());
}
