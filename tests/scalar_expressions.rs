use arrow::{
    array::{Array, ArrayRef, BooleanArray, Datum, Int32Array, Int64Array, Scalar, StringArray},
    compute::{
        concat,
        kernels::{cmp, numeric},
    },
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use roc::{
    error::Error, expr::ExpressionResultType, expr::predicate::select_true,
    expr::scalar::ScalarExprRef, expr::scalar::executor::ScalarExpressionExecutor, expr::scalar::*,
};
use std::sync::Arc;

/// The nullable Int64 column used by most single-column tests.
fn reference(i: usize) -> ScalarExprRef {
    column(i, DataType::Int64, true)
}
/// A column reference carrying the type and nullability the host resolved from
/// its batch schema.
fn column(i: usize, data_type: DataType, nullable: bool) -> ScalarExprRef {
    ReferenceExpression::new(i, ExpressionResultType::new(data_type, nullable)).into_ref()
}
fn int(i: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(i)).into_ref()
}
/// A unary or binary built-in. The host declares the result type the kernel is
/// selected against: the operand type for arithmetic, Boolean for predicates.
fn call(
    f: FunctionKind,
    args: Vec<ScalarExprRef>,
    data_type: DataType,
    nullable: bool,
) -> ScalarExprRef {
    match args.as_slice() {
        [argument] => {
            FunctionExpression::unary(f, argument.clone(), data_type, nullable).into_ref()
        }
        [left, right] => {
            FunctionExpression::binary(f, left.clone(), right.clone(), data_type, nullable)
                .into_ref()
        }
        _ => panic!("function expressions take one or two arguments"),
    }
}
/// Comparisons and IS [NOT] NULL always produce Boolean.
fn predicate(f: FunctionKind, args: Vec<ScalarExprRef>, nullable: bool) -> ScalarExprRef {
    call(f, args, DataType::Boolean, nullable)
}
fn integers(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}

fn evaluate(expr: ScalarExprRef, batch: &RecordBatch) -> ArrayRef {
    let executor = expr.to_evaluation().unwrap();
    executor
        .evaluate(&ScalarExpressionExecutor::new(
            batch.columns(),
            batch.num_rows(),
        ))
        .unwrap()
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

#[test]
fn input_binding_retains_shared_arrays_and_releases_replaced_input() {
    let evaluation = reference(0).to_evaluation().unwrap();
    let mut executor = ScalarExpressionExecutor::default();
    assert!(
        matches!(evaluation.evaluate(&executor), Err(Error::Execution(message)) if message.contains("input"))
    );
    assert!(
        matches!(evaluation.evaluate(&executor).and_then(select_true), Err(Error::Execution(message)) if message.contains("input"))
    );

    let (first_result, first_input) = {
        let batch = integers(vec![Some(2), None, Some(4)]);
        let input = Arc::downgrade(batch.column(0));
        executor.set_input(batch.columns(), batch.num_rows());
        let view = evaluation.evaluate(&executor).unwrap();

        assert!(Arc::ptr_eq(&view, batch.column(0)));
        (view, input)
    };
    assert_eq!(
        ints(&evaluation.evaluate(&executor).unwrap()),
        vec![Some(2), None, Some(4)]
    );
    let next = integers(vec![Some(7)]);
    executor.set_input(next.columns(), next.num_rows());
    assert_eq!(
        ints(&evaluation.evaluate(&executor).unwrap()),
        vec![Some(7)]
    );
    assert_eq!(ints(&first_result), vec![Some(2), None, Some(4)]);
    drop(first_result);
    assert!(first_input.upgrade().is_none());
}

#[test]
fn multiple_expressions_share_input_buffers_and_evaluation_recovers_after_errors() {
    let evaluations = vec![
        reference(0),
        call(
            FunctionKind::Add,
            vec![reference(0), int(3)],
            DataType::Int64,
            true,
        ),
    ]
    .iter()
    .map(|e| e.to_evaluation().unwrap())
    .collect::<Vec<_>>();
    let mut executor = ScalarExpressionExecutor::default();
    assert!(evaluations.iter().all(|e| e.evaluate(&executor).is_err()));
    let batch = integers(vec![Some(2), None, Some(4)]);
    executor.set_input(batch.columns(), batch.num_rows());
    let values = evaluations
        .iter()
        .map(|e| e.evaluate(&executor).unwrap())
        .collect::<Vec<_>>();
    assert!(Arc::ptr_eq(&values[0], batch.column(0)));
    assert_eq!(ints(&values[1]), vec![Some(5), None, Some(7)]);
    executor.set_input(&[batch.column(0).slice(0, 0)], 0);
    assert!(
        evaluations
            .iter()
            .all(|e| e.evaluate(&executor).unwrap().is_empty())
    );

    let guarded = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            predicate(FunctionKind::NotEqual, vec![reference(0), int(0)], true),
            predicate(
                FunctionKind::GreaterThan,
                vec![
                    call(
                        FunctionKind::Divide,
                        vec![int(100), reference(0)],
                        DataType::Int64,
                        true,
                    ),
                    int(30),
                ],
                true,
            ),
        ],
        true,
    )
    .into_ref();
    let evaluation = guarded.to_evaluation().unwrap();
    executor.set_input(
        &[Arc::new(Int64Array::from(vec![
            Some(2),
            Some(0),
            None,
            Some(4),
        ]))],
        4,
    );
    // AND evaluates both children on the same batch, including x=0.
    assert!(
        evaluation
            .evaluate(&executor)
            .and_then(select_true)
            .is_err()
    );
    assert!(evaluation.evaluate(&executor).is_err());
    executor.set_input(
        &[Arc::new(Int64Array::from(vec![Some(2), None, Some(4)]))],
        3,
    );
    assert_eq!(
        evaluation
            .evaluate(&executor)
            .and_then(select_true)
            .unwrap(),
        vec![0]
    );
    assert_eq!(
        evaluation
            .evaluate(&executor)
            .and_then(select_true)
            .unwrap(),
        vec![0]
    );
}

// Compare selected kernels with Arrow's dynamic entry points, including
// constants on either side, NULLs, and one-row inputs. Arrow may return a
// single value for two scalar operands; Roc always returns the batch row count.
fn compare_bound_binary_kernels(left: ArrayRef, right: ArrayRef) {
    use FunctionKind::*;
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
                    // Arithmetic keeps the operand type; comparisons produce
                    // Boolean, and the NULL-safe ones can never produce NULL.
                    let (result_type, nullable) = match function {
                        Add | Subtract | Multiply | Divide | Remainder => {
                            (l.data_type().clone(), true)
                        }
                        IsDistinctFrom | IsNotDistinctFrom => (DataType::Boolean, false),
                        _ => (DataType::Boolean, true),
                    };
                    let argument = |value: &ArrayRef, scalar, index| {
                        if scalar {
                            ConstantExpression::try_new(value.clone())
                                .unwrap()
                                .into_ref()
                        } else {
                            column(index, value.data_type().clone(), true)
                        }
                    };
                    let expr = call(
                        function,
                        vec![argument(&l, left_scalar, 0), argument(&r, right_scalar, 1)],
                        result_type,
                        nullable,
                    );
                    let executor = ScalarExpressionEvaluation::try_new(expr).unwrap();
                    let actual = executor.evaluate(&ScalarExpressionExecutor::new(&columns, rows));
                    let context = format!(
                        "{:?} {function:?}, scalar=({left_scalar},{right_scalar}), index={scalar_index}",
                        l.data_type()
                    );
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => {
                            let expected = if left_scalar && right_scalar {
                                concat(&vec![expected.as_ref(); rows]).unwrap()
                            } else {
                                expected
                            };
                            assert_eq!(actual.len(), rows, "{context}");
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
fn nested_branches_and_predicates_preserve_input_order_and_duplicates() {
    use FunctionKind::*;
    let batch = integers(vec![Some(2), Some(0), None, Some(4), Some(2)]);
    let columns = batch.columns();
    let num_rows = batch.num_rows();
    let guarded = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            predicate(NotEqual, vec![reference(0), int(0)], true),
            predicate(
                GreaterThan,
                vec![
                    CaseExpression::new(
                        vec![(
                            predicate(NotEqual, vec![reference(0), int(0)], true),
                            call(Divide, vec![int(100), reference(0)], DataType::Int64, true),
                        )],
                        int(0),
                        DataType::Int64,
                        true,
                    )
                    .into_ref(),
                    int(30),
                ],
                true,
            ),
        ],
        true,
    )
    .into_ref();
    let filter_predicate = ConjunctionExpression::new(
        Conjunction::Or,
        vec![
            predicate(Equal, vec![reference(0), int(0)], true),
            guarded.clone(),
        ],
        true,
    )
    .into_ref();
    let filter = ScalarExpressionEvaluation::try_new(filter_predicate).unwrap();
    assert_eq!(
        filter
            .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
            .and_then(select_true)
            .unwrap(),
        vec![0, 1, 4]
    );

    let expr =
        CaseExpression::new(vec![(guarded, int(1))], int(0), DataType::Int64, false).into_ref();
    let executor = ScalarExpressionEvaluation::try_new(expr).unwrap();
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(1), Some(0), Some(0), Some(0), Some(1)]
    );

    let coalesce =
        CoalesceExpression::new(vec![reference(0), int(-1)], DataType::Int64, false).into_ref();
    let case = CaseExpression::new(
        vec![
            (predicate(Equal, vec![reference(0), int(0)], true), int(0)),
            (
                predicate(IsNotNull, vec![reference(0)], false),
                call(Divide, vec![int(100), reference(0)], DataType::Int64, true),
            ),
        ],
        coalesce,
        DataType::Int64,
        true,
    )
    .into_ref();
    let executor =
        ScalarExpressionEvaluation::try_new(call(Add, vec![case, int(3)], DataType::Int64, true))
            .unwrap();
    let first = executor
        .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
        .unwrap()
        .clone();
    assert_eq!(
        ints(&first),
        vec![Some(53), Some(3), Some(2), Some(28), Some(53)]
    );
    let one = batch.slice(3, 1);
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(
                    one.columns(),
                    one.num_rows()
                ))
                .unwrap()
        ),
        vec![Some(28)]
    );
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        ints(&first)
    );
}

#[test]
fn nested_case_selects_original_rows_and_recovers_after_branch_errors() {
    use FunctionKind::*;
    // x is column 1 and y is column 2. Nested branch batches must address the
    // original chunk and keep both columns aligned, including duplicate values.
    let x = column(1, DataType::Int64, true);
    let y = column(2, DataType::Int64, false);
    let nested = CaseExpression::new(
        vec![(
            predicate(NotEqual, vec![y.clone(), int(0)], false),
            call(Divide, vec![y, x.clone()], DataType::Int64, true),
        )],
        int(0),
        DataType::Int64,
        true,
    )
    .into_ref();
    let expr = CaseExpression::new(
        vec![(predicate(NotEqual, vec![x, int(0)], true), nested)],
        int(0),
        DataType::Int64,
        true,
    )
    .into_ref();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["unused"; 6])),
        Arc::new(Int64Array::from(vec![
            Some(0),
            Some(2),
            None,
            Some(-2),
            Some(4),
            Some(2),
        ])),
        Arc::new(Int64Array::from(vec![100, 10, 100, 6, 0, 10])),
    ];
    let executor = expr.to_evaluation().unwrap();
    let result = executor
        .evaluate(&ScalarExpressionExecutor::new(&columns, 6))
        .unwrap()
        .clone();
    assert_eq!(
        ints(&result),
        vec![Some(0), Some(5), Some(0), Some(-3), Some(0), Some(5)]
    );

    // This passes the zero guard but overflows inside the nested division.
    let bad_columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["unused"; 2])),
        Arc::new(Int64Array::from(vec![0, -1])),
        Arc::new(Int64Array::from(vec![100, i64::MIN])),
    ];
    assert!(
        executor
            .evaluate(&ScalarExpressionExecutor::new(&bad_columns, 2))
            .is_err()
    );
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(&columns, 6))
                .unwrap()
        ),
        ints(&result)
    );
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(&[], 0))
                .unwrap()
        ),
        vec![]
    );
}

#[test]
fn zero_column_inputs_keep_row_counts_through_branches_and_predicates() {
    let error = call(
        FunctionKind::Divide,
        vec![int(1), int(0)],
        DataType::Int64,
        false,
    );
    let case = CaseExpression::new(
        vec![(ConstantExpression::boolean(Some(true)).into_ref(), int(5))],
        error.clone(),
        DataType::Int64,
        false,
    )
    .into_ref();
    let coalesce = CoalesceExpression::new(
        vec![
            ConstantExpression::null(&DataType::Int64).into_ref(),
            int(7),
            error,
        ],
        DataType::Int64,
        false,
    )
    .into_ref();
    let executors = vec![case, coalesce]
        .iter()
        .map(|e| e.to_evaluation().unwrap())
        .collect::<Vec<_>>();
    let input = ScalarExpressionExecutor::new(&[], 3);
    let values = executors
        .iter()
        .map(|e| e.evaluate(&input).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ints(&values[0]), vec![Some(5); 3]);
    assert_eq!(ints(&values[1]), vec![Some(7); 3]);
    assert!(executors.iter().all(|e| {
        e.evaluate(&ScalarExpressionExecutor::new(&[], 0))
            .unwrap()
            .is_empty()
    }));

    let predicate = ConjunctionExpression::new(
        Conjunction::Or,
        vec![
            ConstantExpression::boolean(None).into_ref(),
            ConstantExpression::boolean(Some(true)).into_ref(),
        ],
        true,
    )
    .into_ref();
    let predicate = predicate.to_evaluation().unwrap();
    assert_eq!(
        predicate
            .evaluate(&ScalarExpressionExecutor::new(&[], 3))
            .and_then(select_true)
            .unwrap(),
        vec![0, 1, 2]
    );
    assert!(
        predicate
            .evaluate(&ScalarExpressionExecutor::new(&[], 0))
            .and_then(select_true)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn coalesce_reuses_branch_state_after_errors_with_selected_rows() {
    // `value` is nullable and `divisor` is not, per the batch layout below.
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![None, Some(7), None, Some(9), None])),
        Arc::new(Int64Array::from(vec![4, 0, 2, 0, 2])),
    ];
    let expr = CoalesceExpression::new(
        vec![
            column(0, DataType::Int64, true),
            call(
                FunctionKind::Divide,
                vec![int(100), column(1, DataType::Int64, false)],
                DataType::Int64,
                true,
            ),
        ],
        DataType::Int64,
        true,
    )
    .into_ref();
    let executor = ScalarExpressionEvaluation::try_new(expr).unwrap();
    let columns = columns.as_slice();
    let num_rows = 5;
    let result = executor
        .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
        .unwrap()
        .clone();
    assert_eq!(
        ints(&result),
        vec![Some(25), Some(7), Some(50), Some(9), Some(50)]
    );
    let error_columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![None, None])),
        Arc::new(Int64Array::from(vec![4, 0])),
    ];
    assert!(
        executor
            .evaluate(&ScalarExpressionExecutor::new(&error_columns, 2))
            .is_err()
    );
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        ints(&result)
    );
    assert_eq!(
        ints(&result),
        vec![Some(25), Some(7), Some(50), Some(9), Some(50)]
    );
}

#[test]
fn constant_only_expressions_return_full_columns_for_each_batch_size() {
    let expressions = vec![
        int(5),
        call(
            FunctionKind::Add,
            vec![int(2), int(3)],
            DataType::Int64,
            false,
        ),
        CastExpression::new(
            ConstantExpression::string(Some("7")).into_ref(),
            DataType::Int64,
            CastMode::Strict,
            false,
        )
        .into_ref(),
        NotExpression::new(ConstantExpression::boolean(Some(true)).into_ref(), false).into_ref(),
        ConjunctionExpression::new(
            Conjunction::And,
            vec![
                ConstantExpression::boolean(Some(true)).into_ref(),
                ConstantExpression::boolean(None).into_ref(),
            ],
            true,
        )
        .into_ref(),
        ConstantExpression::string(Some("value")).into_ref(),
        ConstantExpression::null(&DataType::Utf8).into_ref(),
    ];
    let executors = expressions
        .iter()
        .map(|e| e.to_evaluation().unwrap())
        .collect::<Vec<_>>();
    // Reuse the same evaluations across changing row counts with no input columns.
    for num_rows in [3, 0, 1, 3] {
        let input = ScalarExpressionExecutor::new(&[], num_rows);
        let values = executors
            .iter()
            .map(|e| e.evaluate(&input).unwrap())
            .collect::<Vec<_>>();
        assert!(values.iter().all(|value| value.len() == num_rows));
        assert_eq!(ints(&values[0]), vec![Some(5); num_rows]);
        assert_eq!(ints(&values[1]), vec![Some(5); num_rows]);
        assert_eq!(ints(&values[2]), vec![Some(7); num_rows]);
        assert_eq!(bools(&values[3]), vec![Some(false); num_rows]);
        assert_eq!(bools(&values[4]), vec![None; num_rows]);
        let strings = |index: usize| {
            values[index]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(strings(5), vec![Some("value"); num_rows]);
        assert_eq!(strings(6), vec![None; num_rows]);
    }
}

#[test]
fn typed_kernels_downcast_used_columns_without_validating_the_input_layout() {
    let executor = ScalarExpressionEvaluation::try_new(int(3)).unwrap();
    let wrong_type: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2]))];
    // Constants do not inspect unrelated columns.
    let columns = wrong_type.as_slice();
    let num_rows = 2;
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(3); 2]
    );
    assert_eq!(
        executor
            .evaluate(&ScalarExpressionExecutor::new(&[], 0))
            .unwrap()
            .len(),
        0
    );
    // The host declared column 0 as Int64 (the real schema), but the input
    // carries Int32; the typed kernel reports the mismatch at run time.
    let addition = ScalarExpressionEvaluation::try_new(call(
        FunctionKind::Add,
        vec![column(0, DataType::Int64, false), int(1)],
        DataType::Int64,
        true,
    ))
    .unwrap();
    assert!(
        matches!(addition.evaluate(&ScalarExpressionExecutor::new(columns, num_rows)), Err(Error::Execution(message)) if message == "expected Int64 array")
    );
}

#[test]
fn each_description_builds_an_evaluation() {
    let batch = integers(vec![Some(2), None, Some(4)]);
    let columns = batch.columns();
    let num_rows = batch.num_rows();
    let column_executor =
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true))
            .to_evaluation()
            .unwrap();
    assert_eq!(
        ints(
            &column_executor
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(2), None, Some(4)]
    );
    let constant_executor = ConstantExpression::int64(Some(3)).to_evaluation().unwrap();
    assert_eq!(
        ints(
            &constant_executor
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(3); 3]
    );

    let add = FunctionExpression::binary(
        FunctionKind::Add,
        reference(0),
        int(3),
        DataType::Int64,
        true,
    );
    let binary = add.to_evaluation().unwrap();
    assert_eq!(
        ints(
            &binary
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(5), None, Some(7)]
    );
    // Binding a unary Add still rejects the unsupported function arity.
    assert!(
        FunctionExpression::unary(FunctionKind::Add, reference(0), DataType::Int64, true,)
            .to_evaluation()
            .is_err()
    );

    let negate =
        FunctionExpression::unary(FunctionKind::Negate, reference(0), DataType::Int64, true);
    let unary = negate.to_evaluation().unwrap();
    assert_eq!(
        ints(
            &unary
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(-2), None, Some(-4)]
    );

    let cast = CastExpression::new(reference(0), DataType::Int32, CastMode::Strict, true)
        .to_evaluation()
        .unwrap();
    let cast_output = cast
        .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
        .unwrap();
    assert_eq!(
        cast_output
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2), None, Some(4)]
    );

    let is_null = predicate(FunctionKind::IsNull, vec![reference(0)], false);
    let not = NotExpression::new(is_null.clone(), true)
        .to_evaluation()
        .unwrap();
    assert_eq!(
        bools(
            &not.evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(true), Some(false), Some(true)]
    );
    let conjunction = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            NotExpression::new(is_null.clone(), true).into_ref(),
            predicate(FunctionKind::GreaterThan, vec![reference(0), int(2)], true),
        ],
        true,
    )
    .to_evaluation()
    .unwrap();
    assert_eq!(
        conjunction
            .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
            .and_then(select_true)
            .unwrap(),
        vec![2]
    );

    let case = CaseExpression::new(vec![(is_null, int(7))], reference(0), DataType::Int64, true)
        .to_evaluation()
        .unwrap();
    assert_eq!(
        ints(
            &case
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(2), Some(7), Some(4)]
    );
    let coalesce = CoalesceExpression::new(vec![reference(0), int(7)], DataType::Int64, true)
        .to_evaluation()
        .unwrap();
    assert_eq!(
        ints(
            &coalesce
                .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
                .unwrap()
        ),
        vec![Some(2), Some(7), Some(4)]
    );

    // Standalone executors must retain the empty-input contract and skip kernels.
    let error = call(
        FunctionKind::Divide,
        vec![int(1), int(0)],
        DataType::Int64,
        false,
    );
    let case = CaseExpression::new(vec![], error.clone(), DataType::Int64, false)
        .to_evaluation()
        .unwrap();
    let coalesce = CoalesceExpression::new(vec![error], DataType::Int64, false)
        .to_evaluation()
        .unwrap();
    let empty = batch.slice(0, 0);
    assert_eq!(
        case.evaluate(&ScalarExpressionExecutor::new(
            empty.columns(),
            empty.num_rows()
        ))
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        coalesce
            .evaluate(&ScalarExpressionExecutor::new(
                empty.columns(),
                empty.num_rows()
            ))
            .unwrap()
            .len(),
        0
    );
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
            call(
                FunctionKind::Divide,
                vec![reference(0), int(0)],
                DataType::Int64,
                true
            ),
            &batch
        )),
        vec![None]
    );
    assert_eq!(
        ints(&evaluate(
            call(
                FunctionKind::Remainder,
                vec![int(i64::MIN), int(-1)],
                DataType::Int64,
                false
            ),
            &batch
        )),
        vec![Some(0)]
    );
    let unsigned = ConstantExpression::try_new(Arc::new(arrow::array::UInt64Array::from(vec![1])))
        .unwrap()
        .into_ref();
    // The host declared the operand type UInt64, which has no negation kernel.
    assert!(
        ScalarExpressionEvaluation::try_new(call(
            FunctionKind::Negate,
            vec![unsigned],
            DataType::UInt64,
            false
        ))
        .is_err()
    );
}

#[test]
fn numeric_kernels_evaluate_constants_per_row_and_preserve_nulls() {
    use FunctionKind::*;
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
                call(function, vec![reference(0), int(2)], DataType::Int64, true),
                &batch
            )),
            expected
        );
    }
    assert_eq!(
        ints(&evaluate(
            call(Negate, vec![reference(0)], DataType::Int64, true),
            &batch
        )),
        vec![Some(-6), None, Some(3)]
    );
    let executor = ScalarExpressionEvaluation::try_new(call(
        Add,
        vec![int(2), int(3)],
        DataType::Int64,
        false,
    ))
    .unwrap();
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(
                    batch.columns(),
                    batch.num_rows()
                ))
                .unwrap()
        ),
        vec![Some(5); batch.num_rows()]
    );
    let overflow = call(Add, vec![int(i64::MAX), int(1)], DataType::Int64, false);
    assert!(
        ScalarExpressionEvaluation::try_new(overflow)
            .unwrap()
            .evaluate(&ScalarExpressionExecutor::new(
                batch.columns(),
                batch.num_rows()
            ))
            .is_err()
    );
}

#[test]
fn comparisons_and_null_safe_comparisons() {
    use FunctionKind::*;
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
        // The NULL-safe comparisons never produce NULL results.
        let nullable = !matches!(function, IsDistinctFrom | IsNotDistinctFrom);
        assert_eq!(
            bools(&evaluate(
                predicate(function, vec![reference(0), reference(1)], nullable),
                &batch
            )),
            expected
        );
    }
    assert_eq!(
        bools(&evaluate(
            predicate(IsNull, vec![reference(0)], false),
            &batch
        )),
        vec![Some(false), Some(false), Some(true), Some(true)]
    );
    assert_eq!(
        bools(&evaluate(
            predicate(IsNotNull, vec![reference(0)], false),
            &batch
        )),
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
    let and = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            column(0, DataType::Boolean, true),
            column(1, DataType::Boolean, true),
        ],
        true,
    )
    .into_ref();
    let or = ConjunctionExpression::new(
        Conjunction::Or,
        vec![
            column(0, DataType::Boolean, true),
            column(1, DataType::Boolean, true),
        ],
        true,
    )
    .into_ref();
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
            NotExpression::new(column(0, DataType::Boolean, true), true).into_ref(),
            &batch
        )),
        a.iter().map(|v| v.map(|v| !v)).collect::<Vec<_>>()
    );
    for (expr, expected) in [(and, expected_and), (or, expected_or)] {
        let evaluation = expr.to_evaluation().unwrap();
        let mask = evaluation
            .evaluate(&ScalarExpressionExecutor::new(
                batch.columns(),
                batch.num_rows(),
            ))
            .and_then(select_true)
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
    use FunctionKind::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2), Some(0)]);
    let expr = CaseExpression::new(
        vec![
            (predicate(IsNull, vec![reference(0)], false), int(-1)),
            (
                predicate(NotEqual, vec![reference(0), int(0)], true),
                call(Divide, vec![int(100), reference(0)], DataType::Int64, true),
            ),
        ],
        int(0),
        DataType::Int64,
        true,
    )
    .into_ref();
    let executor = ScalarExpressionEvaluation::try_new(expr.clone()).unwrap();
    let result = executor
        .evaluate(&ScalarExpressionExecutor::new(
            batch.columns(),
            batch.num_rows(),
        ))
        .unwrap()
        .clone();
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
                .evaluate(&ScalarExpressionExecutor::new(
                    next.columns(),
                    next.num_rows()
                ))
                .unwrap()
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
            call(Divide, vec![int(1), int(0)], DataType::Int64, false),
        )],
        int(7),
        DataType::Int64,
        false,
    )
    .into_ref();
    assert_eq!(ints(&evaluate(expr, &batch)), vec![Some(7); 5]);
}

#[test]
fn coalesce_selects_remaining_rows_and_skips_unused_errors() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("divisor", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None, Some(9), None])),
            Arc::new(Int64Array::from(vec![0, 2, 0, 4])),
        ],
    )
    .unwrap();
    let expr = CoalesceExpression::new(
        vec![
            column(0, DataType::Int64, true),
            call(
                FunctionKind::Divide,
                vec![int(100), column(1, DataType::Int64, false)],
                DataType::Int64,
                true,
            ),
            call(
                FunctionKind::Divide,
                vec![int(1), int(0)],
                DataType::Int64,
                false,
            ),
        ],
        DataType::Int64,
        true,
    )
    .into_ref();
    assert_eq!(
        ints(&evaluate(expr, &batch)),
        vec![Some(7), Some(50), Some(9), Some(25)]
    );
    let all_null = CoalesceExpression::new(
        vec![
            ConstantExpression::int64(None).into_ref(),
            ConstantExpression::int64(None).into_ref(),
        ],
        DataType::Int64,
        true,
    )
    .into_ref();
    assert_eq!(ints(&evaluate(all_null, &batch)), vec![None; 4]);
}

#[test]
fn filter_selection_uses_the_boolean_value_result() {
    use FunctionKind::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2)]);
    // CASE protects the division; AND itself evaluates both Boolean inputs.
    let division = CaseExpression::new(
        vec![(
            predicate(NotEqual, vec![reference(0), int(0)], true),
            call(Divide, vec![int(100), reference(0)], DataType::Int64, true),
        )],
        ConstantExpression::int64(None).into_ref(),
        DataType::Int64,
        true,
    )
    .into_ref();
    let expr = ConjunctionExpression::new(
        Conjunction::And,
        vec![
            predicate(NotEqual, vec![reference(0), int(0)], true),
            predicate(GreaterThan, vec![division, int(30)], true),
        ],
        true,
    )
    .into_ref();
    let evaluation = expr.to_evaluation().unwrap();
    let executor = ScalarExpressionExecutor::new(batch.columns(), batch.num_rows());
    let result = evaluation.evaluate(&executor).unwrap();
    assert_eq!(
        bools(&result),
        vec![Some(false), Some(false), None, Some(true)]
    );
    assert_eq!(select_true(result).unwrap(), vec![3]);
    assert_eq!(
        select_true(evaluation.evaluate(&executor).unwrap()).unwrap(),
        vec![3]
    );
}

#[test]
fn cast_modes_result_metadata_and_empty_inputs() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["42", "bad"]))]).unwrap();
    // The declaring host says a TRY cast may introduce NULLs; a strict cast of a
    // non-nullable column does not.
    let strict = CastExpression::new(
        column(0, DataType::Utf8, false),
        DataType::Int64,
        CastMode::Strict,
        false,
    )
    .into_ref();
    let try_cast = CastExpression::new(
        column(0, DataType::Utf8, false),
        DataType::Int64,
        CastMode::Try,
        true,
    )
    .into_ref();
    assert!(try_cast.result_type().is_nullable());
    let executor = ScalarExpressionEvaluation::try_new(try_cast).unwrap();
    assert_eq!(
        ints(
            &executor
                .evaluate(&ScalarExpressionExecutor::new(
                    batch.columns(),
                    batch.num_rows()
                ))
                .unwrap()
        ),
        vec![Some(42), None]
    );
    assert!(
        ScalarExpressionEvaluation::try_new(strict)
            .unwrap()
            .evaluate(&ScalarExpressionExecutor::new(
                batch.columns(),
                batch.num_rows()
            ))
            .is_err()
    );
    let error_expr = call(
        FunctionKind::Divide,
        vec![int(1), int(0)],
        DataType::Int64,
        false,
    );
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

// The descriptions carry host-supplied metadata, so the library performs no
// type inference and no plan-time validation of its own. Only kernel selection
// and executor construction can still reject a description here; every other
// host mistake is deferred to execution.
#[test]
fn plan_time_validation_is_limited_to_kernel_selection() {
    let batch = integers(vec![Some(1), None]);
    let columns = batch.columns();
    let num_rows = batch.num_rows();
    let i32_constant = ConstantExpression::try_new(Arc::new(Int32Array::from(vec![1])))
        .unwrap()
        .into_ref();

    // `Add` has no unary kernel, so the unary entry point rejects the
    // description while selecting a kernel.
    assert!(matches!(
        call(FunctionKind::Add, vec![reference(0)], DataType::Int64, true).to_evaluation(),
        Err(Error::InvalidPlan(_))
    ));
    // The binary entry point likewise rejects a unary-only kind.
    let binary_is_null = FunctionExpression::binary(
        FunctionKind::IsNull,
        reference(0),
        reference(0),
        DataType::Boolean,
        false,
    );
    assert!(matches!(
        binary_is_null.to_evaluation(),
        Err(Error::InvalidPlan(_))
    ));

    // COALESCE has no value without at least one argument.
    assert!(matches!(
        CoalesceExpression::new(vec![], DataType::Int64, true).to_evaluation(),
        Err(Error::InvalidPlan(message)) if message.contains("coalesce")
    ));

    // A constant still has to hold exactly one value.
    assert!(ConstantExpression::try_new(Arc::new(Int64Array::from(vec![1, 2]))).is_err());

    // An out-of-range column index is not checked while binding; the library
    // only reads the column when the executor runs.
    let out_of_range =
        ReferenceExpression::new(1, ExpressionResultType::new(DataType::Int64, true)).into_ref();
    assert!(ScalarExpressionEvaluation::try_new(out_of_range).is_ok());

    // NOT of a non-Boolean argument is accepted at plan time and fails when the
    // executor downcasts the value.
    let not_integer = NotExpression::new(reference(0), true).into_ref();
    let executor = ScalarExpressionEvaluation::try_new(not_integer).unwrap();
    assert!(matches!(
        executor.evaluate(&ScalarExpressionExecutor::new(columns, num_rows)),
        Err(Error::Execution(_))
    ));

    // Mismatched operand types are accepted at plan time; the typed kernel
    // reports the mismatch when it downcasts.
    let mismatched_operands = call(
        FunctionKind::Add,
        vec![reference(0), i32_constant],
        DataType::Int64,
        true,
    );
    let executor = ScalarExpressionEvaluation::try_new(mismatched_operands).unwrap();
    assert!(matches!(
        executor.evaluate(&ScalarExpressionExecutor::new(columns, num_rows)),
        Err(Error::Execution(_))
    ));

    // A non-Boolean CASE condition is likewise not validated up front.
    let non_boolean_condition =
        CaseExpression::new(vec![(int(1), int(2))], int(3), DataType::Int64, false).into_ref();
    let executor = ScalarExpressionEvaluation::try_new(non_boolean_condition).unwrap();
    assert!(matches!(
        executor.evaluate(&ScalarExpressionExecutor::new(columns, num_rows)),
        Err(Error::Execution(_))
    ));

    // A branch whose type disagrees with the declared result type only fails
    // when both pieces are actually interleaved.
    let mixed_branches = CaseExpression::new(
        vec![(
            predicate(FunctionKind::IsNotNull, vec![reference(0)], false),
            int(2),
        )],
        ConstantExpression::string(Some("bad")).into_ref(),
        DataType::Int64,
        true,
    )
    .into_ref();
    let executor = ScalarExpressionEvaluation::try_new(mixed_branches).unwrap();
    assert!(
        executor
            .evaluate(&ScalarExpressionExecutor::new(columns, num_rows))
            .is_err()
    );
}
