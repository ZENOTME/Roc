use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use roc::{
    error::Error,
    expr::scalar::BoundScalarExprRef,
    expr::scalar::executor::{ExpressionExecutor, ExpressionValue},
    expr::scalar::*,
};
use std::sync::Arc;

fn reference(i: usize) -> BoundScalarExprRef {
    BoundReferenceExpression::new(i).into_ref()
}
fn int(i: i64) -> BoundScalarExprRef {
    BoundConstantExpression::int64(Some(i)).into_ref()
}
fn call(f: ScalarFunction, args: Vec<BoundScalarExprRef>) -> BoundScalarExprRef {
    BoundFunctionExpression::new(f, args).into_ref()
}
fn integers(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}
fn evaluate(expr: BoundScalarExprRef, batch: &RecordBatch) -> ArrayRef {
    ExpressionExecutor::try_new(vec![expr], batch.schema())
        .unwrap()
        .evaluate(batch)
        .unwrap()
        .remove(0)
        .into_array(batch.num_rows())
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
    assert!(matches!(
        executor.evaluate(&batch).unwrap()[0],
        ExpressionValue::Scalar(_)
    ));
    let overflow = call(Add, vec![int(i64::MAX), int(1)]);
    assert!(
        ExpressionExecutor::try_new(vec![overflow], batch.schema())
            .unwrap()
            .evaluate(&batch)
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
    let and = BoundConjunctionExpression::new(Conjunction::And, vec![reference(0), reference(1)])
        .into_ref();
    let or = BoundConjunctionExpression::new(Conjunction::Or, vec![reference(0), reference(1)])
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
            BoundNotExpression::new(reference(0)).into_ref(),
            &batch
        )),
        a.iter().map(|v| v.map(|v| !v)).collect::<Vec<_>>()
    );
    for (expr, expected) in [(and, expected_and), (or, expected_or)] {
        let mask = ExpressionExecutor::try_new(vec![expr], batch.schema())
            .unwrap()
            .select(&batch)
            .unwrap();
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            expected
                .iter()
                .map(|v| Some(*v == Some(true)))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn case_executes_only_matching_rows_in_original_order() {
    use ScalarFunction::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2), Some(0)]);
    let expr = BoundCaseExpression::new(
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
        .evaluate(&batch)
        .unwrap()
        .remove(0)
        .into_array(batch.num_rows())
        .unwrap();
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
                .evaluate(&next)
                .unwrap()
                .remove(0)
                .into_array(2)
                .unwrap()
        ),
        vec![Some(20), Some(0)]
    );
    assert_eq!(
        ints(&result),
        vec![Some(0), Some(25), Some(-1), Some(50), Some(0)]
    );
    // Unselected scalar errors must also stay unevaluated.
    let expr = BoundCaseExpression::new(
        vec![(
            BoundConstantExpression::boolean(Some(false)).into_ref(),
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
    let expr = BoundCoalesceExpression::new(vec![
        reference(0),
        call(ScalarFunction::Divide, vec![int(100), reference(1)]),
        call(ScalarFunction::Divide, vec![int(1), int(0)]),
    ])
    .into_ref();
    assert_eq!(
        ints(&evaluate(expr, &batch)),
        vec![Some(7), Some(50), Some(9), Some(25)]
    );
    let all_null = BoundCoalesceExpression::new(vec![
        BoundConstantExpression::int64(None).into_ref(),
        BoundConstantExpression::int64(None).into_ref(),
    ])
    .into_ref();
    assert_eq!(ints(&evaluate(all_null, &batch)), vec![None; 4]);
}

#[test]
fn filter_selection_short_circuits_but_value_evaluation_keeps_its_contract() {
    use ScalarFunction::*;
    let batch = integers(vec![Some(0), Some(4), None, Some(2)]);
    let expr = BoundConjunctionExpression::new(
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
        executor.select(&batch).unwrap(),
        BooleanArray::from(vec![false, false, false, true])
    );
    assert!(executor.evaluate(&batch).is_err());
    assert_eq!(
        executor.select(&batch).unwrap(),
        BooleanArray::from(vec![false, false, false, true])
    );
}

#[test]
fn cast_modes_result_metadata_and_empty_inputs() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["42", "bad"]))]).unwrap();
    let strict =
        BoundCastExpression::new(reference(0), DataType::Int64, CastMode::Strict).into_ref();
    let try_cast =
        BoundCastExpression::new(reference(0), DataType::Int64, CastMode::Try).into_ref();
    let mut executor = ExpressionExecutor::try_new(vec![try_cast], batch.schema()).unwrap();
    assert!(executor.results().next().unwrap().nullable);
    assert_eq!(
        ints(
            &executor
                .evaluate(&batch)
                .unwrap()
                .remove(0)
                .into_array(2)
                .unwrap()
        ),
        vec![Some(42), None]
    );
    assert!(
        ExpressionExecutor::try_new(vec![strict], batch.schema())
            .unwrap()
            .evaluate(&batch)
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
    let i32_constant = BoundConstantExpression::try_new(Arc::new(Int32Array::from(vec![1])))
        .unwrap()
        .into_ref();
    for expr in [
        reference(1),
        call(ScalarFunction::Add, vec![reference(0)]),
        call(ScalarFunction::Add, vec![reference(0), i32_constant]),
        BoundNotExpression::new(reference(0)).into_ref(),
        BoundCoalesceExpression::new(vec![]).into_ref(),
        BoundCaseExpression::new(vec![(int(1), int(2))], int(3)).into_ref(),
        BoundCaseExpression::new(
            vec![(
                BoundConstantExpression::boolean(Some(true)).into_ref(),
                int(2),
            )],
            BoundConstantExpression::string(Some("bad")).into_ref(),
        )
        .into_ref(),
    ] {
        assert!(matches!(
            ExpressionExecutor::try_new(vec![expr], batch.schema()),
            Err(Error::InvalidPlan(_))
        ));
    }
    assert!(BoundConstantExpression::try_new(Arc::new(Int64Array::from(vec![1, 2]))).is_err());
    let mut executor = ExpressionExecutor::try_new(vec![reference(0)], batch.schema()).unwrap();
    let changed = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    assert!(executor.evaluate(&changed).is_err());
}
