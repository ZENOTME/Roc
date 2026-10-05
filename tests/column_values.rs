use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use roc::expr::{ExpressionResultType, scalar::executor::ScalarExpressionExecutor, scalar::*};
use std::sync::Arc;

#[test]
fn nested_scalar_functions_stay_scalar_until_consumed_by_an_array() {
    let pair = FunctionExpression::binary(
        FunctionKind::Add,
        ConstantExpression::int64(Some(1)).into_ref(),
        ConstantExpression::int64(Some(2)).into_ref(),
        DataType::Int64,
        false,
    )
    .into_ref();
    let batch: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(20)]));
    let input = ScalarExpressionExecutor::new(std::slice::from_ref(&batch), 3);
    assert!(matches!(
        pair.to_evaluation().unwrap().evaluate(&input).unwrap(),
        ColumnValue::Scalar(ScalarValue::Int64(Some(3)))
    ));
    let add = FunctionExpression::binary(
        FunctionKind::Add,
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true)).into_ref(),
        pair,
        DataType::Int64,
        true,
    )
    .to_evaluation()
    .unwrap();
    let ColumnValue::Array(output) = add.evaluate(&input).unwrap() else {
        panic!("expected array")
    };
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(13), None, Some(23)]
    );
}

#[test]
fn scalar_cast_not_null_tests_and_conjunction_keep_scalar_results() {
    let input = ScalarExpressionExecutor::new(&[], 4096);
    let cast = CastExpression::new(
        ConstantExpression::string(Some("42")).into_ref(),
        DataType::Int64,
        CastMode::Strict,
        false,
    );
    assert!(matches!(
        cast.to_evaluation().unwrap().evaluate(&input).unwrap(),
        ColumnValue::Scalar(ScalarValue::Int64(Some(42)))
    ));
    let cast = CastExpression::new(
        ConstantExpression::string(Some("bad")).into_ref(),
        DataType::Int64,
        CastMode::Try,
        true,
    );
    assert!(matches!(
        cast.to_evaluation().unwrap().evaluate(&input).unwrap(),
        ColumnValue::Scalar(ScalarValue::Int64(None))
    ));
    for value in [Some(false), Some(true), None] {
        let not = NotExpression::new(ConstantExpression::boolean(value).into_ref(), true);
        assert!(
            matches!(not.to_evaluation().unwrap().evaluate(&input).unwrap(), ColumnValue::Scalar(ScalarValue::Boolean(v)) if v == value.map(|v| !v))
        );
        for function in [FunctionKind::IsNull, FunctionKind::IsNotNull] {
            let expression = FunctionExpression::unary(
                function,
                ConstantExpression::boolean(value).into_ref(),
                DataType::Boolean,
                false,
            );
            assert!(
                matches!(expression.to_evaluation().unwrap().evaluate(&input).unwrap(), ColumnValue::Scalar(ScalarValue::Boolean(Some(v))) if v == (value.is_none() != (function == FunctionKind::IsNotNull)))
            );
        }
        for other in [Some(false), Some(true), None] {
            for and in [true, false] {
                let expression = ConjunctionExpression::new(
                    if and {
                        Conjunction::And
                    } else {
                        Conjunction::Or
                    },
                    vec![
                        ConstantExpression::boolean(value).into_ref(),
                        ConstantExpression::boolean(other).into_ref(),
                    ],
                    true,
                );
                let left = arrow::array::BooleanArray::from(vec![value]);
                let right = arrow::array::BooleanArray::from(vec![other]);
                let expected = if and {
                    arrow::compute::and_kleene(&left, &right).unwrap()
                } else {
                    arrow::compute::or_kleene(&left, &right).unwrap()
                };
                let expected = expected.iter().next().unwrap();
                assert!(
                    matches!(expression.to_evaluation().unwrap().evaluate(&input).unwrap(), ColumnValue::Scalar(ScalarValue::Boolean(v)) if v == expected)
                );
            }
        }
    }
}
