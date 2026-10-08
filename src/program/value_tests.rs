#[cfg(test)]
mod tests {
    use crate::expr::ExpressionResultType;
    use crate::expr::scalar::*;
    use crate::program::test_support::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 0, 4]))],
        )
        .unwrap()
    }
    fn reference() -> ScalarExprRef {
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, false)).into_ref()
    }
    fn int(value: i64) -> ScalarExprRef {
        ConstantExpression::int64(Some(value)).into_ref()
    }
    fn binary(kind: FunctionKind, left: ScalarExprRef, right: ScalarExprRef) -> ScalarExprRef {
        FunctionExpression::binary(kind, left, right, DataType::Int64, false).into_ref()
    }
    fn ints(value: &ColumnValue) -> Vec<i64> {
        let len = match value {
            ColumnValue::Array(a) => a.len(),
            ColumnValue::Scalar(_) => 1,
        };
        let array = value.clone().into_array(len).unwrap();
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn returned_results_are_owned_by_the_caller() {
        let input = batch();
        let executor = Value::input(input.columns(), input.num_rows());
        let selected_input =
            RecordBatch::try_new(input.schema(), vec![Arc::new(Int64Array::from(vec![4, 2]))])
                .unwrap();
        let selected = Value::input(selected_input.columns(), selected_input.num_rows());
        let expression = binary(
            FunctionKind::Add,
            reference(),
            binary(FunctionKind::Multiply, reference(), int(2)),
        );
        let mut evaluation = expression.program().unwrap();
        let output = evaluation.run_value(&selected).unwrap();

        assert_eq!(ints(&output), vec![12, 6]);
        let stored = output.clone();
        let ColumnValue::Array(array) = &output else {
            panic!("expected array")
        };
        let previous = Arc::downgrade(array);
        drop(output);
        assert!(previous.upgrade().is_some());

        assert_eq!(
            ints(&evaluation.run_value(&executor).unwrap()),
            vec![3, 6, 0, 12]
        );
        assert_eq!(ints(&stored), vec![12, 6]);
        drop(stored);
        assert!(previous.upgrade().is_none());
    }

    #[test]
    fn failed_evaluation_does_not_affect_previous_outputs() {
        let input = batch();
        let executor = Value::input(input.columns(), input.num_rows());
        let expression = binary(FunctionKind::Divide, int(100), reference());
        let mut evaluation = expression.program().unwrap();
        let valid_input = RecordBatch::try_new(
            batch().schema(),
            vec![Arc::new(Int64Array::from(vec![4, 2]))],
        )
        .unwrap();
        let valid = Value::input(valid_input.columns(), valid_input.num_rows());
        let saved = evaluation.run_value(&valid).unwrap();
        assert_eq!(ints(&saved), vec![25, 50]);
        assert!(evaluation.run_value(&executor).is_err());
        let next_input = input.slice(0, 1);
        let next = Value::input(next_input.columns(), next_input.num_rows());
        assert_eq!(ints(&evaluation.run_value(&next).unwrap()), vec![100]);
        assert_eq!(ints(&saved), vec![25, 50]);
    }
}
