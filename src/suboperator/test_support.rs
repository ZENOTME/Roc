use crate::error::{Error, Result};
use crate::expr::scalar::{ScalarExprRef, ScalarExpression};
use crate::suboperator::{ProgramContext, ProgramExecutionContext};
use arrow::array::ArrayRef;
use std::sync::Arc;
#[derive(Default)]
pub struct TestInput {
    columns: Option<Vec<ArrayRef>>,
    num_rows: usize,
}
impl TestInput {
    pub fn new(columns: &[ArrayRef], num_rows: usize) -> Self {
        Self {
            columns: Some(columns.to_vec()),
            num_rows,
        }
    }
    #[allow(dead_code)] // Used by the integration test copy of this helper.
    pub fn set_input(&mut self, columns: &[ArrayRef], num_rows: usize) {
        self.columns = Some(columns.to_vec());
        self.num_rows = num_rows;
    }
}
pub struct TestProgram(ProgramContext);
impl TestProgram {
    pub fn try_new(expression: ScalarExprRef) -> Result<Self> {
        Ok(Self(ProgramContext::from_expressions(
            &[expression],
            crate::suboperator::ExecutionMode::Compiled,
        )?))
    }
    pub fn evaluate(&self, input: &TestInput) -> Result<ArrayRef> {
        let columns = input
            .columns
            .as_deref()
            .ok_or_else(|| Error::Execution("input columns are not set".into()))?;
        Ok(self
            .0
            .execute(
                columns,
                input.num_rows,
                &mut ProgramExecutionContext::default(),
            )?
            .remove(0))
    }
}
pub trait PrepareTest {
    fn prepare_test(&self) -> Result<TestProgram>;
}
impl<T: Clone + Into<ScalarExpression>> PrepareTest for T {
    fn prepare_test(&self) -> Result<TestProgram> {
        TestProgram::try_new(Arc::new(self.clone().into()))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{ExpressionResultType, scalar::*};
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
    fn ints(array: &ArrayRef) -> Vec<i64> {
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
        let executor = TestInput::new(input.columns(), input.num_rows());
        let selected_input =
            RecordBatch::try_new(input.schema(), vec![Arc::new(Int64Array::from(vec![4, 2]))])
                .unwrap();
        let selected = TestInput::new(selected_input.columns(), selected_input.num_rows());
        let expression = binary(
            FunctionKind::Add,
            reference(),
            binary(FunctionKind::Multiply, reference(), int(2)),
        );
        let evaluation = expression.prepare_test().unwrap();
        let output = evaluation.evaluate(&selected).unwrap();

        assert_eq!(ints(&output), vec![12, 6]);
        let stored = output.clone();
        let previous = Arc::downgrade(&output);
        drop(output);
        assert!(previous.upgrade().is_some());

        assert_eq!(
            ints(&evaluation.evaluate(&executor).unwrap()),
            vec![3, 6, 0, 12]
        );
        assert_eq!(ints(&stored), vec![12, 6]);
        drop(stored);
        assert!(previous.upgrade().is_none());
    }

    #[test]
    fn failed_evaluation_does_not_affect_previous_outputs() {
        let input = batch();
        let executor = TestInput::new(input.columns(), input.num_rows());
        let expression = binary(FunctionKind::Divide, int(100), reference());
        let evaluation = expression.prepare_test().unwrap();
        let valid_input = RecordBatch::try_new(
            batch().schema(),
            vec![Arc::new(Int64Array::from(vec![4, 2]))],
        )
        .unwrap();
        let valid = TestInput::new(valid_input.columns(), valid_input.num_rows());
        let saved = evaluation.evaluate(&valid).unwrap();
        assert_eq!(ints(&saved), vec![25, 50]);
        assert!(evaluation.evaluate(&executor).is_err());
        let next_input = input.slice(0, 1);
        let next = TestInput::new(next_input.columns(), next_input.num_rows());
        assert_eq!(ints(&evaluation.evaluate(&next).unwrap()), vec![100]);
        assert_eq!(ints(&saved), vec![25, 50]);
    }
}

pub fn select_true(result: ArrayRef) -> Result<Vec<usize>> {
    use arrow::array::{Array, AsArray};
    let value = result
        .as_boolean_opt()
        .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
    Ok((0..value.len())
        .filter(|&i| value.is_valid(i) && value.value(i))
        .collect())
}

#[cfg(test)]
mod predicate_tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray};
    use std::sync::Arc;

    #[test]
    fn selection_uses_batch_positions_and_excludes_nulls() {
        let col: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(true),
        ]));
        assert_eq!(select_true(col).unwrap(), vec![1, 3]);
    }
}
