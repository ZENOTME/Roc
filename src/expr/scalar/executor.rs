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

//! Shared input and immutable scalar evaluations.
use super::*;
use crate::error::{Error, Result};
use arrow::array::ArrayRef;

/// The executor provides shared Arrow columns and an explicit input row count.
/// It does not own evaluations or their outputs; results follow input row order.
#[derive(Debug, Default)]
pub struct ScalarExpressionExecutor {
    columns: Option<Vec<ArrayRef>>,
    num_rows: usize,
}
impl ScalarExpressionExecutor {
    pub fn new(columns: &[ArrayRef], num_rows: usize) -> Self {
        Self {
            columns: Some(columns.to_vec()),
            num_rows,
        }
    }
    pub fn set_input(&mut self, columns: &[ArrayRef], num_rows: usize) {
        self.columns = Some(columns.to_vec());
        self.num_rows = num_rows;
    }
    pub(super) fn columns(&self) -> Result<&[ArrayRef]> {
        self.columns
            .as_deref()
            .ok_or_else(|| Error::Execution("input columns are not set".into()))
    }
    pub fn num_rows(&self) -> Result<usize> {
        self.columns()?;
        Ok(self.num_rows)
    }
}

/// Dispatch to the concrete evaluation. Outputs are returned to the caller;
/// results correspond to the executor's input rows.
#[derive(Debug)]
pub enum ScalarExpressionEvaluation {
    Reference(ReferenceExpressionEvaluation),
    Constant(ConstantExpressionEvaluation),
    UnaryFunction(UnaryFunctionExpressionEvaluation),
    BinaryFunction(BinaryFunctionExpressionEvaluation),
    Cast(CastExpressionEvaluation),
    And(AndExpressionEvaluation),
    Or(OrExpressionEvaluation),
    Not(NotExpressionEvaluation),
    Case(CaseExpressionEvaluation),
    Coalesce(CoalesceExpressionEvaluation),
}
impl ScalarExpressionEvaluation {
    pub fn try_new(expression: ScalarExprRef) -> Result<Self> {
        expression.to_evaluation()
    }
    /// Evaluate the expression tree; concrete nodes prepare their child inputs.
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        match self {
            Self::Reference(e) => e.evaluate(executor),
            Self::Constant(e) => e.evaluate(executor),
            Self::UnaryFunction(e) => e.evaluate(executor),
            Self::BinaryFunction(e) => e.evaluate(executor),
            Self::Cast(e) => e.evaluate(executor),
            Self::And(e) => e.evaluate(executor),
            Self::Or(e) => e.evaluate(executor),
            Self::Not(e) => e.evaluate(executor),
            Self::Case(e) => e.evaluate(executor),
            Self::Coalesce(e) => e.evaluate(executor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let selected_input =
            RecordBatch::try_new(input.schema(), vec![Arc::new(Int64Array::from(vec![4, 2]))])
                .unwrap();
        let selected =
            ScalarExpressionExecutor::new(selected_input.columns(), selected_input.num_rows());
        let expression = binary(
            FunctionKind::Add,
            reference(),
            binary(FunctionKind::Multiply, reference(), int(2)),
        );
        let evaluation = expression.to_evaluation().unwrap();
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
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let expression = binary(FunctionKind::Divide, int(100), reference());
        let evaluation = expression.to_evaluation().unwrap();
        let valid_input = RecordBatch::try_new(
            batch().schema(),
            vec![Arc::new(Int64Array::from(vec![4, 2]))],
        )
        .unwrap();
        let valid = ScalarExpressionExecutor::new(valid_input.columns(), valid_input.num_rows());
        let saved = evaluation.evaluate(&valid).unwrap();
        assert_eq!(ints(&saved), vec![25, 50]);
        assert!(evaluation.evaluate(&executor).is_err());
        let next_input = input.slice(0, 1);
        let next = ScalarExpressionExecutor::new(next_input.columns(), next_input.num_rows());
        assert_eq!(ints(&evaluation.evaluate(&next).unwrap()), vec![100]);
        assert_eq!(ints(&saved), vec![25, 50]);
    }
}
