use super::ScalarExprRef;
use super::executor::{BinaryEvalFn, UnaryEvalFn};
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use super::{ExpressionResultType, kernels};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, new_empty_array},
    datatypes::DataType,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionKind {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Negate,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

/// A unary or binary built-in call, such as an arithmetic operator, a
/// comparison, or a NULL test.
#[derive(Clone, Debug)]
pub struct FunctionExpression {
    function: FunctionKind,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}

impl FunctionExpression {
    pub fn unary(
        function: FunctionKind,
        argument: ScalarExprRef,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            function,
            arguments: vec![argument],
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
        }
    }

    pub fn binary(
        function: FunctionKind,
        left: ScalarExprRef,
        right: ScalarExprRef,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            function,
            arguments: vec![left, right],
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
        }
    }

    pub fn function(&self) -> FunctionKind {
        self.function
    }

    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }

    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        match &self.arguments[..] {
            [argument] => Ok(UnaryFunctionExpressionEvaluation::try_new(self, argument)?.into()),
            [left, right] => {
                Ok(BinaryFunctionExpressionEvaluation::try_new(self, left, right)?.into())
            }
            _ => unreachable!("function expressions are built as unary or binary"),
        }
    }
}

#[derive(Debug)]
pub struct UnaryFunctionExpressionEvaluation {
    data_type: DataType,
    argument: Box<ScalarExpressionEvaluation>,
    eval_fn: UnaryEvalFn,
}
impl UnaryFunctionExpressionEvaluation {
    pub(super) fn try_new(
        expression: &FunctionExpression,
        argument: &ScalarExprRef,
    ) -> Result<Self> {
        let argument_evaluation = argument.to_evaluation()?;
        // Kernels are keyed by the declared operand type; the result type only
        // describes the output of e.g. a comparison.
        let eval_fn = kernels::bind_unary(expression.function, &argument.result_type().data_type)?;
        Ok(Self {
            data_type: expression.result_type.data_type.clone(),
            argument: Box::new(argument_evaluation),
            eval_fn,
        })
    }
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let argument = self.argument.evaluate(executor)?;
        self.eval(executor, &[&argument])
    }
    fn eval(&self, executor: &ScalarExpressionExecutor, input: &[&ArrayRef]) -> Result<ArrayRef> {
        Ok(if executor.num_rows()? == 0 {
            new_empty_array(&self.data_type)
        } else {
            let [argument] = input else {
                return Err(Error::Execution(
                    "unary function requires one input result".into(),
                ));
            };

            (self.eval_fn)(argument)?
        })
    }
}

#[derive(Debug)]
pub struct BinaryFunctionExpressionEvaluation {
    data_type: DataType,
    left: Box<ScalarExpressionEvaluation>,
    right: Box<ScalarExpressionEvaluation>,
    eval_fn: BinaryEvalFn,
}

impl BinaryFunctionExpressionEvaluation {
    pub(super) fn try_new(
        expression: &FunctionExpression,
        left: &ScalarExprRef,
        right: &ScalarExprRef,
    ) -> Result<Self> {
        let left_evaluation = left.to_evaluation()?;
        let right_evaluation = right.to_evaluation()?;
        // Kernels are keyed by the declared operand type, which for arithmetic
        // is also the result type but for comparisons is not.
        let eval_fn = kernels::bind_binary(expression.function, &left.result_type().data_type)?;
        Ok(Self {
            data_type: expression.result_type.data_type.clone(),
            left: Box::new(left_evaluation),
            right: Box::new(right_evaluation),
            eval_fn,
        })
    }
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let left = self.left.evaluate(executor)?;
        let right = self.right.evaluate(executor)?;
        self.eval(executor, &[&left, &right])
    }
    fn eval(&self, executor: &ScalarExpressionExecutor, input: &[&ArrayRef]) -> Result<ArrayRef> {
        Ok(if executor.num_rows()? == 0 {
            new_empty_array(&self.data_type)
        } else {
            let [left, right] = input else {
                return Err(Error::Execution(
                    "binary function requires two input results".into(),
                ));
            };

            (self.eval_fn)(left, right)?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::scalar::{ConstantExpression, ReferenceExpression};
    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    fn ints(array: &ArrayRef) -> Vec<i64> {
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }
    #[test]
    fn eval_consumes_supplied_arrays_without_executing_children() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 0, 4]))],
        )
        .unwrap();
        let input = batch.slice(0, 3);
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        // This child cannot read its column. Supplied-input eval must use the
        // two argument arrays instead of recursively touching the bound child.
        let missing =
            ReferenceExpression::new(99, ExpressionResultType::new(DataType::Int64, false))
                .into_ref();
        let ScalarExpressionEvaluation::BinaryFunction(evaluation) = FunctionExpression::binary(
            FunctionKind::Add,
            missing,
            ConstantExpression::int64(Some(1000)).into_ref(),
            DataType::Int64,
            false,
        )
        .to_evaluation()
        .unwrap() else {
            unreachable!()
        };
        let left: ArrayRef = Arc::new(Int64Array::from(vec![30, 10, 30]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 1]));
        let input = [&left, &right];
        let output = evaluation.eval(&executor, &input).unwrap();

        assert_eq!(ints(&output), vec![32, 12, 31]);
        assert!(evaluation.eval(&executor, &input[..1]).is_err());
        assert_eq!(ints(&output), vec![32, 12, 31]);
        // Zero rows skip arguments and kernels, even with invalid children.
        let empty = ScalarExpressionExecutor::new(&[], 0);
        assert!(evaluation.eval(&empty, &[]).unwrap().is_empty());
        assert!(evaluation.evaluate(&executor).is_err());
    }
}
