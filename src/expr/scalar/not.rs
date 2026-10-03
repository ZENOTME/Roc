use super::ExpressionResultType;
use super::ScalarExprRef;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, AsArray, new_empty_array},
    compute::not,
    datatypes::DataType,
};
use std::sync::Arc;

/// An expression that negates a Boolean input.
#[derive(Clone, Debug)]
pub struct NotExpression {
    input: ScalarExprRef,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct NotExpressionEvaluation {
    argument: Box<ScalarExpressionEvaluation>,
}

impl NotExpression {
    pub fn new(input: ScalarExprRef, nullable: bool) -> Self {
        Self {
            input,
            result_type: ExpressionResultType {
                data_type: DataType::Boolean,
                nullable,
            },
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Not(self.bind()?))
    }

    pub(super) fn bind(&self) -> Result<NotExpressionEvaluation> {
        Ok(NotExpressionEvaluation {
            argument: Box::new(self.input.to_evaluation()?),
        })
    }
}

impl NotExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let argument = self.argument.evaluate(executor)?;
        self.eval(executor, &[&argument])
    }
    fn eval(&self, executor: &ScalarExpressionExecutor, input: &[&ArrayRef]) -> Result<ArrayRef> {
        Ok(if executor.num_rows()? == 0 {
            new_empty_array(&DataType::Boolean)
        } else {
            let [argument] = input else {
                return Err(Error::Execution("not requires one input result".into()));
            };

            let argument = argument
                .as_boolean_opt()
                .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
            Arc::new(not(argument)?)
        })
    }
}
