use super::ExpressionResultType;
use super::ScalarExprRef;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, new_empty_array},
    compute::{and_kleene, or_kleene},
    datatypes::DataType,
};
use std::sync::Arc;
type BooleanKernel = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}
/// A variadic Boolean AND or OR over its arguments.
#[derive(Clone, Debug)]
pub struct ConjunctionExpression {
    conjunction: Conjunction,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct ConjunctionExpressionEvaluation {
    arguments: Vec<ScalarExpressionEvaluation>,
    and: bool,
    kernel: BooleanKernel,
}

impl ConjunctionExpression {
    pub fn new(conjunction: Conjunction, arguments: Vec<ScalarExprRef>, nullable: bool) -> Self {
        Self {
            conjunction,
            arguments,
            result_type: ExpressionResultType {
                data_type: DataType::Boolean,
                nullable,
            },
        }
    }
    pub fn conjunction(&self) -> Conjunction {
        self.conjunction
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Conjunction(self.bind()?))
    }

    pub(super) fn bind(&self) -> Result<ConjunctionExpressionEvaluation> {
        let mut arguments = Vec::with_capacity(self.arguments.len());
        for argument in &self.arguments {
            arguments.push(argument.to_evaluation()?);
        }
        let and = self.conjunction == Conjunction::And;
        Ok(ConjunctionExpressionEvaluation {
            arguments,
            and,
            kernel: if and {
                |a, b| Ok(and_kleene(a, b)?)
            } else {
                |a, b| Ok(or_kleene(a, b)?)
            },
        })
    }
}

impl ConjunctionExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let values = self
            .arguments
            .iter()
            .map(|argument| argument.evaluate(executor))
            .collect::<Result<Vec<_>>>()?;
        self.eval(executor, &values.iter().collect::<Vec<_>>())
    }
    fn eval(&self, executor: &ScalarExpressionExecutor, input: &[&ArrayRef]) -> Result<ArrayRef> {
        Ok(if executor.num_rows()? == 0 {
            new_empty_array(&DataType::Boolean)
        } else {
            let mut col = BooleanArray::from(vec![self.and; executor.num_rows()?]);
            for value in input {
                let value = value
                    .as_boolean_opt()
                    .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
                col = (self.kernel)(&col, value)?;
            }
            Arc::new(col)
        })
    }
}
