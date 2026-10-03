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
