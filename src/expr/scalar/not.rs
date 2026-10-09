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

use super::ScalarExprRef;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use super::{ColumnValue, ExpressionResultType, ScalarValue};
use crate::error::{Error, Result};
use arrow::{
    array::{AsArray, new_empty_array},
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
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ColumnValue> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let argument = self.argument.evaluate(executor)?;
        self.eval(executor, &[argument])
    }
    fn eval(
        &self,
        executor: &ScalarExpressionExecutor,
        input: &[ColumnValue],
    ) -> Result<ColumnValue> {
        Ok(if executor.num_rows()? == 0 {
            ColumnValue::Array(new_empty_array(&DataType::Boolean))
        } else {
            let [argument] = input else {
                return Err(Error::internal("not requires one input result".into()));
            };

            match argument {
                ColumnValue::Scalar(value) => {
                    ColumnValue::Scalar(ScalarValue::Boolean(value.as_boolean()?.map(|v| !v)))
                }
                ColumnValue::Array(value) => {
                    let value = value.as_boolean_opt().ok_or_else(|| {
                        Error::invalid_input("expected Boolean expression".into())
                    })?;
                    ColumnValue::Array(Arc::new(not(value).map_err(|source| {
                        Error::internal("failed to negate Boolean values".into())
                            .with_source(source)
                    })?))
                }
            }
        })
    }
}
