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
    array::new_empty_array,
    compute::{CastOptions, cast_with_options},
    datatypes::DataType,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastMode {
    Strict,
    Try,
}

#[derive(Debug)]
pub struct CastExpressionEvaluation {
    argument: Box<ScalarExpressionEvaluation>,
    target: DataType,
    options: CastOptions<'static>,
}

/// An expression that converts its input to the target type.
#[derive(Clone, Debug)]
pub struct CastExpression {
    input: ScalarExprRef,
    mode: CastMode,
    result_type: ExpressionResultType,
}

impl CastExpression {
    pub fn new(input: ScalarExprRef, target: DataType, mode: CastMode, nullable: bool) -> Self {
        Self {
            input,
            mode,
            result_type: ExpressionResultType {
                data_type: target,
                nullable,
            },
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn target(&self) -> &DataType {
        &self.result_type.data_type
    }
    pub fn mode(&self) -> CastMode {
        self.mode
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Cast(self.bind()?))
    }

    pub(super) fn bind(&self) -> Result<CastExpressionEvaluation> {
        Ok(CastExpressionEvaluation {
            argument: Box::new(self.input.to_evaluation()?),
            target: self.result_type.data_type.clone(),
            options: CastOptions {
                safe: self.mode == CastMode::Try,
                ..Default::default()
            },
        })
    }
}

impl CastExpressionEvaluation {
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
            ColumnValue::Array(new_empty_array(&self.target))
        } else {
            let [argument] = input else {
                return Err(Error::Execution("cast requires one input result".into()));
            };

            match argument {
                ColumnValue::Array(value) => ColumnValue::Array(cast_with_options(
                    value.as_ref(),
                    &self.target,
                    &self.options,
                )?),
                ColumnValue::Scalar(value) => {
                    let output =
                        cast_with_options(value.to_array()?.as_ref(), &self.target, &self.options)?;
                    ColumnValue::Scalar(ScalarValue::try_from_array(&output, 0)?)
                }
            }
        })
    }
}
