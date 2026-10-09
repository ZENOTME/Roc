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
use crate::error::{Error, ErrorContext, Result, ResultExt};
use arrow::{
    array::{Array, ArrayRef, new_empty_array},
    compute::{CastOptions, can_cast_types, cast_with_options},
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
        let from = self.input.result_type().data_type();
        if !can_cast_types(from, &self.result_type.data_type) {
            return Err(Error::unsupported(format!(
                "cannot cast {from} to {}",
                self.result_type.data_type
            ))
            .with_context(
                ErrorContext::new("cast.bind")
                    .field("from", from)
                    .field("to", &self.result_type.data_type),
            ));
        }
        Ok(CastExpressionEvaluation {
            argument: Box::new(self.input.to_evaluation().with_location()?),
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
        let argument = self.argument.evaluate(executor).with_location()?;
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
                return Err(Error::internal("cast requires one input result".into()));
            };

            match argument {
                ColumnValue::Array(value) => ColumnValue::Array(self.cast(value.as_ref())?),
                ColumnValue::Scalar(value) => {
                    let array = value.to_array().with_location()?;
                    let output = self.cast(array.as_ref())?;
                    ColumnValue::Scalar(ScalarValue::try_from_array(&output, 0).with_location()?)
                }
            }
        })
    }
    fn cast(&self, value: &dyn Array) -> Result<ArrayRef> {
        // Type support is checked during binding. At this boundary a failed
        // conversion means the input cannot be cast as requested.
        cast_with_options(value, &self.target, &self.options)
            .map_err(|source| Error::invalid_input("cast failed".into()).with_source(source))
            .with_context(|| {
                ErrorContext::new("cast.evaluate")
                    .field("from", value.data_type())
                    .field("to", &self.target)
                    .field("mode", if self.options.safe { "try" } else { "strict" })
            })
    }
}
