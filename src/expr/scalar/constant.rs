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

use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use super::{ColumnValue, ExpressionResultType, ScalarValue};
use crate::error::{Error, Result};
use arrow::{array::ArrayRef, datatypes::DataType};

/// A typed scalar constant, independent of the input batch length.
#[derive(Clone, Debug)]
pub struct ConstantExpression {
    value: ScalarValue,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct ConstantExpressionEvaluation {
    value: ScalarValue,
}

impl ConstantExpression {
    pub fn new(value: ScalarValue) -> Self {
        let result_type = ExpressionResultType {
            data_type: value.data_type(),
            nullable: value.is_null(),
        };
        Self { value, result_type }
    }
    /// Compatibility constructor for callers supplying one Arrow value.
    pub fn try_new(value: ArrayRef) -> Result<Self> {
        if value.len() != 1 {
            return Err(Error::InvalidPlan(
                "constant must contain exactly one value".into(),
            ));
        }
        Ok(Self::new(ScalarValue::try_from_array(&value, 0)?))
    }
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }
    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ConstantExpressionEvaluation {
            value: self.value.clone(),
        }
        .into())
    }
    pub fn null(data_type: &DataType) -> Self {
        Self::new(ScalarValue::Null(data_type.clone()))
    }
    pub fn int64(value: Option<i64>) -> Self {
        Self::new(ScalarValue::Int64(value))
    }
    pub fn float64(value: Option<f64>) -> Self {
        Self::new(ScalarValue::Float64(value))
    }
    pub fn boolean(value: Option<bool>) -> Self {
        Self::new(ScalarValue::Boolean(value))
    }
    pub fn string(value: Option<&str>) -> Self {
        Self::new(ScalarValue::Utf8(value.map(str::to_owned)))
    }
}
impl ConstantExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ColumnValue> {
        executor.num_rows()?;
        Ok(ColumnValue::Scalar(self.value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn constant_stays_scalar_at_every_batch_length() {
        for value in [Some(7), None] {
            let expression = ConstantExpression::int64(value).to_evaluation().unwrap();
            for len in [0, 1, 3, 4096] {
                let output = expression
                    .evaluate(&ScalarExpressionExecutor::new(&[], len))
                    .unwrap();
                assert!(matches!(output, ColumnValue::Scalar(ScalarValue::Int64(v)) if v == value));
                let array = output.into_array(len).unwrap();
                assert_eq!(array.len(), len);
                assert_eq!(array.data_type(), &DataType::Int64);
            }
        }
    }
}
