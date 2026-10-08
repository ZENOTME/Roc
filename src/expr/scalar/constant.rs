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

use super::{ExpressionResultType, ScalarValue};
use crate::error::{Error, Result};
use arrow::{array::ArrayRef, datatypes::DataType};

/// A typed scalar constant, independent of the input batch length.
#[derive(Clone, Debug)]
pub struct ConstantExpression {
    value: ScalarValue,
    result_type: ExpressionResultType,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::scalar::ColumnValue;
    #[test]
    fn constant_stays_scalar_at_every_batch_length() {
        for value in [Some(7), None] {
            let mut expression = ConstantExpression::int64(value).program().unwrap();
            for len in [0, 1, 3, 4096] {
                let output = expression.run_value(&Value::input(&[], len)).unwrap();
                assert!(matches!(output, ColumnValue::Scalar(ScalarValue::Int64(v)) if v == value));
                let array = output.into_array(len).unwrap();
                assert_eq!(array.len(), len);
                assert_eq!(array.data_type(), &DataType::Int64);
            }
        }
    }
}

#[cfg(test)]
use crate::program::test_support::*;
