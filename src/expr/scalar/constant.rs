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
use crate::arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, new_null_array},
    datatypes::DataType,
};
use crate::error::{Error, Result};
use std::sync::Arc;
#[derive(Clone, Debug)]
pub struct ConstantExpression {
    value: ArrayRef,
    result_type: ExpressionResultType,
}
impl ConstantExpression {
    pub(crate) fn build_suboperator(
        &self,
        builder: &mut crate::suboperator::SuboperatorScopeBuilder,
    ) -> crate::error::Result<crate::suboperator::VariableId> {
        builder.constant(self.value().clone())
    }

    pub fn try_new(value: ArrayRef) -> Result<Self> {
        if value.len() != 1 {
            return Err(Error::InvalidPlan(
                "constant must contain exactly one value".into(),
            ));
        }
        Ok(Self::from_array(value))
    }
    fn from_array(value: ArrayRef) -> Self {
        let result_type = ExpressionResultType {
            data_type: value.data_type().clone(),
            nullable: value.logical_null_count() != 0,
        };
        Self { value, result_type }
    }
    pub fn value(&self) -> &ArrayRef {
        &self.value
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn null(data_type: &DataType) -> Self {
        Self::from_array(new_null_array(data_type, 1))
    }
    pub fn int64(value: Option<i64>) -> Self {
        Self::from_array(Arc::new(Int64Array::from(vec![value])))
    }
    pub fn float64(value: Option<f64>) -> Self {
        Self::from_array(Arc::new(Float64Array::from(vec![value])))
    }
    pub fn boolean(value: Option<bool>) -> Self {
        Self::from_array(Arc::new(BooleanArray::from(vec![value])))
    }
    pub fn string(value: Option<&str>) -> Self {
        Self::from_array(Arc::new(StringArray::from(vec![value])))
    }
}
