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

use crate::arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, new_null_array},
    datatypes::DataType,
};
use crate::column::ColumnId;
use crate::error::{Error, Result};
use crate::kernel::{
    ColumnVariableBinding, ConstantValue, KernelBuilder, PendingErrors, VariableType, scalar,
};
use std::sync::Arc;

/// A one-row literal. Its Arrow type must equal the output column type.
#[derive(Clone, Debug)]
pub struct ConstantExpression {
    value: ArrayRef,
    output: ColumnId,
}

impl ConstantExpression {
    pub fn try_new(value: ArrayRef, output: ColumnId) -> Result<Self> {
        if value.len() != 1 {
            return Err(Error::invalid_plan(
                "constant must contain exactly one value".into(),
            ));
        }
        Ok(Self { value, output })
    }
    pub fn value(&self) -> &ArrayRef {
        &self.value
    }
    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub fn null(data_type: &DataType, output: ColumnId) -> Self {
        Self {
            value: new_null_array(data_type, 1),
            output,
        }
    }
    pub fn int64(value: Option<i64>, output: ColumnId) -> Self {
        Self {
            value: Arc::new(Int64Array::from(vec![value])),
            output,
        }
    }
    pub fn float64(value: Option<f64>, output: ColumnId) -> Self {
        Self {
            value: Arc::new(Float64Array::from(vec![value])),
            output,
        }
    }
    pub fn boolean(value: Option<bool>, output: ColumnId) -> Self {
        Self {
            value: Arc::new(BooleanArray::from(vec![value])),
            output,
        }
    }
    pub fn string(value: Option<&str>, output: ColumnId) -> Self {
        Self {
            value: Arc::new(StringArray::from(vec![value])),
            output,
        }
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        let data_type = self.value.data_type();
        if !scalar::supported(data_type) {
            return Err(Error::unsupported(format!(
                "unsupported constant type: {data_type}"
            )));
        }
        if self.value.is_null(0) || *data_type == DataType::Null {
            return Ok((builder.null_binding(data_type)?, vec![]));
        }
        let cell = scalar::decode(&self.value)?[0];
        if scalar::bytes_type(data_type) {
            // SAFETY: the cell borrows `self.value`, alive during this call.
            let bytes: Arc<[u8]> = Arc::from(unsafe { cell.as_bytes() });
            let ptr = builder.constant(ConstantValue::BytePointer(bytes))?;
            let len = builder.constant(ConstantValue::Usize(cell.len))?;
            return Ok((
                ColumnVariableBinding::Bytes {
                    ptr,
                    len,
                    validity: None,
                },
                vec![],
            ));
        }
        let ty = VariableType::of(data_type)
            .ok_or_else(|| Error::unsupported(format!("unsupported constant type: {data_type}")))?;
        let value = builder.constant(ConstantValue::from_bits(ty, cell.bits))?;
        Ok((ColumnVariableBinding::fixed(ty, value, None)?, vec![]))
    }
}
