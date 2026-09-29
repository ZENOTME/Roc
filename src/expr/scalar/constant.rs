use crate::error::{Error, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, new_null_array};
use arrow::datatypes::DataType;
use std::sync::Arc;

/// A typed scalar stored as a single-element Arrow array.
#[derive(Clone, Debug)]
pub struct BoundConstantExpression {
    value: ArrayRef,
}
impl BoundConstantExpression {
    pub fn try_new(value: ArrayRef) -> Result<Self> {
        if value.len() != 1 {
            return Err(Error::InvalidPlan(
                "constant must contain exactly one value".into(),
            ));
        }
        Ok(Self { value })
    }
    pub fn value(&self) -> &ArrayRef {
        &self.value
    }
    pub fn null(data_type: &DataType) -> Self {
        Self {
            value: new_null_array(data_type, 1),
        }
    }
    pub fn int64(value: Option<i64>) -> Self {
        Self {
            value: Arc::new(Int64Array::from(vec![value])),
        }
    }
    pub fn float64(value: Option<f64>) -> Self {
        Self {
            value: Arc::new(Float64Array::from(vec![value])),
        }
    }
    pub fn boolean(value: Option<bool>) -> Self {
        Self {
            value: Arc::new(BooleanArray::from(vec![value])),
        }
    }
    pub fn string(value: Option<&str>) -> Self {
        Self {
            value: Arc::new(StringArray::from(vec![value])),
        }
    }
}
