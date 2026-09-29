use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, Datum, Scalar, UInt64Array, new_empty_array},
    compute::take,
};

/// Scalar broadcasting is retained until an operator needs a full column.
#[derive(Clone, Debug)]
pub enum ExpressionValue {
    Scalar(Scalar<ArrayRef>),
    Array(ArrayRef),
}
impl ExpressionValue {
    pub fn scalar(value: ArrayRef) -> Result<Self> {
        if value.len() != 1 {
            return Err(Error::Execution("scalar result must have one value".into()));
        }
        Ok(Self::Scalar(Scalar::new(value)))
    }
    pub fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }
    pub fn datum(&self) -> &dyn Datum {
        match self {
            Self::Scalar(value) => value,
            Self::Array(value) => value,
        }
    }
    pub fn into_array(self, rows: usize) -> Result<ArrayRef> {
        match self {
            Self::Array(value) if value.len() == rows => Ok(value),
            Self::Array(_) => Err(Error::Execution(
                "expression result length does not match input".into(),
            )),
            Self::Scalar(value) => {
                let value = value.into_inner();
                if rows == 0 {
                    return Ok(new_empty_array(value.data_type()));
                }
                if rows == 1 {
                    return Ok(value);
                }
                Ok(take(
                    value.as_ref(),
                    &UInt64Array::from(vec![0; rows]),
                    None,
                )?)
            }
        }
    }
}
