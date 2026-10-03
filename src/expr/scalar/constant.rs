use super::ExpressionResultType;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::error::{Error, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array, new_null_array,
};
use arrow::compute::take;
use arrow::datatypes::DataType;
use std::sync::Arc;

/// An expression that evaluates to a single typed constant, stored as a
/// one-element Arrow array.
#[derive(Clone, Debug)]
pub struct ConstantExpression {
    value: ArrayRef,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct ConstantExpressionEvaluation {
    value: ArrayRef,
}

impl ConstantExpression {
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

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Constant(self.bind()))
    }

    pub(super) fn bind(&self) -> ConstantExpressionEvaluation {
        ConstantExpressionEvaluation {
            value: self.value.clone(),
        }
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

impl ConstantExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        self.eval(executor, &[])
    }
    fn eval(&self, executor: &ScalarExpressionExecutor, _input: &[&ArrayRef]) -> Result<ArrayRef> {
        let num_rows = executor.num_rows()?;
        Ok(if num_rows == 0 {
            self.value.slice(0, 0)
        } else if num_rows == 1 {
            self.value.clone()
        } else if self.value.logical_null_count() != 0 {
            new_null_array(self.value.data_type(), num_rows)
        } else {
            take(
                self.value.as_ref(),
                &UInt64Array::from(vec![0; num_rows]),
                None,
            )?
        })
    }
}
