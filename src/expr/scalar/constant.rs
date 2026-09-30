use super::{BindScalarExpression, ExpressionInput, ExpressionResult};
use crate::error::{Error, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, new_null_array};
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

/// A typed scalar stored as a single-element Arrow array.
#[derive(Clone, Debug)]
pub struct ConstantExpression {
    value: ArrayRef,
}

#[derive(Debug)]
pub struct ConstantExpressionExecutor {
    value: ArrayRef,
}
impl BindScalarExpression for ConstantExpressionExecutor {
    type Expression = ConstantExpression;
    fn bind(
        expression: &Self::Expression,
        _schema: &SchemaRef,
    ) -> Result<(Self, ExpressionResult)> {
        let value = expression.value().clone();
        let result = ExpressionResult {
            data_type: value.data_type().clone(),
            nullable: value.logical_null_count() != 0,
        };
        Ok((Self { value }, result))
    }
}
impl ConstantExpressionExecutor {
    pub fn try_new(expression: &ConstantExpression, input_schema: SchemaRef) -> Result<Self> {
        Self::bind(expression, &input_schema).map(|(executor, _)| executor)
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let value = self.evaluate(input)?;
        super::materialize(value, self.is_scalar(), input.len())
    }

    pub fn is_scalar(&self) -> bool {
        true
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        Ok(if input.is_empty() {
            self.value.slice(0, 0)
        } else {
            self.value.clone()
        })
    }
}
impl ConstantExpression {
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

impl ConstantExpression {
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<ConstantExpressionExecutor> {
        ConstantExpressionExecutor::try_new(self, input_schema)
    }
}
