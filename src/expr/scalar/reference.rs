use super::{BindScalarExpression, ExpressionInput, ExpressionResult};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, UInt64Array},
    compute::take,
    datatypes::SchemaRef,
};

/// A physical column position in the input batch, never a catalog identifier.
#[derive(Clone, Debug)]
pub struct ReferenceExpression {
    index: usize,
}

#[derive(Debug)]
pub struct ReferenceExpressionExecutor {
    index: usize,
}
impl BindScalarExpression for ReferenceExpressionExecutor {
    type Expression = ReferenceExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let field = schema.fields().get(expression.index()).ok_or_else(|| {
            Error::InvalidPlan(format!("column index {} out of bounds", expression.index()))
        })?;
        Ok((
            Self {
                index: expression.index(),
            },
            ExpressionResult {
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            },
        ))
    }
}
impl ReferenceExpressionExecutor {
    pub fn try_new(expression: &ReferenceExpression, input_schema: SchemaRef) -> Result<Self> {
        Self::bind(expression, &input_schema).map(|(executor, _)| executor)
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let value = self.evaluate(input)?;
        super::materialize(value, self.is_scalar(), input.len())
    }

    pub fn is_scalar(&self) -> bool {
        false
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let column = &input.columns()[self.index];
        if input.is_empty() {
            return Ok(column.slice(0, 0));
        }
        match input.selection() {
            None => Ok(column.clone()),
            Some(rows) => {
                let indices = UInt64Array::from_iter_values(rows.iter().map(|&i| i as u64));
                Ok(take(column.as_ref(), &indices, None)?)
            }
        }
    }
}
impl ReferenceExpression {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
    pub fn index(&self) -> usize {
        self.index
    }
}

impl ReferenceExpression {
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<ReferenceExpressionExecutor> {
        ReferenceExpressionExecutor::try_new(self, input_schema)
    }
}
