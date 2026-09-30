use super::ScalarExprRef;
use super::{
    BindScalarExpression, ExpressionInput, ExpressionResult, ScalarExpressionExecutor, boolean,
    require_boolean,
};
use crate::error::Result;
use arrow::{
    array::{ArrayRef, BooleanArray},
    compute::not,
    datatypes::SchemaRef,
};
use std::sync::Arc;
#[derive(Clone, Debug)]
pub struct NotExpression {
    input: ScalarExprRef,
}

#[derive(Debug)]
pub struct NotExpressionExecutor {
    argument: Box<ScalarExpressionExecutor>,
}
impl BindScalarExpression for NotExpressionExecutor {
    type Expression = NotExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let (argument, result) = ScalarExpressionExecutor::bind(expression.input.as_ref(), schema)?;
        require_boolean(&result)?;
        Ok((
            Self {
                argument: Box::new(argument),
            },
            result,
        ))
    }
}
impl NotExpressionExecutor {
    pub fn try_new(expression: &NotExpression, input_schema: SchemaRef) -> Result<Self> {
        Self::bind(expression, &input_schema).map(|(executor, _)| executor)
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let value = self.evaluate(input)?;
        super::materialize(value, self.is_scalar(), input.len())
    }

    pub fn is_scalar(&self) -> bool {
        self.argument.is_scalar()
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        if input.is_empty() {
            return Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())));
        }
        let argument = self.argument.evaluate(input)?;
        Ok(Arc::new(not(boolean(argument.as_ref())?)?))
    }
}
impl NotExpression {
    pub fn new(input: ScalarExprRef) -> Self {
        Self { input }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
}

impl NotExpression {
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<NotExpressionExecutor> {
        NotExpressionExecutor::try_new(self, input_schema)
    }
}
