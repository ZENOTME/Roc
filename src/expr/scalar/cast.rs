use super::ScalarExprRef;
use super::{BindScalarExpression, ExpressionInput, ExpressionResult, ScalarExpressionExecutor};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, new_empty_array},
    compute::{CastOptions, can_cast_types, cast_with_options},
    datatypes::{DataType, SchemaRef},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastMode {
    Strict,
    Try,
}

#[derive(Debug)]
pub struct CastExpressionExecutor {
    argument: Box<ScalarExpressionExecutor>,
    target: DataType,
    options: CastOptions<'static>,
}
impl BindScalarExpression for CastExpressionExecutor {
    type Expression = CastExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let (argument, argument_result) =
            ScalarExpressionExecutor::bind(expression.input.as_ref(), schema)?;
        if !can_cast_types(&argument_result.data_type, &expression.target) {
            return Err(Error::InvalidPlan(format!(
                "unsupported cast to {}",
                expression.target
            )));
        }
        let result = ExpressionResult {
            data_type: expression.target.clone(),
            nullable: argument_result.nullable || expression.mode == CastMode::Try,
        };
        Ok((
            Self {
                argument: Box::new(argument),
                target: expression.target.clone(),
                options: CastOptions {
                    safe: expression.mode == CastMode::Try,
                    ..Default::default()
                },
            },
            result,
        ))
    }
}
impl CastExpressionExecutor {
    pub fn try_new(expression: &CastExpression, input_schema: SchemaRef) -> Result<Self> {
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
            return Ok(new_empty_array(&self.target));
        }
        let argument = self.argument.evaluate(input)?;
        Ok(cast_with_options(
            argument.as_ref(),
            &self.target,
            &self.options,
        )?)
    }
}
#[derive(Clone, Debug)]
pub struct CastExpression {
    input: ScalarExprRef,
    target: DataType,
    mode: CastMode,
}
impl CastExpression {
    pub fn new(input: ScalarExprRef, target: DataType, mode: CastMode) -> Self {
        Self {
            input,
            target,
            mode,
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn target(&self) -> &DataType {
        &self.target
    }
    pub fn mode(&self) -> CastMode {
        self.mode
    }
}

impl CastExpression {
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<CastExpressionExecutor> {
        CastExpressionExecutor::try_new(self, input_schema)
    }
}
