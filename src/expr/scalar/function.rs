use super::ScalarExprRef;
use super::executor::ScalarFunction;
use super::{
    BindScalarExpression, ExpressionInput, ExpressionResult, ScalarExpressionExecutor, kernels,
};
use crate::error::Result;
use arrow::{
    array::{ArrayRef, new_empty_array},
    datatypes::SchemaRef,
};

/// An already selected built-in implementation; no name lookup or coercion occurs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarFunction {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Negate,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Clone, Debug)]
pub struct FunctionExpression {
    function: ScalarFunction,
    arguments: Vec<ScalarExprRef>,
}

impl FunctionExpression {
    pub fn new(function: ScalarFunction, arguments: Vec<ScalarExprRef>) -> Self {
        Self {
            function,
            arguments,
        }
    }

    pub fn function(&self) -> ScalarFunction {
        self.function
    }

    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }

    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<FunctionExpressionExecutor> {
        FunctionExpressionExecutor::try_new(self, input_schema)
    }
}

#[derive(Debug)]
pub struct FunctionExpressionExecutor {
    arguments: Vec<ScalarExpressionExecutor>,
    kernel: ScalarFunction,
    values: Vec<ArrayRef>,
    empty: ArrayRef,
}
impl BindScalarExpression for FunctionExpressionExecutor {
    type Expression = FunctionExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let (arguments, results): (Vec<_>, Vec<_>) = expression
            .arguments
            .iter()
            .map(|argument| ScalarExpressionExecutor::bind(argument.as_ref(), schema))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();
        let result = kernels::prepare(expression.function, &results)?;
        let scalars = arguments
            .iter()
            .map(|argument| argument.is_scalar())
            .collect::<Vec<_>>();
        let kernel = kernels::bind(expression.function, &results[0].data_type, &scalars)?;
        let values = Vec::with_capacity(arguments.len());
        let empty = new_empty_array(&result.data_type);
        Ok((
            Self {
                arguments,
                kernel,
                values,
                empty,
            },
            result,
        ))
    }
}
impl FunctionExpressionExecutor {
    pub fn try_new(expression: &FunctionExpression, input_schema: SchemaRef) -> Result<Self> {
        Self::bind(expression, &input_schema).map(|(executor, _)| executor)
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let value = self.evaluate(input)?;
        super::materialize(value, self.is_scalar(), input.len())
    }
    pub fn is_scalar(&self) -> bool {
        self.arguments.iter().all(|argument| argument.is_scalar())
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        if input.is_empty() {
            return Ok(self.empty.clone());
        }
        self.values.clear();
        let result = self.evaluate_arguments(input);
        // Reuse the allocation, but release child results on success and error.
        self.values.clear();
        result
    }
    fn evaluate_arguments(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        for argument in &mut self.arguments {
            self.values.push(argument.evaluate(input)?);
        }
        (self.kernel)(&self.values)
    }
}
