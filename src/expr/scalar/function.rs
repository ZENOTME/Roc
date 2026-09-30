use super::ScalarExprRef;
use super::executor::{BinaryEvalFn, UnaryEvalFn};
use super::{
    BindScalarExpression, ExpressionInput, ExpressionResult, ScalarExpressionExecutor, kernels,
};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, new_empty_array},
    datatypes::SchemaRef,
};

/// An already selected built-in implementation; no name lookup or coercion occurs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionKind {
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
    function: FunctionKind,
    arguments: Vec<ScalarExprRef>,
}

impl FunctionExpression {
    pub fn new(function: FunctionKind, arguments: Vec<ScalarExprRef>) -> Self {
        Self {
            function,
            arguments,
        }
    }

    pub fn function(&self) -> FunctionKind {
        self.function
    }

    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }

    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<ScalarExpressionExecutor> {
        match self.arguments.len() {
            1 => Ok(UnaryFunctionExpressionExecutor::try_new(self, input_schema)?.into()),
            2 => Ok(BinaryFunctionExpressionExecutor::try_new(self, input_schema)?.into()),
            _ => Err(Error::InvalidPlan(format!(
                "invalid argument count for {:?}",
                self.function
            ))),
        }
    }
}

#[derive(Debug)]
pub struct UnaryFunctionExpressionExecutor {
    argument: Box<ScalarExpressionExecutor>,
    eval_fn: UnaryEvalFn,
    empty: ArrayRef,
}
impl BindScalarExpression for UnaryFunctionExpressionExecutor {
    type Expression = FunctionExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        if expression.arguments.len() != 1 {
            return Err(Error::InvalidPlan(
                "unary function requires one argument".into(),
            ));
        }
        let (argument, argument_result) =
            ScalarExpressionExecutor::bind(expression.arguments[0].as_ref(), schema)?;
        let result = kernels::prepare(expression.function, std::slice::from_ref(&argument_result))?;
        let eval_fn = kernels::bind_unary(expression.function, &argument_result.data_type)?;
        let empty = new_empty_array(&result.data_type);
        Ok((
            Self {
                argument: Box::new(argument),
                eval_fn,
                empty,
            },
            result,
        ))
    }
}
impl UnaryFunctionExpressionExecutor {
    pub fn try_new(expression: &FunctionExpression, input_schema: SchemaRef) -> Result<Self> {
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
            return Ok(self.empty.clone());
        }
        let argument = self.argument.evaluate(input)?;
        (self.eval_fn)(&argument)
    }
}

#[derive(Debug)]
pub struct BinaryFunctionExpressionExecutor {
    left: Box<ScalarExpressionExecutor>,
    right: Box<ScalarExpressionExecutor>,
    eval_fn: BinaryEvalFn,
    empty: ArrayRef,
}
impl BindScalarExpression for BinaryFunctionExpressionExecutor {
    type Expression = FunctionExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        if expression.arguments.len() != 2 {
            return Err(Error::InvalidPlan(
                "binary function requires two arguments".into(),
            ));
        }
        let (left, left_result) =
            ScalarExpressionExecutor::bind(expression.arguments[0].as_ref(), schema)?;
        let (right, right_result) =
            ScalarExpressionExecutor::bind(expression.arguments[1].as_ref(), schema)?;
        let result = kernels::prepare(expression.function, &[left_result.clone(), right_result])?;
        let eval_fn = kernels::bind_binary(
            expression.function,
            &left_result.data_type,
            left.is_scalar(),
            right.is_scalar(),
        )?;
        let empty = new_empty_array(&result.data_type);
        Ok((
            Self {
                left: Box::new(left),
                right: Box::new(right),
                eval_fn,
                empty,
            },
            result,
        ))
    }
}
impl BinaryFunctionExpressionExecutor {
    pub fn try_new(expression: &FunctionExpression, input_schema: SchemaRef) -> Result<Self> {
        Self::bind(expression, &input_schema).map(|(executor, _)| executor)
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let value = self.evaluate(input)?;
        super::materialize(value, self.is_scalar(), input.len())
    }
    pub fn is_scalar(&self) -> bool {
        self.left.is_scalar() && self.right.is_scalar()
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        if input.is_empty() {
            return Ok(self.empty.clone());
        }
        let left = self.left.evaluate(input)?;
        let right = self.right.evaluate(input)?;
        (self.eval_fn)(&left, &right)
    }
}
