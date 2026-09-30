//! Dispatch independently constructed, worker-local scalar executors.
pub use super::kernels::{BinaryScalarKernel, UnaryScalarKernel, is_number};
use super::*;
use super::{BindScalarExpression, SelectExpression, row_index};
pub use super::{boolean, require_boolean};
use crate::error::{Error, Result};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionResult {
    pub data_type: DataType,
    pub nullable: bool,
}

/// Selection entries index the original columns; output follows their order,
/// including duplicates. The caller supplies the bound layout and valid indices.
#[derive(Clone, Copy, Debug)]
pub struct ExpressionInput<'a> {
    columns: &'a [ArrayRef],
    num_rows: usize,
    selection: Option<&'a [usize]>,
}
impl<'a> ExpressionInput<'a> {
    pub fn new(columns: &'a [ArrayRef], num_rows: usize) -> Self {
        Self {
            columns,
            num_rows,
            selection: None,
        }
    }
    pub fn with_selection(mut self, selection: &'a [usize]) -> Self {
        self.selection = Some(selection);
        self
    }
    pub fn columns(&self) -> &'a [ArrayRef] {
        self.columns
    }
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
    pub fn selection(&self) -> Option<&'a [usize]> {
        self.selection
    }
    pub fn len(&self) -> usize {
        self.selection.map_or(self.num_rows, <[usize]>::len)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Output metadata belongs to this collection, not to the dispatch enum.
#[derive(Debug)]
pub struct ExpressionExecutor {
    executors: Vec<ScalarExpressionExecutor>,
    results: Vec<ExpressionResult>,
}
impl ExpressionExecutor {
    pub fn try_new(expressions: Vec<ScalarExprRef>, input_schema: SchemaRef) -> Result<Self> {
        let (executors, results) = expressions
            .iter()
            .map(|e| ScalarExpressionExecutor::bind(e.as_ref(), &input_schema))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();
        Ok(Self { executors, results })
    }
    pub fn results(&self) -> impl ExactSizeIterator<Item = &ExpressionResult> {
        self.results.iter()
    }
    pub fn executors(&self) -> &[ScalarExpressionExecutor] {
        &self.executors
    }
    /// Scalars hold one value; arrays hold input.len() values. Empty inputs
    /// always return empty arrays without invoking function kernels.
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<ArrayRef>> {
        self.executors
            .iter_mut()
            .map(|e| e.evaluate(input))
            .collect()
    }
    pub fn evaluate_arrays(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<ArrayRef>> {
        self.executors
            .iter_mut()
            .map(|e| e.evaluate_array(input))
            .collect()
    }
    /// Return original input row indices, in selection order. NULL never passes.
    pub fn select(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<usize>> {
        if self.executors.len() != 1 {
            return Err(Error::Execution("select requires one expression".into()));
        }
        self.executors[0].select(input)
    }
}

/// Dispatch to the concrete executor for each expression.
#[derive(Debug)]
pub enum ScalarExpressionExecutor {
    Reference(ReferenceExpressionExecutor),
    Constant(ConstantExpressionExecutor),
    UnaryFunction(UnaryFunctionExpressionExecutor),
    BinaryFunction(BinaryFunctionExpressionExecutor),
    Cast(CastExpressionExecutor),
    Conjunction(ConjunctionExpressionExecutor),
    Not(NotExpressionExecutor),
    Case(CaseExpressionExecutor),
    Coalesce(CoalesceExpressionExecutor),
}
impl ScalarExpressionExecutor {
    pub fn try_new(expression: ScalarExprRef, input_schema: SchemaRef) -> Result<Self> {
        expression.create_executor(input_schema)
    }
    pub fn is_scalar(&self) -> bool {
        match self {
            Self::Reference(e) => e.is_scalar(),
            Self::Constant(e) => e.is_scalar(),
            Self::UnaryFunction(e) => e.is_scalar(),
            Self::BinaryFunction(e) => e.is_scalar(),
            Self::Cast(e) => e.is_scalar(),
            Self::Conjunction(e) => e.is_scalar(),
            Self::Not(e) => e.is_scalar(),
            Self::Case(e) => e.is_scalar(),
            Self::Coalesce(e) => e.is_scalar(),
        }
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        match self {
            Self::Reference(e) => e.evaluate(input),
            Self::Constant(e) => e.evaluate(input),
            Self::UnaryFunction(e) => e.evaluate(input),
            Self::BinaryFunction(e) => e.evaluate(input),
            Self::Cast(e) => e.evaluate(input),
            Self::Conjunction(e) => e.evaluate(input),
            Self::Not(e) => e.evaluate(input),
            Self::Case(e) => e.evaluate(input),
            Self::Coalesce(e) => e.evaluate(input),
        }
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        match self {
            Self::Reference(e) => e.evaluate_array(input),
            Self::Constant(e) => e.evaluate_array(input),
            Self::UnaryFunction(e) => e.evaluate_array(input),
            Self::BinaryFunction(e) => e.evaluate_array(input),
            Self::Cast(e) => e.evaluate_array(input),
            Self::Conjunction(e) => e.evaluate_array(input),
            Self::Not(e) => e.evaluate_array(input),
            Self::Case(e) => e.evaluate_array(input),
            Self::Coalesce(e) => e.evaluate_array(input),
        }
    }
    pub fn select(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<usize>> {
        Ok(self
            .select_positions(input)?
            .into_iter()
            .map(|i| row_index(input, i))
            .collect())
    }
}
impl BindScalarExpression for ScalarExpressionExecutor {
    type Expression = ScalarExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        macro_rules! bind {
            ($expression:expr, $executor:ty, $variant:ident) => {{
                let (executor, result) = <$executor>::bind($expression, schema)?;
                Ok((Self::$variant(executor), result))
            }};
        }
        match expression {
            ScalarExpression::Reference(e) => bind!(e, ReferenceExpressionExecutor, Reference),
            ScalarExpression::Constant(e) => bind!(e, ConstantExpressionExecutor, Constant),
            ScalarExpression::Function(e) => match e.arguments().len() {
                1 => bind!(e, UnaryFunctionExpressionExecutor, UnaryFunction),
                2 => bind!(e, BinaryFunctionExpressionExecutor, BinaryFunction),
                _ => Err(Error::InvalidPlan(format!(
                    "invalid argument count for {:?}",
                    e.function()
                ))),
            },
            ScalarExpression::Cast(e) => bind!(e, CastExpressionExecutor, Cast),
            ScalarExpression::Conjunction(e) => {
                bind!(e, ConjunctionExpressionExecutor, Conjunction)
            }
            ScalarExpression::Not(e) => bind!(e, NotExpressionExecutor, Not),
            ScalarExpression::Case(e) => bind!(e, CaseExpressionExecutor, Case),
            ScalarExpression::Coalesce(e) => bind!(e, CoalesceExpressionExecutor, Coalesce),
        }
    }
}
impl SelectExpression for ScalarExpressionExecutor {
    fn select_positions(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<usize>> {
        if input.is_empty() {
            return Ok(vec![]);
        }
        if let Self::Conjunction(e) = self {
            return e.select_positions(input);
        }
        let scalar = self.is_scalar();
        let value = self.evaluate(input)?;
        let value = boolean(value.as_ref())?;
        if scalar {
            return Ok(if value.is_valid(0) && value.value(0) {
                (0..input.len()).collect()
            } else {
                vec![]
            });
        }
        Ok((0..value.len())
            .filter(|&i| value.is_valid(i) && value.value(i))
            .collect())
    }
}
