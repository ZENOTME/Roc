use super::ScalarExprRef;
use super::{
    BindScalarExpression, BranchBuffers, ExpressionInput, ExpressionResult,
    ScalarExpressionExecutor, materialize, require_same_type, selected_input,
};
use crate::error::{Error, Result};
use arrow::{array::ArrayRef, datatypes::SchemaRef};
#[derive(Clone, Debug)]
pub struct CoalesceExpression {
    arguments: Vec<ScalarExprRef>,
}
impl CoalesceExpression {
    pub fn new(arguments: Vec<ScalarExprRef>) -> Self {
        Self { arguments }
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
}

#[derive(Debug)]
pub struct CoalesceExpressionExecutor {
    arguments: Vec<ScalarExpressionExecutor>,
    buffers: BranchBuffers,
}
impl BindScalarExpression for CoalesceExpressionExecutor {
    type Expression = CoalesceExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let mut arguments = Vec::with_capacity(expression.arguments.len());
        let mut results = Vec::with_capacity(expression.arguments.len());
        for expression in &expression.arguments {
            let (argument, result) = ScalarExpressionExecutor::bind(expression.as_ref(), schema)?;
            arguments.push(argument);
            results.push(result);
        }
        let mut result = results
            .first()
            .ok_or_else(|| Error::InvalidPlan("coalesce requires at least one argument".into()))?
            .clone();
        for argument in &results {
            require_same_type(&result, argument)?;
        }
        result.nullable = results.iter().all(|r| r.nullable);
        Ok((
            Self {
                arguments,
                buffers: BranchBuffers::default(),
            },
            result,
        ))
    }
}
impl CoalesceExpressionExecutor {
    pub fn try_new(expression: &CoalesceExpression, input_schema: SchemaRef) -> Result<Self> {
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
        if input.is_empty() {
            return self.arguments[0].evaluate(input);
        }
        self.buffers.reset(input.len());
        let result = self.evaluate_arguments(input);
        self.buffers.pieces.clear();
        result
    }
    fn evaluate_arguments(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let last = self.arguments.len() - 1;
        let buffers = &mut self.buffers;
        for (child_index, child) in self.arguments.iter_mut().enumerate() {
            if buffers.selection.remaining.is_empty() {
                break;
            }
            buffers.selection.map_rows(input);
            let value = child.evaluate(&selected_input(input, &buffers.selection.rows))?;
            let value = materialize(value, child.is_scalar(), buffers.selection.remaining.len())?;
            let nulls = value.logical_nulls();
            buffers.selection.next.clear();
            for (i, position) in buffers.selection.remaining.drain(..).enumerate() {
                if child_index != last && nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    buffers.selection.next.push(position);
                } else {
                    buffers.mapping[position] = (buffers.pieces.len(), i);
                }
            }
            buffers.pieces.push(value);
            std::mem::swap(
                &mut buffers.selection.remaining,
                &mut buffers.selection.next,
            );
        }
        buffers.finish()
    }
}

impl CoalesceExpression {
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<CoalesceExpressionExecutor> {
        CoalesceExpressionExecutor::try_new(self, input_schema)
    }
}
