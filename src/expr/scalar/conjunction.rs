use super::{
    BindScalarExpression, ExpressionInput, ExpressionResult, ScalarExpressionExecutor,
    SelectExpression, SelectionBuffers, boolean, materialize, require_boolean, row_index,
    selected_input,
};
use crate::error::Result;
use arrow::{
    array::{ArrayRef, BooleanArray},
    compute::{and_kleene, or_kleene},
    datatypes::{DataType, SchemaRef},
};
use std::sync::Arc;
type BooleanKernel = fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray>;
use super::ScalarExprRef;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}
#[derive(Clone, Debug)]
pub struct ConjunctionExpression {
    conjunction: Conjunction,
    arguments: Vec<ScalarExprRef>,
}
impl ConjunctionExpression {
    pub fn new(conjunction: Conjunction, arguments: Vec<ScalarExprRef>) -> Self {
        Self {
            conjunction,
            arguments,
        }
    }
    pub fn conjunction(&self) -> Conjunction {
        self.conjunction
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
}

#[derive(Debug)]
pub struct ConjunctionExpressionExecutor {
    arguments: Vec<ScalarExpressionExecutor>,
    and: bool,
    kernel: BooleanKernel,
    selection: SelectionBuffers,
    accepted: Vec<bool>,
}
impl BindScalarExpression for ConjunctionExpressionExecutor {
    type Expression = ConjunctionExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let mut arguments = Vec::with_capacity(expression.arguments.len());
        let mut nullable = false;
        for expression in &expression.arguments {
            let (argument, result) = ScalarExpressionExecutor::bind(expression.as_ref(), schema)?;
            require_boolean(&result)?;
            nullable |= result.nullable;
            arguments.push(argument);
        }
        let and = expression.conjunction == Conjunction::And;
        Ok((
            Self {
                arguments,
                and,
                kernel: if and {
                    |a, b| Ok(and_kleene(a, b)?)
                } else {
                    |a, b| Ok(or_kleene(a, b)?)
                },
                selection: SelectionBuffers::default(),
                accepted: vec![],
            },
            ExpressionResult {
                data_type: DataType::Boolean,
                nullable,
            },
        ))
    }
}
impl ConjunctionExpressionExecutor {
    pub fn try_new(expression: &ConjunctionExpression, input_schema: SchemaRef) -> Result<Self> {
        Self::bind(expression, &input_schema).map(|(executor, _)| executor)
    }
    pub fn evaluate_array(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let value = self.evaluate(input)?;
        super::materialize(value, self.is_scalar(), input.len())
    }

    pub fn is_scalar(&self) -> bool {
        self.arguments.iter().all(|a| a.is_scalar())
    }
    pub fn select(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<usize>> {
        Ok(self
            .select_positions(input)?
            .into_iter()
            .map(|i| row_index(input, i))
            .collect())
    }
    pub fn evaluate(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        if input.is_empty() {
            return Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())));
        }
        let len = if self.is_scalar() { 1 } else { input.len() };
        let mut result = BooleanArray::from(vec![self.and; len]);
        for argument in &mut self.arguments {
            let value = argument.evaluate(input)?;
            let value = materialize(value, argument.is_scalar(), len)?;
            result = (self.kernel)(&result, boolean(value.as_ref())?)?;
        }
        Ok(Arc::new(result))
    }
}
impl SelectExpression for ConjunctionExpressionExecutor {
    fn select_positions(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<usize>> {
        self.selection.reset(input.len());
        self.accepted.clear();
        self.accepted.resize(input.len(), false);
        for argument in &mut self.arguments {
            if self.selection.remaining.is_empty() {
                break;
            }
            self.selection.map_rows(input);
            let selected =
                argument.select_positions(&selected_input(input, &self.selection.rows))?;
            let mut selected = selected.into_iter().peekable();
            self.selection.next.clear();
            for (i, position) in self.selection.remaining.drain(..).enumerate() {
                let matches = selected.peek() == Some(&i);
                if matches {
                    selected.next();
                }
                if self.and {
                    if matches {
                        self.selection.next.push(position);
                    }
                } else if matches {
                    self.accepted[position] = true;
                } else {
                    self.selection.next.push(position);
                }
            }
            std::mem::swap(&mut self.selection.remaining, &mut self.selection.next);
        }
        if self.and {
            Ok(self.selection.remaining.clone())
        } else {
            Ok(self
                .accepted
                .iter()
                .enumerate()
                .filter_map(|(i, &v)| v.then_some(i))
                .collect())
        }
    }
}

impl ConjunctionExpression {
    pub fn create_executor(
        &self,
        input_schema: SchemaRef,
    ) -> Result<ConjunctionExpressionExecutor> {
        ConjunctionExpressionExecutor::try_new(self, input_schema)
    }
}
