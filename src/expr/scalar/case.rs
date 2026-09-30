use super::ScalarExprRef;
use super::{
    BindScalarExpression, BranchBuffers, ExpressionInput, ExpressionResult,
    ScalarExpressionExecutor, SelectExpression, materialize, require_boolean, require_same_type,
    row_index, selected_input,
};
use crate::error::Result;
use arrow::{array::ArrayRef, datatypes::SchemaRef};
/// Searched CASE; all result branches must have the same type.
#[derive(Clone, Debug)]
pub struct CaseExpression {
    branches: Vec<(ScalarExprRef, ScalarExprRef)>,
    else_expr: ScalarExprRef,
}
impl CaseExpression {
    pub fn new(branches: Vec<(ScalarExprRef, ScalarExprRef)>, else_expr: ScalarExprRef) -> Self {
        Self {
            branches,
            else_expr,
        }
    }
    pub fn branches(&self) -> &[(ScalarExprRef, ScalarExprRef)] {
        &self.branches
    }
    pub fn else_expr(&self) -> &ScalarExprRef {
        &self.else_expr
    }
}

#[derive(Debug)]
pub struct CaseExpressionExecutor {
    branches: Vec<(ScalarExpressionExecutor, ScalarExpressionExecutor)>,
    otherwise: Box<ScalarExpressionExecutor>,
    buffers: BranchBuffers,
}
impl BindScalarExpression for CaseExpressionExecutor {
    type Expression = CaseExpression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)> {
        let (otherwise, mut result) =
            ScalarExpressionExecutor::bind(expression.else_expr.as_ref(), schema)?;
        let mut branches = Vec::with_capacity(expression.branches.len());
        for (condition, value) in &expression.branches {
            let (condition, condition_result) =
                ScalarExpressionExecutor::bind(condition.as_ref(), schema)?;
            let (value, value_result) = ScalarExpressionExecutor::bind(value.as_ref(), schema)?;
            require_boolean(&condition_result)?;
            require_same_type(&result, &value_result)?;
            result.nullable |= value_result.nullable;
            branches.push((condition, value));
        }
        Ok((
            Self {
                branches,
                otherwise: Box::new(otherwise),
                buffers: BranchBuffers::default(),
            },
            result,
        ))
    }
}
impl CaseExpressionExecutor {
    pub fn try_new(expression: &CaseExpression, input_schema: SchemaRef) -> Result<Self> {
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
            return self.otherwise.evaluate(input);
        }
        self.buffers.reset(input.len());
        let result = self.evaluate_branches(input);
        self.buffers.pieces.clear(); // Release intermediates on errors as well.
        result
    }
    fn evaluate_branches(&mut self, input: &ExpressionInput<'_>) -> Result<ArrayRef> {
        let buffers = &mut self.buffers;
        for (condition, branch) in &mut self.branches {
            if buffers.selection.remaining.is_empty() {
                break;
            }
            buffers.selection.map_rows(input);
            let mut selected = condition
                .select_positions(&selected_input(input, &buffers.selection.rows))?
                .into_iter()
                .peekable();
            buffers.matched.clear();
            buffers.selection.next.clear();
            for (i, position) in buffers.selection.remaining.drain(..).enumerate() {
                if selected.peek() == Some(&i) {
                    selected.next();
                    buffers.matched.push(position);
                } else {
                    buffers.selection.next.push(position);
                }
            }
            std::mem::swap(
                &mut buffers.selection.remaining,
                &mut buffers.selection.next,
            );
            if !buffers.matched.is_empty() {
                buffers.selection.rows.clear();
                buffers
                    .selection
                    .rows
                    .extend(buffers.matched.iter().map(|&i| row_index(input, i)));
                let value = branch.evaluate(&selected_input(input, &buffers.selection.rows))?;
                let value = materialize(value, branch.is_scalar(), buffers.matched.len())?;
                // Borrow fields separately while retaining their allocated capacity.
                for (i, &position) in buffers.matched.iter().enumerate() {
                    buffers.mapping[position] = (buffers.pieces.len(), i);
                }
                buffers.pieces.push(value);
            }
        }
        if !buffers.selection.remaining.is_empty() {
            buffers.selection.map_rows(input);
            let value = self
                .otherwise
                .evaluate(&selected_input(input, &buffers.selection.rows))?;
            let value = materialize(
                value,
                self.otherwise.is_scalar(),
                buffers.selection.remaining.len(),
            )?;
            for (i, &position) in buffers.selection.remaining.iter().enumerate() {
                buffers.mapping[position] = (buffers.pieces.len(), i);
            }
            buffers.pieces.push(value);
        }
        buffers.finish()
    }
}

impl CaseExpression {
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<CaseExpressionExecutor> {
        CaseExpressionExecutor::try_new(self, input_schema)
    }
}
