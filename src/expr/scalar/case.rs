// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::ExpressionResultType;
use super::ScalarExprRef;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::error::Result;
use crate::expr::predicate::select_true;
use arrow::{
    array::{ArrayRef, UInt64Array, new_empty_array},
    compute::{kernels::interleave::interleave, take},
    datatypes::DataType,
};
/// A searched CASE: the value of the first branch whose condition is true,
/// otherwise the ELSE expression.
#[derive(Clone, Debug)]
pub struct CaseExpression {
    branches: Vec<(ScalarExprRef, ScalarExprRef)>,
    else_expr: ScalarExprRef,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct CaseExpressionEvaluation {
    data_type: DataType,
    branches: Vec<(ScalarExpressionEvaluation, ScalarExpressionEvaluation)>,
    otherwise: Box<ScalarExpressionEvaluation>,
}

impl CaseExpression {
    pub fn new(
        branches: Vec<(ScalarExprRef, ScalarExprRef)>,
        else_expr: ScalarExprRef,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            branches,
            else_expr,
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
        }
    }
    pub fn branches(&self) -> &[(ScalarExprRef, ScalarExprRef)] {
        &self.branches
    }
    pub fn else_expr(&self) -> &ScalarExprRef {
        &self.else_expr
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Case(self.bind()?))
    }

    pub(super) fn bind(&self) -> Result<CaseExpressionEvaluation> {
        let mut branches = Vec::with_capacity(self.branches.len());
        for (condition, value) in &self.branches {
            branches.push((condition.to_evaluation()?, value.to_evaluation()?));
        }
        Ok(CaseExpressionEvaluation {
            data_type: self.result_type.data_type.clone(),
            branches,
            otherwise: Box::new(self.else_expr.to_evaluation()?),
        })
    }
}

impl CaseExpressionEvaluation {
    /// Evaluate required rows and combine branch outputs in input order.
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        let num_rows = executor.num_rows()?;
        if num_rows == 0 {
            return self.eval(executor, &[], &[]);
        }
        let mut buffers = CaseBuffers::new(num_rows);
        self.eval_branches(executor, &mut buffers)?;
        self.eval(
            executor,
            &buffers.pieces.iter().collect::<Vec<_>>(),
            &buffers.mapping,
        )
    }
    fn eval(
        &self,
        executor: &ScalarExpressionExecutor,
        input: &[&ArrayRef],
        mapping: &[(usize, usize)],
    ) -> Result<ArrayRef> {
        Ok(if executor.num_rows()? == 0 {
            new_empty_array(&self.data_type)
        } else {
            interleave(
                &input.iter().map(|value| value.as_ref()).collect::<Vec<_>>(),
                mapping,
            )?
        })
    }

    fn eval_branches(
        &self,
        executor: &ScalarExpressionExecutor,
        buffers: &mut CaseBuffers,
    ) -> Result<()> {
        for (condition, branch) in &self.branches {
            if buffers.remaining.is_empty() {
                break;
            }
            let input = branch_columns(
                executor.columns()?,
                executor.num_rows()?,
                &buffers.remaining,
            )?;
            let candidates = ScalarExpressionExecutor::new(&input, buffers.remaining.len());
            let mut selected = select_true(condition.evaluate(&candidates)?)?
                .into_iter()
                .peekable();
            buffers.matched.clear();
            buffers.next.clear();
            for (i, position) in buffers.remaining.drain(..).enumerate() {
                if selected.peek() == Some(&i) {
                    selected.next();
                    buffers.matched.push(position);
                } else {
                    buffers.next.push(position);
                }
            }
            std::mem::swap(&mut buffers.remaining, &mut buffers.next);
            if !buffers.matched.is_empty() {
                let input =
                    branch_columns(executor.columns()?, executor.num_rows()?, &buffers.matched)?;
                let matched = ScalarExpressionExecutor::new(&input, buffers.matched.len());
                let value = branch.evaluate(&matched)?;
                for (i, &position) in buffers.matched.iter().enumerate() {
                    buffers.mapping[position] = (buffers.pieces.len(), i);
                }
                buffers.pieces.push(value);
            }
        }
        if !buffers.remaining.is_empty() {
            let input = branch_columns(
                executor.columns()?,
                executor.num_rows()?,
                &buffers.remaining,
            )?;
            let remaining = ScalarExpressionExecutor::new(&input, buffers.remaining.len());
            let value = self.otherwise.evaluate(&remaining)?;
            for (i, &position) in buffers.remaining.iter().enumerate() {
                buffers.mapping[position] = (buffers.pieces.len(), i);
            }
            buffers.pieces.push(value);
        }
        Ok(())
    }
}

/// Branches evaluate compact columns. Their row count is rows.len(), even
/// when there are no columns; the parent retains positions to restore order.
fn branch_columns(columns: &[ArrayRef], num_rows: usize, rows: &[usize]) -> Result<Vec<ArrayRef>> {
    if rows.len() == num_rows {
        return Ok(columns.to_vec());
    }
    let indices = UInt64Array::from_iter_values(rows.iter().map(|&row| row as u64));
    columns
        .iter()
        .map(|col| Ok(take(col.as_ref(), &indices, None)?))
        .collect()
}

#[derive(Debug)]
struct CaseBuffers {
    remaining: Vec<usize>,
    next: Vec<usize>,
    matched: Vec<usize>,
    pieces: Vec<ArrayRef>,
    mapping: Vec<(usize, usize)>,
}

impl CaseBuffers {
    fn new(len: usize) -> Self {
        Self {
            remaining: (0..len).collect(),
            next: Vec::new(),
            matched: Vec::new(),
            pieces: Vec::new(),
            mapping: vec![(0, 0); len],
        }
    }
}
