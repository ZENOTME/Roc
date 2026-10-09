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

use super::ScalarExprRef;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use super::{ColumnValue, ExpressionResultType};
use crate::error::{Error, Result};
use arrow::{
    array::{ArrayRef, UInt64Array, new_empty_array},
    compute::{kernels::interleave::interleave, take},
    datatypes::DataType,
};

/// An expression that returns the first non-NULL argument.
#[derive(Clone, Debug)]
pub struct CoalesceExpression {
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct CoalesceExpressionEvaluation {
    data_type: DataType,
    arguments: Vec<ScalarExpressionEvaluation>,
}

impl CoalesceExpression {
    pub fn new(arguments: Vec<ScalarExprRef>, data_type: DataType, nullable: bool) -> Self {
        Self {
            arguments,
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
        }
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Coalesce(self.bind()?))
    }

    pub(super) fn bind(&self) -> Result<CoalesceExpressionEvaluation> {
        // The executor indexes its first argument; COALESCE has no value without one.
        if self.arguments.is_empty() {
            return Err(Error::invalid_plan(
                "coalesce requires at least one argument".into(),
            ));
        }
        let mut arguments = Vec::with_capacity(self.arguments.len());
        for argument in &self.arguments {
            arguments.push(argument.to_evaluation()?);
        }
        Ok(CoalesceExpressionEvaluation {
            data_type: self.result_type.data_type.clone(),
            arguments,
        })
    }
}

impl CoalesceExpressionEvaluation {
    /// Evaluate required rows and combine branch outputs in input order.
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ColumnValue> {
        let num_rows = executor.num_rows()?;
        if num_rows == 0 {
            return self.eval(executor, &[], &[]);
        }
        let mut buffers = CoalesceBuffers::new(num_rows);
        self.eval_arguments(executor, &mut buffers)?;
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
    ) -> Result<ColumnValue> {
        Ok(ColumnValue::Array(if executor.num_rows()? == 0 {
            new_empty_array(&self.data_type)
        } else {
            interleave(
                &input.iter().map(|value| value.as_ref()).collect::<Vec<_>>(),
                mapping,
            )?
        }))
    }

    fn eval_arguments(
        &self,
        executor: &ScalarExpressionExecutor,
        buffers: &mut CoalesceBuffers,
    ) -> Result<()> {
        let last = self.arguments.len() - 1;
        for (child_index, child) in self.arguments.iter().enumerate() {
            if buffers.remaining.is_empty() {
                break;
            }
            let input = branch_columns(
                executor.columns()?,
                executor.num_rows()?,
                &buffers.remaining,
            )?;
            let remaining = ScalarExpressionExecutor::new(&input, buffers.remaining.len());
            let value = child.evaluate(&remaining)?;
            let value = value.into_array(remaining.num_rows()?)?;
            let nulls = value.logical_nulls();
            buffers.next.clear();
            for (i, position) in buffers.remaining.drain(..).enumerate() {
                if child_index != last && nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    buffers.next.push(position);
                } else {
                    buffers.mapping[position] = (buffers.pieces.len(), i);
                }
            }
            buffers.pieces.push(value);
            std::mem::swap(&mut buffers.remaining, &mut buffers.next);
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
struct CoalesceBuffers {
    remaining: Vec<usize>,
    next: Vec<usize>,
    pieces: Vec<ArrayRef>,
    mapping: Vec<(usize, usize)>,
}

impl CoalesceBuffers {
    fn new(len: usize) -> Self {
        Self {
            remaining: (0..len).collect(),
            next: Vec::new(),
            pieces: Vec::new(),
            mapping: vec![(0, 0); len],
        }
    }
}
