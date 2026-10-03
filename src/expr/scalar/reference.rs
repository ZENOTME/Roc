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
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::error::{Error, Result};
use arrow::array::ArrayRef;

/// An expression that reads one column of the input batch by index.
#[derive(Clone, Debug)]
pub struct ReferenceExpression {
    index: usize,
    result_type: ExpressionResultType,
}

impl ReferenceExpression {
    pub fn new(index: usize, result_type: ExpressionResultType) -> Self {
        Self { index, result_type }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        Ok(ScalarExpressionEvaluation::Reference(self.bind()))
    }

    pub(super) fn bind(&self) -> ReferenceExpressionEvaluation {
        ReferenceExpressionEvaluation { index: self.index }
    }
}

#[derive(Debug)]
pub struct ReferenceExpressionEvaluation {
    index: usize,
}

impl ReferenceExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        self.eval(executor, &[])
    }
    fn eval(&self, executor: &ScalarExpressionExecutor, _input: &[&ArrayRef]) -> Result<ArrayRef> {
        let num_rows = executor.num_rows()?;
        let col = executor.columns()?.get(self.index).ok_or_else(|| {
            Error::Execution(format!("column index {} out of bounds", self.index))
        })?;
        Ok(if num_rows == 0 {
            col.slice(0, 0)
        } else {
            col.clone()
        })
    }
}
