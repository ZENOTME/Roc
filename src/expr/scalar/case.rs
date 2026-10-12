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

use super::{ExpressionResultType, ScalarExprRef};
use arrow::datatypes::DataType;
#[derive(Clone, Debug)]
pub struct CaseExpression {
    branches: Vec<(ScalarExprRef, ScalarExprRef)>,
    else_expr: ScalarExprRef,
    result_type: ExpressionResultType,
}
impl CaseExpression {
    pub(crate) fn build_suboperator(
        &self,
        builder: &mut crate::suboperator::SuboperatorBuilder,
    ) -> crate::error::Result<crate::suboperator::VariableId> {
        let branches = self
            .branches()
            .iter()
            .map(|(a, b)| Ok((a.build_suboperator(builder)?, b.build_suboperator(builder)?)))
            .collect::<crate::error::Result<_>>()?;
        let otherwise = self.else_expr().build_suboperator(builder)?;
        builder.case(branches, otherwise)
    }

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
}
