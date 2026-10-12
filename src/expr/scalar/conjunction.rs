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
use crate::arrow::datatypes::DataType;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}
#[derive(Clone, Debug)]
pub struct ConjunctionExpression {
    conjunction: Conjunction,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}
impl ConjunctionExpression {
    pub(crate) fn build_suboperator(
        &self,
        builder: &mut crate::suboperator::SuboperatorScopeBuilder,
    ) -> crate::error::Result<crate::suboperator::VariableId> {
        let inputs = self
            .arguments()
            .iter()
            .map(|a| a.build_suboperator(builder))
            .collect::<crate::error::Result<_>>()?;
        builder.boolean(self.conjunction(), inputs)
    }

    pub fn new(conjunction: Conjunction, arguments: Vec<ScalarExprRef>, nullable: bool) -> Self {
        Self {
            conjunction,
            arguments,
            result_type: ExpressionResultType {
                data_type: DataType::Boolean,
                nullable,
            },
        }
    }
    pub fn conjunction(&self) -> Conjunction {
        self.conjunction
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }
}
