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
use arrow::datatypes::DataType;

/// An expression that returns the first non-NULL argument.
#[derive(Clone, Debug)]
pub struct CoalesceExpression {
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
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
}
