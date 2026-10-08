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

/// An expression that negates a Boolean input.
#[derive(Clone, Debug)]
pub struct NotExpression {
    input: ScalarExprRef,
    result_type: ExpressionResultType,
}

impl NotExpression {
    pub fn new(input: ScalarExprRef, nullable: bool) -> Self {
        Self {
            input,
            result_type: ExpressionResultType {
                data_type: DataType::Boolean,
                nullable,
            },
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }
}
