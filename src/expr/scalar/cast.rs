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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastMode {
    Strict,
    Try,
}

/// An expression that converts its input to the target type.
#[derive(Clone, Debug)]
pub struct CastExpression {
    input: ScalarExprRef,
    mode: CastMode,
    result_type: ExpressionResultType,
}

impl CastExpression {
    pub fn new(input: ScalarExprRef, target: DataType, mode: CastMode, nullable: bool) -> Self {
        Self {
            input,
            mode,
            result_type: ExpressionResultType {
                data_type: target,
                nullable,
            },
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn target(&self) -> &DataType {
        &self.result_type.data_type
    }
    pub fn mode(&self) -> CastMode {
        self.mode
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }
}
