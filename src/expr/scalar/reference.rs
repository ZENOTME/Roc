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
}
