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

pub mod agg;
pub mod predicate;
pub mod scalar;

use arrow::datatypes::DataType;

/// An expression's result type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionResultType {
    data_type: DataType,
    nullable: bool,
}

impl ExpressionResultType {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}
