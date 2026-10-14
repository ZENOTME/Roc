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

use super::{ScalarExprRef, build_input};
use crate::column::ColumnId;
use crate::error::{Error, Result, ResultExt};
use crate::kernel::{ColumnVariableBinding, KernelBuilder, PendingErrors};

#[derive(Clone, Debug)]
pub struct NotExpression {
    input: ScalarExprRef,
    output: ColumnId,
}

impl NotExpression {
    pub fn new(input: ScalarExprRef, output: ColumnId) -> Self {
        Self { input, output }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        let (input, errors) = build_input(builder, &self.input).with_location()?;
        let ColumnVariableBinding::Boolean { value, validity } = input else {
            return Err(Error::invalid_plan("NOT requires a Boolean input".into()));
        };
        let value = builder.not(value)?;
        Ok((ColumnVariableBinding::Boolean { value, validity }, errors))
    }
}
