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
use crate::error::{Error, Result};
use crate::kernel::{ColumnVariableBinding, KernelBuilder, PendingErrors, cast_supported, scalar};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastMode {
    /// Invalid values fail execution.
    Strict,
    /// Invalid values become NULL.
    Try,
}

/// Converts the input to the output column's type.
#[derive(Clone, Debug)]
pub struct CastExpression {
    input: ScalarExprRef,
    mode: CastMode,
    output: ColumnId,
}

impl CastExpression {
    pub fn new(input: ScalarExprRef, mode: CastMode, output: ColumnId) -> Self {
        Self {
            input,
            mode,
            output,
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn mode(&self) -> CastMode {
        self.mode
    }
    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        let (input, mut errors) = build_input(builder, &self.input)?;
        let from = &builder.column_type(self.input.output())?.data_type;
        let to = &builder.column_type(self.output)?.data_type;
        if !scalar::supported(from) || !scalar::supported(to) || !cast_supported(from, to) {
            return Err(Error::InvalidPlan(format!(
                "unsupported cast from {from} to {to}"
            )));
        }
        let (binding, cast_errors) = builder.cast_binding(&input, from, to, self.mode)?;
        errors.extend(cast_errors);
        Ok((binding, errors))
    }
}
