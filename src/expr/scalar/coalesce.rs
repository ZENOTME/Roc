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
use crate::kernel::{ColumnVariableBinding, KernelBuilder, PendingErrors};

/// The first non-NULL argument. An argument is evaluated only when every
/// earlier one is NULL: errors of other rows are not raised.
#[derive(Clone, Debug)]
pub struct CoalesceExpression {
    arguments: Vec<ScalarExprRef>,
    output: ColumnId,
}

impl CoalesceExpression {
    pub fn new(arguments: Vec<ScalarExprRef>, output: ColumnId) -> Self {
        Self { arguments, output }
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        let ty = builder.column_type(self.output)?.data_type.clone();
        if self.arguments.is_empty() {
            return Err(Error::InvalidPlan(
                "coalesce requires at least one argument".into(),
            ));
        }
        let mut errors = vec![];
        // True on rows where every earlier argument is NULL; None is true.
        let mut all_null = None;
        let mut args = vec![];
        for argument in &self.arguments {
            if builder.column_type(argument.output())?.data_type != ty {
                return Err(Error::InvalidPlan("coalesce argument types differ".into()));
            }
            let (binding, e) = build_input(builder, argument)?;
            errors.extend(builder.guard_errors(e, all_null)?);
            args.push(binding);
            let Some(valid) = binding.validity() else {
                // Later arguments are never evaluated.
                break;
            };
            let invalid = builder.not(valid)?;
            all_null = builder.and_validity(all_null, Some(invalid))?;
        }
        let mut result = args.pop().unwrap();
        for arg in args.into_iter().rev() {
            let valid = arg
                .validity()
                .expect("only the last argument may be non-null");
            result = builder.select_binding(valid, &arg, &result)?;
        }
        Ok((result, errors))
    }
}
