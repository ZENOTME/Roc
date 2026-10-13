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

/// Searched CASE. Branch values and ELSE have the output type. A condition is
/// evaluated only when no earlier branch matched, a value only when its branch
/// is selected: errors of other rows are not raised.
#[derive(Clone, Debug)]
pub struct CaseExpression {
    branches: Vec<(ScalarExprRef, ScalarExprRef)>,
    else_expr: ScalarExprRef,
    output: ColumnId,
}

impl CaseExpression {
    pub fn new(
        branches: Vec<(ScalarExprRef, ScalarExprRef)>,
        else_expr: ScalarExprRef,
        output: ColumnId,
    ) -> Self {
        Self {
            branches,
            else_expr,
            output,
        }
    }
    pub fn branches(&self) -> &[(ScalarExprRef, ScalarExprRef)] {
        &self.branches
    }
    pub fn else_expr(&self) -> &ScalarExprRef {
        &self.else_expr
    }
    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        let ty = builder.column_type(self.output)?.data_type.clone();
        let check = |builder: &KernelBuilder<'_>, e: &ScalarExprRef| {
            if builder.column_type(e.output())?.data_type != ty {
                return Err(Error::InvalidPlan("CASE branch types differ".into()));
            }
            Ok(())
        };
        let mut errors = vec![];
        // True on rows where no earlier condition matched; None is true.
        let mut none_before = None;
        let mut arms = vec![];
        for (cond, value) in &self.branches {
            let (c, e) = build_input(builder, cond)?;
            if !matches!(c, ColumnVariableBinding::Boolean { .. }) {
                return Err(Error::InvalidPlan("CASE condition must be Boolean".into()));
            }
            errors.extend(builder.guard_errors(e, none_before)?);
            let p = builder.predicate(&c)?;
            let selected = builder.and_validity(none_before, Some(p))?;
            check(builder, value)?;
            let (v, e) = build_input(builder, value)?;
            errors.extend(builder.guard_errors(e, selected)?);
            arms.push((p, v));
            let not_p = builder.not(p)?;
            none_before = builder.and_validity(none_before, Some(not_p))?;
        }
        check(builder, &self.else_expr)?;
        let (mut result, e) = build_input(builder, &self.else_expr)?;
        errors.extend(builder.guard_errors(e, none_before)?);
        for (p, v) in arms.into_iter().rev() {
            result = builder.select_binding(p, &v, &result)?;
        }
        Ok((result, errors))
    }
}
