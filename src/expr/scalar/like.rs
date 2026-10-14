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
use crate::kernel::{ColumnVariableBinding, KernelBuilder, PendingErrors, ScalarFunction};

/// `input [NOT] LIKE pattern [ESCAPE escape]`: `%` matches any characters,
/// `_` one character. NULL when either string is NULL.
#[derive(Clone, Debug)]
pub struct LikeExpression {
    input: ScalarExprRef,
    pattern: ScalarExprRef,
    negated: bool,
    escape: Option<char>,
    output: ColumnId,
}

impl LikeExpression {
    pub fn new(
        input: ScalarExprRef,
        pattern: ScalarExprRef,
        negated: bool,
        escape: Option<char>,
        output: ColumnId,
    ) -> Self {
        Self {
            input,
            pattern,
            negated,
            escape,
            output,
        }
    }
    pub fn input(&self) -> &ScalarExprRef {
        &self.input
    }
    pub fn pattern(&self) -> &ScalarExprRef {
        &self.pattern
    }
    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        let (input, mut errors) = build_input(builder, &self.input).with_location()?;
        let (pattern, e) = build_input(builder, &self.pattern).with_location()?;
        errors.extend(e);
        let (
            ColumnVariableBinding::Bytes { ptr, len, .. },
            ColumnVariableBinding::Bytes {
                ptr: p_ptr,
                len: p_len,
                ..
            },
        ) = (input, pattern)
        else {
            return Err(Error::invalid_plan("LIKE compares strings".into()));
        };
        let escape = match self.escape {
            None => 0,
            Some(c) if c.is_ascii() => c as u8,
            Some(c) => return Err(Error::unsupported(format!("LIKE escape {c:?}"))),
        };
        let mut value = builder.call(
            ScalarFunction::Like { escape },
            vec![ptr, len, p_ptr, p_len],
        )?[0];
        if self.negated {
            value = builder.not(value)?;
        }
        let validity = builder.strict_validity(&[input, pattern])?;
        Ok((ColumnVariableBinding::Boolean { value, validity }, errors))
    }
}
