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
use crate::kernel::{ColumnVariableBinding, KernelBuilder, PendingErrors, VariableId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}

/// Three-valued AND / OR. Every argument is evaluated; errors of any argument
/// are raised even when another argument decides the result.
#[derive(Clone, Debug)]
pub struct ConjunctionExpression {
    conjunction: Conjunction,
    arguments: Vec<ScalarExprRef>,
    output: ColumnId,
}

impl ConjunctionExpression {
    pub fn new(conjunction: Conjunction, arguments: Vec<ScalarExprRef>, output: ColumnId) -> Self {
        Self {
            conjunction,
            arguments,
            output,
        }
    }
    pub fn conjunction(&self) -> Conjunction {
        self.conjunction
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
        let and = self.conjunction == Conjunction::And;
        let mut errors = vec![];
        let mut result: Option<(VariableId, Option<VariableId>)> = None;
        for argument in &self.arguments {
            let (binding, e) = build_input(builder, argument)?;
            errors.extend(e);
            let ColumnVariableBinding::Boolean { value, validity } = binding else {
                return Err(Error::InvalidPlan(
                    "AND / OR require Boolean arguments".into(),
                ));
            };
            result = Some(match result {
                None => (value, validity),
                Some((a, av)) => kleene(builder, and, (a, av), (value, validity))?,
            });
        }
        let (value, validity) = match result {
            Some(r) => r,
            None => (builder.boolean(and)?, None),
        };
        Ok((ColumnVariableBinding::Boolean { value, validity }, errors))
    }
}

/// The result is known when both inputs are, or when either input alone
/// decides it (false for AND, true for OR).
fn kleene(
    builder: &mut KernelBuilder<'_>,
    and: bool,
    (a, av): (VariableId, Option<VariableId>),
    (b, bv): (VariableId, Option<VariableId>),
) -> Result<(VariableId, Option<VariableId>)> {
    let value = if and {
        builder.and(a, b)?
    } else {
        builder.or(a, b)?
    };
    if av.is_none() && bv.is_none() {
        return Ok((value, None));
    }
    let decisive = |builder: &mut KernelBuilder<'_>, x, xv: Option<VariableId>| {
        let x = if and { builder.not(x)? } else { x };
        builder.and_validity(xv, Some(x))
    };
    let both = builder.and_validity(av, bv)?.unwrap();
    let da = decisive(builder, a, av)?.unwrap();
    let db = decisive(builder, b, bv)?.unwrap();
    let validity = builder.or(both, da)?;
    let validity = builder.or(validity, db)?;
    Ok((value, Some(validity)))
}
