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

//! Typed, stateless descriptions of scalar expressions.
//!
//! Every value-producing node declares the column that receives its result;
//! the column's type in the ColumnContext is the result type. A node lowers
//! itself into kernels and binds its output column. Building a node whose
//! output is already bound reuses that binding.
use crate::column::ColumnId;
use crate::error::Result;
use crate::kernel::{ColumnVariableBinding, KernelBuilder, PendingErrors};
use std::sync::Arc;

mod case;
mod cast;
mod coalesce;
mod conjunction;
mod constant;
mod function;
mod not;
mod reference;
#[cfg(test)]
pub(crate) mod test_support;

pub use case::CaseExpression;
pub use cast::{CastExpression, CastMode};
pub use coalesce::CoalesceExpression;
pub use conjunction::{Conjunction, ConjunctionExpression};
pub use constant::ConstantExpression;
pub use function::{FunctionExpression, FunctionKind};
pub use not::NotExpression;
pub use reference::ReferenceExpression;

pub type ScalarExprRef = Arc<ScalarExpression>;

#[derive(Clone, Debug)]
pub enum ScalarExpression {
    Reference(ReferenceExpression),
    Constant(ConstantExpression),
    Function(FunctionExpression),
    Cast(CastExpression),
    Conjunction(ConjunctionExpression),
    Not(NotExpression),
    Case(CaseExpression),
    Coalesce(CoalesceExpression),
}

impl ScalarExpression {
    /// The column holding the result; a Reference returns the referenced one.
    pub fn output(&self) -> ColumnId {
        match self {
            Self::Reference(e) => e.column(),
            Self::Constant(e) => e.output(),
            Self::Function(e) => e.output(),
            Self::Cast(e) => e.output(),
            Self::Conjunction(e) => e.output(),
            Self::Not(e) => e.output(),
            Self::Case(e) => e.output(),
            Self::Coalesce(e) => e.output(),
        }
    }

    /// Direct operands, in evaluation order.
    pub fn children(&self) -> Vec<&ScalarExprRef> {
        match self {
            Self::Reference(_) | Self::Constant(_) => vec![],
            Self::Function(e) => e.arguments().iter().collect(),
            Self::Cast(e) => vec![e.input()],
            Self::Conjunction(e) => e.arguments().iter().collect(),
            Self::Not(e) => vec![e.input()],
            Self::Case(e) => e
                .branches()
                .iter()
                .flat_map(|(when, then)| [when, then])
                .chain([e.else_expr()])
                .collect(),
            Self::Coalesce(e) => e.arguments().iter().collect(),
        }
    }

    /// Lowers the expression in the current domain and binds its output.
    /// Returns the errors that have not been raised; the consumer of the root
    /// expression raises them (`KernelBuilder::raise`).
    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<PendingErrors> {
        let output = self.output();
        if builder.is_bound(output) || matches!(self, Self::Reference(_)) {
            return builder.column_errors(output);
        }
        let (binding, errors) = match self {
            Self::Reference(_) => unreachable!(),
            Self::Constant(e) => e.lower(builder)?,
            Self::Function(e) => e.lower(builder)?,
            Self::Cast(e) => e.lower(builder)?,
            Self::Conjunction(e) => e.lower(builder)?,
            Self::Not(e) => e.lower(builder)?,
            Self::Case(e) => e.lower(builder)?,
            Self::Coalesce(e) => e.lower(builder)?,
        };
        builder.bind_column(output, binding, errors.clone())?;
        Ok(errors)
    }
}

/// A built child: its binding and unraised errors.
pub(crate) fn build_input(
    builder: &mut KernelBuilder<'_>,
    expr: &ScalarExpression,
) -> Result<(ColumnVariableBinding, PendingErrors)> {
    let errors = expr.build_kernels(builder)?;
    Ok((builder.column_binding(expr.output())?, errors))
}

macro_rules! scalar_node {
    ($($variant:ident($ty:ty)),* $(,)?) => {$(
        impl From<$ty> for ScalarExpression {
            fn from(value: $ty) -> Self { Self::$variant(value) }
        }
        impl $ty {
            pub fn into_ref(self) -> ScalarExprRef {
                Arc::new(ScalarExpression::$variant(self))
            }
        }
    )*};
}
scalar_node! {
    Reference(ReferenceExpression), Constant(ConstantExpression),
    Function(FunctionExpression), Cast(CastExpression),
    Conjunction(ConjunctionExpression), Not(NotExpression),
    Case(CaseExpression), Coalesce(CoalesceExpression),
}
