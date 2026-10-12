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
use crate::{
    error::{Error, Result},
    expr::ExpressionResultType,
    suboperator::{SuboperatorScopeBuilder, VariableId},
};
use std::sync::Arc;

mod case;
mod cast;
mod coalesce;
mod conjunction;
mod constant;
mod function;
mod not;
mod reference;

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
    /// Build this expression once per input view. Concrete nodes own the lowering;
    /// the builder only retains the shared-node binding and suboperator graph.
    pub fn build_suboperator(
        self: &Arc<Self>,
        builder: &mut SuboperatorScopeBuilder,
    ) -> Result<VariableId> {
        if let Some(id) = builder.cached(self) {
            return Ok(id);
        }
        let id = match self.as_ref() {
            Self::Reference(e) => e.build_suboperator(builder),
            Self::Constant(e) => e.build_suboperator(builder),
            Self::Function(e) => e.build_suboperator(builder),
            Self::Cast(e) => e.build_suboperator(builder),
            Self::Conjunction(e) => e.build_suboperator(builder),
            Self::Not(e) => e.build_suboperator(builder),
            Self::Case(e) => e.build_suboperator(builder),
            Self::Coalesce(e) => e.build_suboperator(builder),
        }?;
        if builder.result_type(id)?.data_type() != self.result_type().data_type() {
            return Err(Error::InvalidPlan(
                "declared expression type differs from suboperator result".into(),
            ));
        }
        builder.cache(self.clone(), id)?;
        Ok(id)
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        match self {
            Self::Reference(e) => e.result_type(),
            Self::Constant(e) => e.result_type(),
            Self::Function(e) => e.result_type(),
            Self::Cast(e) => e.result_type(),
            Self::Conjunction(e) => e.result_type(),
            Self::Not(e) => e.result_type(),
            Self::Case(e) => e.result_type(),
            Self::Coalesce(e) => e.result_type(),
        }
    }
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
