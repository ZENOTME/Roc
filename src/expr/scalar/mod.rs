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
use crate::error::Result;
use crate::expr::ExpressionResultType;
pub use executor::ScalarExpressionEvaluation;
use std::sync::Arc;

mod case;
mod cast;
mod coalesce;
mod conjunction;
mod constant;
pub mod executor;
mod function;
mod kernels;
mod not;
mod reference;

pub use case::{CaseExpression, CaseExpressionEvaluation};
pub use cast::{CastExpression, CastExpressionEvaluation, CastMode};
pub use coalesce::{CoalesceExpression, CoalesceExpressionEvaluation};
pub use conjunction::{
    AndExpressionEvaluation, Conjunction, ConjunctionExpression, OrExpressionEvaluation,
};
pub use constant::{ConstantExpression, ConstantExpressionEvaluation};
pub use function::{
    BinaryFunctionExpressionEvaluation, FunctionExpression, FunctionKind,
    UnaryFunctionExpressionEvaluation,
};
pub use not::{NotExpression, NotExpressionEvaluation};
pub use reference::{ReferenceExpression, ReferenceExpressionEvaluation};

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

    /// Bind kernels and build an immutable evaluation for this expression.
    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        match self {
            Self::Reference(e) => e.to_evaluation(),
            Self::Constant(e) => e.to_evaluation(),
            Self::Function(e) => e.to_evaluation(),
            Self::Cast(e) => e.to_evaluation(),
            Self::Conjunction(e) => e.to_evaluation(),
            Self::Not(e) => e.to_evaluation(),
            Self::Case(e) => e.to_evaluation(),
            Self::Coalesce(e) => e.to_evaluation(),
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

macro_rules! scalar_evaluation {
    ($($variant:ident($evaluation:ty)),* $(,)?) => {$(
        impl From<$evaluation> for ScalarExpressionEvaluation {
            fn from(evaluation: $evaluation) -> Self { Self::$variant(evaluation) }
        }
    )*};
}
scalar_evaluation! {
    Reference(ReferenceExpressionEvaluation),
    Constant(ConstantExpressionEvaluation),
    UnaryFunction(UnaryFunctionExpressionEvaluation),
    BinaryFunction(BinaryFunctionExpressionEvaluation),
    Cast(CastExpressionEvaluation),
    And(AndExpressionEvaluation),
    Or(OrExpressionEvaluation),
    Not(NotExpressionEvaluation),
    Case(CaseExpressionEvaluation),
    Coalesce(CoalesceExpressionEvaluation),
}
