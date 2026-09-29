//! Scalar expressions contain no inferred result metadata or execution state.
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
mod value;

pub use case::BoundCaseExpression;
pub use cast::{BoundCastExpression, CastMode};
pub use coalesce::BoundCoalesceExpression;
pub use conjunction::{BoundConjunctionExpression, Conjunction};
pub use constant::BoundConstantExpression;
pub use function::{BoundFunctionExpression, ScalarFunction};
pub use not::BoundNotExpression;
pub use reference::BoundReferenceExpression;

pub type BoundScalarExprRef = Arc<BoundScalarExpression>;

#[derive(Clone, Debug)]
pub enum BoundScalarExpression {
    Reference(BoundReferenceExpression),
    Constant(BoundConstantExpression),
    Function(BoundFunctionExpression),
    Cast(BoundCastExpression),
    Conjunction(BoundConjunctionExpression),
    Not(BoundNotExpression),
    Case(BoundCaseExpression),
    Coalesce(BoundCoalesceExpression),
}

impl BoundScalarExpression {
    pub fn into_ref(self) -> BoundScalarExprRef {
        Arc::new(self)
    }
}

macro_rules! scalar_node {
    ($($variant:ident($ty:ty)),* $(,)?) => {$(
        impl From<$ty> for BoundScalarExpression {
            fn from(value: $ty) -> Self { Self::$variant(value) }
        }
        impl $ty {
            pub fn into_ref(self) -> BoundScalarExprRef {
                Arc::new(BoundScalarExpression::$variant(self))
            }
        }
    )*};
}
scalar_node! {
    Reference(BoundReferenceExpression), Constant(BoundConstantExpression),
    Function(BoundFunctionExpression), Cast(BoundCastExpression),
    Conjunction(BoundConjunctionExpression), Not(BoundNotExpression),
    Case(BoundCaseExpression), Coalesce(BoundCoalesceExpression),
}
