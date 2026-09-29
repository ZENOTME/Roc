//! Aggregate descriptions are separate from scalar expressions.
mod accumulator;
mod aggregate;
pub mod executor;
pub use aggregate::{AggregateFunction, BoundAggregateExpression};
