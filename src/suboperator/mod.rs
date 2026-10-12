//! Dense variables and explicit scope boundaries lower to typed machine code.
//! Mutable state belongs exclusively to each execution instance.
pub(crate) mod aggregation;
mod column;
mod context;
mod jit;
mod program;
mod runtime;
pub(crate) mod scalar;
mod signatures;
pub use crate::expr::scalar::{CastMode, Conjunction, FunctionKind};
pub use context::{
    AggregateKernel, DependencyToken, ScopeBoundary, ScopeSuboperator, SourceId, State,
    StateBuilder, StateId, Suboperator, SuboperatorScope, SuboperatorScopeBuilder, Variable,
    VariableId,
};
pub use program::{Program, ProgramBuilder, ProgramContext, ProgramExecutionContext};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    #[default]
    Compiled,
}

#[cfg(test)]
pub(crate) mod test_support;

/// Control returned by a synchronous local-state update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramControl {
    Continue,
    Stop,
}
/// Precompiled batch update. Receives an ordinary typed state slot, not an executor.
pub type BatchUpdate = fn(
    &mut State,
    &crate::arrow::record_batch::RecordBatch,
) -> crate::error::Result<ProgramControl>;
