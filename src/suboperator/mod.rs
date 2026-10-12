//! Dense Variables connect immutable suboperators. Prepared vector Programs bind
//! native entry points or Cranelift code; mutable state belongs to each instance.
pub(crate) mod aggregation;
mod buffer;
mod context;
#[cfg(feature = "jit")]
mod jit;
pub(crate) mod native;
mod program;
pub use crate::expr::scalar::{CastMode, Conjunction, FunctionKind};
pub use buffer::ColumnBuffer;
pub use context::{
    SourceId, State, StateBuilder, StateId, Suboperator, SuboperatorBuilder, SuboperatorContext,
    Variable, VariableId,
};
pub use program::{Program, ProgramBuilder, ProgramContext, ProgramExecutionContext};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    #[default]
    Native,
    /// Fuse supported regions; other operations retain explicit native programs.
    Compiled,
    /// Compile short compute phases (at most four scalar operations per loop).
    /// Stateful and selection boundaries are retained in both compiled modes.
    Staged,
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
pub type BatchUpdate =
    fn(&mut State, &arrow::record_batch::RecordBatch) -> crate::error::Result<ProgramControl>;
