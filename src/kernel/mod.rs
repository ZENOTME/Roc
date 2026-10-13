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

//! The physical execution layer.
//!
//! Operators and expressions lower a pipeline into kernels over variables in
//! row domains through a `KernelBuilder`. The resulting `KernelGraph` is
//! compiled into one fused `Program` per pipeline.
pub(crate) mod aggregation;
mod builder;
pub(crate) mod io;
mod jit;
mod lower;
mod model;
mod plan;
mod program;
pub(crate) mod runtime;
pub(crate) mod scalar;
mod state;
mod vector;

pub use builder::{
    GraphInput, GraphOutput, KernelBuilder, KernelGraph, PendingError, PendingErrors,
};
pub use jit::prewarm_kernels;
pub(crate) use lower::cast_supported;
pub use model::*;
pub use program::{ExecutionMode, KernelLibrary, LocalState, Program};
#[cfg(test)]
pub(crate) use program::{MODES, agree};
pub use state::{State, StateBuilder, StateId};
