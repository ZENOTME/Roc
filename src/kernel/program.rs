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

//! A pipeline program and its per-thread state.
use super::plan::{Plan, StateKind};
use super::steps::Steps;
use super::*;
use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::error::{Error, Result};
use asyncband::shutdown::ShutdownGuard;
use std::fmt::Write;
use std::sync::Arc;

/// How a program executes its kernel graph. Both modes run the same plan with
/// identical results and errors.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Generates code when the program is prepared: each run of consecutive
    /// kernels becomes one fused, vectorized function.
    #[default]
    Compile,
    /// Runs every kernel from the library precompiled into the binary;
    /// preparing a program generates no code.
    ZeroCompile,
}

/// Every execution mode; tests compare their results.
#[cfg(test)]
pub(crate) const MODES: [ExecutionMode; 2] = [ExecutionMode::Compile, ExecutionMode::ZeroCompile];

/// The common result of every mode: results equal, or errors with equal text.
#[cfg(test)]
pub(crate) fn agree<T>(
    results: Vec<(ExecutionMode, Result<T>)>,
    eq: impl Fn(&T, &T) -> bool,
) -> Result<T> {
    let mut results = results.into_iter();
    let (_, first) = results.next().expect("at least one mode");
    for (mode, other) in results {
        match (&first, &other) {
            (Ok(a), Ok(b)) => assert!(eq(a, b), "{mode:?} differs from {:?}", MODES[0]),
            (Err(a), Err(b)) => {
                assert_eq!(format!("{a:?}"), format!("{b:?}"), "{mode:?} error differs")
            }
            (Ok(_), Err(e)) => panic!("{mode:?} failed alone: {e}"),
            (Err(e), Ok(_)) => panic!("{mode:?} succeeded where {:?} failed: {e}", MODES[0]),
        }
    }
    first
}

/// The code of one pipeline: reads source chunks, updates states and produces
/// at most one output chunk per input chunk.
#[derive(Debug)]
pub struct Program {
    mode: ExecutionMode,
    steps: Steps,
    /// Indexed by StateId; None for states of dead kernels.
    kinds: Vec<Option<StateKind>>,
    /// Per output column: the input whose array it reuses.
    aliases: Vec<Option<usize>>,
    /// The chunk position of every output column when the program only
    /// passes input arrays through.
    passthrough: Option<Vec<usize>>,
    inputs: Vec<GraphInput>,
    output: Option<GraphOutput>,
    states: Vec<StateBuilder>,
    explain: String,
}

/// The states of one execution instance, created by `Program::new_local`.
#[derive(Debug, Default)]
pub struct LocalState {
    states: Vec<Option<State>>,
    /// Window slots, reused across chunks.
    pub(crate) scratch: Vec<u64>,
    /// A chunk schema whose fields satisfy every input column type.
    checked: Option<SchemaRef>,
    pub(crate) buffers: Box<super::steps::Buffers>,
}

impl LocalState {
    /// Moves a state out; later runs of the program fail.
    pub fn take_state(&mut self, id: StateId) -> Option<State> {
        self.states.get_mut(id.0)?.take()
    }
    /// The number of groups of a group state.
    pub(crate) fn group_count(&self, id: StateId) -> Option<usize> {
        let state = self.states.get(id.0)?.as_ref()?;
        state
            .get::<super::aggregation::GroupState>()
            .ok()
            .map(|g| g.index.len())
    }
    pub fn state_mut(&mut self, id: StateId) -> Result<&mut State> {
        self.states
            .get_mut(id.0)
            .and_then(Option::as_mut)
            .ok_or_else(|| Error::internal("state is not available".into()))
    }
}

impl Program {
    pub fn new(graph: KernelGraph, mode: ExecutionMode) -> Result<Self> {
        let plan = Plan::new(&graph);
        let steps = Steps::prepare(&graph, &plan, mode == ExecutionMode::Compile)?;
        let passthrough = (steps.is_output_only() && graph.output.is_some())
            .then(|| {
                plan.aliases
                    .iter()
                    .map(|a| a.map(|i| graph.inputs[i].position))
                    .collect::<Option<Vec<_>>>()
            })
            .flatten();
        Ok(Self {
            mode,
            explain: explain(&graph) + "steps:\n" + &steps.describe(),
            steps,
            passthrough,
            kinds: plan.states,
            aliases: plan.aliases,
            inputs: graph.inputs,
            output: graph.output,
            states: graph.states,
        })
    }

    pub fn compile(graph: KernelGraph) -> Result<Self> {
        Self::new(graph, ExecutionMode::Compile)
    }

    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// A program without inputs, kernels or output.
    pub fn empty() -> Self {
        let columns = crate::column::ColumnContext::new();
        let graph = KernelBuilder::new(&columns)
            .finish(None)
            .expect("an empty kernel graph is valid");
        Self::compile(graph).expect("an empty kernel graph compiles")
    }

    pub fn output_schema(&self) -> Option<SchemaRef> {
        self.output.as_ref().map(|o| o.schema.clone())
    }

    /// The kernel graph the program was compiled from.
    pub fn explain(&self) -> &str {
        &self.explain
    }

    /// LLVM IR of the generated functions; empty when nothing is generated.
    pub fn generated_ir(&self) -> &str {
        self.steps.ir()
    }

    pub fn new_local(&self) -> Result<LocalState> {
        let states = self
            .states
            .iter()
            .zip(&self.kinds)
            .map(|(builder, kind)| kind.map(|_| builder.build()).transpose())
            .collect::<Result<_>>()?;
        Ok(LocalState {
            states,
            ..LocalState::default()
        })
    }

    /// Runs the program over one chunk. Returns the output chunk when the
    /// program has an output, even if it is empty.
    /// Checks the input columns of a chunk against their column types.
    fn check(&self, batch: &RecordBatch) -> Result<()> {
        for input in &self.inputs {
            let array = batch.columns().get(input.position).ok_or_else(|| {
                Error::invalid_input(format!("column index {} out of bounds", input.position))
            })?;
            if array.data_type() != &input.ty.data_type {
                return Err(Error::invalid_input(format!(
                    "source column type {} differs from column type {}",
                    array.data_type(),
                    input.ty.data_type
                )));
            }
            if !input.ty.nullable && !input.binding.is_null_type() && array.null_count() != 0 {
                return Err(Error::invalid_input(
                    "NULL in a non-nullable source column".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn run(
        &self,
        batch: &RecordBatch,
        local: &mut LocalState,
        guard: &ShutdownGuard,
    ) -> Result<Option<RecordBatch>> {
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        // A chunk's columns have the types of its schema's fields and no
        // NULLs in non-nullable ones, so a schema that satisfies the input
        // columns once does for every chunk.
        if !local
            .checked
            .as_ref()
            .is_some_and(|s| Arc::ptr_eq(s, batch.schema_ref()))
        {
            self.check(batch)?;
            let schema = batch.schema_ref();
            let satisfied = self.inputs.iter().all(|i| {
                let field = schema.field(i.position);
                i.ty.nullable || i.binding.is_null_type() || !field.is_nullable()
            });
            local.checked = satisfied.then(|| schema.clone());
        }
        if let (Some(positions), Some(output)) = (&self.passthrough, &self.output) {
            let arrays = positions.iter().map(|&p| batch.column(p).clone()).collect();
            // SAFETY: the checks above match each array to the column type
            // its output field is made of.
            return Ok(Some(unsafe {
                RecordBatch::new_unchecked(output.schema.clone(), arrays, batch.num_rows())
            }));
        }
        self.steps.run(
            batch,
            &self.inputs,
            self.output.as_ref(),
            &self.aliases,
            local,
            guard,
        )
    }
}

fn explain(graph: &KernelGraph) -> String {
    let mut out = String::new();
    for (i, input) in graph.inputs.iter().enumerate() {
        let _ = writeln!(
            out,
            "input {i}: column {} {:?}",
            input.position, input.binding
        );
    }
    for (i, domain) in graph.domains.iter().enumerate().skip(1) {
        let _ = writeln!(
            out,
            "domain d{i}: parent d{}",
            domain.parent.map_or(0, |p| p.0)
        );
    }
    for (domain, kernel) in &graph.kernels {
        let _ = match kernel {
            Kernel::Scalar(k) => writeln!(out, "d{}: {k:?}", domain.0),
            Kernel::Domain(k) => writeln!(out, "d{}: {k:?}", domain.0),
        };
    }
    if let Some(output) = &graph.output {
        let _ = writeln!(out, "output d{}: {:?}", output.domain.0, output.bindings);
    }
    out
}
