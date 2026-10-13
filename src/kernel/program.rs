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
use super::aggregation::{AccumulatorState, GroupState};
use super::io::{self, OutputColumn};
use super::jit::Compiled;
use super::plan::{Plan, StateKind};
use super::runtime::Runtime;
use super::vector::Steps;
use super::*;
use crate::arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, Result};
use asyncband::shutdown::ShutdownGuard;
use std::fmt::Write;

/// How a program executes its kernel graph. Both modes run the same plan with
/// identical results.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Generates one fused row loop per pipeline.
    #[default]
    Compile,
    /// Runs the kernels one after another over whole chunks; preparing a
    /// program generates no code for it.
    ZeroCompile(KernelLibrary),
}

/// Where zero-compile execution takes each kernel's implementation from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KernelLibrary {
    /// Code generated once per kernel signature by the same emitter as fused
    /// programs, kept in a process-wide cache. `prewarm_kernels` fills the
    /// cache; a signature missing from it is generated on first use.
    #[default]
    Generated,
    /// Rust implementations built into the binary.
    Native,
}

/// Every execution mode; tests compare their results.
#[cfg(test)]
pub(crate) const MODES: [ExecutionMode; 3] = [
    ExecutionMode::Compile,
    ExecutionMode::ZeroCompile(KernelLibrary::Generated),
    ExecutionMode::ZeroCompile(KernelLibrary::Native),
];

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
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{mode:?} error differs"),
            (Ok(_), Err(e)) => panic!("{mode:?} failed alone: {e}"),
            (Err(e), Ok(_)) => panic!("{mode:?} succeeded where {:?} failed: {e}", MODES[0]),
        }
    }
    first
}

#[derive(Debug)]
enum Code {
    Compiled(Box<Compiled>),
    Steps(Steps),
}

/// The code of one pipeline: reads source chunks, updates states and produces
/// at most one output chunk per input chunk.
#[derive(Debug)]
pub struct Program {
    mode: ExecutionMode,
    code: Code,
    /// Indexed by StateId; None for states of dead kernels.
    kinds: Vec<Option<StateKind>>,
    /// Per output column: the input whose array it reuses.
    aliases: Vec<Option<usize>>,
    inputs: Vec<GraphInput>,
    output: Option<GraphOutput>,
    states: Vec<StateBuilder>,
    explain: String,
}

/// The states of one execution instance, created by `Program::new_local`.
#[derive(Debug, Default)]
pub struct LocalState {
    states: Vec<Option<State>>,
    /// Columns reused by zero-compile execution across chunks.
    pub(crate) scratch: Vec<Vec<u64>>,
}

impl LocalState {
    /// Moves a state out; later runs of the program fail.
    pub fn take_state(&mut self, id: StateId) -> Option<State> {
        self.states.get_mut(id.0)?.take()
    }
    pub fn state_mut(&mut self, id: StateId) -> Result<&mut State> {
        self.states
            .get_mut(id.0)
            .and_then(Option::as_mut)
            .ok_or_else(|| Error::Execution("state is not available".into()))
    }
}

impl Program {
    pub fn new(graph: KernelGraph, mode: ExecutionMode) -> Result<Self> {
        let plan = Plan::new(&graph);
        let code = match mode {
            ExecutionMode::Compile => Code::Compiled(Box::new(Compiled::compile(&graph, &plan)?)),
            ExecutionMode::ZeroCompile(library) => {
                Code::Steps(Steps::prepare(&graph, &plan, library)?)
            }
        };
        Ok(Self {
            mode,
            code,
            kinds: plan.states,
            aliases: plan.aliases,
            explain: explain(&graph),
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

    /// Cranelift IR of the generated functions; empty when nothing is generated.
    pub fn generated_ir(&self) -> &str {
        match &self.code {
            Code::Compiled(code) => &code.ir,
            Code::Steps(_) => "",
        }
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
            scratch: vec![],
        })
    }

    /// Runs the program over one chunk. Returns the output chunk when the
    /// program has an output, even if it is empty.
    pub fn run(
        &self,
        batch: &RecordBatch,
        local: &mut LocalState,
        guard: &ShutdownGuard,
    ) -> Result<Option<RecordBatch>> {
        if guard.is_shutdown_requested() {
            return Err(Error::Cancelled);
        }
        for input in &self.inputs {
            let array = batch.columns().get(input.position).ok_or_else(|| {
                Error::Execution(format!("column index {} out of bounds", input.position))
            })?;
            if array.data_type() != &input.ty.data_type {
                return Err(Error::Execution(format!(
                    "source column type {} differs from column type {}",
                    array.data_type(),
                    input.ty.data_type
                )));
            }
            if !input.ty.nullable && !input.binding.is_null_type() && array.null_count() != 0 {
                return Err(Error::Execution(
                    "NULL in a non-nullable source column".into(),
                ));
            }
        }
        let code = match &self.code {
            Code::Compiled(code) => code,
            Code::Steps(steps) => {
                return steps.run(
                    batch,
                    &self.inputs,
                    self.output.as_ref(),
                    &self.aliases,
                    local,
                    guard,
                );
            }
        };
        let rows = batch.num_rows();
        let inputs = self
            .inputs
            .iter()
            .map(|i| io::input(batch.column(i.position)))
            .collect::<Result<Vec<_>>>()?;
        let mut states = vec![std::ptr::null_mut::<()>(); self.kinds.len()];
        for (i, kind) in self.kinds.iter().enumerate() {
            let Some(kind) = kind else { continue };
            let state = local.state_mut(StateId(i))?;
            states[i] = match kind {
                StateKind::Group => (state.get_mut::<GroupState>()? as *mut GroupState).cast(),
                StateKind::Accumulator => {
                    (state.get_mut::<AccumulatorState>()? as *mut AccumulatorState).cast()
                }
            };
        }
        let mut runtime = Runtime {
            guard,
            ..Runtime::default()
        };
        if let Some(reduction) = code.reduction {
            // SAFETY: as for the entry below; a reduction writes no output.
            let status = unsafe {
                reduction(
                    &mut runtime,
                    inputs.as_ptr(),
                    std::ptr::null(),
                    std::ptr::null_mut(),
                    states.as_ptr(),
                    rows,
                )
            };
            Self::check(code, status, &mut runtime)?;
            return Ok(None);
        }
        let mut columns = vec![];
        let mut count = rows;
        if let Some(entry) = code.entry {
            if let Some(output) = &self.output {
                for (j, binding) in output.bindings.iter().enumerate() {
                    let nullable = binding.validity().is_some() && !binding.is_null_type();
                    columns.push(self.aliases[j].is_none().then(|| {
                        OutputColumn::new(output.schema.field(j).data_type(), nullable, rows)
                    }));
                }
            }
            let descriptors = columns
                .iter_mut()
                .map(|c| {
                    c.as_mut()
                        .map_or([std::ptr::null_mut(); io::OUTPUT_WORDS], |c| c.descriptor())
                })
                .collect::<Vec<_>>();
            count = 0;
            // SAFETY: the signature and buffer layouts are private to this
            // module. Inputs, states and output capacity stay live and
            // exclusive for the call; each input row writes at most one row.
            let status = unsafe {
                entry(
                    &mut runtime,
                    inputs.as_ptr(),
                    descriptors.as_ptr(),
                    &mut count,
                    states.as_ptr(),
                    rows,
                )
            };
            Self::check(code, status, &mut runtime)?;
        }
        let Some(output) = &self.output else {
            return Ok(None);
        };
        let mut arrays = Vec::with_capacity(output.bindings.len());
        for (j, field) in output.schema.fields().iter().enumerate() {
            let array = match self.aliases[j] {
                Some(i) => batch.column(self.inputs[i].position).clone(),
                None => columns[j]
                    .take()
                    .unwrap()
                    .finish(field.data_type(), count)?,
            };
            arrays.push(array);
        }
        // Strings formatted by generated code are copied by `finish` above.
        drop(runtime);
        Ok(Some(RecordBatch::try_new_with_options(
            output.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?))
    }

    fn check(code: &Compiled, status: i32, runtime: &mut Runtime) -> Result<()> {
        match status {
            0 => Ok(()),
            1 => Err(runtime
                .error
                .take()
                .unwrap_or_else(|| Error::Execution("generated runtime call failed".into()))),
            s => {
                let site = code
                    .sites
                    .get((s - 2) as usize)
                    .ok_or_else(|| Error::Execution(format!("unknown generated status {s}")))?;
                Err(site.to_error(&runtime.raise_args))
            }
        }
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
