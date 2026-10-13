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

//! Zero-compile execution of a kernel plan.
//!
//! Preparing a program selects one precompiled function per kernel: generated
//! code from the process-wide kernel cache, or a Rust implementation. A chunk
//! runs in windows of `WINDOW` rows, so the columns a step reads stay in cache;
//! each step runs over the whole window, in plan order.
//! A variable is a constant or a column of words indexed by root row; a Filter
//! domain is a selection vector of root rows, so reading an ancestor variable
//! needs no gather. Words are canonical: signed integers sign-extended,
//! unsigned integers and Booleans zero-extended, floats as their IEEE bits.
use super::aggregation::{AccumulatorState, GroupState};
use super::jit::{KernelEntry, generated};
use super::plan::{Item, Plan};
use super::runtime::Runtime;
use super::scalar::Cell;
use super::*;
use crate::arrow::{
    array::*,
    datatypes::*,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, Result};
use crate::exec::aggregate::group_index::WordEntry;
use crate::expr::agg::accumulator::{self as agg, Accumulator, RowKernel};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

mod functions;
use functions::{CallFn, FormatFn, Operand, Resolved};

/// Rows per window.
const WINDOW: usize = 1024;

#[derive(Default)]
enum Register {
    #[default]
    Empty,
    Constant(u64),
    Column(Vec<u64>),
}

/// The rows of a domain as root row indices.
#[derive(Clone, Copy)]
enum Rows<'a> {
    /// Root rows `start..end`.
    Range(usize, usize),
    Selected(&'a [u32]),
}

impl Rows<'_> {
    fn len(self) -> usize {
        match self {
            Self::Range(start, end) => end - start,
            Self::Selected(s) => s.len(),
        }
    }
    #[inline(always)]
    fn for_each(self, mut f: impl FnMut(usize)) {
        match self {
            Self::Range(start, end) => (start..end).for_each(f),
            Self::Selected(s) => s.iter().for_each(|&r| f(r as usize)),
        }
    }
    fn find(self, f: impl Fn(usize) -> bool) -> Option<usize> {
        match self {
            Self::Range(start, end) => (start..end).find(|&r| f(r)),
            Self::Selected(s) => s.iter().map(|&r| r as usize).find(|&r| f(r)),
        }
    }
    /// The root row of the i-th row.
    #[inline(always)]
    fn at(self, i: usize) -> usize {
        match self {
            Self::Range(start, _) => start + i,
            Self::Selected(s) => s[i] as usize,
        }
    }
}

/// The output rows of the windows run so far.
enum Emitted {
    /// Root rows `0..end`.
    Dense(usize),
    Sparse(Vec<u32>),
}

impl Emitted {
    fn extend(&mut self, rows: Rows<'_>) {
        match (&mut *self, rows) {
            (Self::Dense(end), Rows::Range(start, stop)) if *end == start => *end = stop,
            (Self::Dense(end), rows) => {
                let mut sparse = (0..*end as u32).collect::<Vec<_>>();
                rows.for_each(|r| sparse.push(r as u32));
                *self = Self::Sparse(sparse);
            }
            (Self::Sparse(sparse), rows) => rows.for_each(|r| sparse.push(r as u32)),
        }
    }
}

type UpdateFn = fn(&mut Accumulator, usize, &[Cell]) -> Result<()>;

#[derive(Clone, Copy)]
enum Callable {
    Native(CallFn),
    Generated(KernelEntry),
}

enum Step {
    Input {
        input: usize,
        variables: Vec<VariableId>,
    },
    Constant {
        bits: u64,
        output: VariableId,
    },
    Call {
        function: Callable,
        inputs: Vec<VariableId>,
        outputs: Vec<VariableId>,
        /// Whether the function may run at rows outside its domain: it reads
        /// no memory through its inputs and cannot fail, so such rows only
        /// produce words no one reads.
        total: bool,
    },
    Format {
        function: FormatFn,
        input: VariableId,
        ptr: VariableId,
        len: VariableId,
    },
    Filter {
        predicate: VariableId,
        output: DomainId,
    },
    RaiseIf {
        cond: VariableId,
        error: KernelError,
        args: Vec<VariableId>,
    },
    GroupLookup {
        state: StateId,
        keys: Vec<ColumnVariableBinding>,
        output: VariableId,
    },
    AggregateUpdate {
        state: StateId,
        kind: AggregateUpdateKind,
        update: UpdateFn,
        group: VariableId,
        arguments: Vec<ColumnVariableBinding>,
    },
    Output,
}

/// The prepared steps of one program, each with the domain it runs in.
pub(crate) struct Steps {
    steps: Vec<(Step, DomainId)>,
    variables: usize,
    domains: usize,
    _constants: Vec<Arc<[u8]>>,
}

impl std::fmt::Debug for Steps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Steps")
            .field("steps", &self.steps.len())
            .finish()
    }
}

impl Steps {
    pub fn prepare(graph: &KernelGraph, plan: &Plan, library: KernelLibrary) -> Result<Self> {
        use AggregateUpdateKind as K;
        // Output columns aliasing an input reuse its array, so an input
        // variable only they read is never loaded.
        let mut read = vec![false; graph.variables.len()];
        for &(item, _) in &plan.items {
            if let Item::Kernel(k) = item {
                let inputs = match &graph.kernels[k].1 {
                    Kernel::Scalar(kernel) => kernel.inputs(),
                    Kernel::Domain(kernel) => kernel.inputs(),
                };
                inputs.iter().for_each(|v| read[v.0] = true);
            }
        }
        if let Some(output) = &graph.output {
            for (j, binding) in output.bindings.iter().enumerate() {
                if plan.aliases.get(j).copied().flatten().is_none() {
                    binding.variables().iter().for_each(|v| read[v.0] = true);
                }
            }
        }
        let mut constants = vec![];
        let mut steps = Vec::with_capacity(plan.items.len());
        for &(item, domain) in &plan.items {
            let step = match item {
                Item::Input(input) => {
                    let variables = plan.input_vars[input]
                        .iter()
                        .copied()
                        .filter(|v| read[v.0])
                        .collect::<Vec<_>>();
                    if variables.is_empty() {
                        continue;
                    }
                    Step::Input { input, variables }
                }
                Item::Output => Step::Output,
                Item::Kernel(k) => match &graph.kernels[k].1 {
                    Kernel::Domain(DomainKernel::Filter { predicate, output }) => Step::Filter {
                        predicate: *predicate,
                        output: *output,
                    },
                    Kernel::Domain(_) => {
                        return Err(Error::InvalidPlan(
                            "only Filter domain kernels are supported".into(),
                        ));
                    }
                    Kernel::Scalar(kernel) => match kernel.clone() {
                        ScalarKernel::Constant { value, output } => {
                            if let ConstantValue::BytePointer(bytes) = &value {
                                constants.push(bytes.clone());
                            }
                            Step::Constant {
                                bits: value.bits(),
                                output,
                            }
                        }
                        ScalarKernel::Call {
                            function,
                            inputs,
                            outputs,
                        } => {
                            let types = inputs
                                .iter()
                                .map(|v| graph.variables[v.0].ty)
                                .collect::<Vec<_>>();
                            match functions::resolve(function, &types)? {
                                Resolved::Call(native) => Step::Call {
                                    function: match library {
                                        KernelLibrary::Native => Callable::Native(native),
                                        KernelLibrary::Generated => {
                                            Callable::Generated(generated(function, &types)?)
                                        }
                                    },
                                    total: inputs.iter().chain(&outputs).all(|v| {
                                        graph.variables[v.0].ty != VariableType::BytePointer
                                    }),
                                    inputs,
                                    outputs,
                                },
                                Resolved::Format(function) => Step::Format {
                                    function,
                                    input: inputs[0],
                                    ptr: outputs[0],
                                    len: outputs[1],
                                },
                            }
                        }
                        ScalarKernel::Select {
                            cond,
                            then,
                            otherwise,
                            output,
                        } => Step::Call {
                            // A word select in both libraries; it has no
                            // type-specific semantics.
                            function: Callable::Native(functions::select()),
                            inputs: vec![cond, then, otherwise],
                            outputs: vec![output],
                            total: true,
                        },
                        ScalarKernel::RaiseIf { cond, error, args } => {
                            Step::RaiseIf { cond, error, args }
                        }
                        ScalarKernel::GroupLookup {
                            state,
                            keys,
                            output,
                        } => Step::GroupLookup {
                            state,
                            keys,
                            output,
                        },
                        ScalarKernel::AggregateUpdate {
                            state,
                            kind,
                            group,
                            arguments,
                        } => Step::AggregateUpdate {
                            state,
                            kind,
                            update: match kind {
                                K::CountRows => agg::CountRows::update,
                                K::CountValues => agg::CountValues::update,
                                K::CountDistinct => agg::CountDistinct::update,
                                K::SumI64 | K::SumU64 => agg::SumInteger::update,
                                K::SumF64 => agg::SumFloat::update,
                                K::Avg => agg::Average::update,
                                K::CovarPop => agg::CovarianceUpdate::update,
                                K::Min => agg::Minimum::update,
                                K::Max => agg::Maximum::update,
                            },
                            group,
                            arguments,
                        },
                    },
                },
            };
            steps.push((step, domain));
        }
        Ok(Self {
            steps,
            variables: graph.variables.len(),
            domains: graph.domains.len(),
            _constants: constants,
        })
    }

    pub fn run(
        &self,
        batch: &RecordBatch,
        inputs: &[GraphInput],
        output: Option<&GraphOutput>,
        aliases: &[Option<usize>],
        local: &mut LocalState,
        guard: &ShutdownGuard,
    ) -> Result<Option<RecordBatch>> {
        if u32::try_from(batch.num_rows()).is_err() {
            return Err(Error::Execution("chunk has too many rows".into()));
        }
        let rows = batch.num_rows();
        let mut frame = Frame {
            rows,
            start: 0,
            end: 0,
            registers: (0..self.variables).map(|_| Register::Empty).collect(),
            selections: vec![None; self.domains],
            pool: std::mem::take(&mut local.scratch),
            strings: vec![],
            limit: rows,
            raised: None,
            emitted: Emitted::Dense(0),
        };
        let mut result = Ok(());
        // A chunk without rows still runs one empty window.
        'windows: while frame.end < rows || frame.end == 0 {
            frame.start = frame.end;
            frame.end = rows.min(frame.start + WINDOW);
            if frame.limit <= frame.start && frame.raised.is_some() {
                break;
            }
            frame.selections.iter_mut().for_each(|s| *s = None);
            for (step, domain) in &self.steps {
                if guard.is_shutdown_requested() {
                    result = Err(Error::Cancelled);
                    break 'windows;
                }
                if frame.raised.is_some() && frame.limit <= frame.start {
                    break;
                }
                let done = frame.with_rows(*domain, |frame, rows| {
                    frame.step(step, rows, batch, inputs, local)
                });
                if let Err(e) = done {
                    result = Err(e);
                    break 'windows;
                }
            }
            if rows == 0 {
                break;
            }
        }
        let result = match (result, frame.raised.take()) {
            (Err(e), _) | (Ok(()), Some(e)) => Err(e),
            (Ok(()), None) => match output {
                None => Ok(None),
                Some(output) => {
                    let emitted = std::mem::replace(&mut frame.emitted, Emitted::Dense(0));
                    let rows = match &emitted {
                        Emitted::Dense(end) => Rows::Range(0, *end),
                        Emitted::Sparse(rows) => Rows::Selected(rows),
                    };
                    frame.output(batch, inputs, output, aliases, rows).map(Some)
                }
            },
        };
        for register in frame.registers.drain(..) {
            if let Register::Column(column) = register {
                frame.pool.push(column);
            }
        }
        local.scratch = frame.pool;
        result
    }
}

struct Frame {
    rows: usize,
    /// The root rows `start..end` of the current window.
    start: usize,
    end: usize,
    registers: Vec<Register>,
    /// Per domain; None while the domain has every root row.
    selections: Vec<Option<Vec<u32>>>,
    /// Columns reused across chunks; each has `rows` words once taken.
    pool: Vec<Vec<u64>>,
    /// Owns strings produced by FormatText until the output is copied.
    strings: Vec<Box<[u8]>>,
    /// Steps see only root rows below this. Fused code stops at the first
    /// failing row, so once a RaiseIf fails at a row, later steps only run the
    /// rows before it: an error they raise is then the one fused code raises.
    limit: usize,
    /// The error of the RaiseIf that set `limit`.
    raised: Option<Error>,
    /// The rows the Output step selected in earlier windows.
    emitted: Emitted,
}

fn operand(registers: &[Register], v: VariableId) -> Result<Operand<'_>> {
    match &registers[v.0] {
        Register::Constant(c) => Ok(Operand::Constant(*c)),
        Register::Column(c) => Ok(Operand::Column(c)),
        Register::Empty => Err(Error::Execution(format!("variable {} has no value", v.0))),
    }
}

/// A column of `rows` words; rows outside the writer's domain hold stale values.
fn column(pool: &mut Vec<Vec<u64>>, rows: usize) -> Vec<u64> {
    let mut column = pool.pop().unwrap_or_default();
    if column.len() != rows {
        column.resize(rows, 0);
    }
    column
}

impl Frame {
    /// The whole window instead of a selection keeping most of it, for steps
    /// that may compute rows outside their domain.
    fn widen<'a>(&self, rows: Rows<'a>) -> Rows<'a> {
        match rows {
            Rows::Selected(s) if s.len() * 4 >= self.end - self.start => {
                Rows::Range(self.start, self.end)
            }
            rows => rows,
        }
    }

    /// The column of `v`: the one written by earlier windows, or a new one.
    fn target(&mut self, v: VariableId) -> Vec<u64> {
        match std::mem::take(&mut self.registers[v.0]) {
            Register::Column(c) => c,
            _ => column(&mut self.pool, self.rows),
        }
    }
}

/// `[bits, len, valid]` of a binding, as in the runtime Cell.
fn cell_operands<'a>(
    registers: &'a [Register],
    binding: &ColumnVariableBinding,
) -> Result<[Operand<'a>; 3]> {
    let valid = match binding.validity() {
        Some(v) if !binding.is_null_type() => operand(registers, v)?,
        _ => Operand::Constant(1),
    };
    Ok(match *binding {
        ColumnVariableBinding::Null { .. } => [Operand::Constant(0); 3],
        ColumnVariableBinding::Bytes { ptr, len, .. } => {
            [operand(registers, ptr)?, operand(registers, len)?, valid]
        }
        _ => [
            operand(registers, binding.value().unwrap())?,
            Operand::Constant(0),
            valid,
        ],
    })
}

#[inline(always)]
fn cell([bits, len, valid]: [Operand<'_>; 3], row: usize) -> Cell {
    Cell {
        bits: bits.get(row),
        len: len.get(row),
        valid: valid.get(row),
    }
}

#[inline(always)]
fn fill(rows: Rows<'_>, out: &mut [u64], f: impl Fn(usize) -> u64) {
    match rows {
        Rows::Range(start, end) => {
            for (r, o) in out[start..end].iter_mut().enumerate() {
                *o = f(start + r);
            }
        }
        Rows::Selected(s) => {
            for &r in s {
                out[r as usize] = f(r as usize);
            }
        }
    }
}

fn invoke(
    function: Callable,
    operands: &[Operand<'_>],
    rows: Rows<'_>,
    outputs: &mut [Vec<u64>],
) -> Result<()> {
    let entry = match function {
        Callable::Native(f) => {
            f(operands, rows, outputs);
            return Ok(());
        }
        Callable::Generated(entry) => entry,
    };
    let constants = operands
        .iter()
        .map(|o| match o {
            Operand::Constant(c) => *c,
            Operand::Column(_) => 0,
        })
        .collect::<Vec<_>>();
    // A range runs as rows `0..n` of columns offset to its start.
    let (offset, selection, n) = match rows {
        Rows::Range(start, end) => (start, std::ptr::null(), end - start),
        Rows::Selected(s) => (0, s.as_ptr(), s.len()),
    };
    let sources = operands
        .iter()
        .zip(&constants)
        .map(|(o, c)| match o {
            Operand::Constant(_) => [c as *const u64 as u64, 0],
            Operand::Column(v) => [v[offset..].as_ptr() as u64, u64::MAX],
        })
        .collect::<Vec<_>>();
    let sinks = outputs
        .iter_mut()
        .map(|o| o[offset..].as_mut_ptr())
        .collect::<Vec<_>>();
    let mut runtime = Runtime::default();
    // SAFETY: input columns and outputs have a word for every root row listed
    // by `rows`; constants outlive the call.
    let status = unsafe { entry(&mut runtime, sources.as_ptr(), sinks.as_ptr(), selection, n) };
    if status == 0 {
        Ok(())
    } else {
        Err(runtime
            .error
            .take()
            .unwrap_or_else(|| Error::Execution(format!("generated kernel failed: {status}"))))
    }
}

fn overflow() -> Error {
    Error::Execution("aggregate arithmetic overflow".into())
}

impl Frame {
    fn with_rows<R>(&mut self, domain: DomainId, f: impl FnOnce(&mut Self, Rows<'_>) -> R) -> R {
        let selection = self.selections[domain.0].take();
        let rows = match &selection {
            None => Rows::Range(self.start, self.end.min(self.limit).max(self.start)),
            Some(s) => Rows::Selected(&s[..s.partition_point(|&r| (r as usize) < self.limit)]),
        };
        let result = f(self, rows);
        self.selections[domain.0] = selection;
        result
    }

    fn step(
        &mut self,
        step: &Step,
        rows: Rows<'_>,
        batch: &RecordBatch,
        inputs: &[GraphInput],
        local: &mut LocalState,
    ) -> Result<()> {
        match step {
            Step::Input { input, variables } => {
                let input = &inputs[*input];
                let rows = self.widen(rows);
                self.input(
                    batch.column(input.position),
                    &input.binding,
                    variables,
                    rows,
                )?;
            }
            Step::Constant { bits, output } => {
                self.registers[output.0] = Register::Constant(*bits);
            }
            Step::Call {
                function,
                inputs,
                outputs,
                total,
            } => {
                let rows = if *total { self.widen(rows) } else { rows };
                self.call(*function, inputs, outputs, rows)?
            }
            Step::Format {
                function,
                input,
                ptr,
                len,
            } => self.format(*function, *input, *ptr, *len, rows)?,
            Step::Filter { predicate, output } => self.filter(*predicate, *output, rows)?,
            Step::RaiseIf { cond, error, args } => {
                let hit = match (operand(&self.registers, *cond)?, rows) {
                    (Operand::Constant(c), rows) => (c != 0).then(|| rows.find(|_| true)).flatten(),
                    // Failures are rare: OR whole blocks, which vectorizes,
                    // before looking for the row.
                    (Operand::Column(c), Rows::Range(start, end)) => c[start..end]
                        .chunks(64)
                        .enumerate()
                        .find(|(_, block)| block.iter().fold(0, |a, &w| a | w) != 0)
                        .and_then(|(i, block)| {
                            Some(start + i * 64 + block.iter().position(|&w| w != 0)?)
                        }),
                    (Operand::Column(c), rows) => {
                        let window = &c[self.start..self.end];
                        if window.iter().fold(0, |a, &w| a | w) == 0 {
                            None
                        } else {
                            rows.find(|r| c[r] != 0)
                        }
                    }
                };
                if let Some(r) = hit {
                    let args = args
                        .iter()
                        .map(|v| Ok(operand(&self.registers, *v)?.get(r)))
                        .collect::<Result<Vec<_>>>()?;
                    self.raised = Some(error.to_error(&args));
                    self.limit = r;
                }
            }
            Step::GroupLookup {
                state,
                keys,
                output,
            } => {
                let index = &mut local.state_mut(*state)?.get_mut::<GroupState>()?.index;
                self.group_lookup(index, keys, *output, rows)?;
            }
            Step::AggregateUpdate {
                state,
                kind,
                update,
                group,
                arguments,
            } => {
                let accumulator = &mut local
                    .state_mut(*state)?
                    .get_mut::<AccumulatorState>()?
                    .accumulator;
                self.aggregate_update(accumulator, *kind, *update, *group, arguments, rows)?;
            }
            Step::Output => {
                if self.raised.is_none() {
                    self.emitted.extend(rows);
                }
            }
        }
        Ok(())
    }

    fn input(
        &mut self,
        array: &ArrayRef,
        binding: &ColumnVariableBinding,
        variables: &[VariableId],
        rows: Rows<'_>,
    ) -> Result<()> {
        for &v in variables {
            let register = if Some(v) == binding.validity() {
                match array.nulls() {
                    None => Register::Constant(1),
                    Some(nulls) => {
                        let mut c = self.target(v);
                        match rows {
                            Rows::Range(start, end) => {
                                let bits = nulls.inner().slice(start, end - start);
                                let out = &mut c[start..end];
                                let chunks = bits.bit_chunks();
                                let n = chunks.chunk_len() * 64;
                                for (block, chunk) in
                                    out[..n].chunks_exact_mut(64).zip(chunks.iter())
                                {
                                    for (i, o) in block.iter_mut().enumerate() {
                                        *o = (chunk >> i) & 1;
                                    }
                                }
                                let rest = chunks.remainder_bits();
                                for (i, o) in out[n..].iter_mut().enumerate() {
                                    *o = (rest >> i) & 1;
                                }
                            }
                            rows => fill(rows, &mut c, |r| nulls.is_valid(r) as u64),
                        }
                        Register::Column(c)
                    }
                }
            } else {
                let mut c = self.target(v);
                match *binding {
                    ColumnVariableBinding::Bytes { ptr, .. } => {
                        bytes(array, v == ptr, rows, &mut c)?
                    }
                    _ => values(array, rows, &mut c)?,
                }
                Register::Column(c)
            };
            self.registers[v.0] = register;
        }
        Ok(())
    }

    fn call(
        &mut self,
        function: Callable,
        inputs: &[VariableId],
        outputs: &[VariableId],
        rows: Rows<'_>,
    ) -> Result<()> {
        if inputs
            .iter()
            .all(|v| matches!(self.registers[v.0], Register::Constant(_)))
        {
            let operands = inputs
                .iter()
                .map(|v| operand(&self.registers, *v))
                .collect::<Result<Vec<_>>>()?;
            let mut values = vec![vec![0]; outputs.len()];
            invoke(function, &operands, Rows::Range(0, 1), &mut values)?;
            for (v, value) in outputs.iter().zip(values) {
                self.registers[v.0] = Register::Constant(value[0]);
            }
            return Ok(());
        }
        let mut columns = outputs.iter().map(|&v| self.target(v)).collect::<Vec<_>>();
        let operands = inputs
            .iter()
            .map(|v| operand(&self.registers, *v))
            .collect::<Result<Vec<_>>>()?;
        invoke(function, &operands, rows, &mut columns)?;
        for (v, c) in outputs.iter().zip(columns) {
            self.registers[v.0] = Register::Column(c);
        }
        Ok(())
    }

    fn format(
        &mut self,
        function: FormatFn,
        input: VariableId,
        ptr: VariableId,
        len: VariableId,
        rows: Rows<'_>,
    ) -> Result<()> {
        let store = |strings: &mut Vec<Box<[u8]>>, bits| {
            let text = function(bits).into_bytes().into_boxed_slice();
            let word = (text.as_ptr() as u64, text.len() as u64);
            strings.push(text);
            word
        };
        match operand(&self.registers, input)? {
            Operand::Constant(bits) => {
                let (p, n) = store(&mut self.strings, bits);
                self.registers[ptr.0] = Register::Constant(p);
                self.registers[len.0] = Register::Constant(n);
            }
            Operand::Column(_) => {
                let mut p = self.target(ptr);
                let mut n = self.target(len);
                let Operand::Column(values) = operand(&self.registers, input)? else {
                    unreachable!("checked above")
                };
                rows.for_each(|r| (p[r], n[r]) = store(&mut self.strings, values[r]));
                self.registers[ptr.0] = Register::Column(p);
                self.registers[len.0] = Register::Column(n);
            }
        }
        Ok(())
    }

    fn filter(&mut self, predicate: VariableId, output: DomainId, rows: Rows<'_>) -> Result<()> {
        let selection = match operand(&self.registers, predicate)? {
            Operand::Constant(c) if c != 0 => match rows {
                Rows::Range(start, end) if (start, end) == (self.start, self.end) => None,
                rows => {
                    let mut selection = Vec::with_capacity(rows.len());
                    rows.for_each(|r| selection.push(r as u32));
                    Some(selection)
                }
            },
            Operand::Constant(_) => Some(vec![]),
            Operand::Column(p) => {
                let mut selection = vec![0u32; rows.len()];
                let mut kept = 0;
                match rows {
                    Rows::Range(start, end) => {
                        for (r, &p) in p[start..end].iter().enumerate() {
                            selection[kept] = (start + r) as u32;
                            kept += (p != 0) as usize;
                        }
                    }
                    Rows::Selected(s) => {
                        for &r in s {
                            selection[kept] = r;
                            kept += (p[r as usize] != 0) as usize;
                        }
                    }
                }
                if matches!(rows, Rows::Range(start, end)
                    if (start, end) == (self.start, self.end) && kept == end - start)
                {
                    None
                } else {
                    selection.truncate(kept);
                    Some(selection)
                }
            }
        };
        self.selections[output.0] = selection;
        Ok(())
    }

    fn group_lookup(
        &mut self,
        index: &mut crate::exec::aggregate::group_index::GroupIndex,
        keys: &[ColumnVariableBinding],
        output: VariableId,
        rows: Rows<'_>,
    ) -> Result<()> {
        if keys.is_empty() {
            self.registers[output.0] = Register::Constant(0);
            return Ok(());
        }
        let mut ids = self.target(output);
        let keys = keys
            .iter()
            .map(|k| cell_operands(&self.registers, k))
            .collect::<Result<Vec<_>>>()?;
        if let ([[bits, _, valid]], Some(_)) = (keys.as_slice(), &index.cache) {
            // A single word key probes the index's word cache inline, as
            // generated code does.
            let constants = [bits.constant(), valid.constant()];
            let (bits, valid) = (bits.view(&constants[0]), valid.view(&constants[1]));
            let cache: *const [WordEntry; 1024] = &**index.cache.as_ref().unwrap();
            rows.for_each(|r| {
                let (b, v) = (bits.at(r), valid.at(r));
                let key = if v != 0 { b } else { 0 };
                // SAFETY: interning rewrites cache entries in place and never
                // replaces the boxed cache.
                let cached = unsafe { (*cache)[key as usize & 1023] };
                ids[r] = if cached.key == key && cached.tag == v + 1 {
                    cached.id
                } else {
                    index.intern_word(Cell {
                        bits: b,
                        len: 0,
                        valid: v,
                    }) as u64
                };
            });
        } else if let [key] = keys.as_slice() {
            rows.for_each(|r| ids[r] = index.intern_row(&[cell(*key, r)]) as u64);
        } else {
            let mut cells = Vec::with_capacity(keys.len());
            rows.for_each(|r| {
                cells.clear();
                cells.extend(keys.iter().map(|k| cell(*k, r)));
                ids[r] = index.intern_row(&cells) as u64;
            });
        }
        drop(keys);
        self.registers[output.0] = Register::Column(ids);
        Ok(())
    }

    fn aggregate_update(
        &mut self,
        accumulator: &mut Accumulator,
        kind: AggregateUpdateKind,
        update: UpdateFn,
        group: VariableId,
        arguments: &[ColumnVariableBinding],
        rows: Rows<'_>,
    ) -> Result<()> {
        use AggregateUpdateKind as K;
        use ColumnVariableBinding as B;
        let group = operand(&self.registers, group)?;
        let max = match group {
            Operand::Constant(g) => (rows.len() != 0).then_some(g),
            Operand::Column(ids) => {
                let mut max = None;
                rows.for_each(|r| max = max.max(Some(ids[r])));
                max
            }
        };
        let Some(max) = max else { return Ok(()) };
        accumulator.resize(max as usize + 1);
        let args = arguments
            .iter()
            .map(|a| cell_operands(&self.registers, a))
            .collect::<Result<Vec<_>>>()?;
        let groups = accumulator.groups_mut();
        // Count and Sum update group slots like the inlined generated updates.
        match (kind, arguments, group) {
            (K::CountRows, _, Operand::Constant(g)) => {
                let count = &mut groups[g as usize].count;
                *count = count.checked_add(rows.len() as i64).ok_or_else(overflow)?;
                return Ok(());
            }
            (K::CountValues, [_], Operand::Constant(g)) => {
                let valid = args[0][2];
                let mut n = 0i64;
                match (valid, rows) {
                    (Operand::Column(v), Rows::Range(s, e)) => {
                        n = v[s..e].iter().sum::<u64>() as i64
                    }
                    _ => rows.for_each(|r| n += valid.get(r) as i64),
                }
                let count = &mut groups[g as usize].count;
                *count = count.checked_add(n).ok_or_else(overflow)?;
                return Ok(());
            }
            (K::CountRows | K::CountValues, _, Operand::Column(ids)) if arguments.len() <= 1 => {
                let valid = match (kind, args.first()) {
                    (K::CountValues, Some(a)) => a[2],
                    _ => Operand::Constant(1),
                };
                let constant = valid.constant();
                let valid = valid.view(&constant);
                let mut overflowed = false;
                let mut add = |id: u64, v: u64| {
                    let count = &mut groups[id as usize].count;
                    let (c, o) = count.overflowing_add(v as i64);
                    *count = c;
                    overflowed |= o;
                };
                match rows {
                    Rows::Range(s, e) if constant == 1 => {
                        ids[s..e].iter().for_each(|&id| add(id, 1))
                    }
                    Rows::Range(s, e) => (s..e).for_each(|r| add(ids[r], valid.at(r))),
                    rows => rows.for_each(|r| add(ids[r], valid.at(r))),
                }
                return if overflowed { Err(overflow()) } else { Ok(()) };
            }
            (K::SumI64, [B::Int64 { .. }], _) | (K::SumU64, [B::UInt64 { .. }], _) => {
                let [bits, _, valid] = args[0];
                let constants = [bits.constant(), valid.constant()];
                let (bits, valid) = (bits.view(&constants[0]), valid.view(&constants[1]));
                // Valid words are 0 or 1, so `wrapping_neg` masks NULL rows.
                match group {
                    Operand::Constant(g) => {
                        let (mut total, mut seen) = (0u64, 0u64);
                        match (args[0], rows) {
                            ([Operand::Column(b), _, Operand::Column(v)], Rows::Range(s, e)) => {
                                for (&b, &v) in b[s..e].iter().zip(&v[s..e]) {
                                    total = total.wrapping_add(b & v.wrapping_neg());
                                    seen |= v;
                                }
                            }
                            _ => rows.for_each(|r| {
                                let v = valid.at(r);
                                total = total.wrapping_add(bits.at(r) & v.wrapping_neg());
                                seen |= v;
                            }),
                        }
                        let sum = &mut groups[g as usize].sum;
                        sum.bits = sum.bits.wrapping_add(total);
                        sum.valid |= seen;
                    }
                    Operand::Column(ids) => {
                        let mut add = |id: u64, b: u64, v: u64| {
                            let sum = &mut groups[id as usize].sum;
                            sum.bits = sum.bits.wrapping_add(b & v.wrapping_neg());
                            sum.valid |= v;
                        };
                        match (args[0], rows) {
                            ([Operand::Column(b), _, Operand::Column(v)], Rows::Range(s, e)) => {
                                for ((&id, &b), &v) in ids[s..e].iter().zip(&b[s..e]).zip(&v[s..e])
                                {
                                    add(id, b, v);
                                }
                            }
                            _ => rows.for_each(|r| add(ids[r], bits.at(r), valid.at(r))),
                        }
                    }
                }
                return Ok(());
            }
            (K::SumF64, [B::Float64 { .. }], _) => {
                let [bits, _, valid] = args[0];
                rows.for_each(|r| {
                    if valid.get(r) != 0 {
                        let value = f64::from_bits(bits.get(r));
                        let sum = &mut groups[group.get(r) as usize].sum;
                        sum.bits = if sum.valid == 0 {
                            value
                        } else {
                            f64::from_bits(sum.bits) + value
                        }
                        .to_bits();
                        sum.valid = 1;
                    }
                });
                return Ok(());
            }
            _ => {}
        }
        let mut cells = Vec::with_capacity(args.len());
        let mut result = Ok(());
        rows.for_each(|r| {
            if result.is_ok() {
                cells.clear();
                cells.extend(args.iter().map(|a| cell(*a, r)));
                result = update(accumulator, group.get(r) as usize, &cells);
            }
        });
        result
    }

    fn output(
        &self,
        batch: &RecordBatch,
        inputs: &[GraphInput],
        output: &GraphOutput,
        aliases: &[Option<usize>],
        rows: Rows<'_>,
    ) -> Result<RecordBatch> {
        let mut arrays = Vec::with_capacity(output.bindings.len());
        for (j, binding) in output.bindings.iter().enumerate() {
            let array = match aliases.get(j).copied().flatten() {
                Some(i) => batch.column(inputs[i].position).clone(),
                None => self.array(output.schema.field(j).data_type(), binding, rows)?,
            };
            arrays.push(array);
        }
        Ok(RecordBatch::try_new_with_options(
            output.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )?)
    }

    fn array(
        &self,
        t: &DataType,
        binding: &ColumnVariableBinding,
        rows: Rows<'_>,
    ) -> Result<ArrayRef> {
        let n = rows.len();
        if binding.is_null_type() {
            return Ok(Arc::new(NullArray::new(n)));
        }
        let nulls = match binding.validity() {
            None => None,
            Some(v) => match operand(&self.registers, v)? {
                Operand::Constant(c) if c != 0 => None,
                Operand::Constant(_) => Some(NullBuffer::new_null(n)),
                Operand::Column(c) => {
                    let nulls =
                        NullBuffer::new(BooleanBuffer::collect_bool(n, |i| c[rows.at(i)] != 0));
                    (nulls.null_count() != 0).then_some(nulls)
                }
            },
        };
        let valid = |i: usize| nulls.as_ref().is_none_or(|n| n.is_valid(i));
        if let ColumnVariableBinding::Bytes { ptr, len, .. } = *binding {
            let ptr = operand(&self.registers, ptr)?;
            let len = operand(&self.registers, len)?;
            let lengths = (0..n).map(|i| {
                if valid(i) {
                    len.get(rows.at(i)) as usize
                } else {
                    0
                }
            });
            let total = lengths.clone().sum();
            let mut data = Vec::with_capacity(total);
            for (i, length) in lengths.clone().enumerate() {
                if length != 0 {
                    // SAFETY: byte pointers reference input arrays, retained
                    // constants or the string arena, all alive here.
                    data.extend_from_slice(unsafe {
                        std::slice::from_raw_parts(ptr.get(rows.at(i)) as *const u8, length)
                    });
                }
            }
            let data = Buffer::from_vec(data);
            return Ok(match t {
                DataType::Utf8 => Arc::new(StringArray::try_new(
                    OffsetBuffer::from_lengths(lengths),
                    data,
                    nulls,
                )?),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::try_new(
                    OffsetBuffer::from_lengths(lengths),
                    data,
                    nulls,
                )?),
                DataType::Binary => Arc::new(BinaryArray::try_new(
                    OffsetBuffer::from_lengths(lengths),
                    data,
                    nulls,
                )?),
                _ => Arc::new(LargeBinaryArray::try_new(
                    OffsetBuffer::from_lengths(lengths),
                    data,
                    nulls,
                )?),
            });
        }
        let value = operand(&self.registers, binding.value().unwrap())?;
        if *t == DataType::Boolean {
            return Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::collect_bool(n, |i| value.get(rows.at(i)) != 0),
                nulls,
            )));
        }
        macro_rules! words {
            ($t:ty) => {
                match (value, rows) {
                    (Operand::Column(c), Rows::Range(start, end)) => {
                        c[start..end].iter().map(|&w| w as $t).collect::<Vec<_>>()
                    }
                    (Operand::Column(c), Rows::Selected(s)) => {
                        s.iter().map(|&r| c[r as usize] as $t).collect()
                    }
                    (Operand::Constant(w), _) => vec![w as $t; n],
                }
                .into()
            };
        }
        let array: ArrayRef = match VariableType::of(t).map_or(64, VariableType::bits) {
            8 => Arc::new(Int8Array::new(words!(i8), nulls)),
            16 => Arc::new(Int16Array::new(words!(i16), nulls)),
            32 => Arc::new(Int32Array::new(words!(i32), nulls)),
            _ => Arc::new(Int64Array::new(words!(i64), nulls)),
        };
        if array.data_type() == t {
            return Ok(array);
        }
        Ok(make_array(
            array
                .into_data()
                .into_builder()
                .data_type(t.clone())
                .build()?,
        ))
    }
}

fn values(array: &ArrayRef, rows: Rows<'_>, out: &mut [u64]) -> Result<()> {
    use functions::Word;
    macro_rules! copy {
        ($t:ty) => {{
            let values = array.as_primitive::<$t>().values();
            match rows {
                Rows::Range(start, end) => out[start..end]
                    .iter_mut()
                    .zip(&values[start..end])
                    .for_each(|(o, &v)| *o = Word::put(v)),
                rows => fill(rows, out, |r| Word::put(values[r])),
            }
        }};
    }
    match array.data_type() {
        DataType::Boolean => {
            let values = array.as_boolean().values();
            fill(rows, out, |r| values.value(r) as u64)
        }
        DataType::Int8 => copy!(Int8Type),
        DataType::Int16 => copy!(Int16Type),
        DataType::Int32 => copy!(Int32Type),
        DataType::Int64 => copy!(Int64Type),
        DataType::UInt8 => copy!(UInt8Type),
        DataType::UInt16 => copy!(UInt16Type),
        DataType::UInt32 => copy!(UInt32Type),
        DataType::UInt64 => copy!(UInt64Type),
        DataType::Float32 => copy!(Float32Type),
        DataType::Float64 => copy!(Float64Type),
        DataType::Date32 => copy!(Date32Type),
        DataType::Date64 => copy!(Date64Type),
        DataType::Timestamp(TimeUnit::Second, _) => copy!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => copy!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => copy!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => copy!(TimestampNanosecondType),
        t => {
            return Err(Error::InvalidPlan(format!(
                "unsupported input value type: {t}"
            )));
        }
    }
    Ok(())
}

/// Pointers (`pointers`) or lengths of a byte array's rows.
fn bytes(array: &ArrayRef, pointers: bool, rows: Rows<'_>, out: &mut [u64]) -> Result<()> {
    fn write<O: OffsetSizeTrait>(
        offsets: &[O],
        data: *const u8,
        pointers: bool,
        rows: Rows<'_>,
        out: &mut [u64],
    ) {
        if pointers {
            fill(rows, out, |r| data as u64 + offsets[r].as_usize() as u64);
        } else {
            fill(rows, out, |r| {
                (offsets[r + 1].as_usize() - offsets[r].as_usize()) as u64
            });
        }
    }
    match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            write(a.value_offsets(), a.values().as_ptr(), pointers, rows, out)
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            write(a.value_offsets(), a.values().as_ptr(), pointers, rows, out)
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            write(a.value_offsets(), a.values().as_ptr(), pointers, rows, out)
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            write(a.value_offsets(), a.values().as_ptr(), pointers, rows, out)
        }
        t => {
            return Err(Error::InvalidPlan(format!(
                "unsupported input byte type: {t}"
            )));
        }
    }
    Ok(())
}
