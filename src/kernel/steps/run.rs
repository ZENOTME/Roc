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

//! One execution of the steps over a chunk, window by window.
use super::bits::{self, BitState};
use super::output::Outputs;
use super::*;
use crate::arrow::{
    array::{Array, ArrayRef, AsArray},
    record_batch::RecordBatch,
};
use crate::error::ErrorContext;
use crate::kernel::aggregation::{AccumulatorState, GroupState};
use crate::kernel::runtime::{self, TextNumber};
use crate::kernel::scalar::Cell;
use arrow_buffer::{BooleanBuffer, NullBuffer};
use asyncband::shutdown::ShutdownGuard;

/// The Arrow buffers of one input column; the batch keeps them alive.
pub(super) struct Input {
    values: *const u8,
    width: usize,
    bools: Option<BooleanBuffer>,
    nulls: Option<NullBuffer>,
    /// Byte arrays: i32 or i64 offsets.
    offsets: *const u8,
    large: bool,
}

impl Input {
    fn new(array: &ArrayRef) -> Self {
        use crate::arrow::datatypes::*;
        let mut input = Self {
            values: std::ptr::null(),
            width: 0,
            bools: None,
            nulls: array.nulls().filter(|n| n.null_count() != 0).cloned(),
            offsets: std::ptr::null(),
            large: false,
        };
        macro_rules! fixed {
            ($($pattern:pat => $t:ty),*) => {
                match array.data_type() {
                    $($pattern => {
                        let values = array.as_primitive::<$t>().values();
                        input.values = values.as_ptr().cast();
                        input.width = std::mem::size_of::<<$t as ArrowPrimitiveType>::Native>();
                    })*
                    DataType::Boolean => input.bools = Some(array.as_boolean().values().clone()),
                    DataType::Utf8 => {
                        let a = array.as_string::<i32>();
                        (input.offsets, input.values) = (a.value_offsets().as_ptr().cast(), a.values().as_ptr());
                    }
                    DataType::LargeUtf8 => {
                        let a = array.as_string::<i64>();
                        (input.offsets, input.values) = (a.value_offsets().as_ptr().cast(), a.values().as_ptr());
                        input.large = true;
                    }
                    DataType::Binary => {
                        let a = array.as_binary::<i32>();
                        (input.offsets, input.values) = (a.value_offsets().as_ptr().cast(), a.values().as_ptr());
                    }
                    DataType::LargeBinary => {
                        let a = array.as_binary::<i64>();
                        (input.offsets, input.values) = (a.value_offsets().as_ptr().cast(), a.values().as_ptr());
                        input.large = true;
                    }
                    _ => {}
                }
            };
        }
        fixed!(
            DataType::Int8 => Int8Type,
            DataType::Int16 => Int16Type,
            DataType::Int32 => Int32Type,
            DataType::Int64 => Int64Type,
            DataType::UInt8 => UInt8Type,
            DataType::UInt16 => UInt16Type,
            DataType::UInt32 => UInt32Type,
            DataType::UInt64 => UInt64Type,
            DataType::Float32 => Float32Type,
            DataType::Float64 => Float64Type,
            DataType::Date32 => Date32Type,
            DataType::Date64 => Date64Type,
            DataType::Timestamp(TimeUnit::Second, _) => TimestampSecondType,
            DataType::Timestamp(TimeUnit::Millisecond, _) => TimestampMillisecondType,
            DataType::Timestamp(TimeUnit::Microsecond, _) => TimestampMicrosecondType,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => TimestampNanosecondType
        );
        input
    }

    fn offset(&self, row: usize) -> u64 {
        // SAFETY: rows are below the array length.
        unsafe {
            if self.large {
                *(self.offsets as *const i64).add(row) as u64
            } else {
                *(self.offsets as *const i32).add(row) as u32 as u64
            }
        }
    }
}

/// Per-variable vectors of a run, kept by the `LocalState` across chunks.
#[derive(Default)]
pub(crate) struct Buffers {
    ptrs: Vec<*mut u8>,
    states: Vec<BitState>,
    home: Vec<*mut u8>,
    masks: Vec<(*mut u8, BitState)>,
    mask_home: Vec<*mut u8>,
    args: Vec<*mut u8>,
    groups: Vec<*mut GroupState>,
    accumulators: Vec<*mut AccumulatorState>,
    bounds: Vec<usize>,
}

impl std::fmt::Debug for Buffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Buffers")
    }
}

// SAFETY: the pointers are only dereferenced during a run, by its thread;
// between runs they are stale values that each run overwrites first.
unsafe impl Send for Buffers {}

pub(super) struct Frame<'a> {
    pub steps: &'a Steps,
    /// Window rows and the chunk row of the first.
    pub n: usize,
    pub start: usize,
    /// Effects see only window rows below this: fused code stops at the first
    /// failing row, so once a RaiseIf fails, later steps only see the rows
    /// before it and an error they raise is the one fused code raises.
    pub limit: usize,
    pub raised: Option<Error>,
    pub ptrs: Vec<*mut u8>,
    pub states: Vec<BitState>,
    home: Vec<*mut u8>,
    /// Per domain: the mask of this window.
    pub masks: Vec<(*mut u8, BitState)>,
    mask_home: Vec<*mut u8>,
    args: Vec<*mut u8>,
    /// Owns strings produced by FormatText until the output is copied.
    pub strings: Vec<Box<[u8]>>,
    inputs: Vec<Option<Input>>,
    pub outputs: Option<Outputs>,
    pub groups: Vec<*mut GroupState>,
    pub accumulators: Vec<*mut AccumulatorState>,
    /// Per group ID variable: the group count of its GroupLookup, or 0.
    pub bounds: Vec<usize>,
}

/// The canonical 64-bit form of a lane: signed integers sign-extended, other
/// integers zero-extended, floats as their IEEE bits.
#[inline(always)]
pub(super) unsafe fn load(ptr: *const u8, ty: Ty, row: usize) -> u64 {
    unsafe {
        match ty {
            Ty::Bool => (*(ptr as *const u64).add(row / 64) >> (row % 64)) & 1,
            Ty::I8 => *(ptr as *const i8).add(row) as i64 as u64,
            Ty::I16 => *(ptr as *const i16).add(row) as i64 as u64,
            Ty::I32 => *(ptr as *const i32).add(row) as i64 as u64,
            Ty::U8 => *ptr.add(row) as u64,
            Ty::U16 => *(ptr as *const u16).add(row) as u64,
            Ty::U32 | Ty::F32 => *(ptr as *const u32).add(row) as u64,
            Ty::I64 | Ty::U64 | Ty::F64 => *(ptr as *const u64).add(row),
        }
    }
}

#[inline(always)]
pub(super) unsafe fn store(ptr: *mut u8, ty: Ty, row: usize, bits: u64) {
    unsafe {
        match ty.bytes() {
            1 => *ptr.add(row) = bits as u8,
            2 => *(ptr as *mut u16).add(row) = bits as u16,
            4 => *(ptr as *mut u32).add(row) = bits as u32,
            _ => *(ptr as *mut u64).add(row) = bits,
        }
    }
}

impl Steps {
    pub fn run(
        &self,
        batch: &RecordBatch,
        inputs: &[GraphInput],
        output: Option<&GraphOutput>,
        aliases: &[Option<usize>],
        local: &mut LocalState,
        guard: &ShutdownGuard,
    ) -> Result<Option<RecordBatch>> {
        let rows = batch.num_rows();
        let words = self.value_slots * WINDOW + self.bit_slots * WORDS + 8;
        if local.scratch.len() < words {
            local.scratch.resize(words, 0);
        }
        // Slots start at a 64-byte boundary.
        let base = local.scratch.as_mut_ptr();
        let base = unsafe { base.add(base.align_offset(64)) };
        let bitmaps = unsafe { base.add(self.value_slots * WINDOW) };
        let mut b = std::mem::take(&mut *local.buffers);
        b.home.clear();
        b.home.extend(self.vars.iter().map(|v| match v.place {
            Place::Value(s) | Place::InputBytes { slot: s, .. } => unsafe { base.add(s * WINDOW).cast() },
            Place::Bits(s) | Place::InputBits { slot: s, .. } => unsafe { bitmaps.add(s * WORDS).cast() },
            _ => std::ptr::null_mut(),
        }));
        b.mask_home.clear();
        b.mask_home.extend(
            self.masks
                .iter()
                .map(|m| m.map_or(std::ptr::null_mut(), |s| unsafe { bitmaps.add(s * WORDS).cast() })),
        );
        b.groups.clear();
        b.accumulators.clear();
        let states = self.steps.iter().chain(&self.fallback).flat_map(|step| match step {
            Step::GroupLookup { state, .. } => vec![(*state, true)],
            Step::Aggregates { updates, .. } => updates.iter().map(|u| (u.state, false)).collect(),
            _ => vec![],
        });
        for (i, group) in states {
            if b.groups.len() <= i.0 {
                b.groups.resize(i.0 + 1, std::ptr::null_mut());
                b.accumulators.resize(i.0 + 1, std::ptr::null_mut());
            }
            let s = local.state_mut(i)?;
            if group {
                b.groups[i.0] = s.get_mut::<GroupState>()? as *mut GroupState;
            } else {
                b.accumulators[i.0] = s.get_mut::<AccumulatorState>()? as *mut AccumulatorState;
            }
        }
        b.ptrs.clone_from(&b.home);
        b.states.clear();
        b.states.resize(self.vars.len(), BitState::Mixed);
        b.masks.clear();
        b.masks.resize(self.masks.len(), (ones(), BitState::AllTrue));
        b.bounds.clear();
        b.bounds.resize(self.vars.len(), 0);
        let mut frame = Frame {
            steps: self,
            n: 0,
            start: 0,
            limit: 0,
            raised: None,
            ptrs: std::mem::take(&mut b.ptrs),
            states: std::mem::take(&mut b.states),
            home: std::mem::take(&mut b.home),
            masks: std::mem::take(&mut b.masks),
            mask_home: std::mem::take(&mut b.mask_home),
            args: std::mem::take(&mut b.args),
            strings: vec![],
            inputs: inputs
                .iter()
                .zip(&self.read)
                .map(|(i, read)| read.then(|| Input::new(batch.column(i.position))))
                .collect(),
            outputs: output.map(|o| Outputs::new(o, aliases, &self.vars, rows)),
            groups: std::mem::take(&mut b.groups),
            accumulators: std::mem::take(&mut b.accumulators),
            bounds: std::mem::take(&mut b.bounds),
        };
        for (v, var) in self.vars.iter().enumerate() {
            if let Place::Constant(i) = var.place {
                if var.ty == Ty::Bool {
                    let state = BitState::constant(self.pool[i] != 0);
                    frame.states[v] = state;
                    frame.ptrs[v] = state.bitmap().unwrap();
                } else {
                    frame.ptrs[v] = self.pool[i..].as_ptr() as *mut u8;
                }
            }
        }
        let result = frame.windows(rows, guard);
        let result = match (result, output, frame.outputs.take()) {
            (Err(e), _, _) => Err(e),
            (Ok(()), Some(output), Some(outputs)) => outputs.finish(output, batch, inputs, aliases).map(Some),
            (Ok(()), _, _) => Ok(None),
        };
        *local.buffers = Buffers {
            ptrs: frame.ptrs,
            states: frame.states,
            home: frame.home,
            masks: frame.masks,
            mask_home: frame.mask_home,
            args: frame.args,
            groups: frame.groups,
            accumulators: frame.accumulators,
            bounds: frame.bounds,
        };
        result
    }

}

fn ones() -> *mut u8 {
    bits::ONES.0.as_ptr() as *mut u8
}

impl Frame<'_> {
    fn windows(&mut self, rows: usize, guard: &ShutdownGuard) -> Result<()> {
        while self.start < rows {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.n = WINDOW.min(rows - self.start);
            self.limit = self.n;
            self.window();
            for step in &self.steps.steps {
                self.step(step)?;
            }
            if let Some(e) = self.raised.take() {
                return Err(e);
            }
            self.start += self.n;
        }
        Ok(())
    }

    /// Points the variables at this window's rows.
    fn window(&mut self) {
        let (start, n) = (self.start, self.n);
        for &VariableId(v) in &self.steps.windowed {
            match self.steps.vars[v].place {
                Place::Bits(_) => self.ptrs[v] = self.home[v],
                Place::Input(i) => {
                    let input = self.inputs[i].as_ref().unwrap();
                    // SAFETY: the window rows are within the array.
                    self.ptrs[v] = unsafe { input.values.add(start * input.width) } as *mut u8;
                }
                Place::Output(j) => {
                    self.ptrs[v] = self.outputs.as_mut().unwrap().column_ptr(j, start);
                }
                Place::InputBits { input, validity, .. } => {
                    let input = self.inputs[input].as_ref().unwrap();
                    let source = if validity {
                        input.nulls.as_ref().map(|n| n.inner().clone())
                    } else {
                        input.bools.clone()
                    };
                    let state = match source {
                        None => BitState::AllTrue,
                        Some(buffer) => {
                            let offset = buffer.offset() + start;
                            let bytes = buffer.values();
                            let at = offset / 8;
                            // Steps read whole window bitmaps.
                            let aligned = offset.is_multiple_of(64)
                                && (bytes.as_ptr() as usize + at).is_multiple_of(8)
                                && bytes.len() >= at + WORDS * 8;
                            if aligned {
                                self.ptrs[v] = bytes[at..].as_ptr() as *mut u8;
                            } else {
                                // SAFETY: input slots hold a window bitmap.
                                let slot = unsafe { &mut *(self.home[v] as *mut [u64; WORDS]) };
                                bits::extract(bytes, offset, n, slot);
                                self.ptrs[v] = self.home[v];
                            }
                            // SAFETY: the pointer covers the window's words.
                            let words = unsafe { std::slice::from_raw_parts(self.ptrs[v] as *const u64, bits::words(n)) };
                            bits::state(words, n)
                        }
                    };
                    self.set_state(VariableId(v), state);
                }
                Place::InputBytes { input, len, .. } => {
                    let input = self.inputs[input].as_ref().unwrap();
                    let out = self.home[v] as *mut u64;
                    for r in 0..n {
                        let (a, b) = (input.offset(start + r), input.offset(start + r + 1));
                        let word = if len { b - a } else { input.values as u64 + a };
                        // SAFETY: value slots hold a window of words.
                        unsafe { *out.add(r) = word };
                    }
                    self.ptrs[v] = self.home[v];
                }
                _ => {}
            }
        }
        for (d, m) in self.masks.iter_mut().enumerate() {
            *m = if d == 0 {
                (ones(), BitState::AllTrue)
            } else {
                (self.mask_home[d], BitState::AllFalse)
            };
        }
    }

    /// Records a Boolean's state; constant states read the static bitmaps.
    pub fn set_state(&mut self, v: VariableId, state: BitState) {
        self.states[v.0] = state;
        if let Some(p) = state.bitmap() {
            self.ptrs[v.0] = p;
        }
    }

    pub fn words(&self, v: VariableId) -> &[u64] {
        // SAFETY: Boolean pointers cover a window bitmap.
        unsafe { std::slice::from_raw_parts(self.ptrs[v.0] as *const u64, WORDS) }
    }

    pub fn mask(&self, d: DomainId) -> (&[u64], BitState) {
        let (p, state) = self.masks[d.0];
        // SAFETY: masks cover a window bitmap.
        (unsafe { std::slice::from_raw_parts(p as *const u64, WORDS) }, state)
    }

    /// The canonical bits of `v` at a window row.
    pub fn get(&self, v: VariableId, row: usize) -> u64 {
        let var = &self.steps.vars[v.0];
        let row = if var.scalar() && var.ty != Ty::Bool { 0 } else { row };
        // SAFETY: rows are below the window size.
        unsafe { load(self.ptrs[v.0], var.ty, row) }
    }

    pub fn cell(&self, binding: &ColumnVariableBinding, row: usize) -> Cell {
        let valid = binding.validity().map_or(1, |v| self.get(v, row));
        match *binding {
            ColumnVariableBinding::Null { .. } => Cell::default(),
            ColumnVariableBinding::Bytes { ptr, len, .. } => Cell {
                bits: self.get(ptr, row),
                len: self.get(len, row),
                valid,
            },
            _ => Cell {
                bits: self.get(binding.value().unwrap(), row),
                len: 0,
                valid,
            },
        }
    }

    fn operand(&self, o: Operand) -> *mut u8 {
        match o {
            Operand::Var(v) => self.ptrs[v.0],
            Operand::Pool(i) => self.steps.pool[i..].as_ptr() as *mut u8,
            Operand::Domain(d) => self.masks[d.0].0,
        }
    }

    fn call(&mut self, f: KernelFn, args: &[Operand]) -> u64 {
        let mut buffer = std::mem::take(&mut self.args);
        buffer.clear();
        buffer.extend(args.iter().map(|o| self.operand(*o)));
        // SAFETY: operands point at window columns, bitmaps or scalars of the
        // kernel's argument types; outputs are distinct slots.
        let result = unsafe { f(buffer.as_ptr(), self.n as u64) };
        self.args = buffer;
        result
    }

    pub fn step(&mut self, step: &Step) -> Result<()> {
        match step {
            Step::Kernel { f, args, reports, .. } => {
                let result = self.call(*f, args);
                for (j, v) in reports.iter().enumerate() {
                    self.ptrs[v.0] = self.home[v.0];
                    self.set_state(*v, BitState::reported(result, j));
                }
            }
            Step::Fused {
                f,
                args,
                reports,
                fallback,
            } => {
                // After a raise in this window, the steps see the rows below it.
                let result = (self.limit == self.n).then(|| self.call(*f, args));
                match result {
                    Some(result) if result & ir::RAISED == 0 => {
                        for (j, v) in reports.iter().enumerate() {
                            self.ptrs[v.0] = self.home[v.0];
                            self.set_state(*v, BitState::reported(result, j));
                        }
                    }
                    _ => {
                        for step in &self.steps.fallback[fallback.clone()] {
                            self.step(step)?;
                        }
                    }
                }
            }
            Step::Bits { op, a, b, output } => self.bits(*op, *a, *b, *output),
            Step::SelectBits {
                cond,
                then,
                otherwise,
                output,
            } => match self.states[cond.0] {
                BitState::AllTrue => self.copy(*then, *output),
                BitState::AllFalse => self.copy(*otherwise, *output),
                BitState::Mixed => {
                    let out = self.home[output.0] as *mut u64;
                    for w in 0..bits::words(self.n) {
                        let c = self.words(*cond)[w];
                        let word = (c & self.words(*then)[w]) | (!c & self.words(*otherwise)[w]);
                        // SAFETY: Boolean slots hold a window bitmap.
                        unsafe { *out.add(w) = word };
                    }
                    self.ptrs[output.0] = self.home[output.0];
                    self.settle(*output);
                }
            },
            Step::Text { text, domain } => self.text(text, *domain),
            Step::Filter {
                predicate,
                parent,
                domain,
            } => {
                let (parent_bits, parent_state) = self.mask(*parent);
                let parent_ptr = parent_bits.as_ptr();
                let state = match (parent_state, self.states[predicate.0]) {
                    (BitState::AllFalse, _) | (_, BitState::AllFalse) => BitState::AllFalse,
                    (BitState::AllTrue, BitState::AllTrue) => BitState::AllTrue,
                    (BitState::AllTrue, _) => {
                        let src = self.ptrs[predicate.0] as *const u64;
                        // SAFETY: both cover a window bitmap.
                        unsafe { std::ptr::copy_nonoverlapping(src, self.mask_home[domain.0] as *mut u64, WORDS) };
                        BitState::Mixed
                    }
                    (_, BitState::AllTrue) => {
                        // SAFETY: both cover a window bitmap.
                        unsafe { std::ptr::copy_nonoverlapping(parent_ptr, self.mask_home[domain.0] as *mut u64, WORDS) };
                        BitState::Mixed
                    }
                    _ => {
                        let out = self.mask_home[domain.0] as *mut u64;
                        for w in 0..bits::words(self.n) {
                            // SAFETY: as above.
                            unsafe { *out.add(w) = *parent_ptr.add(w) & self.words(*predicate)[w] };
                        }
                        // SAFETY: the mask covers a window bitmap.
                        bits::state(unsafe { std::slice::from_raw_parts(out, WORDS) }, self.n)
                    }
                };
                self.masks[domain.0] = match state {
                    BitState::Mixed => (self.mask_home[domain.0], state),
                    s => (s.bitmap().unwrap(), s),
                };
            }
            Step::RaiseIf {
                cond,
                error,
                args,
                domain,
            } => {
                let (mask, state) = self.mask(*domain);
                if self.states[cond.0] == BitState::AllFalse || state == BitState::AllFalse || self.limit == 0 {
                    return Ok(());
                }
                if let Some(r) = bits::first(&[self.words(*cond), mask], self.limit) {
                    let values = args.iter().map(|v| self.get(*v, r)).collect::<Vec<_>>();
                    // The row is relative to the chunk passed to Program::run.
                    self.raised = Some(error.to_error(&values).with_context(
                        ErrorContext::new("kernel.raise").field("row", self.start + r),
                    ));
                    self.limit = r;
                }
            }
            Step::GroupLookup {
                state,
                keys,
                output,
                domain,
            } => self.group_lookup(*state, keys, *output, *domain)?,
            Step::Aggregates {
                group,
                domain,
                validity,
                updates,
            } => self.aggregates(*group, *domain, *validity, updates)?,
            Step::Output { domain } => {
                if self.raised.is_none() {
                    let mut outputs = self.outputs.take().unwrap();
                    outputs.append(self, *domain);
                    self.outputs = Some(outputs);
                }
            }
        }
        Ok(())
    }

    /// Computes the state of a Boolean written to its slot.
    fn settle(&mut self, v: VariableId) {
        let state = bits::state(self.words(v), self.n);
        self.set_state(v, state);
        if state == BitState::Mixed {
            self.ptrs[v.0] = self.home[v.0];
        }
    }

    fn copy(&mut self, from: VariableId, to: VariableId) {
        match self.states[from.0] {
            BitState::Mixed => {
                // SAFETY: Boolean slots and pointers cover a window bitmap.
                unsafe {
                    std::ptr::copy_nonoverlapping(self.ptrs[from.0] as *const u64, self.home[to.0] as *mut u64, WORDS)
                };
                self.ptrs[to.0] = self.home[to.0];
                self.states[to.0] = BitState::Mixed;
            }
            s => self.set_state(to, s),
        }
    }

    fn bits(&mut self, op: BitOp, a: VariableId, b: VariableId, output: VariableId) {
        use BitState::*;
        let (sa, sb) = (self.states[a.0], self.states[b.0]);
        match op {
            BitOp::Not => match sa {
                AllTrue => return self.set_state(output, AllFalse),
                AllFalse => return self.set_state(output, AllTrue),
                Mixed => {}
            },
            BitOp::And => match (sa, sb) {
                (AllFalse, _) | (_, AllFalse) => return self.set_state(output, AllFalse),
                (AllTrue, _) => return self.copy(b, output),
                (_, AllTrue) => return self.copy(a, output),
                _ => {}
            },
            BitOp::Or => match (sa, sb) {
                (AllTrue, _) | (_, AllTrue) => return self.set_state(output, AllTrue),
                (AllFalse, _) => return self.copy(b, output),
                (_, AllFalse) => return self.copy(a, output),
                _ => {}
            },
        }
        let out = self.home[output.0] as *mut u64;
        for w in 0..bits::words(self.n) {
            let (x, y) = (self.words(a)[w], self.words(b)[w]);
            let word = match op {
                BitOp::And => x & y,
                BitOp::Or => x | y,
                BitOp::Not => !x,
            };
            // SAFETY: Boolean slots hold a window bitmap.
            unsafe { *out.add(w) = word };
        }
        self.ptrs[output.0] = self.home[output.0];
        self.settle(output);
    }

    fn text(&mut self, text: &Text, domain: DomainId) {
        let n = self.n;
        let flags = |frame: &mut Self, v: VariableId, f: &dyn Fn(&Self, usize) -> bool| {
            let out = frame.home[v.0] as *mut u64;
            for w in 0..bits::words(n) {
                let mut word = 0u64;
                for i in 0..64.min(n - w * 64) {
                    word |= (f(frame, w * 64 + i) as u64) << i;
                }
                // SAFETY: Boolean slots hold a window bitmap.
                unsafe { *out.add(w) = word };
            }
            frame.ptrs[v.0] = frame.home[v.0];
            frame.settle(v);
        };
        match *text {
            Text::Compare { pred, inputs, output } => flags(self, output, &|f, r| {
                let o = runtime::compare_bytes(
                    f.get(inputs[0], r),
                    f.get(inputs[1], r),
                    f.get(inputs[2], r),
                    f.get(inputs[3], r),
                )
                .cmp(&0);
                match pred {
                    Pred::Eq => o.is_eq(),
                    Pred::Ne => o.is_ne(),
                    Pred::Lt => o.is_lt(),
                    Pred::Le => o.is_le(),
                    Pred::Gt => o.is_gt(),
                    Pred::Ge => o.is_ge(),
                }
            }),
            Text::Validate { ptr, len, invalid } => flags(self, invalid, &|f, r| {
                runtime::validate_utf8(f.get(ptr, r), f.get(len, r)) != 0
            }),
            Text::Parse {
                to,
                ptr,
                len,
                value,
                invalid,
            } => {
                let ty = self.steps.vars[value.0].ty;
                let out = self.ptrs[value.0];
                let parse = parser(to);
                let mut bad = [0u64; WORDS];
                let mut parsed = [0u64; WORDS];
                for r in 0..n {
                    let mut bits = 0u64;
                    // SAFETY: byte pointers are valid on every row.
                    let failed = unsafe { parse(self.get(ptr, r), self.get(len, r), &mut bits) };
                    bad[r / 64] |= (failed as u64) << (r % 64);
                    if ty == Ty::Bool {
                        parsed[r / 64] |= (bits & 1) << (r % 64);
                    } else {
                        // SAFETY: the value column covers the window.
                        unsafe { store(out, ty, r, bits) };
                    }
                }
                for (v, words) in [(invalid, bad), (value, parsed)] {
                    if self.steps.vars[v.0].ty == Ty::Bool {
                        // SAFETY: Boolean slots hold a window bitmap.
                        unsafe { *(self.home[v.0] as *mut [u64; WORDS]) = words };
                        self.ptrs[v.0] = self.home[v.0];
                        self.settle(v);
                    }
                }
            }
            Text::Format { input, ptr, len } => {
                let format = formatter(self.steps.vars[input.0].vt);
                let (mask, state) = self.mask(domain);
                let mask = mask.to_vec();
                let (p, l) = (self.ptrs[ptr.0] as *mut u64, self.ptrs[len.0] as *mut u64);
                for r in 0..n {
                    let selected = state == BitState::AllTrue || (state == BitState::Mixed && mask[r / 64] >> (r % 64) & 1 != 0);
                    let (address, length) = if selected {
                        let text = format(self.get(input, r)).into_bytes().into_boxed_slice();
                        let word = (text.as_ptr() as u64, text.len() as u64);
                        self.strings.push(text);
                        word
                    } else {
                        (std::ptr::NonNull::<u8>::dangling().as_ptr() as u64, 0)
                    };
                    // SAFETY: value slots hold a window of words.
                    unsafe {
                        *p.add(r) = address;
                        *l.add(r) = length;
                    }
                }
            }
        }
    }
}

type Parser = unsafe extern "C" fn(u64, u64, *mut u64) -> u8;

fn parser(to: VariableType) -> Parser {
    use VariableType as T;
    match to {
        T::Boolean => runtime::parse_text::<bool>,
        T::Int8 => runtime::parse_text::<i8>,
        T::Int16 => runtime::parse_text::<i16>,
        T::Int32 => runtime::parse_text::<i32>,
        T::Int64 => runtime::parse_text::<i64>,
        T::UInt8 => runtime::parse_text::<u8>,
        T::UInt16 => runtime::parse_text::<u16>,
        T::UInt32 => runtime::parse_text::<u32>,
        T::UInt64 => runtime::parse_text::<u64>,
        T::Float32 => runtime::parse_text::<f32>,
        _ => runtime::parse_text::<f64>,
    }
}

fn formatter(from: VariableType) -> fn(u64) -> String {
    fn format<T: TextNumber>(bits: u64) -> String {
        T::from_bits(bits).to_string()
    }
    use VariableType as T;
    match from {
        T::Boolean => format::<bool>,
        T::Int8 => format::<i8>,
        T::Int16 => format::<i16>,
        T::Int32 => format::<i32>,
        T::Int64 => format::<i64>,
        T::UInt8 => format::<u8>,
        T::UInt16 => format::<u16>,
        T::UInt32 => format::<u32>,
        T::UInt64 => format::<u64>,
        T::Float32 => format::<f32>,
        _ => format::<f64>,
    }
}
