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

//! Expansions and table gathers.
//!
//! An Expand lists the `(enclosing row, table row)` pairs of the rows of its
//! input domain below the limit, then runs its block over windows of those
//! pairs. A window first copies the enclosing variables the block reads,
//! gathered by enclosing row; reductions OR their Booleans into the
//! enclosing window after each window. A raise in the block stops the
//! expansion: the enclosing window then only sees rows before the failing
//! row's enclosing row, like after any raise.
use super::bits::{self, BitState};
use super::run::Frame;
use super::*;
use crate::kernel::table::{Probe, Table};

impl Frame<'_> {
    pub(super) fn expand(&mut self, e: &ExpandStep) -> Result<()> {
        let read = self.tables[e.kind.state().0]
            .as_ref()
            .expect("table states are bound");
        // SAFETY: the state keeps the table alive for the run.
        let table: &Table = unsafe { &*read.table };
        let mut pairs = std::mem::take(&mut self.pairs);
        pairs.clear();
        let (mask, state) = self.mask(e.input);
        let mask: [u64; WORDS] = mask.try_into().unwrap();
        if state != BitState::AllFalse {
            let limit = self.limit;
            let mut key = std::mem::take(&mut self.key);
            let mut cells = vec![];
            for w in 0..bits::words(limit) {
                let mut m = if state == BitState::AllTrue {
                    u64::MAX
                } else {
                    mask[w]
                } & bits::tail_mask(w, limit);
                while m != 0 {
                    let r = w * 64 + m.trailing_zeros() as usize;
                    m &= m - 1;
                    let before = pairs.len();
                    match &e.kind {
                        ExpandStepKind::Lookup {
                            keys,
                            nulls_equal,
                            outer,
                            ..
                        } => {
                            if table.word_keys() {
                                let k = &keys[0];
                                if k.validity().is_none_or(|v| self.get(v, r) != 0) {
                                    let word = self.get(k.value().unwrap(), r);
                                    table.find(Probe::Word(word), |t| pairs.push((r as u32, t)));
                                }
                            } else {
                                cells.clear();
                                cells.extend(keys.iter().map(|k| self.cell(k, r)));
                                if table.encode(&cells, *nulls_equal, &mut key) {
                                    table.find(Probe::Encoded(&key), |t| pairs.push((r as u32, t)));
                                }
                            }
                            if *outer && pairs.len() == before {
                                pairs.push((r as u32, table.rows() as u32));
                            }
                        }
                        ExpandStepKind::Product { .. } => {
                            pairs.extend((0..table.rows() as u32).map(|t| (r as u32, t)));
                        }
                    }
                }
            }
            self.key = key;
        }
        let (n, limit) = (self.n, self.limit);
        let parents = std::mem::take(&mut self.parents);
        let roots = std::mem::take(&mut self.roots);
        for r in &e.reduces {
            for (_, out) in &r.any {
                // SAFETY: Boolean slots hold a window bitmap.
                unsafe { *(self.home[out.0] as *mut [u64; WORDS]) = [0; WORDS] };
            }
        }
        let mut failed = None;
        let steps = self.steps;
        for chunk in pairs.chunks(WINDOW) {
            self.n = chunk.len();
            self.limit = self.n;
            self.parents.clear();
            self.parents.extend(chunk.iter().map(|p| p.0));
            self.roots.clear();
            if roots.is_empty() {
                self.roots.extend_from_slice(&self.parents);
            } else {
                self.roots
                    .extend(self.parents.iter().map(|&p| roots[p as usize]));
            }
            self.window(e.block);
            let rows = self.home[e.row.0] as *mut u64;
            for (i, (_, t)) in chunk.iter().enumerate() {
                // SAFETY: value slots hold a window of words.
                unsafe { *rows.add(i) = *t as u64 };
            }
            self.ptrs[e.row.0] = self.home[e.row.0];
            for &(from, to) in &e.imports {
                self.import(from, to);
            }
            for step in &steps.blocks[e.block].steps {
                self.step(step)?;
            }
            for r in &e.reduces {
                let (m, s) = self.mask(r.descendant);
                if s == BitState::AllFalse {
                    continue;
                }
                let m: [u64; WORDS] = m.try_into().unwrap();
                for &(v, out) in &r.any {
                    let (values, vs) = (self.words(v).to_vec(), self.states[v.0]);
                    if vs == BitState::AllFalse {
                        continue;
                    }
                    let target = self.home[out.0] as *mut u64;
                    for w in 0..bits::words(self.limit) {
                        let mut hit = bits::tail_mask(w, self.limit);
                        if s != BitState::AllTrue {
                            hit &= m[w];
                        }
                        if vs != BitState::AllTrue {
                            hit &= values[w];
                        }
                        while hit != 0 {
                            let p = self.parents[w * 64 + hit.trailing_zeros() as usize] as usize;
                            hit &= hit - 1;
                            // SAFETY: enclosing rows are below the window size.
                            unsafe { *target.add(p / 64) |= 1 << (p % 64) };
                        }
                    }
                }
            }
            if self.limit < self.n {
                failed = Some(self.parents[self.limit] as usize);
                break;
            }
        }
        self.n = n;
        self.limit = failed.map_or(limit, |p| p.min(limit));
        self.parents = parents;
        self.roots = roots;
        self.pairs = pairs;
        for r in &e.reduces {
            for (_, out) in &r.any {
                self.ptrs[out.0] = self.home[out.0];
                self.settle(*out);
            }
            self.masks[r.domain.0] = self.masks[e.input.0];
        }
        Ok(())
    }

    /// Copies an enclosing variable to the rows of this window.
    fn import(&mut self, from: VariableId, to: VariableId) {
        let n = self.n;
        let ty = self.steps.vars[to.0].ty;
        let parents = &self.parents;
        if ty == Ty::Bool {
            match self.states[from.0] {
                s @ (BitState::AllTrue | BitState::AllFalse) => self.set_state(to, s),
                BitState::Mixed => {
                    let src = self.ptrs[from.0] as *const u64;
                    let out = self.home[to.0] as *mut u64;
                    for w in 0..bits::words(n) {
                        let mut word = 0u64;
                        for i in 0..64.min(n - w * 64) {
                            let p = parents[w * 64 + i] as usize;
                            // SAFETY: both cover a window bitmap.
                            word |= (unsafe { *src.add(p / 64) } >> (p % 64) & 1) << i;
                        }
                        // SAFETY: as above.
                        unsafe { *out.add(w) = word };
                    }
                    self.ptrs[to.0] = self.home[to.0];
                    self.settle(to);
                }
            }
            return;
        }
        let (src, out) = (self.ptrs[from.0], self.home[to.0]);
        // SAFETY: values cover the enclosing window; the slot covers this one.
        unsafe {
            match ty.bytes() {
                1 => gather_rows(src, out, parents),
                2 => gather_rows(src as *const u16, out as *mut u16, parents),
                4 => gather_rows(src as *const u32, out as *mut u32, parents),
                8 => gather_rows(src as *const u64, out as *mut u64, parents),
                _ => gather_rows(src as *const [u64; 2], out as *mut [u64; 2], parents),
            }
        }
        self.ptrs[to.0] = out;
    }

    /// A TableGather of the rows of this window.
    pub(super) fn gather(
        &mut self,
        state: StateId,
        row: VariableId,
        column: usize,
        output: &ColumnVariableBinding,
    ) {
        let n = self.n;
        let read = self.tables[state.0]
            .as_ref()
            .expect("table states are bound");
        // SAFETY: the state keeps the table alive for the run, and readers
        // are not changed during it.
        let table: &Table = unsafe { &*read.table };
        let input = unsafe { &*(&read.columns[column] as *const super::run::Input) };
        let len = table.rows();
        let rows = self.ptrs[row.0] as *const u64;
        // SAFETY: row slots hold a window of words.
        let at = |i: usize| unsafe { *rows.add(i) } as usize;
        let valid = |t: usize| t < len && input.nulls.as_ref().is_none_or(|n| n.is_valid(t));
        let validity = output.validity().unwrap();
        let mut words = [0u64; WORDS];
        for i in 0..n {
            words[i / 64] |= (valid(at(i)) as u64) << (i % 64);
        }
        // SAFETY: Boolean slots hold a window bitmap.
        unsafe { *(self.home[validity.0] as *mut [u64; WORDS]) = words };
        self.ptrs[validity.0] = self.home[validity.0];
        self.settle(validity);
        match *output {
            ColumnVariableBinding::Bytes {
                ptr, len: length, ..
            } => {
                let (p, l) = (
                    self.home[ptr.0] as *mut u64,
                    self.home[length.0] as *mut u64,
                );
                for i in 0..n {
                    let t = at(i);
                    let (address, size) = if t < len {
                        input.bytes(t)
                    } else {
                        (std::ptr::NonNull::<u8>::dangling().as_ptr() as u64, 0)
                    };
                    // SAFETY: value slots hold a window of words.
                    unsafe {
                        *p.add(i) = address;
                        *l.add(i) = size;
                    }
                }
                self.ptrs[ptr.0] = self.home[ptr.0];
                self.ptrs[length.0] = self.home[length.0];
            }
            ColumnVariableBinding::Boolean { value, .. } => {
                let bools = input.bools.as_ref().unwrap();
                let mut words = [0u64; WORDS];
                for i in 0..n {
                    let t = at(i);
                    words[i / 64] |= ((t < len && bools.value(t)) as u64) << (i % 64);
                }
                // SAFETY: Boolean slots hold a window bitmap.
                unsafe { *(self.home[value.0] as *mut [u64; WORDS]) = words };
                self.ptrs[value.0] = self.home[value.0];
                self.settle(value);
            }
            _ => {
                let value = output.value().unwrap();
                let w = input.width;
                let out = self.home[value.0];
                for i in 0..n {
                    let t = at(i);
                    // SAFETY: the column has `len` values of `w` bytes; the
                    // slot covers the window.
                    unsafe {
                        if t < len {
                            std::ptr::copy_nonoverlapping(
                                input.values.add(t * w),
                                out.add(i * w),
                                w,
                            );
                        } else {
                            std::ptr::write_bytes(out.add(i * w), 0, w);
                        }
                    }
                }
                self.ptrs[value.0] = out;
            }
        }
    }
}

/// `out[i] = src[parents[i]]`.
unsafe fn gather_rows<T: Copy>(src: *const T, out: *mut T, parents: &[u32]) {
    for (i, &p) in parents.iter().enumerate() {
        // SAFETY: the caller's bounds.
        unsafe { *out.add(i) = *src.add(p as usize) };
    }
}
