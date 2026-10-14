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
//! An Expand fills windows with the `(enclosing row, table row)` pairs of
//! the rows of its input domain below the limit, and runs its block over
//! each. The block's Import steps copy the enclosing variables it reads,
//! by enclosing row; reductions OR their Booleans into the enclosing window
//! after each window. A raise in the block stops the expansion: the
//! enclosing window then only sees rows before the failing row's enclosing
//! row, like after any raise.
use super::aggregate::Rows;
use super::bits::{self, BitState};
use super::run::Frame;
use super::*;
use crate::kernel::table::Table;

/// The input rows of an expansion that may match, in order, with the range
/// of table rows to compare with each.
#[derive(Default)]
pub(super) struct Probes {
    rows: Vec<u32>,
    lo: Vec<u32>,
    hi: Vec<u32>,
    /// Word keys, or the end of each encoded key in `keys`.
    words: Vec<u64>,
    keys: Vec<u8>,
}

impl Probes {
    fn clear(&mut self) {
        self.rows.clear();
        self.lo.clear();
        self.hi.clear();
        self.words.clear();
        self.keys.clear();
    }

    fn push(&mut self, row: usize, rows: std::ops::Range<u32>, word: u64) {
        self.rows.push(row as u32);
        self.lo.push(rows.start);
        self.hi.push(rows.end);
        self.words.push(word);
    }
}

impl Frame<'_> {
    /// Fills windows with the matches of the input rows, in input row order,
    /// and runs the block over each.
    pub(super) fn expand(&mut self, e: &ExpandStep) -> Result<()> {
        let read = self.tables[e.kind.state().0]
            .as_ref()
            .expect("table states are bound");
        // SAFETY: the state keeps the table alive for the run.
        let table: &Table = unsafe { &*read.table };
        let (n, limit) = (self.n, self.limit);
        for r in &e.reduces {
            for (_, out) in &r.any {
                // SAFETY: Boolean slots hold a window bitmap.
                unsafe { *(self.home[out.0] as *mut [u64; WORDS]) = [0; WORDS] };
            }
        }
        let mut probes = self.spare.probes.pop().unwrap_or_default();
        self.probes(e, table, &mut probes);
        if e.exists {
            self.exists(e, table, &probes);
            self.spare.probes.push(probes);
            return Ok(());
        }
        let rows = self.spare.rows.pop().unwrap_or_default();
        let enclosing = std::mem::replace(&mut self.parents, rows);
        self.lineage.push(enclosing);
        let outer = matches!(e.kind, ExpandStepKind::Lookup { outer: true, .. });
        let words = table.word_index().map(|i| i.words);
        let product = matches!(e.kind, ExpandStepKind::Product { .. });
        let null_row = table.rows() as u64;
        let mut failed = None;
        let steps = self.steps;
        // The probe being scanned, the next table row to compare and
        // whether the probe matched yet.
        let (mut c, mut at, mut matched) = (0, probes.lo.first().copied().unwrap_or(0), false);
        let mut key_start = 0;
        loop {
            let ids = self.home[e.row.0] as *mut u64;
            let mut count = 0;
            self.parents.clear();
            while count < WINDOW && c < probes.rows.len() {
                let (row, hi) = (probes.rows[c], probes.hi[c]);
                let emit = |t: u32, count: &mut usize, parents: &mut Vec<u32>| {
                    parents.push(row);
                    // SAFETY: row slots hold a window of words.
                    unsafe { *ids.add(*count) = t as u64 };
                    *count += 1;
                };
                if product {
                    while at < hi && count < WINDOW {
                        emit(at, &mut count, &mut self.parents);
                        at += 1;
                    }
                    matched |= at > probes.lo[c];
                } else if let Some(words) = words {
                    let key = probes.words[c];
                    while at < hi && count < WINDOW {
                        if words[at as usize] == key {
                            emit(at, &mut count, &mut self.parents);
                            matched = true;
                        }
                        at += 1;
                    }
                } else {
                    let key = &probes.keys[key_start..probes.words[c] as usize];
                    while at < hi && count < WINDOW {
                        if table.equal(at, key) {
                            emit(at, &mut count, &mut self.parents);
                            matched = true;
                        }
                        at += 1;
                    }
                }
                if at < hi {
                    break;
                }
                if outer && !matched {
                    if count == WINDOW {
                        break;
                    }
                    emit(null_row as u32, &mut count, &mut self.parents);
                }
                if !product && words.is_none() {
                    key_start = probes.words[c] as usize;
                }
                c += 1;
                at = probes.lo.get(c).copied().unwrap_or(0);
                matched = false;
            }
            if count == 0 {
                break;
            }
            self.n = count;
            self.limit = count;
            self.window(e.block);
            self.ptrs[e.row.0] = self.home[e.row.0];
            for step in &steps.blocks[e.block].steps {
                self.step(step)?;
            }
            self.reduce(e);
            if self.limit < self.n {
                failed = Some(self.parents[self.limit] as usize);
                break;
            }
        }
        self.n = n;
        self.limit = failed.map_or(limit, |p| p.min(limit));
        let enclosing = self.lineage.pop().unwrap();
        let rows = std::mem::replace(&mut self.parents, enclosing);
        self.spare.rows.push(rows);
        self.spare.probes.push(probes);
        for r in &e.reduces {
            for (_, out) in &r.any {
                self.ptrs[out.0] = self.home[out.0];
                self.settle(*out);
            }
            self.masks[r.domain.0] = self.masks[e.input.0];
        }
        Ok(())
    }

    /// The input rows below the limit that may match and their buckets;
    /// with `outer`, every input row.
    fn probes(&mut self, e: &ExpandStep, table: &Table, probes: &mut Probes) {
        probes.clear();
        let Some(rows) = Rows::new(&[self.mask(e.input)], self.limit) else {
            return;
        };
        let (keys, nulls_equal, outer) = match &e.kind {
            ExpandStepKind::Product { .. } => {
                let all = 0..table.rows() as u32;
                rows.each(|r| probes.push(r, all.clone(), 0));
                return;
            }
            ExpandStepKind::Lookup {
                keys,
                nulls_equal,
                outer,
                ..
            } => (keys, *nulls_equal, *outer),
        };
        if let Some(index) = table.word_index() {
            let k = &keys[0];
            let mut words = [0u64; WINDOW];
            self.load_words(k.value().unwrap(), &mut words[..self.n]);
            let valid = k
                .validity()
                .filter(|v| self.states[v.0] != BitState::AllTrue)
                .map(|v| -> [u64; WORDS] { self.words(v).try_into().unwrap() });
            rows.each(|r| {
                let ok = valid.is_none_or(|v| v[r / 64] >> (r % 64) & 1 != 0);
                let range = if ok { index.bucket(words[r]) } else { 0..0 };
                if outer || range.start < range.end {
                    probes.push(r, range, words[r]);
                }
            });
            return;
        }
        let mut cells = self.spare.cells.pop().unwrap_or_default();
        let mut key = self.spare.keys.pop().unwrap_or_default();
        rows.each(|r| {
            cells.clear();
            cells.extend(keys.iter().map(|k| self.cell(k, r)));
            let range = if table.encode(&cells, nulls_equal, &mut key) {
                table.bucket(&key)
            } else {
                0..0
            };
            if outer || range.start < range.end {
                probes.keys.extend_from_slice(&key);
                probes.push(r, range, probes.keys.len() as u64);
            }
        });
        self.spare.cells.push(cells);
        self.spare.keys.push(key);
    }

    /// Sets the reduced Booleans of the input rows with a match: the block
    /// computes nothing, so one match is all an input row needs.
    fn exists(&mut self, e: &ExpandStep, table: &Table, probes: &Probes) {
        let mut found = [0u64; WORDS];
        let words = table.word_index().map(|i| i.words);
        let mut key_start = 0;
        for c in 0..probes.rows.len() {
            let rows = probes.lo[c]..probes.hi[c];
            let hit = match (&e.kind, words) {
                (ExpandStepKind::Product { .. }, _) => !rows.is_empty(),
                (_, Some(words)) => {
                    let key = probes.words[c];
                    words[rows.start as usize..rows.end as usize].contains(&key)
                }
                (_, None) => {
                    let end = probes.words[c] as usize;
                    let key = &probes.keys[key_start..end];
                    key_start = end;
                    rows.clone().any(|t| table.equal(t, key))
                }
            };
            let r = probes.rows[c] as usize;
            found[r / 64] |= (hit as u64) << (r % 64);
        }
        for r in &e.reduces {
            for (_, out) in &r.any {
                // SAFETY: Boolean slots hold a window bitmap.
                unsafe { *(self.home[out.0] as *mut [u64; WORDS]) = found };
                self.ptrs[out.0] = self.home[out.0];
                self.settle(*out);
            }
            self.masks[r.domain.0] = self.masks[e.input.0];
        }
    }

    /// ORs the reduced Booleans of a window into its enclosing rows.
    fn reduce(&mut self, e: &ExpandStep) {
        for r in &e.reduces {
            let (m, s) = self.mask(r.descendant);
            if s == BitState::AllFalse {
                continue;
            }
            let m: [u64; WORDS] = m.try_into().unwrap();
            for &(v, out) in &r.any {
                let values: [u64; WORDS] = self.words(v).try_into().unwrap();
                let vs = self.states[v.0];
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
    }

    /// Copies an enclosing variable to the rows of `domain` in this window.
    pub(super) fn import(&mut self, from: VariableId, to: VariableId, domain: DomainId) {
        let ty = self.steps.vars[to.0].ty;
        let Some(rows) = self.selected(domain) else {
            if ty == Ty::Bool {
                self.set_state(to, BitState::AllFalse);
            }
            return;
        };
        let parents = &self.parents;
        if ty == Ty::Bool {
            match self.states[from.0] {
                s @ (BitState::AllTrue | BitState::AllFalse) => self.set_state(to, s),
                BitState::Mixed => {
                    let src = self.ptrs[from.0] as *const u64;
                    let mut words = [0u64; WORDS];
                    rows.each(|i| {
                        let p = parents[i] as usize;
                        // SAFETY: the source covers a window bitmap.
                        words[i / 64] |= (unsafe { *src.add(p / 64) } >> (p % 64) & 1) << (i % 64);
                    });
                    // SAFETY: Boolean slots hold a window bitmap.
                    unsafe { *(self.home[to.0] as *mut [u64; WORDS]) = words };
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
                1 => copy_rows(src, out, &rows, |i| parents[i] as usize),
                2 => copy_rows(src as *const u16, out as *mut u16, &rows, |i| parents[i] as usize),
                4 => copy_rows(src as *const u32, out as *mut u32, &rows, |i| parents[i] as usize),
                8 => copy_rows(src as *const u64, out as *mut u64, &rows, |i| parents[i] as usize),
                _ => copy_rows(
                    src as *const [u64; 2],
                    out as *mut [u64; 2],
                    &rows,
                    |i| parents[i] as usize,
                ),
            }
        }
        self.ptrs[to.0] = out;
    }

    /// A TableGather of the rows of `domain` in this window. Other rows are
    /// NULL; their byte pointers are valid and their values unspecified.
    pub(super) fn gather(
        &mut self,
        state: StateId,
        row: VariableId,
        column: usize,
        output: &ColumnVariableBinding,
        domain: DomainId,
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
        let selected = self.selected(domain);
        let validity = output.validity().unwrap();
        let mut words = [0u64; WORDS];
        if let Some(sel) = &selected {
            match &input.nulls {
                None => sel.each(|i| words[i / 64] |= ((at(i) < len) as u64) << (i % 64)),
                Some(nulls) => sel.each(|i| {
                    let t = at(i);
                    words[i / 64] |= ((t < len && nulls.is_valid(t)) as u64) << (i % 64);
                }),
            }
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
                let empty = std::ptr::NonNull::<u8>::dangling().as_ptr() as u64;
                if !selected.as_ref().is_some_and(Rows::all) {
                    for i in 0..n {
                        // SAFETY: value slots hold a window of words.
                        unsafe {
                            *p.add(i) = empty;
                            *l.add(i) = 0;
                        }
                    }
                }
                if let Some(sel) = &selected {
                    sel.each(|i| {
                        let t = at(i);
                        let (address, size) = if t < len { input.bytes(t) } else { (empty, 0) };
                        // SAFETY: as above.
                        unsafe {
                            *p.add(i) = address;
                            *l.add(i) = size;
                        }
                    });
                }
                self.ptrs[ptr.0] = self.home[ptr.0];
                self.ptrs[length.0] = self.home[length.0];
            }
            ColumnVariableBinding::Boolean { value, .. } => {
                let bools = input.bools.as_ref().unwrap();
                let mut words = [0u64; WORDS];
                if let Some(sel) = &selected {
                    sel.each(|i| {
                        let t = at(i);
                        words[i / 64] |= ((t < len && bools.value(t)) as u64) << (i % 64);
                    });
                }
                // SAFETY: Boolean slots hold a window bitmap.
                unsafe { *(self.home[value.0] as *mut [u64; WORDS]) = words };
                self.ptrs[value.0] = self.home[value.0];
                self.settle(value);
            }
            _ => {
                let value = output.value().unwrap();
                let out = self.home[value.0];
                if let Some(sel) = &selected {
                    let values = input.values;
                    // SAFETY: the column has `len` values of its width; the
                    // slot covers the window.
                    unsafe {
                        match input.width {
                            1 => gather_values(values, out, sel, at, len),
                            2 => gather_values(values as *const u16, out as *mut u16, sel, at, len),
                            4 => gather_values(values as *const u32, out as *mut u32, sel, at, len),
                            8 => gather_values(values as *const u64, out as *mut u64, sel, at, len),
                            _ => gather_values(
                                values as *const [u64; 2],
                                out as *mut [u64; 2],
                                sel,
                                at,
                                len,
                            ),
                        }
                    }
                }
                self.ptrs[value.0] = out;
            }
        }
    }
}

/// `out[i] = src[row(i)]` for the selected rows `i`.
#[inline(always)]
unsafe fn copy_rows<T: Copy>(src: *const T, out: *mut T, rows: &Rows, row: impl Fn(usize) -> usize) {
    rows.each(|i| {
        // SAFETY: the caller's bounds.
        unsafe { *out.add(i) = *src.add(row(i)) };
    });
}

/// `out[i] = src[at(i)]` for the selected rows `i`, or zero past `len`.
#[inline(always)]
unsafe fn gather_values<T: Copy + Default>(
    src: *const T,
    out: *mut T,
    rows: &Rows,
    at: impl Fn(usize) -> usize,
    len: usize,
) {
    rows.each(|i| {
        let t = at(i);
        // SAFETY: the caller's bounds.
        unsafe { *out.add(i) = if t < len { *src.add(t) } else { T::default() } };
    });
}
