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

//! GroupLookup and AggregateUpdate over the rows of a domain below the limit.
use super::bits::{self, BitState};
use super::run::Frame;
use super::*;
use crate::error::ErrorKind;
use crate::expr::agg::accumulator::{self as agg, Accumulator, RowKernel};
use crate::kernel::scalar::Cell;
use std::sync::LazyLock;

/// The masked and the unmasked integer sum.
static SUMS: LazyLock<[KernelFn; 2]> = LazyLock::new(|| {
    [0, 1].map(|scalars| {
        library::kernel(&Key {
            kind: Kind::Sum,
            ty: Ty::I64,
            scalars,
        })
        .expect("the library has the sum kernels")
    })
});

#[track_caller]
fn overflow() -> Error {
    Error::new(
        ErrorKind::ArithmeticOverflow,
        "aggregate arithmetic overflow",
    )
}

/// The rows below `n` set in every bitmap: the domain mask, validity.
pub(super) struct Rows {
    set: Set,
    n: usize,
}

enum Set {
    All,
    /// One bitmap, read in place.
    Single(*const u64),
    /// The AND of two or more bitmaps.
    Own([u64; WORDS]),
}

impl Rows {
    pub(super) fn new(masks: &[(&[u64], BitState)], n: usize) -> Option<Self> {
        if n == 0 || masks.iter().any(|(_, s)| *s == BitState::AllFalse) {
            return None;
        }
        let mut mixed = masks
            .iter()
            .filter(|(_, s)| *s == BitState::Mixed)
            .map(|(m, _)| *m);
        let set = match (mixed.next(), mixed.next()) {
            (None, _) => Set::All,
            (Some(m), None) => Set::Single(m.as_ptr()),
            (Some(a), Some(b)) => {
                let mut own = [0; WORDS];
                for w in 0..bits::words(n) {
                    own[w] = a[w] & b[w];
                }
                for m in mixed {
                    for (o, w) in own.iter_mut().zip(m).take(bits::words(n)) {
                        *o &= w;
                    }
                }
                Set::Own(own)
            }
        };
        Some(Self { set, n })
    }

    pub(super) fn all(&self) -> bool {
        matches!(self.set, Set::All)
    }

    /// The bitmap of the rows; bits at and beyond `n` are unspecified.
    fn bitmap(&self) -> &[u64] {
        match &self.set {
            // SAFETY: masks cover a window bitmap.
            Set::Single(p) => unsafe { std::slice::from_raw_parts(*p, WORDS) },
            Set::All => &bits::ONES.0,
            Set::Own(own) => own,
        }
    }

    /// The word `w` of the rows, without bits at and beyond `n`.
    #[inline(always)]
    fn word(&self, w: usize) -> u64 {
        self.bitmap()[w] & bits::tail_mask(w, self.n)
    }

    fn count(&self) -> usize {
        if self.all() {
            return self.n;
        }
        let words = bits::words(self.n);
        let bitmap = &self.bitmap()[..words];
        let full = bitmap[..words - 1]
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum::<usize>();
        full + (bitmap[words - 1] & bits::tail_mask(words - 1, self.n)).count_ones() as usize
    }

    #[inline(always)]
    pub(super) fn each(&self, mut f: impl FnMut(usize)) {
        if self.all() {
            (0..self.n).for_each(f);
            return;
        }
        for w in 0..bits::words(self.n) {
            let word = self.word(w);
            if word == u64::MAX {
                (w * 64..w * 64 + 64).for_each(&mut f);
            } else {
                let mut m = word;
                while m != 0 {
                    f(w * 64 + m.trailing_zeros() as usize);
                    m &= m - 1;
                }
            }
        }
    }
}

impl Rows {
    /// Calls `f(row, 1)` for each row; dense words call `f(row, 0)` for
    /// the rows they leave out instead of branching on every bit. Values of
    /// left-out rows are not defined, so `f` may only read them masked.
    #[inline(always)]
    fn each_weighted(&self, mut f: impl FnMut(usize, u64)) {
        if self.all() {
            (0..self.n).for_each(|r| f(r, 1));
            return;
        }
        for w in 0..bits::words(self.n) {
            let word = self.word(w);
            let base = w * 64;
            if word.count_ones() >= 16 {
                for i in 0..64.min(self.n - base) {
                    f(base + i, word >> i & 1);
                }
            } else {
                let mut m = word;
                while m != 0 {
                    f(base + m.trailing_zeros() as usize, 1);
                    m &= m - 1;
                }
            }
        }
    }
}

type UpdateFn = fn(&mut Accumulator, usize, &[Cell]) -> Result<()>;

fn row_update(kind: AggregateUpdateKind) -> UpdateFn {
    use AggregateUpdateKind as K;
    match kind {
        K::CountRows => agg::CountRows::update,
        K::CountValues => agg::CountValues::update,
        K::CountDistinct => agg::CountDistinct::update,
        K::SumI64 | K::SumU64 => agg::SumInteger::update,
        K::SumF64 => agg::SumFloat::update,
        K::Avg => agg::Average::update,
        K::SumI128 => agg::SumWide::update,
        K::AvgI128 => agg::AverageWide::update,
        K::Variance => agg::VarianceUpdate::update,
        K::CovarPop => agg::CovarianceUpdate::update,
        K::Min => agg::Minimum::update,
        K::Max => agg::Maximum::update,
    }
}

/// A key lane read as its canonical word.
trait Word: Copy {
    fn word(self) -> u64;
}
macro_rules! words {
    ($($t:ty: $wide:ty),*) => {$(
        impl Word for $t {
            #[inline(always)]
            fn word(self) -> u64 {
                self as $wide as u64
            }
        }
    )*};
}
words!(i8: i64, i16: i64, i32: i64, i64: i64, u8: u64, u16: u64, u32: u64, u64: u64);

/// The groups of word keys.
fn lookup_words<T: Word>(
    index: &mut crate::exec::aggregate::group_index::GroupIndex,
    rows: &Rows,
    keys: *const T,
    valid: Option<&[u64]>,
    ids: *mut u64,
) {
    rows.each(|r| {
        // SAFETY: keys and ids cover the window.
        let bits = unsafe { *keys.add(r) }.word();
        let v = valid.is_none_or(|w| w[r / 64] >> (r % 64) & 1 != 0);
        unsafe { *ids.add(r) = index.intern_word(bits, v) as u64 };
    });
}

impl Frame<'_> {
    /// The window rows of a domain, below the limit or not.
    pub(super) fn selected(&self, domain: DomainId) -> Option<Rows> {
        Rows::new(&[self.mask(domain)], self.n)
    }

    fn rows(&self, domain: DomainId, validity: Option<VariableId>) -> Option<Rows> {
        let mask = self.mask(domain);
        match validity {
            Some(v) => Rows::new(&[mask, (self.words(v), self.states[v.0])], self.limit),
            None => Rows::new(&[mask], self.limit),
        }
    }

    pub(super) fn group_lookup(
        &mut self,
        state: StateId,
        keys: &[ColumnVariableBinding],
        output: VariableId,
        domain: DomainId,
    ) -> Result<()> {
        let ids = self.ptrs[output.0] as *mut u64;
        let rows = self.rows(domain, None);
        if !rows.as_ref().is_some_and(Rows::all) {
            // Rows outside the domain get group 0, so updates may read the
            // ID of every window row; the slot may hold another variable's.
            // SAFETY: ID slots hold a window of words.
            unsafe { std::ptr::write_bytes(ids, 0, self.n) };
        }
        let Some(rows) = rows else {
            return Ok(());
        };
        // SAFETY: the state is exclusively borrowed for the run.
        let index = unsafe { &mut (*self.groups[state.0]).index };
        if let ([key], true) = (keys, index.word_keys())
            && let Some(value) = key.value()
            && !self.steps.vars[value.0].scalar()
            && self.steps.vars[value.0].ty != Ty::Bool
        {
            let valid = key
                .validity()
                .filter(|v| self.states[v.0] != BitState::AllTrue)
                .map(|v| self.words(v));
            let keys = self.ptrs[value.0];
            match self.steps.vars[value.0].ty {
                Ty::I8 => lookup_words(index, &rows, keys as *const i8, valid, ids),
                Ty::I16 => lookup_words(index, &rows, keys as *const i16, valid, ids),
                Ty::I32 => lookup_words(index, &rows, keys as *const i32, valid, ids),
                Ty::U8 => lookup_words(index, &rows, keys, valid, ids),
                Ty::U16 => lookup_words(index, &rows, keys as *const u16, valid, ids),
                Ty::U32 => lookup_words(index, &rows, keys as *const u32, valid, ids),
                Ty::I64 => lookup_words(index, &rows, keys as *const i64, valid, ids),
                _ => lookup_words(index, &rows, keys as *const u64, valid, ids),
            }
            self.bounds[output.0] = index.len();
            return Ok(());
        }
        if let Some(width) = index.fixed_width() {
            // The key words of every window row, filled column by column.
            let n = self.n;
            let mut words = self.spare.words.pop().unwrap_or_default();
            words.clear();
            words.resize(n * width, 0);
            let mut column = [0u64; WINDOW];
            let mut at = 0;
            for (i, k) in keys.iter().enumerate() {
                let valid = k
                    .validity()
                    .filter(|v| self.states[v.0] != BitState::AllTrue)
                    .map(|v| -> [u64; WORDS] { self.words(v).try_into().unwrap() });
                let is_valid = |r: usize| valid.is_none_or(|v| v[r / 64] >> (r % 64) & 1 != 0);
                let value = k.value().unwrap();
                let wide = matches!(k, ColumnVariableBinding::Int128 { .. });
                if wide {
                    for r in 0..n {
                        let [lo, hi] = self.get_wide(value, r);
                        let ok = is_valid(r);
                        words[r * width + at] = if ok { lo } else { 0 };
                        words[r * width + at + 1] = if ok { hi } else { 0 };
                    }
                } else {
                    self.load_words(value, &mut column[..n]);
                    for (r, &w) in column[..n].iter().enumerate() {
                        words[r * width + at] = if is_valid(r) { w } else { 0 };
                    }
                }
                for r in 0..n {
                    words[r * width + width - 1] |= (is_valid(r) as u64) << i;
                }
                at += 1 + wide as usize;
            }
            rows.each(|r| {
                // SAFETY: ids cover the window.
                unsafe {
                    *ids.add(r) = index.intern_fixed(&words[r * width..(r + 1) * width]) as u64
                };
            });
            self.spare.words.push(words);
            self.bounds[output.0] = index.len();
            return Ok(());
        }
        let mut cells = self.spare.cells.pop().unwrap_or_default();
        rows.each(|r| {
            cells.clear();
            cells.extend(keys.iter().map(|k| self.cell(k, r)));
            // SAFETY: ids cover the window.
            unsafe { *ids.add(r) = index.intern_row(&cells) as u64 };
        });
        self.spare.cells.push(cells);
        self.bounds[output.0] = index.len();
        Ok(())
    }

    pub(super) fn aggregates(
        &mut self,
        group: VariableId,
        domain: DomainId,
        validity: Option<VariableId>,
        updates: &[Aggregate],
    ) -> Result<()> {
        let Some(rows) = self.rows(domain, validity) else {
            return Ok(());
        };
        let constant = self.steps.vars[group.0].scalar();
        let ids = self.ptrs[group.0] as *const u64;
        // SAFETY: group IDs cover the window.
        let id = |r: usize| if constant { unsafe { *ids } } else { unsafe { *ids.add(r) } } as usize;
        // Group IDs from a GroupLookup are below its group count.
        let groups = if constant {
            id(0) + 1
        } else if self.bounds[group.0] != 0 {
            self.bounds[group.0]
        } else {
            let mut max = 0;
            rows.each(|r| max = max.max(id(r)));
            max + 1
        };
        let groups = groups.max(1);
        let count = rows.count();
        for aggregate in updates {
            // SAFETY: the state is exclusively borrowed for the run.
            let accumulator = unsafe { &mut (*self.accumulators[aggregate.state.0]).accumulator };
            if accumulator.groups_mut().len() < groups {
                accumulator.resize(groups);
            }
            self.aggregate(accumulator, aggregate, &rows, count, constant, ids)?;
        }
        Ok(())
    }

    fn aggregate(
        &self,
        accumulator: &mut Accumulator,
        aggregate: &Aggregate,
        rows: &Rows,
        count: usize,
        constant: bool,
        ids: *const u64,
    ) -> Result<()> {
        use AggregateUpdateKind as K;
        let (kind, sum) = (aggregate.update.kind, aggregate.update.sum);
        if aggregate.update.none {
            return Ok(());
        }
        // SAFETY: group IDs cover the window.
        let id = |r: usize| if constant { unsafe { *ids } } else { unsafe { *ids.add(r) } } as usize;
        match (kind, sum) {
            (K::CountRows, _) | (K::CountValues, _) if aggregate.arguments.len() <= 1 => {
                let groups = accumulator.groups_mut();
                if constant {
                    let total = &mut groups[id(0)].count;
                    *total = total.checked_add(count as i64).ok_or_else(overflow)?;
                } else {
                    let base = groups.as_mut_ptr();
                    let mut overflowed = false;
                    rows.each_weighted(|r, valid| {
                        // Every window row has an ID below the group count;
                        // left-out rows add nothing.
                        // SAFETY: as above.
                        let count = unsafe { &mut (*base.add(*ids.add(r) as usize)).count };
                        let (c, o) = count.overflowing_add(valid as i64);
                        *count = c;
                        overflowed |= o;
                    });
                    if overflowed {
                        return Err(overflow());
                    }
                }
                return Ok(());
            }
            (_, Some(value)) => {
                let values = self.ptrs[value.0] as *const u64;
                let groups = accumulator.groups_mut();
                if constant {
                    // With few rows left out, sum every row and subtract them.
                    let total = if (rows.n - count) * 64 <= rows.n {
                        let mut args = [values as *mut u8];
                        // SAFETY: the values cover the window.
                        let mut total = unsafe { SUMS[1](args.as_mut_ptr(), rows.n as u64) };
                        let bitmap = rows.bitmap();
                        for (w, &word) in bitmap.iter().enumerate().take(bits::words(rows.n)) {
                            let mut out = !word & bits::tail_mask(w, rows.n);
                            while out != 0 {
                                // SAFETY: as above.
                                let v =
                                    unsafe { *values.add(w * 64 + out.trailing_zeros() as usize) };
                                total = total.wrapping_sub(v);
                                out &= out - 1;
                            }
                        }
                        total
                    } else {
                        let mut args = [values as *mut u8, rows.bitmap().as_ptr() as *mut u8];
                        // SAFETY: the values cover the window and the mask is
                        // a window bitmap.
                        unsafe { SUMS[0](args.as_mut_ptr(), rows.n as u64) }
                    };
                    let sum = &mut groups[id(0)].sum;
                    sum.bits = sum.bits.wrapping_add(total);
                    sum.valid |= (count != 0) as u64;
                } else {
                    let base = groups.as_mut_ptr();
                    rows.each_weighted(|r, valid| {
                        // As for counts; the values cover the window.
                        let (sum, v) =
                            unsafe { (&mut (*base.add(*ids.add(r) as usize)).sum, *values.add(r)) };
                        sum.bits = sum.bits.wrapping_add(v & valid.wrapping_neg());
                        sum.valid |= valid;
                    });
                }
                return Ok(());
            }
            _ => {}
        }
        if let Some(value) = aggregate.update.wide {
            // Rows are valid arguments; sums wrap like the row kernels'.
            let values = self.ptrs[value.0] as *const i128;
            let average = kind == K::AvgI128;
            let groups = accumulator.groups_mut();
            if constant {
                let mut total = 0i128;
                // SAFETY: 128-bit slots hold a window of aligned values.
                rows.each(|r| total = total.wrapping_add(unsafe { *values.add(r) }));
                let g = &mut groups[id(0)];
                if count != 0 {
                    g.sum = Cell::wide(g.sum.value128().wrapping_add(total));
                }
                if average {
                    g.count = g.count.checked_add(count as i64).ok_or_else(overflow)?;
                }
            } else {
                let base = groups.as_mut_ptr();
                let mut overflowed = false;
                rows.each(|r| {
                    // SAFETY: as for counts; the values cover the window.
                    let (g, v) = unsafe { (&mut *base.add(*ids.add(r) as usize), *values.add(r)) };
                    g.sum = Cell::wide(g.sum.value128().wrapping_add(v));
                    if average {
                        let (c, o) = g.count.overflowing_add(1);
                        g.count = c;
                        overflowed |= o;
                    }
                });
                if overflowed {
                    return Err(overflow());
                }
            }
            return Ok(());
        }
        let update = row_update(kind);
        let mut cells = Vec::with_capacity(aggregate.arguments.len());
        let mut result = Ok(());
        rows.each(|r| {
            if result.is_ok() {
                cells.clear();
                cells.extend(aggregate.arguments.iter().map(|a| self.cell(a, r)));
                result = update(accumulator, id(r), &cells);
            }
        });
        result
    }
}
