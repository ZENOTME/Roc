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

//! Output columns, appended window by window. Root outputs computed by a
//! kernel are written in place; other rows are copied, compacted to the
//! rows of the output domain.
use super::bits::{self, BitBuilder, BitState};
use super::run::Frame;
use super::*;
use crate::arrow::{
    array::*,
    datatypes::DataType,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_buffer::{Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};

/// Spare capacity for writes one row past the last kept row.
const SLACK: usize = 64;

enum Values {
    Alias,
    Null,
    Fixed {
        data: MutableBuffer,
        width: usize,
        value: VariableId,
        in_place: bool,
    },
    Bool {
        value: VariableId,
        bits: BitBuilder,
    },
    Bytes {
        ptr: VariableId,
        len: VariableId,
        ptrs: Vec<u64>,
        lens: Vec<u64>,
    },
}

struct Column {
    values: Values,
    validity: Option<(VariableId, BitBuilder)>,
}

pub(super) struct Outputs {
    columns: Vec<Column>,
    count: usize,
}

/// Copies the rows of `src` selected by `mask` below `n`; returns their count.
unsafe fn compact<T: Copy>(src: *const T, mask: &[u64], n: usize, dst: *mut T) -> usize {
    let mut k = 0;
    for (w, &m) in mask.iter().enumerate().take(bits::words(n)) {
        let m = m & bits::tail_mask(w, n);
        let base = w * 64;
        // SAFETY: rows are below `n`; `dst` has room for one row past the kept ones.
        unsafe {
            if m == u64::MAX {
                std::ptr::copy_nonoverlapping(src.add(base), dst.add(k), 64);
                k += 64;
            } else if m != 0 {
                for i in 0..64.min(n - base) {
                    *dst.add(k) = *src.add(base + i);
                    k += (m >> i & 1) as usize;
                }
            }
        }
    }
    k
}

unsafe fn compact_width(
    width: usize,
    src: *const u8,
    mask: &[u64],
    n: usize,
    dst: *mut u8,
) -> usize {
    unsafe {
        match width {
            1 => compact(src, mask, n, dst),
            2 => compact(src as *const u16, mask, n, dst as *mut u16),
            4 => compact(src as *const u32, mask, n, dst as *mut u32),
            _ => compact(src as *const u64, mask, n, dst as *mut u64),
        }
    }
}

fn append_bits(
    frame: &Frame<'_>,
    v: VariableId,
    mask: &[u64],
    state: BitState,
    selected: usize,
    out: &mut BitBuilder,
) {
    let n = frame.n;
    match frame.states[v.0] {
        BitState::AllTrue => out.fill(true, selected),
        BitState::AllFalse => out.fill(false, selected),
        BitState::Mixed if state == BitState::AllTrue => {
            for (w, &word) in frame.words(v).iter().enumerate().take(bits::words(n)) {
                out.push(word, 64.min(n - w * 64));
            }
        }
        BitState::Mixed => out.select(frame.words(v), mask, n),
    }
}

impl Outputs {
    pub fn new(output: &GraphOutput, aliases: &[Option<usize>], vars: &[Var], rows: usize) -> Self {
        let capacity = rows + SLACK;
        let columns = output
            .bindings
            .iter()
            .enumerate()
            .map(|(j, binding)| {
                let values = match *binding {
                    _ if aliases.get(j).copied().flatten().is_some() => Values::Alias,
                    ColumnVariableBinding::Null { .. } => Values::Null,
                    ColumnVariableBinding::Boolean { value, .. } => Values::Bool {
                        value,
                        bits: BitBuilder::with_capacity(rows),
                    },
                    ColumnVariableBinding::Bytes { ptr, len, .. } => Values::Bytes {
                        ptr,
                        len,
                        ptrs: Vec::with_capacity(capacity),
                        lens: Vec::with_capacity(capacity),
                    },
                    _ => {
                        let value = binding.value().unwrap();
                        let width = vars[value.0].ty.bytes();
                        Values::Fixed {
                            data: MutableBuffer::with_capacity(capacity * width),
                            width,
                            value,
                            in_place: vars[value.0].place == Place::Output(j),
                        }
                    }
                };
                let validity = match values {
                    Values::Alias | Values::Null => None,
                    _ => binding
                        .validity()
                        .map(|v| (v, BitBuilder::with_capacity(rows))),
                };
                Column { values, validity }
            })
            .collect();
        Self { columns, count: 0 }
    }

    /// The in-place values of column `j` at chunk row `start`.
    pub fn column_ptr(&mut self, j: usize, start: usize) -> *mut u8 {
        match &mut self.columns[j].values {
            // SAFETY: the buffer has capacity for every row of the chunk.
            Values::Fixed { data, width, .. } => unsafe { data.as_mut_ptr().add(start * *width) },
            _ => unreachable!("only fixed-width outputs are written in place"),
        }
    }

    pub fn append(&mut self, frame: &Frame<'_>, domain: DomainId) {
        let n = frame.n;
        let (mask, state) = frame.mask(domain);
        let selected = match state {
            BitState::AllFalse => return,
            BitState::AllTrue => n,
            BitState::Mixed => bits::count(&[mask], n),
        };
        let count = self.count;
        for column in &mut self.columns {
            match &mut column.values {
                Values::Alias | Values::Null => {}
                // Kernels already wrote these rows.
                Values::Fixed { in_place: true, .. } => {}
                Values::Fixed {
                    data, width, value, ..
                } => {
                    let w = *width;
                    let src = frame.ptrs[value.0] as *const u8;
                    // SAFETY: the buffer has capacity for every row of the chunk
                    // plus slack; sources cover the window or are one value.
                    unsafe {
                        let dst = data.as_mut_ptr().add(count * w);
                        if frame.steps.vars[value.0].scalar() {
                            for i in 0..selected {
                                std::ptr::copy_nonoverlapping(src, dst.add(i * w), w);
                            }
                        } else if state == BitState::AllTrue {
                            std::ptr::copy_nonoverlapping(src, dst, n * w);
                        } else {
                            compact_width(w, src, mask, n, dst);
                        }
                    }
                }
                Values::Bool { value, bits } => {
                    append_bits(frame, *value, mask, state, selected, bits)
                }
                Values::Bytes {
                    ptr,
                    len,
                    ptrs,
                    lens,
                } => {
                    for (v, out) in [(*ptr, ptrs), (*len, lens)] {
                        // SAFETY: as for fixed-width values, in words.
                        unsafe {
                            let dst = out.as_mut_ptr().add(count);
                            let src = frame.ptrs[v.0] as *const u64;
                            if frame.steps.vars[v.0].scalar() {
                                for i in 0..selected {
                                    *dst.add(i) = *src;
                                }
                            } else if state == BitState::AllTrue {
                                std::ptr::copy_nonoverlapping(src, dst, n);
                            } else {
                                compact(src, mask, n, dst);
                            }
                            out.set_len(count + selected);
                        }
                    }
                }
            }
            if let Some((v, bits)) = &mut column.validity {
                append_bits(frame, *v, mask, state, selected, bits);
            }
        }
        self.count += selected;
    }

    pub fn finish(
        self,
        output: &GraphOutput,
        batch: &RecordBatch,
        inputs: &[GraphInput],
        aliases: &[Option<usize>],
    ) -> Result<RecordBatch> {
        let rows = self.count;
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (j, column) in self.columns.into_iter().enumerate() {
            let t = output.schema.field(j).data_type();
            let nulls = column
                .validity
                .map(|(_, bits)| NullBuffer::new(bits.finish()))
                .filter(|n| n.null_count() != 0);
            let array: ArrayRef = match column.values {
                Values::Alias => batch.column(inputs[aliases[j].unwrap()].position).clone(),
                Values::Null => Arc::new(NullArray::new(rows)),
                Values::Bool { bits, .. } => Arc::new(BooleanArray::new(bits.finish(), nulls)),
                Values::Fixed {
                    mut data, width, ..
                } => {
                    // SAFETY: windows wrote every row up to `rows`.
                    unsafe { data.set_len(rows * width) };
                    let buffer = Buffer::from(data);
                    let array: ArrayRef = match width {
                        1 => Arc::new(Int8Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                        2 => Arc::new(Int16Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                        4 => Arc::new(Int32Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                        _ => Arc::new(Int64Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                    };
                    if array.data_type() == t {
                        array
                    } else {
                        make_array(
                            array
                                .into_data()
                                .into_builder()
                                .data_type(t.clone())
                                .build()
                                .map_err(invalid_output)?,
                        )
                    }
                }
                Values::Bytes { ptrs, lens, .. } => {
                    let lengths = lens.iter().enumerate().map(|(i, &l)| {
                        if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                            0
                        } else {
                            l as usize
                        }
                    });
                    let total = lengths.clone().sum();
                    let mut bytes = MutableBuffer::with_capacity(total);
                    for (&p, l) in ptrs.iter().zip(lengths.clone()) {
                        if l != 0 {
                            // SAFETY: pointers refer to input arrays, retained
                            // constants or formatted strings, all alive here.
                            bytes.extend_from_slice(unsafe {
                                std::slice::from_raw_parts(p as *const u8, l)
                            });
                        }
                    }
                    let bytes = Buffer::from(bytes);
                    match t {
                        DataType::Utf8 => Arc::new(
                            StringArray::try_new(OffsetBuffer::from_lengths(lengths), bytes, nulls)
                                .map_err(invalid_output)?,
                        ),
                        DataType::LargeUtf8 => Arc::new(
                            LargeStringArray::try_new(
                                OffsetBuffer::from_lengths(lengths),
                                bytes,
                                nulls,
                            )
                            .map_err(invalid_output)?,
                        ),
                        DataType::Binary => Arc::new(
                            BinaryArray::try_new(OffsetBuffer::from_lengths(lengths), bytes, nulls)
                                .map_err(invalid_output)?,
                        ),
                        _ => Arc::new(
                            LargeBinaryArray::try_new(
                                OffsetBuffer::from_lengths(lengths),
                                bytes,
                                nulls,
                            )
                            .map_err(invalid_output)?,
                        ),
                    }
                }
            };
            arrays.push(array);
        }
        RecordBatch::try_new_with_options(
            output.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(invalid_output)
    }
}

/// Output arrays are built from the declared column types and validated
/// values, so Arrow rejecting one violates an execution invariant.
#[cold]
fn invalid_output(source: crate::arrow::error::ArrowError) -> Error {
    Error::internal("output does not match its declared columns".into()).with_source(source)
}
