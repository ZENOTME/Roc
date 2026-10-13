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

//! Borrowed Arrow input buffers and owned generated output buffers.
use super::builder::is_bytes;
use crate::arrow::{array::*, datatypes::*};
use crate::error::{Error, Result};
use arrow_buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use std::sync::Arc;

/// Words read by generated code for one input column.
#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct InputDescriptor {
    /// Fixed-width values, the Boolean value bitmap, or the byte data.
    pub data: *const u8,
    /// i32 or i64 offsets of byte arrays, already sliced.
    pub offsets: *const u8,
    /// Null when the array has no validity buffer.
    pub validity: *const u8,
    pub validity_offset: usize,
    /// Bit offset of the Boolean value bitmap.
    pub value_offset: usize,
}

impl Default for InputDescriptor {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            offsets: std::ptr::null(),
            validity: std::ptr::null(),
            validity_offset: 0,
            value_offset: 0,
        }
    }
}

pub(crate) const INPUT_WORDS: usize = 5;
pub(crate) const OUTPUT_WORDS: usize = 3;

pub(crate) fn input(array: &ArrayRef) -> Result<InputDescriptor> {
    let mut d = InputDescriptor {
        validity: array
            .nulls()
            .map_or(std::ptr::null(), |n| n.validity().as_ptr()),
        validity_offset: array.nulls().map_or(0, |n| n.offset()),
        ..Default::default()
    };
    macro_rules! primitive {
        ($t:ty) => {
            array.as_primitive::<$t>().values().as_ptr().cast::<u8>()
        };
    }
    d.data = match array.data_type() {
        DataType::Null => std::ptr::null(),
        DataType::Boolean => {
            let values = array.as_boolean().values();
            d.value_offset = values.offset();
            values.values().as_ptr()
        }
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            d.offsets = a.value_offsets().as_ptr().cast();
            a.values().as_ptr()
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            d.offsets = a.value_offsets().as_ptr().cast();
            a.values().as_ptr()
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            d.offsets = a.value_offsets().as_ptr().cast();
            a.values().as_ptr()
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            d.offsets = a.value_offsets().as_ptr().cast();
            a.values().as_ptr()
        }
        t => {
            return Err(Error::InvalidPlan(format!(
                "unsupported generated value type: {t}"
            )));
        }
    };
    Ok(d)
}

/// Byte width of a fixed-width logical type's values.
pub(crate) fn width(t: &DataType) -> usize {
    super::VariableType::of(t).map_or(8, |v| (v.bits() / 8) as usize)
}

/// Capacity for one output column; one input row writes at most one row.
pub(crate) struct OutputColumn {
    data: MutableBuffer,
    aux: Vec<u64>,
    validity: Option<MutableBuffer>,
}

impl OutputColumn {
    pub fn new(t: &DataType, nullable: bool, rows: usize) -> Self {
        let bitmap = || MutableBuffer::from_len_zeroed(rows.div_ceil(8));
        let (data, aux) = match t {
            DataType::Null => (MutableBuffer::new(0), vec![]),
            DataType::Boolean => (bitmap(), vec![]),
            t if is_bytes(t) => (MutableBuffer::from_len_zeroed(rows * 8), vec![0; rows]),
            t => (MutableBuffer::from_len_zeroed(rows * width(t)), vec![]),
        };
        Self {
            data,
            aux,
            validity: nullable.then(bitmap),
        }
    }

    pub fn descriptor(&mut self) -> [*mut u8; OUTPUT_WORDS] {
        [
            self.data.as_mut_ptr(),
            self.aux.as_mut_ptr().cast(),
            self.validity
                .as_mut()
                .map_or(std::ptr::null_mut(), |v| v.as_mut_ptr()),
        ]
    }

    pub fn finish(mut self, t: &DataType, rows: usize) -> Result<ArrayRef> {
        let nulls = self
            .validity
            .take()
            .map(|v| NullBuffer::new(BooleanBuffer::new(Buffer::from(v), 0, rows)));
        let nulls = nulls.filter(|n| n.null_count() != 0);
        Ok(match t {
            DataType::Null => Arc::new(NullArray::new(rows)),
            DataType::Boolean => Arc::new(BooleanArray::new(
                BooleanBuffer::new(Buffer::from(self.data), 0, rows),
                nulls,
            )),
            DataType::Utf8 => Arc::new(StringArray::try_new(
                self.offsets::<i32>(rows, nulls.as_ref()),
                self.bytes(rows, nulls.as_ref()),
                nulls,
            )?),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::try_new(
                self.offsets::<i64>(rows, nulls.as_ref()),
                self.bytes(rows, nulls.as_ref()),
                nulls,
            )?),
            DataType::Binary => Arc::new(BinaryArray::try_new(
                self.offsets::<i32>(rows, nulls.as_ref()),
                self.bytes(rows, nulls.as_ref()),
                nulls,
            )?),
            DataType::LargeBinary => Arc::new(LargeBinaryArray::try_new(
                self.offsets::<i64>(rows, nulls.as_ref()),
                self.bytes(rows, nulls.as_ref()),
                nulls,
            )?),
            t => {
                let mut data = self.data;
                data.truncate(rows * width(t));
                let buffer = Buffer::from(data);
                let array: ArrayRef = match width(t) {
                    1 => Arc::new(Int8Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                    2 => Arc::new(Int16Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                    4 => Arc::new(Int32Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                    _ => Arc::new(Int64Array::new(ScalarBuffer::new(buffer, 0, rows), nulls)),
                };
                make_array(
                    array
                        .into_data()
                        .into_builder()
                        .data_type(t.clone())
                        .build()?,
                )
            }
        })
    }

    fn pointers(&self, rows: usize) -> &[u64] {
        &self.data.typed_data::<u64>()[..rows]
    }

    /// Row lengths; NULL rows are empty, whatever their slot holds.
    fn lengths<'a>(
        &'a self,
        rows: usize,
        nulls: Option<&'a NullBuffer>,
    ) -> impl Iterator<Item = usize> + 'a {
        self.aux[..rows].iter().enumerate().map(move |(i, &len)| {
            if nulls.is_some_and(|n| n.is_null(i)) {
                0
            } else {
                len as usize
            }
        })
    }

    fn offsets<O: OffsetSizeTrait>(
        &self,
        rows: usize,
        nulls: Option<&NullBuffer>,
    ) -> OffsetBuffer<O> {
        OffsetBuffer::<O>::from_lengths(self.lengths(rows, nulls))
    }

    // Pointers refer to input arrays, retained constants or the invocation
    // arena; all remain alive while the output is copied.
    fn bytes(&self, rows: usize, nulls: Option<&NullBuffer>) -> Buffer {
        let total = self.lengths(rows, nulls).sum();
        let mut out = MutableBuffer::with_capacity(total);
        for (&ptr, len) in self.pointers(rows).iter().zip(self.lengths(rows, nulls)) {
            if len != 0 {
                out.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr as *const u8, len) });
            }
        }
        out.into()
    }
}
