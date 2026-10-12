//! Borrowed Arrow buffers and owned generated output buffers. No scalar evaluation.
use super::scalar::{self, Cell};
use crate::{
    arrow::{array::*, datatypes::*},
    error::Result,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub(super) struct Column {
    pub data: *const u8,
    pub validity: *const u8,
    pub offset: usize,
}
pub(super) fn direct(t: &DataType) -> bool {
    scalar::numeric(t)
        || matches!(
            t,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
        )
}
pub(super) fn direct_output(t: &DataType) -> bool {
    direct(t) && scalar::width(t) == 64
}
pub(super) fn input(array: &ArrayRef, cells: &mut Option<Vec<Cell>>) -> Result<Column> {
    if direct(array.data_type()) {
        macro_rules! address {
            ($t:ty) => {
                array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$t>>()
                    .unwrap()
                    .values()
                    .as_ptr()
                    .cast::<u8>()
            };
        }
        let data = match array.data_type() {
            DataType::Int8 => address!(Int8Type),
            DataType::Int16 => address!(Int16Type),
            DataType::Int32 => address!(Int32Type),
            DataType::Int64 => address!(Int64Type),
            DataType::UInt8 => address!(UInt8Type),
            DataType::UInt16 => address!(UInt16Type),
            DataType::UInt32 => address!(UInt32Type),
            DataType::UInt64 => address!(UInt64Type),
            DataType::Float32 => address!(Float32Type),
            DataType::Float64 => address!(Float64Type),
            DataType::Date32 => address!(Date32Type),
            DataType::Date64 => address!(Date64Type),
            DataType::Timestamp(TimeUnit::Second, _) => address!(TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => address!(TimestampMillisecondType),
            DataType::Timestamp(TimeUnit::Microsecond, _) => address!(TimestampMicrosecondType),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => address!(TimestampNanosecondType),
            _ => unreachable!(),
        };
        Ok(Column {
            data,
            validity: array
                .nulls()
                .map_or(std::ptr::null(), |n| n.validity().as_ptr()),
            offset: array.nulls().map_or(0, |n| n.offset()),
        })
    } else {
        *cells = Some(scalar::decode(array)?);
        Ok(Column {
            data: cells.as_ref().unwrap().as_ptr().cast(),
            ..Column::default()
        })
    }
}
pub(super) struct Output {
    words: Vec<u64>,
    valid: Vec<u8>,
    cells: Vec<Cell>,
}
impl Output {
    pub fn new(ty: &DataType, rows: usize) -> Self {
        if direct_output(ty) {
            Self {
                words: vec![0; rows],
                valid: vec![0; rows.div_ceil(8)],
                cells: vec![],
            }
        } else {
            Self {
                words: vec![],
                valid: vec![],
                cells: vec![Cell::default(); rows],
            }
        }
    }
    pub fn descriptor(&mut self, ty: &DataType) -> Column {
        if direct_output(ty) {
            Column {
                data: self.words.as_mut_ptr().cast(),
                validity: self.valid.as_mut_ptr(),
                offset: 0,
            }
        } else {
            Column {
                data: self.cells.as_mut_ptr().cast(),
                ..Column::default()
            }
        }
    }
    pub fn row_id(&self, row: usize) -> usize {
        self.words[row] as usize
    }
    pub fn finish(mut self, ty: &DataType, rows: usize) -> Result<ArrayRef> {
        if !direct_output(ty) {
            return scalar::encode(ty, &self.cells[..rows]);
        }
        self.words.truncate(rows);
        self.valid.truncate(rows.div_ceil(8));
        let nulls = NullBuffer::new(BooleanBuffer::new(Buffer::from(self.valid), 0, rows));
        let nulls = (nulls.null_count() != 0).then_some(nulls);
        let array = UInt64Array::new(self.words.into(), nulls);
        let data = array
            .into_data()
            .into_builder()
            .data_type(ty.clone())
            .build()?;
        Ok(make_array(data))
    }
}
