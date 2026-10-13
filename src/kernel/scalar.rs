//! Values at the generated function ABI. Arrow is decoded/encoded only at
//! batch boundaries; no operation here evaluates an expression or a query plan.
use crate::{
    arrow::{array::*, datatypes::*},
    error::{Error, Result},
};
use std::sync::Arc;

/// Strings borrow stable input buffers or the invocation's string arena. Numeric
/// payloads use their native bits. Only generated code may manufacture cells.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub(crate) struct Cell {
    pub bits: u64,
    pub len: u64,
    pub valid: u64,
}
impl Cell {
    pub fn word(bits: u64) -> Self {
        Self {
            bits,
            len: 0,
            valid: 1,
        }
    }
    pub fn bytes(bytes: &[u8]) -> Self {
        Self {
            bits: bytes.as_ptr() as u64,
            len: bytes.len() as u64,
            valid: 1,
        }
    }
    // SAFETY: callers retain the input array/owned key/arena until the last use.
    pub unsafe fn as_bytes<'a>(self) -> &'a [u8] {
        if self.valid == 0 || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.bits as *const u8, self.len as usize) }
        }
    }
}
pub(crate) fn supported(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}
pub(crate) fn numeric(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}
pub(crate) fn bytes_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}
pub(crate) fn signed(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}
pub(crate) fn decode(a: &ArrayRef) -> Result<Vec<Cell>> {
    macro_rules! primitive {
        ($t:ty, $convert:expr) => {{
            let a = a.as_any().downcast_ref::<PrimitiveArray<$t>>().unwrap();
            a.iter()
                .map(|v| v.map(|v| Cell::word(($convert)(v))).unwrap_or_default())
                .collect()
        }};
    }
    macro_rules! bytes {
        ($t:ty) => {{
            let a = a.as_any().downcast_ref::<$t>().unwrap();
            a.iter()
                .map(|v| v.map(|v| Cell::bytes(v.as_ref())).unwrap_or_default())
                .collect()
        }};
    }
    Ok(match a.data_type() {
        DataType::Null => vec![Cell::default(); a.len()],
        DataType::Boolean => a
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(|v| Cell::word(v as u64)).unwrap_or_default())
            .collect(),
        DataType::Int8 => primitive!(Int8Type, |v: i8| v as u64),
        DataType::Int16 => primitive!(Int16Type, |v: i16| v as u64),
        DataType::Int32 => primitive!(Int32Type, |v: i32| v as u64),
        DataType::Int64 => primitive!(Int64Type, |v: i64| v as u64),
        DataType::UInt8 => primitive!(UInt8Type, |v: u8| v as u64),
        DataType::UInt16 => primitive!(UInt16Type, |v: u16| v as u64),
        DataType::UInt32 => primitive!(UInt32Type, |v: u32| v as u64),
        DataType::UInt64 => primitive!(UInt64Type, |v: u64| v),
        DataType::Float32 => primitive!(Float32Type, |v: f32| v.to_bits() as u64),
        DataType::Float64 => primitive!(Float64Type, |v: f64| v.to_bits()),
        DataType::Date32 => primitive!(Date32Type, |v: i32| v as u64),
        DataType::Date64 => primitive!(Date64Type, |v: i64| v as u64),
        DataType::Timestamp(TimeUnit::Second, _) => {
            primitive!(TimestampSecondType, |v: i64| v as u64)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondType, |v: i64| v as u64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondType, |v: i64| v as u64)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondType, |v: i64| v as u64)
        }
        DataType::Utf8 => bytes!(StringArray),
        DataType::LargeUtf8 => bytes!(LargeStringArray),
        DataType::Binary => bytes!(BinaryArray),
        DataType::LargeBinary => bytes!(LargeBinaryArray),
        t => {
            return Err(Error::InvalidPlan(format!(
                "unsupported generated value type: {t}"
            )));
        }
    })
}
pub(crate) fn encode(t: &DataType, cells: &[Cell]) -> Result<ArrayRef> {
    macro_rules! primitive {
        ($t:ty, $convert:expr) => {
            Arc::new(
                PrimitiveArray::<$t>::from_iter(
                    cells
                        .iter()
                        .map(|c| (c.valid != 0).then(|| ($convert)(c.bits))),
                )
                .with_data_type(t.clone()),
            ) as ArrayRef
        };
    }
    // Cell pointers refer to input arrays, retained constants, or the invocation
    // arena. All three remain alive while this boundary copies the output.
    Ok(match t {
        DataType::Null => Arc::new(NullArray::new(cells.len())),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            cells.iter().map(|c| (c.valid != 0).then_some(c.bits != 0)),
        )),
        DataType::Int8 => primitive!(Int8Type, |v| v as i8),
        DataType::Int16 => primitive!(Int16Type, |v| v as i16),
        DataType::Int32 => primitive!(Int32Type, |v| v as i32),
        DataType::Int64 => primitive!(Int64Type, |v| v as i64),
        DataType::UInt8 => primitive!(UInt8Type, |v| v as u8),
        DataType::UInt16 => primitive!(UInt16Type, |v| v as u16),
        DataType::UInt32 => primitive!(UInt32Type, |v| v as u32),
        DataType::UInt64 => primitive!(UInt64Type, |v| v),
        DataType::Float32 => primitive!(Float32Type, |v| f32::from_bits(v as u32)),
        DataType::Float64 => primitive!(Float64Type, f64::from_bits),
        DataType::Date32 => primitive!(Date32Type, |v| v as i32),
        DataType::Date64 => primitive!(Date64Type, |v| v as i64),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType, |v| v as i64),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondType, |v| v as i64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondType, |v| v as i64)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondType, |v| v as i64)
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let strings = cells
                .iter()
                .map(|c| {
                    if c.valid == 0 {
                        Ok(None)
                    } else {
                        std::str::from_utf8(unsafe { c.as_bytes() })
                            .map(Some)
                            .map_err(|e| Error::Execution(e.to_string()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            if *t == DataType::Utf8 {
                Arc::new(StringArray::from(strings))
            } else {
                Arc::new(LargeStringArray::from(strings))
            }
        }
        DataType::Binary => Arc::new(BinaryArray::from_iter(
            cells
                .iter()
                .map(|c| (c.valid != 0).then(|| unsafe { c.as_bytes() })),
        )),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from_iter(
            cells
                .iter()
                .map(|c| (c.valid != 0).then(|| unsafe { c.as_bytes() })),
        )),
        _ => {
            return Err(Error::InvalidPlan(format!(
                "unsupported generated output type: {t}"
            )));
        }
    })
}
/// Owned state values: variable-width payloads cannot borrow the current batch.
#[derive(Clone, Debug)]
pub(crate) struct OwnedCell {
    pub cell: Cell,
    bytes: Option<Vec<u8>>,
}
impl OwnedCell {
    pub fn get(&self) -> Cell {
        self.bytes.as_ref().map_or(self.cell, |b| Cell::bytes(b))
    }
}
pub(crate) type KeyEncoder = fn(Cell, &mut Vec<u8>);
pub(crate) type OwnValue = fn(Cell) -> OwnedCell;
pub(crate) fn bind_owner(ty: &DataType) -> OwnValue {
    if bytes_type(ty) {
        |cell| OwnedCell {
            cell,
            bytes: (cell.valid != 0).then(|| unsafe { cell.as_bytes().to_vec() }),
        }
    } else {
        |cell| OwnedCell { cell, bytes: None }
    }
}
pub(crate) fn bind_key(ty: &DataType) -> Result<KeyEncoder> {
    if !supported(ty) {
        return Err(Error::InvalidPlan(format!("unsupported key type: {ty}")));
    }
    Ok(if bytes_type(ty) {
        key_bytes
    } else {
        match ty {
            DataType::Float32 => key_word::<2>,
            DataType::Float64 => key_word::<3>,
            _ if signed(ty) => key_word::<1>,
            _ => key_word::<0>,
        }
    })
}
fn key_bytes(cell: Cell, out: &mut Vec<u8>) {
    out.push((cell.valid != 0) as u8);
    if cell.valid != 0 {
        for &b in unsafe { cell.as_bytes() } {
            out.extend_from_slice(&[1, b]);
        }
        out.push(0);
    }
}
fn key_word<const KIND: u8>(cell: Cell, out: &mut Vec<u8>) {
    out.push((cell.valid != 0) as u8);
    if cell.valid == 0 {
        return;
    }
    let v = cell.bits;
    let bits = match KIND {
        1 => v ^ (1 << 63),
        2 => {
            let v = v as u32;
            if v & (1 << 31) != 0 {
                !v as u64
            } else {
                (v ^ (1 << 31)) as u64
            }
        }
        3 => {
            if v & (1 << 63) != 0 {
                !v
            } else {
                v ^ (1 << 63)
            }
        }
        _ => v,
    };
    out.extend_from_slice(&bits.to_be_bytes());
}
#[cfg(test)]
pub(crate) fn bind_float(ty: &DataType) -> Result<fn(Cell) -> f64> {
    Ok(match ty {
        DataType::Float32 => |c| f32::from_bits(c.bits as u32) as f64,
        DataType::Float64 => |c| f64::from_bits(c.bits),
        _ if signed(ty) => |c| c.bits as i64 as f64,
        _ if numeric(ty) || *ty == DataType::Boolean => |c| c.bits as f64,
        _ => {
            return Err(Error::InvalidPlan(format!(
                "non-numeric aggregate input: {ty}"
            )));
        }
    })
}
