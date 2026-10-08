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

//! Precompiled vector primitives over a selection. Each expression node still
//! produces its own compact array; this is not expression fusion or JIT.
use super::{ColumnValue, ScalarValue};
use crate::error::{Error, Result};
use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array},
    buffer::BooleanBuffer,
    datatypes::{DataType, Int64Type},
};
use std::sync::Arc;

pub(crate) type Kernel = fn(&Value, &Value, &BooleanBuffer, usize) -> Result<ColumnValue>;
#[derive(Debug)]
pub(crate) enum Value {
    Physical(ArrayRef),
    Compact(ColumnValue),
}

// Access layout is selected once per primitive, outside its row loop.
enum Operand<'a> {
    Physical(&'a Int64Array),
    Compact(&'a Int64Array),
    Scalar(Option<i64>),
}
fn operand(value: &Value) -> Result<Operand<'_>> {
    fn array(a: &ArrayRef) -> Result<&Int64Array> {
        a.as_primitive_opt::<Int64Type>()
            .ok_or_else(|| Error::Execution("expected Int64 array".into()))
    }
    Ok(match value {
        Value::Physical(a) => Operand::Physical(array(a)?),
        Value::Compact(ColumnValue::Array(a)) => Operand::Compact(array(a)?),
        Value::Compact(ColumnValue::Scalar(ScalarValue::Int64(v))) => Operand::Scalar(*v),
        Value::Compact(ColumnValue::Scalar(ScalarValue::Null(DataType::Int64))) => {
            Operand::Scalar(None)
        }
        _ => return Err(Error::Execution("expected Int64 scalar".into())),
    })
}
pub(crate) fn kernel<const OP: u8>(
    left: &Value,
    right: &Value,
    selection: &BooleanBuffer,
    rows: usize,
) -> Result<ColumnValue> {
    let left = operand(left)?;
    let right = operand(right)?;
    let has_nulls = |v: &Operand<'_>| match v {
        Operand::Physical(a) | Operand::Compact(a) => a.null_count() != 0,
        Operand::Scalar(v) => v.is_none(),
    };
    let nullable = has_nulls(&left) || has_nulls(&right);
    macro_rules! dispatch_right {
        ($left:expr) => {
            match right {
                Operand::Physical(a) if a.null_count() == 0 => {
                    apply::<OP>($left, |_, p| Some(a.value(p)), selection, rows, nullable)
                }
                Operand::Physical(a) => apply::<OP>(
                    $left,
                    |_, p| a.is_valid(p).then(|| a.value(p)),
                    selection,
                    rows,
                    nullable,
                ),
                Operand::Compact(a) if a.null_count() == 0 => {
                    apply::<OP>($left, |i, _| Some(a.value(i)), selection, rows, nullable)
                }
                Operand::Compact(a) => apply::<OP>(
                    $left,
                    |i, _| a.is_valid(i).then(|| a.value(i)),
                    selection,
                    rows,
                    nullable,
                ),
                Operand::Scalar(v) => apply::<OP>($left, |_, _| v, selection, rows, nullable),
            }
        };
    }
    match left {
        Operand::Physical(a) if a.null_count() == 0 => dispatch_right!(|_, p| Some(a.value(p))),
        Operand::Physical(a) => dispatch_right!(|_, p| a.is_valid(p).then(|| a.value(p))),
        Operand::Compact(a) if a.null_count() == 0 => dispatch_right!(|i, _| Some(a.value(i))),
        Operand::Compact(a) => dispatch_right!(|i, _| a.is_valid(i).then(|| a.value(i))),
        Operand::Scalar(v) => dispatch_right!(|_, _| v),
    }
}
fn apply<const OP: u8>(
    left: impl Fn(usize, usize) -> Option<i64>,
    right: impl Fn(usize, usize) -> Option<i64>,
    selection: &BooleanBuffer,
    rows: usize,
    nullable: bool,
) -> Result<ColumnValue> {
    if nullable {
        apply_rows::<OP, true>(left, right, selection, rows)
    } else {
        apply_rows::<OP, false>(left, right, selection, rows)
    }
}
fn apply_rows<const OP: u8, const NULLABLE: bool>(
    left: impl Fn(usize, usize) -> Option<i64>,
    right: impl Fn(usize, usize) -> Option<i64>,
    selection: &BooleanBuffer,
    rows: usize,
) -> Result<ColumnValue> {
    let mut integers = Vec::with_capacity(if OP < 3 { rows } else { 0 });
    let mut booleans = Vec::with_capacity(if OP >= 3 { rows } else { 0 });
    let mut valid = arrow::array::BooleanBufferBuilder::new(if NULLABLE { rows } else { 0 });
    for (logical, physical) in selection.set_indices().enumerate() {
        let pair = left(logical, physical).zip(right(logical, physical));
        if NULLABLE {
            valid.append(pair.is_some());
        }
        let (a, b) = pair.unwrap_or((0, 0));
        if OP < 3 {
            let value = match OP {
                0 => a.checked_add(b),
                1 => a.checked_sub(b),
                _ => a.checked_mul(b),
            }
            .ok_or_else(|| {
                Error::from(arrow::error::ArrowError::ArithmeticOverflow(
                    "selected Int64 arithmetic overflow".into(),
                ))
            })?;
            integers.push(value);
        } else {
            booleans.push(match OP {
                3 => a == b,
                4 => a != b,
                5 => a < b,
                6 => a <= b,
                7 => a > b,
                _ => a >= b,
            });
        }
    }
    let nulls = if NULLABLE {
        Some(arrow::buffer::NullBuffer::new(valid.finish()))
    } else {
        None
    };
    Ok(ColumnValue::Array(if OP < 3 {
        Arc::new(Int64Array::new(integers.into(), nulls))
    } else {
        Arc::new(BooleanArray::new(BooleanBuffer::from(booleans), nulls))
    }))
}
