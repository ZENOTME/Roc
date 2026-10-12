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

//! Select operations and primitive types during initialization.
use super::FunctionKind;
use crate::error::{Error, Result};
use arrow::{
    array::{
        Array, ArrayRef, ArrowNativeTypeOp, BooleanArray, Datum, PrimitiveArray, Scalar,
        new_empty_array,
    },
    buffer::{BooleanBuffer, NullBuffer},
    compute::{binary, is_not_null, is_null, kernels::cmp, try_binary},
    datatypes::{
        ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
        Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
    error::ArrowError,
};
use std::sync::Arc;

pub(crate) type UnaryEvalFn = fn(&ArrayRef) -> Result<ArrayRef>;
pub(crate) type BinaryEvalFn = fn(&ArrayRef, &ArrayRef) -> Result<ArrayRef>;

pub(crate) fn bind_unary(function: FunctionKind, data_type: &DataType) -> Result<UnaryEvalFn> {
    use FunctionKind::*;
    Ok(match function {
        IsNull => |value| Ok(Arc::new(is_null(value.as_ref())?)),
        IsNotNull => |value| Ok(Arc::new(is_not_null(value.as_ref())?)),
        Negate => match data_type {
            DataType::Int8 => negate::<Int8Type, false>,
            DataType::Int16 => negate::<Int16Type, false>,
            DataType::Int32 => negate::<Int32Type, false>,
            DataType::Int64 => negate::<Int64Type, false>,
            DataType::Float32 => negate::<Float32Type, true>,
            DataType::Float64 => negate::<Float64Type, true>,
            _ => {
                return Err(Error::InvalidPlan(format!(
                    "unsupported negation type: {data_type}"
                )));
            }
        },
        _ => return Err(Error::InvalidPlan(format!("{function:?} is not unary"))),
    })
}

fn negate<T: ArrowPrimitiveType, const FLOAT: bool>(value: &ArrayRef) -> Result<ArrayRef> {
    let value = primitive::<T>(value)?;
    let output: PrimitiveArray<T> = if FLOAT {
        value.unary(|v| v.neg_wrapping())
    } else {
        value.try_unary(|v| v.neg_checked())?
    };
    Ok(Arc::new(output))
}

pub(crate) fn bind_binary(function: FunctionKind, data_type: &DataType) -> Result<BinaryEvalFn> {
    use FunctionKind::*;
    match function {
        Add => bind_arithmetic::<AddOp>(data_type),
        Subtract => bind_arithmetic::<SubtractOp>(data_type),
        Multiply => bind_arithmetic::<MultiplyOp>(data_type),
        Divide => bind_arithmetic::<DivideOp>(data_type),
        Remainder => bind_arithmetic::<RemainderOp>(data_type),
        Equal => bind_comparison::<EqualOp>(data_type),
        NotEqual => bind_comparison::<NotEqualOp>(data_type),
        LessThan => bind_comparison::<LessOp>(data_type),
        LessThanOrEqual => bind_comparison::<LessEqualOp>(data_type),
        GreaterThan => bind_comparison::<GreaterOp>(data_type),
        GreaterThanOrEqual => bind_comparison::<GreaterEqualOp>(data_type),
        IsDistinctFrom => bind_comparison::<DistinctOp>(data_type),
        IsNotDistinctFrom => bind_comparison::<NotDistinctOp>(data_type),
        _ => Err(Error::InvalidPlan(format!("{function:?} is not binary"))),
    }
}

pub(crate) fn bind_binary_scalar(
    function: FunctionKind,
    data_type: &DataType,
    left_scalar: bool,
) -> Result<BinaryEvalFn> {
    if left_scalar {
        bind_scalar::<true>(function, data_type)
    } else {
        bind_scalar::<false>(function, data_type)
    }
}

fn bind_scalar<const LEFT: bool>(
    function: FunctionKind,
    data_type: &DataType,
) -> Result<BinaryEvalFn> {
    use FunctionKind::*;
    match function {
        Add => bind_scalar_arithmetic::<AddOp, LEFT>(data_type),
        Subtract => bind_scalar_arithmetic::<SubtractOp, LEFT>(data_type),
        Multiply => bind_scalar_arithmetic::<MultiplyOp, LEFT>(data_type),
        Divide => bind_scalar_arithmetic::<DivideOp, LEFT>(data_type),
        Remainder => bind_scalar_arithmetic::<RemainderOp, LEFT>(data_type),
        Equal => bind_scalar_comparison::<EqualOp, LEFT>(data_type),
        NotEqual => bind_scalar_comparison::<NotEqualOp, LEFT>(data_type),
        LessThan => bind_scalar_comparison::<LessOp, LEFT>(data_type),
        LessThanOrEqual => bind_scalar_comparison::<LessEqualOp, LEFT>(data_type),
        GreaterThan => bind_scalar_comparison::<GreaterOp, LEFT>(data_type),
        GreaterThanOrEqual => bind_scalar_comparison::<GreaterEqualOp, LEFT>(data_type),
        IsDistinctFrom => bind_scalar_comparison::<DistinctOp, LEFT>(data_type),
        IsNotDistinctFrom => bind_scalar_comparison::<NotDistinctOp, LEFT>(data_type),
        _ => Err(Error::InvalidPlan(format!("{function:?} is not binary"))),
    }
}

trait ArithmeticOperation {
    fn checked<T: ArrowNativeTypeOp>(left: T, right: T) -> std::result::Result<T, ArrowError>;
    fn float<T: ArrowNativeTypeOp>(left: T, right: T) -> T;
}
macro_rules! arithmetic_operation {
    ($name:ident, $checked:ident, $float:ident) => {
        struct $name;
        impl ArithmeticOperation for $name {
            fn checked<T: ArrowNativeTypeOp>(
                left: T,
                right: T,
            ) -> std::result::Result<T, ArrowError> {
                left.$checked(right)
            }
            fn float<T: ArrowNativeTypeOp>(left: T, right: T) -> T {
                left.$float(right)
            }
        }
    };
}
arithmetic_operation!(AddOp, add_checked, add_wrapping);
arithmetic_operation!(SubtractOp, sub_checked, sub_wrapping);
arithmetic_operation!(MultiplyOp, mul_checked, mul_wrapping);
arithmetic_operation!(DivideOp, div_checked, div_wrapping);
struct RemainderOp;
impl ArithmeticOperation for RemainderOp {
    fn checked<T: ArrowNativeTypeOp>(left: T, right: T) -> std::result::Result<T, ArrowError> {
        if right.is_zero() {
            Err(ArrowError::DivideByZero)
        } else {
            Ok(left.mod_wrapping(right))
        } // MIN % -1 is zero, matching Arrow.
    }
    fn float<T: ArrowNativeTypeOp>(left: T, right: T) -> T {
        left.mod_wrapping(right)
    }
}

fn bind_arithmetic<O: ArithmeticOperation>(data_type: &DataType) -> Result<BinaryEvalFn> {
    Ok(match data_type {
        DataType::Int8 => arithmetic::<Int8Type, O, false>,
        DataType::Int16 => arithmetic::<Int16Type, O, false>,
        DataType::Int32 => arithmetic::<Int32Type, O, false>,
        DataType::Int64 => arithmetic::<Int64Type, O, false>,
        DataType::UInt8 => arithmetic::<UInt8Type, O, false>,
        DataType::UInt16 => arithmetic::<UInt16Type, O, false>,
        DataType::UInt32 => arithmetic::<UInt32Type, O, false>,
        DataType::UInt64 => arithmetic::<UInt64Type, O, false>,
        DataType::Float32 => arithmetic::<Float32Type, O, true>,
        DataType::Float64 => arithmetic::<Float64Type, O, true>,
        _ => {
            return Err(Error::InvalidPlan(format!(
                "unsupported numeric type: {data_type}"
            )));
        }
    })
}

fn arithmetic<T: ArrowPrimitiveType, O: ArithmeticOperation, const FLOAT: bool>(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    let left = primitive::<T>(left)?;
    let right = primitive::<T>(right)?;
    let output: PrimitiveArray<T> = if FLOAT {
        binary(left, right, O::float)?
    } else {
        try_binary(left, right, O::checked)?
    };
    Ok(Arc::new(output))
}

fn bind_scalar_arithmetic<O: ArithmeticOperation, const LEFT: bool>(
    data_type: &DataType,
) -> Result<BinaryEvalFn> {
    Ok(match data_type {
        DataType::Int8 => scalar_arithmetic::<Int8Type, O, false, LEFT>,
        DataType::Int16 => scalar_arithmetic::<Int16Type, O, false, LEFT>,
        DataType::Int32 => scalar_arithmetic::<Int32Type, O, false, LEFT>,
        DataType::Int64 => scalar_arithmetic::<Int64Type, O, false, LEFT>,
        DataType::UInt8 => scalar_arithmetic::<UInt8Type, O, false, LEFT>,
        DataType::UInt16 => scalar_arithmetic::<UInt16Type, O, false, LEFT>,
        DataType::UInt32 => scalar_arithmetic::<UInt32Type, O, false, LEFT>,
        DataType::UInt64 => scalar_arithmetic::<UInt64Type, O, false, LEFT>,
        DataType::Float32 => scalar_arithmetic::<Float32Type, O, true, LEFT>,
        DataType::Float64 => scalar_arithmetic::<Float64Type, O, true, LEFT>,
        _ => {
            return Err(Error::InvalidPlan(format!(
                "unsupported numeric type: {data_type}"
            )));
        }
    })
}

fn scalar_arithmetic<
    T: ArrowPrimitiveType,
    O: ArithmeticOperation,
    const FLOAT: bool,
    const LEFT: bool,
>(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    let (scalar, array) = if LEFT { (left, right) } else { (right, left) };
    let scalar = primitive::<T>(scalar)?;
    let array = primitive::<T>(array)?;
    if scalar.is_null(0) {
        return Ok(Arc::new(PrimitiveArray::<T>::new_null(array.len())));
    }
    let scalar = scalar.value(0);
    let output: PrimitiveArray<T> = if FLOAT {
        array.unary(|value| {
            if LEFT {
                O::float(scalar, value)
            } else {
                O::float(value, scalar)
            }
        })
    } else {
        // Only valid input values may fail. In particular, division by a zero
        // literal must not raise an error for an entirely NULL input array.
        array.try_unary(|value| {
            if LEFT {
                O::checked(scalar, value)
            } else {
                O::checked(value, scalar)
            }
        })?
    };
    Ok(Arc::new(output))
}

fn primitive<T: ArrowPrimitiveType>(value: &ArrayRef) -> Result<&PrimitiveArray<T>> {
    value
        .as_any()
        .downcast_ref()
        .ok_or_else(|| Error::Execution(format!("expected {} array", T::DATA_TYPE)))
}

trait ComparisonOperation {
    const NULL_SAFE: bool = false;
    fn compare<T: ArrowNativeTypeOp>(left: T, right: T) -> bool;
    fn null_result(_left_valid: bool, _right_valid: bool) -> bool {
        false
    }
    fn arrow(left: &dyn Datum, right: &dyn Datum) -> std::result::Result<BooleanArray, ArrowError>;
}
macro_rules! comparison_operation {
    ($name:ident, $native:ident, $arrow:ident) => {
        struct $name;
        impl ComparisonOperation for $name {
            fn compare<T: ArrowNativeTypeOp>(left: T, right: T) -> bool {
                left.$native(right)
            }
            fn arrow(
                left: &dyn Datum,
                right: &dyn Datum,
            ) -> std::result::Result<BooleanArray, ArrowError> {
                cmp::$arrow(left, right)
            }
        }
    };
}
comparison_operation!(EqualOp, is_eq, eq);
comparison_operation!(NotEqualOp, is_ne, neq);
comparison_operation!(LessOp, is_lt, lt);
comparison_operation!(LessEqualOp, is_le, lt_eq);
comparison_operation!(GreaterOp, is_gt, gt);
comparison_operation!(GreaterEqualOp, is_ge, gt_eq);
struct DistinctOp;
impl ComparisonOperation for DistinctOp {
    const NULL_SAFE: bool = true;
    fn compare<T: ArrowNativeTypeOp>(left: T, right: T) -> bool {
        left.is_ne(right)
    }
    fn null_result(left_valid: bool, right_valid: bool) -> bool {
        left_valid != right_valid
    }
    fn arrow(left: &dyn Datum, right: &dyn Datum) -> std::result::Result<BooleanArray, ArrowError> {
        cmp::distinct(left, right)
    }
}
struct NotDistinctOp;
impl ComparisonOperation for NotDistinctOp {
    const NULL_SAFE: bool = true;
    fn compare<T: ArrowNativeTypeOp>(left: T, right: T) -> bool {
        left.is_eq(right)
    }
    fn null_result(left_valid: bool, right_valid: bool) -> bool {
        left_valid == right_valid
    }
    fn arrow(left: &dyn Datum, right: &dyn Datum) -> std::result::Result<BooleanArray, ArrowError> {
        cmp::not_distinct(left, right)
    }
}

fn bind_comparison<O: ComparisonOperation>(data_type: &DataType) -> Result<BinaryEvalFn> {
    Ok(match data_type {
        DataType::Int8 => comparison::<Int8Type, O>,
        DataType::Int16 => comparison::<Int16Type, O>,
        DataType::Int32 => comparison::<Int32Type, O>,
        DataType::Int64 => comparison::<Int64Type, O>,
        DataType::UInt8 => comparison::<UInt8Type, O>,
        DataType::UInt16 => comparison::<UInt16Type, O>,
        DataType::UInt32 => comparison::<UInt32Type, O>,
        DataType::UInt64 => comparison::<UInt64Type, O>,
        DataType::Float32 => comparison::<Float32Type, O>,
        DataType::Float64 => comparison::<Float64Type, O>,
        _ => {
            // Preserve Arrow's other comparison signatures. This fallback still
            // dispatches types inside Arrow; primitive numeric kernels do not.
            let empty = new_empty_array(data_type);
            O::arrow(&empty, &empty).map_err(|e| Error::InvalidPlan(e.to_string()))?;
            arrow_comparison::<O>
        }
    })
}

fn comparison<T: ArrowPrimitiveType, O: ComparisonOperation>(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    let left = primitive::<T>(left)?;
    let right = primitive::<T>(right)?;
    let len = left.len();
    let nulls = if O::NULL_SAFE {
        None
    } else {
        NullBuffer::union(left.nulls(), right.nulls())
    };
    let values = if !O::NULL_SAFE || (left.null_count() == 0 && right.null_count() == 0) {
        // Ordinary comparison validity comes from the output bitmap; comparison
        // itself cannot fail, so NULL slots need no per-row validity branch.
        BooleanBuffer::collect_bool(len, |i| O::compare(left.value(i), right.value(i)))
    } else {
        BooleanBuffer::collect_bool(len, |i| {
            let lv = left.is_valid(i);
            let rv = right.is_valid(i);
            if !lv || !rv {
                O::null_result(lv, rv)
            } else {
                O::compare(left.value(i), right.value(i))
            }
        })
    };
    Ok(Arc::new(BooleanArray::new(values, nulls)))
}

fn arrow_comparison<O: ComparisonOperation>(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    Ok(Arc::new(O::arrow(left, right)?))
}

fn bind_scalar_comparison<O: ComparisonOperation, const LEFT: bool>(
    data_type: &DataType,
) -> Result<BinaryEvalFn> {
    Ok(match data_type {
        DataType::Int8 => scalar_comparison::<Int8Type, O, LEFT>,
        DataType::Int16 => scalar_comparison::<Int16Type, O, LEFT>,
        DataType::Int32 => scalar_comparison::<Int32Type, O, LEFT>,
        DataType::Int64 => scalar_comparison::<Int64Type, O, LEFT>,
        DataType::UInt8 => scalar_comparison::<UInt8Type, O, LEFT>,
        DataType::UInt16 => scalar_comparison::<UInt16Type, O, LEFT>,
        DataType::UInt32 => scalar_comparison::<UInt32Type, O, LEFT>,
        DataType::UInt64 => scalar_comparison::<UInt64Type, O, LEFT>,
        DataType::Float32 => scalar_comparison::<Float32Type, O, LEFT>,
        DataType::Float64 => scalar_comparison::<Float64Type, O, LEFT>,
        _ => {
            let empty = new_empty_array(data_type);
            O::arrow(&empty, &empty).map_err(|e| Error::InvalidPlan(e.to_string()))?;
            arrow_scalar_comparison::<O, LEFT>
        }
    })
}

fn scalar_comparison<T: ArrowPrimitiveType, O: ComparisonOperation, const LEFT: bool>(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    // Retain the bound primitive type contract before delegating bitmap
    // production and scalar NULL handling to Arrow's comparison kernel.
    primitive::<T>(left)?;
    primitive::<T>(right)?;
    arrow_scalar_comparison::<O, LEFT>(left, right)
}

fn arrow_scalar_comparison<O: ComparisonOperation, const LEFT: bool>(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    Ok(Arc::new(if LEFT {
        O::arrow(&Scalar::new(left), right)?
    } else {
        O::arrow(left, &Scalar::new(right))?
    }))
}
