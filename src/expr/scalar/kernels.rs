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
use super::value::ScalarPrimitiveType;
use super::{ColumnValue, FunctionKind, ScalarValue};
use crate::error::{Error, ErrorKind, Result};
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

pub(super) type EvalFn = fn(&[ColumnValue]) -> Result<ColumnValue>;

// Only the arithmetic kernels use this translation. Other Arrow operations
// decide their own failure semantics at their call sites.
#[cold]
#[track_caller]
fn arithmetic_error(source: ArrowError) -> Error {
    let error = match &source {
        ArrowError::DivideByZero => Error::new(ErrorKind::DivisionByZero, "division by zero"),
        ArrowError::ArithmeticOverflow(_) => {
            Error::new(ErrorKind::ArithmeticOverflow, "arithmetic overflow")
        }
        // Operand lengths/types are checked before invoking these primitive
        // kernels. Other failures violate that execution invariant.
        ArrowError::NotYetImplemented(_)
        | ArrowError::ExternalError(_)
        | ArrowError::IoError(_, _)
        | ArrowError::DictionaryKeyOverflowError
        | ArrowError::RunEndIndexOverflowError
        | ArrowError::OffsetOverflowError(_)
        | ArrowError::CastError(_)
        | ArrowError::MemoryError(_)
        | ArrowError::ParseError(_)
        | ArrowError::SchemaError(_)
        | ArrowError::ComputeError(_)
        | ArrowError::CsvError(_)
        | ArrowError::JsonError(_)
        | ArrowError::AvroError(_)
        | ArrowError::IpcError(_)
        | ArrowError::InvalidArgumentError(_)
        | ArrowError::ParquetError(_)
        | ArrowError::CDataInterface(_) => {
            Error::internal("checked arithmetic kernel failed".into())
        }
    };
    error.with_source(source)
}

#[cold]
#[track_caller]
fn comparison_error(source: ArrowError) -> Error {
    Error::invalid_input("cannot compare input values".into()).with_source(source)
}

fn unary_input(input: &[ColumnValue]) -> Result<&ColumnValue> {
    let [value] = input else {
        return Err(Error::internal(
            "unary function requires one input result".into(),
        ));
    };
    Ok(value)
}
fn binary_input(input: &[ColumnValue]) -> Result<(&ColumnValue, &ColumnValue)> {
    let [left, right] = input else {
        return Err(Error::internal(
            "binary function requires two input results".into(),
        ));
    };
    if let (ColumnValue::Array(left), ColumnValue::Array(right)) = (left, right) {
        if left.len() != right.len() {
            return Err(Error::invalid_input("binary input lengths differ".into()));
        }
    }
    Ok((left, right))
}
fn null_test<const NOT: bool>(input: &[ColumnValue]) -> Result<ColumnValue> {
    Ok(match unary_input(input)? {
        ColumnValue::Scalar(value) => {
            ColumnValue::Scalar(ScalarValue::Boolean(Some(value.is_null() != NOT)))
        }
        ColumnValue::Array(value) => ColumnValue::Array(Arc::new(if NOT {
            is_not_null(value.as_ref()).map_err(|source| {
                Error::internal("failed to evaluate IS NOT NULL".into()).with_source(source)
            })?
        } else {
            is_null(value.as_ref()).map_err(|source| {
                Error::internal("failed to evaluate IS NULL".into()).with_source(source)
            })?
        })),
    })
}

pub(super) fn bind_unary(function: FunctionKind, data_type: &DataType) -> Result<EvalFn> {
    use FunctionKind::*;
    Ok(match function {
        IsNull => null_test::<false>,
        IsNotNull => null_test::<true>,
        Negate => match data_type {
            DataType::Int8 => negate::<Int8Type, false>,
            DataType::Int16 => negate::<Int16Type, false>,
            DataType::Int32 => negate::<Int32Type, false>,
            DataType::Int64 => negate::<Int64Type, false>,
            DataType::Float32 => negate::<Float32Type, true>,
            DataType::Float64 => negate::<Float64Type, true>,
            _ => {
                return Err(Error::unsupported(format!(
                    "unsupported negation type: {data_type}"
                )));
            }
        },
        _ => return Err(Error::invalid_plan(format!("{function:?} is not unary"))),
    })
}

fn negate<T: ScalarPrimitiveType, const FLOAT: bool>(input: &[ColumnValue]) -> Result<ColumnValue> {
    Ok(match unary_input(input)? {
        ColumnValue::Scalar(value) => {
            let value = T::scalar(value)?
                .map(|v| {
                    if FLOAT {
                        Ok(v.neg_wrapping())
                    } else {
                        v.neg_checked()
                    }
                })
                .transpose()
                .map_err(|source| arithmetic_error(source))?;
            ColumnValue::Scalar(T::value(value))
        }
        ColumnValue::Array(value) => {
            let value = primitive::<T>(value)?;
            let output: PrimitiveArray<T> = if FLOAT {
                value.unary(|v| v.neg_wrapping())
            } else {
                value
                    .try_unary(|v| v.neg_checked())
                    .map_err(|source| arithmetic_error(source))?
            };
            ColumnValue::Array(Arc::new(output))
        }
    })
}

pub(super) fn bind_binary(function: FunctionKind, data_type: &DataType) -> Result<EvalFn> {
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
        _ => Err(Error::invalid_plan(format!("{function:?} is not binary"))),
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

fn bind_arithmetic<O: ArithmeticOperation>(data_type: &DataType) -> Result<EvalFn> {
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
            return Err(Error::unsupported(format!(
                "unsupported numeric type: {data_type}"
            )));
        }
    })
}

fn arithmetic<T: ScalarPrimitiveType, O: ArithmeticOperation, const FLOAT: bool>(
    input: &[ColumnValue],
) -> Result<ColumnValue> {
    let (left, right) = binary_input(input)?;
    let operation = |left, right| {
        if FLOAT {
            Ok(O::float(left, right))
        } else {
            O::checked(left, right)
        }
    };
    Ok(match (left, right) {
        (ColumnValue::Array(left), ColumnValue::Array(right)) => {
            let left = primitive::<T>(left)?;
            let right = primitive::<T>(right)?;
            let output: PrimitiveArray<T> = if FLOAT {
                binary(left, right, O::float).map_err(|source| arithmetic_error(source))?
            } else {
                try_binary(left, right, O::checked).map_err(|source| arithmetic_error(source))?
            };
            ColumnValue::Array(Arc::new(output))
        }
        (ColumnValue::Scalar(left), ColumnValue::Scalar(right)) => {
            let value = match (T::scalar(left)?, T::scalar(right)?) {
                (Some(left), Some(right)) => {
                    Some(operation(left, right).map_err(|source| arithmetic_error(source))?)
                }
                _ => None,
            };
            ColumnValue::Scalar(T::value(value))
        }
        (ColumnValue::Array(array), ColumnValue::Scalar(scalar))
        | (ColumnValue::Scalar(scalar), ColumnValue::Array(array)) => {
            let left_scalar = matches!(left, ColumnValue::Scalar(_));
            let array = primitive::<T>(array)?;
            let output = match T::scalar(scalar)? {
                None => PrimitiveArray::<T>::new_null(array.len()),
                Some(scalar) if FLOAT => array.unary(|v| {
                    if left_scalar {
                        O::float(scalar, v)
                    } else {
                        O::float(v, scalar)
                    }
                }),
                // Checked Arrow unary evaluates only valid slots; NULL/0 must not fail.
                Some(scalar) => array
                    .try_unary(|v| {
                        if left_scalar {
                            O::checked(scalar, v)
                        } else {
                            O::checked(v, scalar)
                        }
                    })
                    .map_err(|source| arithmetic_error(source))?,
            };
            ColumnValue::Array(Arc::new(output))
        }
    })
}

fn primitive<T: ArrowPrimitiveType>(value: &ArrayRef) -> Result<&PrimitiveArray<T>> {
    value
        .as_any()
        .downcast_ref()
        .ok_or_else(|| Error::invalid_input(format!("expected {} array", T::DATA_TYPE)))
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

fn bind_comparison<O: ComparisonOperation>(data_type: &DataType) -> Result<EvalFn> {
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
            O::arrow(&empty, &empty).map_err(|source| {
                Error::unsupported(format!("unsupported comparison type: {data_type}"))
                    .with_source(source)
            })?;
            arrow_comparison::<O>
        }
    })
}

fn flat_comparison<T: ArrowPrimitiveType, O: ComparisonOperation>(
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

fn comparison<T: ScalarPrimitiveType, O: ComparisonOperation>(
    input: &[ColumnValue],
) -> Result<ColumnValue> {
    let (left, right) = binary_input(input)?;
    Ok(match (left, right) {
        (ColumnValue::Array(left), ColumnValue::Array(right)) => {
            ColumnValue::Array(flat_comparison::<T, O>(left, right)?)
        }
        (ColumnValue::Scalar(left), ColumnValue::Scalar(right)) => {
            let value = match (T::scalar(left)?, T::scalar(right)?) {
                (Some(left), Some(right)) => Some(O::compare(left, right)),
                (left, right) if O::NULL_SAFE => {
                    Some(O::null_result(left.is_some(), right.is_some()))
                }
                _ => None,
            };
            ColumnValue::Scalar(ScalarValue::Boolean(value))
        }
        (ColumnValue::Scalar(scalar), ColumnValue::Array(array)) => {
            ColumnValue::Array(scalar_comparison::<T, O, true>(scalar, array)?)
        }
        (ColumnValue::Array(array), ColumnValue::Scalar(scalar)) => {
            ColumnValue::Array(scalar_comparison::<T, O, false>(scalar, array)?)
        }
    })
}

/// Arrow Datum provides broadcasting for non-numeric comparisons.
fn arrow_comparison<O: ComparisonOperation>(input: &[ColumnValue]) -> Result<ColumnValue> {
    let (left, right) = binary_input(input)?;
    let output = match (left, right) {
        (ColumnValue::Array(left), ColumnValue::Array(right)) => {
            O::arrow(left, right).map_err(|source| comparison_error(source))?
        }
        (ColumnValue::Scalar(left), ColumnValue::Array(right)) => {
            O::arrow(&Scalar::new(left.to_array()?), right)
                .map_err(|source| comparison_error(source))?
        }
        (ColumnValue::Array(left), ColumnValue::Scalar(right)) => {
            O::arrow(left, &Scalar::new(right.to_array()?))
                .map_err(|source| comparison_error(source))?
        }
        (ColumnValue::Scalar(left), ColumnValue::Scalar(right)) => {
            let value: ArrayRef = Arc::new(
                O::arrow(
                    &Scalar::new(left.to_array()?),
                    &Scalar::new(right.to_array()?),
                )
                .map_err(|source| comparison_error(source))?,
            );
            return Ok(ColumnValue::Scalar(ScalarValue::try_from_array(&value, 0)?));
        }
    };
    Ok(ColumnValue::Array(Arc::new(output)))
}

fn scalar_comparison<T: ScalarPrimitiveType, O: ComparisonOperation, const LEFT: bool>(
    scalar: &ScalarValue,
    array: &ArrayRef,
) -> Result<ArrayRef> {
    // Check the bound operand type before using Arrow Datum broadcasting.
    T::scalar(scalar)?;
    primitive::<T>(array)?;
    let scalar = Scalar::new(scalar.to_array()?);
    Ok(Arc::new(if LEFT {
        O::arrow(&scalar, array).map_err(|source| {
            Error::internal("failed to compare validated numeric operands".into())
                .with_source(source)
        })?
    } else {
        O::arrow(array, &scalar).map_err(|source| {
            Error::internal("failed to compare validated numeric operands".into())
                .with_source(source)
        })?
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_pairs_match_broadcast_for_numeric_types_and_nulls() {
        let functions = [
            FunctionKind::Add,
            FunctionKind::Subtract,
            FunctionKind::Multiply,
            FunctionKind::Divide,
            FunctionKind::Remainder,
            FunctionKind::Equal,
            FunctionKind::NotEqual,
            FunctionKind::LessThan,
            FunctionKind::LessThanOrEqual,
            FunctionKind::GreaterThan,
            FunctionKind::GreaterThanOrEqual,
            FunctionKind::IsDistinctFrom,
            FunctionKind::IsNotDistinctFrom,
        ];
        macro_rules! check {
            ($ty:ty, $native:ty) => {
                for left in [Some(2 as $native), Some(0 as $native), None] {
                    for right in [Some(3 as $native), Some(0 as $native), None] {
                        for function in functions {
                            let eval = bind_binary(function, &<$ty>::DATA_TYPE).unwrap();
                            let left = <$ty>::value(left);
                            let right = <$ty>::value(right);
                            let expected = eval(&[
                                ColumnValue::Array(left.to_array_of_size(3).unwrap()),
                                ColumnValue::Array(right.to_array_of_size(3).unwrap()),
                            ]);
                            let actual =
                                eval(&[ColumnValue::Scalar(left), ColumnValue::Scalar(right)]);
                            match (actual, expected) {
                                (Ok(actual), Ok(expected)) => {
                                    assert!(matches!(actual, ColumnValue::Scalar(_)));
                                    assert_eq!(
                                        actual.into_array(3).unwrap().to_data(),
                                        expected.into_array(3).unwrap().to_data()
                                    );
                                }
                                (Err(actual), Err(expected)) => {
                                    assert_eq!(actual.to_string(), expected.to_string())
                                }
                                (actual, expected) => {
                                    panic!("{function:?}: {actual:?} != {expected:?}")
                                }
                            }
                        }
                    }
                }
            };
        }
        check!(Int8Type, i8);
        check!(Int16Type, i16);
        check!(Int32Type, i32);
        check!(Int64Type, i64);
        check!(UInt8Type, u8);
        check!(UInt16Type, u16);
        check!(UInt32Type, u32);
        check!(UInt64Type, u64);
        check!(Float32Type, f32);
        check!(Float64Type, f64);
    }

    #[test]
    fn bound_functions_validate_arity_lengths_and_scalar_types() {
        let add = bind_binary(FunctionKind::Add, &DataType::Int64).unwrap();
        assert!(add(&[]).is_err());
        assert!(add(&[ColumnValue::Scalar(ScalarValue::Int64(Some(1)))]).is_err());
        assert!(
            add(&[
                ColumnValue::Array(Arc::new(arrow::array::Int64Array::from(vec![1, 2]))),
                ColumnValue::Array(Arc::new(arrow::array::Int64Array::from(vec![3])))
            ])
            .is_err()
        );
        assert!(
            add(&[
                ColumnValue::Scalar(ScalarValue::Int64(Some(1))),
                ColumnValue::Scalar(ScalarValue::Int32(Some(2)))
            ])
            .is_err()
        );
    }
}
