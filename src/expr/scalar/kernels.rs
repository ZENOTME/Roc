//! Select operations, primitive types, and broadcasting during initialization.
use super::{ScalarFunction, executor::ExpressionResult};
use crate::error::{Error, Result};
use arrow::{
    array::{
        Array, ArrayRef, ArrowNativeTypeOp, BooleanArray, Datum, PrimitiveArray, new_empty_array,
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

pub type ScalarFunction = fn(&[ArrayRef]) -> Result<ArrayRef>;

pub fn is_number(t: &DataType) -> bool {
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

pub(super) fn prepare(
    function: ScalarFunction,
    args: &[ExpressionResult],
) -> Result<ExpressionResult> {
    use ScalarFunction::*;
    let arity = if matches!(function, Negate | IsNull | IsNotNull) {
        1
    } else {
        2
    };
    if args.len() != arity {
        return Err(Error::InvalidPlan(format!(
            "{function:?} requires {arity} arguments"
        )));
    }
    if arity == 2 && args[0].data_type != args[1].data_type {
        return Err(Error::InvalidPlan(format!(
            "{function:?} requires matching types; supply explicit casts"
        )));
    }
    if matches!(
        function,
        Add | Subtract | Multiply | Divide | Remainder | Negate
    ) && !is_number(&args[0].data_type)
    {
        return Err(Error::InvalidPlan(format!(
            "unsupported numeric type: {}",
            args[0].data_type
        )));
    }
    Ok(ExpressionResult {
        data_type: if matches!(
            function,
            Add | Subtract | Multiply | Divide | Remainder | Negate
        ) {
            args[0].data_type.clone()
        } else {
            DataType::Boolean
        },
        nullable: !matches!(
            function,
            IsNull | IsNotNull | IsDistinctFrom | IsNotDistinctFrom
        ) && args.iter().any(|a| a.nullable),
    })
}

/// Arity and argument types have been resolved by prepare(). Shape is bound once.
pub(super) fn bind(
    function: ScalarFunction,
    data_type: &DataType,
    scalars: &[bool],
) -> Result<ScalarFunction> {
    if matches!(
        function,
        ScalarFunction::Negate | ScalarFunction::IsNull | ScalarFunction::IsNotNull
    ) {
        bind_unary(function, data_type)
    } else {
        bind_binary(function, data_type, scalars[0], scalars[1])
    }
}

fn bind_unary(function: ScalarFunction, data_type: &DataType) -> Result<ScalarFunction> {
    use ScalarFunction::*;
    Ok(match function {
        IsNull => |arguments| Ok(Arc::new(is_null(arguments[0].as_ref())?)),
        IsNotNull => |arguments| Ok(Arc::new(is_not_null(arguments[0].as_ref())?)),
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

fn negate<T: ArrowPrimitiveType, const FLOAT: bool>(arguments: &[ArrayRef]) -> Result<ArrayRef> {
    let value = primitive::<T>(&arguments[0])?;
    let output: PrimitiveArray<T> = if FLOAT {
        value.unary(|v| v.neg_wrapping())
    } else {
        value.try_unary(|v| v.neg_checked())?
    };
    Ok(Arc::new(output))
}

fn bind_binary(
    function: ScalarFunction,
    data_type: &DataType,
    left_scalar: bool,
    right_scalar: bool,
) -> Result<ScalarFunction> {
    match (left_scalar, right_scalar) {
        (false, false) => bind_binary_shape::<false, false>(function, data_type),
        (false, true) => bind_binary_shape::<false, true>(function, data_type),
        (true, false) => bind_binary_shape::<true, false>(function, data_type),
        (true, true) => bind_binary_shape::<true, true>(function, data_type),
    }
}

fn bind_binary_shape<const L: bool, const R: bool>(
    function: ScalarFunction,
    data_type: &DataType,
) -> Result<ScalarFunction> {
    use ScalarFunction::*;
    match function {
        Add => bind_arithmetic::<AddOp, L, R>(data_type),
        Subtract => bind_arithmetic::<SubtractOp, L, R>(data_type),
        Multiply => bind_arithmetic::<MultiplyOp, L, R>(data_type),
        Divide => bind_arithmetic::<DivideOp, L, R>(data_type),
        Remainder => bind_arithmetic::<RemainderOp, L, R>(data_type),
        Equal => bind_comparison::<EqualOp, L, R>(data_type),
        NotEqual => bind_comparison::<NotEqualOp, L, R>(data_type),
        LessThan => bind_comparison::<LessOp, L, R>(data_type),
        LessThanOrEqual => bind_comparison::<LessEqualOp, L, R>(data_type),
        GreaterThan => bind_comparison::<GreaterOp, L, R>(data_type),
        GreaterThanOrEqual => bind_comparison::<GreaterEqualOp, L, R>(data_type),
        IsDistinctFrom => bind_comparison::<DistinctOp, L, R>(data_type),
        IsNotDistinctFrom => bind_comparison::<NotDistinctOp, L, R>(data_type),
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

fn bind_arithmetic<O: ArithmeticOperation, const L: bool, const R: bool>(
    data_type: &DataType,
) -> Result<ScalarFunction> {
    Ok(match data_type {
        DataType::Int8 => arithmetic::<Int8Type, O, L, R, false>,
        DataType::Int16 => arithmetic::<Int16Type, O, L, R, false>,
        DataType::Int32 => arithmetic::<Int32Type, O, L, R, false>,
        DataType::Int64 => arithmetic::<Int64Type, O, L, R, false>,
        DataType::UInt8 => arithmetic::<UInt8Type, O, L, R, false>,
        DataType::UInt16 => arithmetic::<UInt16Type, O, L, R, false>,
        DataType::UInt32 => arithmetic::<UInt32Type, O, L, R, false>,
        DataType::UInt64 => arithmetic::<UInt64Type, O, L, R, false>,
        DataType::Float32 => arithmetic::<Float32Type, O, L, R, true>,
        DataType::Float64 => arithmetic::<Float64Type, O, L, R, true>,
        _ => {
            return Err(Error::InvalidPlan(format!(
                "unsupported numeric type: {data_type}"
            )));
        }
    })
}

fn arithmetic<
    T: ArrowPrimitiveType,
    O: ArithmeticOperation,
    const L: bool,
    const R: bool,
    const FLOAT: bool,
>(
    arguments: &[ArrayRef],
) -> Result<ArrayRef> {
    let left = primitive::<T>(&arguments[0])?;
    let right = primitive::<T>(&arguments[1])?;
    let output: PrimitiveArray<T> = if L && !R {
        if left.is_null(0) {
            PrimitiveArray::new_null(right.len())
        } else if FLOAT {
            right.unary(|r| O::float(left.value(0), r))
        } else {
            right.try_unary(|r| O::checked(left.value(0), r))?
        }
    } else if !L && R {
        if right.is_null(0) {
            PrimitiveArray::new_null(left.len())
        } else if FLOAT {
            left.unary(|l| O::float(l, right.value(0)))
        } else {
            left.try_unary(|l| O::checked(l, right.value(0)))?
        }
    } else if FLOAT {
        binary(left, right, O::float)?
    } else {
        try_binary(left, right, O::checked)?
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

fn bind_comparison<O: ComparisonOperation, const L: bool, const R: bool>(
    data_type: &DataType,
) -> Result<ScalarFunction> {
    Ok(match data_type {
        DataType::Int8 => comparison::<Int8Type, O, L, R>,
        DataType::Int16 => comparison::<Int16Type, O, L, R>,
        DataType::Int32 => comparison::<Int32Type, O, L, R>,
        DataType::Int64 => comparison::<Int64Type, O, L, R>,
        DataType::UInt8 => comparison::<UInt8Type, O, L, R>,
        DataType::UInt16 => comparison::<UInt16Type, O, L, R>,
        DataType::UInt32 => comparison::<UInt32Type, O, L, R>,
        DataType::UInt64 => comparison::<UInt64Type, O, L, R>,
        DataType::Float32 => comparison::<Float32Type, O, L, R>,
        DataType::Float64 => comparison::<Float64Type, O, L, R>,
        _ => {
            // Preserve Arrow's other comparison signatures. This fallback still
            // dispatches types inside Arrow; primitive numeric kernels do not.
            let empty = new_empty_array(data_type);
            O::arrow(&empty, &empty).map_err(|e| Error::InvalidPlan(e.to_string()))?;
            arrow_comparison::<O, L, R>
        }
    })
}

fn comparison<T: ArrowPrimitiveType, O: ComparisonOperation, const L: bool, const R: bool>(
    arguments: &[ArrayRef],
) -> Result<ArrayRef> {
    let left = primitive::<T>(&arguments[0])?;
    let right = primitive::<T>(&arguments[1])?;
    let len = if L && !R { right.len() } else { left.len() };
    let nulls = if O::NULL_SAFE {
        None
    } else if L && !R {
        if left.is_null(0) {
            return Ok(Arc::new(BooleanArray::new_null(len)));
        }
        right.nulls().cloned()
    } else if !L && R {
        if right.is_null(0) {
            return Ok(Arc::new(BooleanArray::new_null(len)));
        }
        left.nulls().cloned()
    } else {
        NullBuffer::union(left.nulls(), right.nulls())
    };
    let values = if !O::NULL_SAFE || (left.null_count() == 0 && right.null_count() == 0) {
        // Ordinary comparison validity comes from the output bitmap; comparison
        // itself cannot fail, so NULL slots need no per-row validity branch.
        BooleanBuffer::collect_bool(len, |i| {
            O::compare(
                left.value(if L { 0 } else { i }),
                right.value(if R { 0 } else { i }),
            )
        })
    } else {
        BooleanBuffer::collect_bool(len, |i| {
            let li = if L { 0 } else { i };
            let ri = if R { 0 } else { i };
            let lv = left.is_valid(li);
            let rv = right.is_valid(ri);
            if !lv || !rv {
                O::null_result(lv, rv)
            } else {
                O::compare(left.value(li), right.value(ri))
            }
        })
    };
    Ok(Arc::new(BooleanArray::new(values, nulls)))
}

// A borrowed Datum adapter carries statically bound shape flags, without Arc clones.
struct ArrayDatum<'a, const SCALAR: bool>(&'a ArrayRef);
impl<const SCALAR: bool> Datum for ArrayDatum<'_, SCALAR> {
    fn get(&self) -> (&dyn Array, bool) {
        (self.0.as_ref(), SCALAR)
    }
}
fn arrow_comparison<O: ComparisonOperation, const L: bool, const R: bool>(
    arguments: &[ArrayRef],
) -> Result<ArrayRef> {
    Ok(Arc::new(O::arrow(
        &ArrayDatum::<L>(&arguments[0]),
        &ArrayDatum::<R>(&arguments[1]),
    )?))
}
