use super::executor::{ExpressionResult, ExpressionValue};
use crate::{
    error::{Error, Result},
    expr::scalar::ScalarFunction,
};
use arrow::{
    array::{ArrayRef, new_empty_array},
    compute::{
        is_not_null, is_null,
        kernels::{cmp, numeric},
    },
    datatypes::DataType,
};
use std::sync::Arc;

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
    // Validate the exact Arrow signature without evaluating any user values.
    let empty = args
        .iter()
        .map(|a| ExpressionValue::Array(new_empty_array(&a.data_type)))
        .collect::<Vec<_>>();
    let output = execute(function, &empty).map_err(|e| Error::InvalidPlan(e.to_string()))?;
    Ok(ExpressionResult {
        data_type: output.datum().get().0.data_type().clone(),
        nullable: !matches!(
            function,
            IsNull | IsNotNull | IsDistinctFrom | IsNotDistinctFrom
        ) && args.iter().any(|a| a.nullable),
    })
}

pub(super) fn execute(
    function: ScalarFunction,
    args: &[ExpressionValue],
) -> Result<ExpressionValue> {
    use ScalarFunction::*;
    let left = args[0].datum();
    let array: ArrayRef = match function {
        Negate => numeric::neg(left.get().0)?,
        IsNull => Arc::new(is_null(left.get().0)?),
        IsNotNull => Arc::new(is_not_null(left.get().0)?),
        _ => {
            let right = args[1].datum();
            match function {
                Add => numeric::add(left, right)?,
                Subtract => numeric::sub(left, right)?,
                Multiply => numeric::mul(left, right)?,
                Divide => numeric::div(left, right)?,
                Remainder => numeric::rem(left, right)?,
                Equal => Arc::new(cmp::eq(left, right)?),
                NotEqual => Arc::new(cmp::neq(left, right)?),
                LessThan => Arc::new(cmp::lt(left, right)?),
                LessThanOrEqual => Arc::new(cmp::lt_eq(left, right)?),
                GreaterThan => Arc::new(cmp::gt(left, right)?),
                GreaterThanOrEqual => Arc::new(cmp::gt_eq(left, right)?),
                IsDistinctFrom => Arc::new(cmp::distinct(left, right)?),
                IsNotDistinctFrom => Arc::new(cmp::not_distinct(left, right)?),
                _ => unreachable!(),
            }
        }
    };
    if args.iter().all(ExpressionValue::is_scalar) {
        ExpressionValue::scalar(array)
    } else {
        Ok(ExpressionValue::Array(array))
    }
}
