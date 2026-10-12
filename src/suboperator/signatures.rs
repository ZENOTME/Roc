//! Validate signatures before code generation, including on empty inputs.
use super::{FunctionKind, scalar};
use crate::{
    arrow::datatypes::DataType,
    error::{Error, Result},
};
pub(crate) fn unary(f: FunctionKind, t: &DataType) -> Result<()> {
    use FunctionKind::*;
    if match f {
        IsNull | IsNotNull => scalar::supported(t),
        Negate => {
            scalar::numeric(t)
                && !matches!(
                    t,
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                )
        }
        _ => false,
    } {
        Ok(())
    } else {
        Err(Error::InvalidPlan(format!(
            "unsupported unary signature: {f:?}({t})"
        )))
    }
}
pub(crate) fn binary(f: FunctionKind, t: &DataType) -> Result<()> {
    use FunctionKind::*;
    if match f {
        Add | Subtract | Multiply | Divide | Remainder => scalar::numeric(t),
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual
        | IsDistinctFrom | IsNotDistinctFrom => scalar::supported(t),
        _ => false,
    } {
        Ok(())
    } else {
        Err(Error::InvalidPlan(format!(
            "unsupported binary signature: {f:?}({t})"
        )))
    }
}
pub(crate) fn cast(from: &DataType, to: &DataType) -> bool {
    scalar::supported(from)
        && scalar::supported(to)
        && (from == to
            || *from == DataType::Null
            || ((scalar::numeric(from) || scalar::bytes_type(from) || *from == DataType::Boolean)
                && (scalar::numeric(to) || scalar::bytes_type(to) || *to == DataType::Boolean)))
}
