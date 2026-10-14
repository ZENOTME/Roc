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

//! Variables, row domains and kernels.
use super::StateId;
use crate::arrow::datatypes::DataType;
use crate::error::{Error, ErrorKind, Result};
use std::sync::Arc;

/// Meaningful only within its KernelBuilder; independent of ColumnId.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VariableId(pub(crate) usize);

impl VariableId {
    pub fn index(self) -> usize {
        self.0
    }
}

/// Physical value types. Nullability is not part of a variable: validity is a
/// separate Boolean variable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VariableType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    /// The unscaled value of a decimal.
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    BytePointer,
    Usize,
}

impl VariableType {
    pub fn bits(self) -> u32 {
        match self {
            Self::Boolean | Self::Int8 | Self::UInt8 => 8,
            Self::Int16 | Self::UInt16 => 16,
            Self::Int32 | Self::UInt32 | Self::Float32 => 32,
            Self::Int128 => 128,
            _ => 64,
        }
    }
    pub fn is_signed(self) -> bool {
        matches!(
            self,
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 | Self::Int128
        )
    }
    pub fn is_float(self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }
    pub fn is_integer(self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
        )
    }
    pub fn is_numeric(self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// The value variable type of a fixed-width logical type. Variable-width and
    /// NULL types have no single value variable.
    pub fn of(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 | DataType::Date32 => Self::Int32,
            DataType::Int64 | DataType::Date64 | DataType::Timestamp(..) => Self::Int64,
            DataType::Decimal128(..) => Self::Int128,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            _ => return None,
        })
    }
}

/// A row in a domain is meaningful only within that domain.
#[derive(Clone, Copy, Debug)]
pub struct Variable {
    pub ty: VariableType,
    pub domain: DomainId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DomainId(pub(crate) usize);

impl DomainId {
    /// One row per row of the current root slice.
    pub const ROOT: DomainId = DomainId(0);

    pub fn index(self) -> usize {
        self.0
    }
}

/// A row space. Domains form a tree whose root corresponds to one slice of the
/// input chunk.
///
/// Every non-root domain has `parent_row`: a Usize variable of the domain that
/// maps each row to its row in the direct parent, monotone non-decreasing. Its
/// values are slice-local indices for the execution layer only and never take
/// part in query semantics.
///
/// A domain may read the variables of itself and of its ancestors. Variables of
/// siblings or descendants reach it only through Union or Reduce. How an
/// ancestor reference is realized (an alias when fused, compaction or a gather
/// otherwise) is an execution strategy.
#[derive(Clone, Copy, Debug)]
pub struct RowDomain {
    /// None for the root. The parent of a Filter / Expand / Reduce domain is its
    /// input domain; the parent of a Union domain is the common ancestor.
    pub parent: Option<DomainId>,
    pub parent_row: Option<VariableId>,
}

/// A unit of row-wise semantics in one domain. A kernel is not an
/// implementation: it may be compiled alone or fused with others, with
/// identical results.
#[derive(Clone, Debug)]
pub enum Kernel {
    Scalar(ScalarKernel),
    Domain(DomainKernel),
}

/// Executes row by row in one domain D without changing it.
///
/// Contract:
///   - Inputs are variables of D or its ancestors and declared states; outputs
///     are new variables of D or state updates.
///   - A row's result depends only on that row (and its ancestor rows).
///   - Total: every input produces a result. Fallible operations output a
///     Boolean error flag; RaiseIf is the only kernel that fails execution.
///   - Pointer outputs reference valid memory on every row, including NULL rows
///     and rows whose result is discarded.
///   - Within one root slice, no row reads state written by another row.
///
/// NULL handling is not part of a kernel: kernels compute values and lowering
/// combines validity with separate kernels.
#[derive(Clone, Debug)]
pub enum ScalarKernel {
    /// The same value on every row of the domain.
    Constant {
        value: ConstantValue,
        output: VariableId,
    },
    /// A pure row function. The function determines input and output types;
    /// fallible functions output error flags after their values.
    Call {
        function: ScalarFunction,
        inputs: Vec<VariableId>,
        outputs: Vec<VariableId>,
    },
    Select {
        cond: VariableId,
        then: VariableId,
        otherwise: VariableId,
        output: VariableId,
    },
    /// Fails execution when cond is true on any row. Which row or error is
    /// reported is unspecified; `args` are row values included in the error.
    RaiseIf {
        cond: VariableId,
        error: KernelError,
        args: Vec<VariableId>,
    },
    /// Finds or inserts the group of the key row and outputs its UInt64 ID.
    /// Inserting is an effect: it runs even when the ID is unused.
    GroupLookup {
        state: StateId,
        keys: Vec<ColumnVariableBinding>,
        output: VariableId,
    },
    /// Accumulates one row into a group of an accumulator state.
    AggregateUpdate {
        state: StateId,
        kind: AggregateUpdateKind,
        group: VariableId,
        arguments: Vec<ColumnVariableBinding>,
    },
}

impl ScalarKernel {
    pub fn inputs(&self) -> Vec<VariableId> {
        match self {
            Self::Constant { .. } => vec![],
            Self::Call { inputs, .. } => inputs.clone(),
            Self::Select {
                cond,
                then,
                otherwise,
                ..
            } => vec![*cond, *then, *otherwise],
            Self::RaiseIf { cond, args, .. } => {
                std::iter::once(*cond).chain(args.clone()).collect()
            }
            Self::GroupLookup { keys, .. } => keys.iter().flat_map(|k| k.variables()).collect(),
            Self::AggregateUpdate {
                group, arguments, ..
            } => std::iter::once(*group)
                .chain(arguments.iter().flat_map(|a| a.variables()))
                .collect(),
        }
    }
    pub fn outputs(&self) -> Vec<VariableId> {
        match self {
            Self::Constant { output, .. }
            | Self::Select { output, .. }
            | Self::GroupLookup { output, .. } => vec![*output],
            Self::Call { outputs, .. } => outputs.clone(),
            Self::RaiseIf { .. } | Self::AggregateUpdate { .. } => vec![],
        }
    }
    pub fn states(&self) -> Vec<StateId> {
        match self {
            Self::GroupLookup { state, .. } | Self::AggregateUpdate { state, .. } => vec![*state],
            _ => vec![],
        }
    }
    /// Kernels that must run even when no output is used.
    pub fn has_effect(&self) -> bool {
        matches!(
            self,
            Self::RaiseIf { .. } | Self::GroupLookup { .. } | Self::AggregateUpdate { .. }
        )
    }
}

/// Creates a new domain. Common contract: the new domain gets a `parent_row`
/// defined by this kernel, and output variables belong to the new domain.
#[derive(Clone, Debug)]
pub enum DomainKernel {
    /// Keeps the rows of the input domain where predicate is true. The
    /// predicate is a non-null Boolean; validity is folded in by lowering.
    Filter {
        predicate: VariableId,
        output: DomainId,
    },
    /// Produces zero or more rows per input row.
    Expand {
        input: DomainId,
        kind: ExpandKind,
        outputs: Vec<VariableId>,
        output: DomainId,
    },
    /// Merges descendant domains of `ancestor`, ordered by ancestor row. Each
    /// arm passes its values like SSA block parameters.
    Union {
        ancestor: DomainId,
        arms: Vec<UnionArm>,
        outputs: Vec<VariableId>,
        output: DomainId,
    },
    /// One row per parent row: a grouped reduction of descendant rows.
    Reduce {
        parent: DomainId,
        descendant: DomainId,
        aggregates: Vec<(ReduceKind, VariableId)>,
        outputs: Vec<VariableId>,
        output: DomainId,
    },
}

impl DomainKernel {
    pub fn output(&self) -> DomainId {
        match self {
            Self::Filter { output, .. }
            | Self::Expand { output, .. }
            | Self::Union { output, .. }
            | Self::Reduce { output, .. } => *output,
        }
    }
    pub fn inputs(&self) -> Vec<VariableId> {
        match self {
            Self::Filter { predicate, .. } => vec![*predicate],
            Self::Expand { .. } => vec![],
            Self::Union { arms, .. } => arms.iter().flat_map(|a| a.values.clone()).collect(),
            Self::Reduce { aggregates, .. } => aggregates.iter().map(|(_, v)| *v).collect(),
        }
    }
    pub fn outputs(&self) -> Vec<VariableId> {
        match self {
            Self::Filter { .. } => vec![],
            Self::Expand { outputs, .. }
            | Self::Union { outputs, .. }
            | Self::Reduce { outputs, .. } => outputs.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct UnionArm {
    pub domain: DomainId,
    pub values: Vec<VariableId>,
}

/// Join probes, unnest and other expansions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpandKind {}

/// Count, Sum, Any and other per-parent reductions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceKind {}

/// The value of a Constant kernel.
#[derive(Clone, Debug)]
pub enum ConstantValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    /// The program retains the bytes; the variable holds their address.
    BytePointer(Arc<[u8]>),
    Usize(u64),
}

impl ConstantValue {
    pub fn ty(&self) -> VariableType {
        match self {
            Self::Boolean(_) => VariableType::Boolean,
            Self::Int8(_) => VariableType::Int8,
            Self::Int16(_) => VariableType::Int16,
            Self::Int32(_) => VariableType::Int32,
            Self::Int64(_) => VariableType::Int64,
            Self::Int128(_) => VariableType::Int128,
            Self::UInt8(_) => VariableType::UInt8,
            Self::UInt16(_) => VariableType::UInt16,
            Self::UInt32(_) => VariableType::UInt32,
            Self::UInt64(_) => VariableType::UInt64,
            Self::Float32(_) => VariableType::Float32,
            Self::Float64(_) => VariableType::Float64,
            Self::BytePointer(_) => VariableType::BytePointer,
            Self::Usize(_) => VariableType::Usize,
        }
    }
    /// The zero value of a type: the safe default of a NULL row.
    pub fn zero(ty: VariableType) -> Self {
        match ty {
            VariableType::Boolean => Self::Boolean(false),
            VariableType::Int8 => Self::Int8(0),
            VariableType::Int16 => Self::Int16(0),
            VariableType::Int32 => Self::Int32(0),
            VariableType::Int64 => Self::Int64(0),
            VariableType::Int128 => Self::Int128(0),
            VariableType::UInt8 => Self::UInt8(0),
            VariableType::UInt16 => Self::UInt16(0),
            VariableType::UInt32 => Self::UInt32(0),
            VariableType::UInt64 => Self::UInt64(0),
            VariableType::Float32 => Self::Float32(0.0),
            VariableType::Float64 => Self::Float64(0.0),
            VariableType::BytePointer => Self::BytePointer(Arc::from(&[][..])),
            VariableType::Usize => Self::Usize(0),
        }
    }
    /// Reverses `words` for fixed-width types.
    pub fn from_words(ty: VariableType, [lo, hi]: [u64; 2]) -> Self {
        match ty {
            VariableType::Int128 => Self::Int128((lo as u128 | (hi as u128) << 64) as i128),
            _ => Self::from_bits(ty, lo),
        }
    }

    /// The value in two words: the low word holds `bits`, the high word the
    /// rest of a 128-bit value (sign-extended).
    pub fn words(&self) -> [u64; 2] {
        match self {
            Self::Int128(v) => [*v as u64, (*v >> 64) as u64],
            other => {
                let bits = other.bits();
                [
                    bits,
                    if (bits as i64) < 0 && other.ty().is_signed() {
                        u64::MAX
                    } else {
                        0
                    },
                ]
            }
        }
    }

    /// Reverses `bits` for fixed-width types up to 64 bits.
    pub fn from_bits(ty: VariableType, bits: u64) -> Self {
        match ty {
            VariableType::Boolean => Self::Boolean(bits != 0),
            VariableType::Int8 => Self::Int8(bits as i8),
            VariableType::Int16 => Self::Int16(bits as i16),
            VariableType::Int32 => Self::Int32(bits as i32),
            VariableType::Int64 => Self::Int64(bits as i64),
            VariableType::Int128 => Self::Int128(bits as i64 as i128),
            VariableType::UInt8 => Self::UInt8(bits as u8),
            VariableType::UInt16 => Self::UInt16(bits as u16),
            VariableType::UInt32 => Self::UInt32(bits as u32),
            VariableType::UInt64 => Self::UInt64(bits),
            VariableType::Float32 => Self::Float32(f32::from_bits(bits as u32)),
            VariableType::Float64 => Self::Float64(f64::from_bits(bits)),
            VariableType::BytePointer | VariableType::Usize => Self::Usize(bits),
        }
    }

    /// Raw bits, sign-extended for signed integers; the low word of Int128.
    pub fn bits(&self) -> u64 {
        match self {
            Self::Int128(v) => *v as u64,
            Self::Boolean(v) => *v as u64,
            Self::Int8(v) => *v as i64 as u64,
            Self::Int16(v) => *v as i64 as u64,
            Self::Int32(v) => *v as i64 as u64,
            Self::Int64(v) => *v as u64,
            Self::UInt8(v) => *v as u64,
            Self::UInt16(v) => *v as u64,
            Self::UInt32(v) => *v as u64,
            Self::UInt64(v) => *v,
            Self::Float32(v) => v.to_bits() as u64,
            Self::Float64(v) => v.to_bits(),
            Self::BytePointer(v) => v.as_ptr() as u64,
            Self::Usize(v) => *v,
        }
    }
}

/// Row functions. Signatures are checked by `output_types`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScalarFunction {
    /// Integers output `[value, overflow]`; floats output `[value]`.
    Add,
    Subtract,
    Multiply,
    /// Signed integers output `[value, overflow]`; floats output `[value]`.
    Negate,
    /// Integers output `[value, divide_by_zero, overflow]`; floats `[value]`.
    Divide,
    /// Integers output `[value, divide_by_zero]`; floats `[value]`.
    Remainder,
    /// Integer division rounding halves away from zero: `[value,
    /// divide_by_zero]`. The quotient cannot overflow.
    DivideRound,
    /// Signed integers output `[value, overflow]`; unsigned integers and
    /// floats `[value]`.
    Abs,
    /// Floats rounded to an integer value, halves away from zero.
    Round,
    /// Integer bitwise operations: `[value]`.
    BitAnd,
    BitOr,
    /// `a` shifted right by `b` modulo its bits; signed types extend the sign.
    ShiftRight,
    /// Fixed-width operands `[a, b]`, or byte strings `[a_ptr, a_len, b_ptr,
    /// b_len]`. Floats use a total order. Outputs `[Boolean]`.
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    /// Two-valued logic on Boolean values.
    And,
    Or,
    Not,
    /// Between numeric and Boolean types: `[value, invalid]`, where invalid
    /// marks out-of-range or NaN inputs.
    Cast {
        to: VariableType,
    },
    /// `[ptr, len]` to `[value, invalid]`.
    ParseText {
        to: VariableType,
    },
    /// `[value]` to `[ptr, len]` in the invocation's string arena.
    FormatText,
    /// `[ptr, len]` to `[invalid]`.
    ValidateUtf8,
}

impl ScalarFunction {
    pub fn output_types(&self, inputs: &[VariableType]) -> Result<Vec<VariableType>> {
        use ScalarFunction::*;
        use VariableType as T;
        let invalid = || {
            Error::unsupported(format!(
                "unsupported kernel signature: {self:?}({inputs:?})"
            ))
        };
        Ok(match (self, inputs) {
            (Add | Subtract | Multiply, [a, b]) if a == b && a.is_integer() => {
                vec![*a, T::Boolean]
            }
            (Add | Subtract | Multiply | Divide | Remainder, [a, b]) if a == b && a.is_float() => {
                vec![*a]
            }
            (Negate, [a]) if a.is_signed() => vec![*a, T::Boolean],
            (Negate, [a]) if a.is_float() => vec![*a],
            (Divide, [a, b]) if a == b && a.is_integer() => vec![*a, T::Boolean, T::Boolean],
            (Remainder | DivideRound, [a, b]) if a == b && a.is_integer() => {
                vec![*a, T::Boolean]
            }
            (Abs, [a]) if a.is_signed() => vec![*a, T::Boolean],
            (Abs, [a]) if a.is_numeric() => vec![*a],
            (Round, [a]) if a.is_float() => vec![*a],
            (BitAnd | BitOr | ShiftRight, [a, b]) if a == b && a.is_integer() => vec![*a],
            (
                Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual,
                [a, b],
            ) if a == b && (a.is_numeric() || *a == T::Boolean) => {
                vec![T::Boolean]
            }
            (
                Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual,
                [T::BytePointer, T::Usize, T::BytePointer, T::Usize],
            ) => vec![T::Boolean],
            (And | Or, [T::Boolean, T::Boolean]) => vec![T::Boolean],
            (Not, [T::Boolean]) => vec![T::Boolean],
            (Cast { to }, [from])
                if (from.is_numeric() || *from == T::Boolean)
                    && (to.is_numeric() || *to == T::Boolean) =>
            {
                vec![*to, T::Boolean]
            }
            (ParseText { to }, [T::BytePointer, T::Usize])
                if to.is_numeric() || *to == T::Boolean =>
            {
                vec![*to, T::Boolean]
            }
            (FormatText, [from]) if from.is_numeric() || *from == T::Boolean => {
                vec![T::BytePointer, T::Usize]
            }
            (ValidateUtf8, [T::BytePointer, T::Usize]) => vec![T::Boolean],
            _ => return Err(invalid()),
        })
    }
}

/// Errors reported by RaiseIf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KernelError {
    ArithmeticOverflow,
    DivideByZero,
    /// args: the source text `[ptr, len]`, when the input is a string.
    InvalidCast,
    InvalidUtf8,
}

impl KernelError {
    /// Classifies a raised row failure. The Arrow error is the source Arrow's
    /// own kernels would report for the same row.
    pub(crate) fn to_error(self, args: &[u64]) -> Error {
        use crate::arrow::error::ArrowError;
        match self {
            Self::ArithmeticOverflow => {
                Error::new(ErrorKind::ArithmeticOverflow, "arithmetic overflow").with_source(
                    ArrowError::ArithmeticOverflow("generated integer arithmetic".into()),
                )
            }
            Self::DivideByZero => Error::new(ErrorKind::DivisionByZero, "division by zero")
                .with_source(ArrowError::DivideByZero),
            Self::InvalidCast => {
                let message = match args {
                    [ptr, len] => {
                        // SAFETY: RaiseIf passes a pointer output, valid on every row.
                        let text =
                            unsafe { std::slice::from_raw_parts(*ptr as *const u8, *len as usize) };
                        format!("invalid scalar cast: '{}'", String::from_utf8_lossy(text))
                    }
                    _ => "invalid scalar cast".into(),
                };
                Error::invalid_input("cast failed".into())
                    .with_source(ArrowError::CastError(message))
            }
            Self::InvalidUtf8 => Error::invalid_input("cast failed".into())
                .with_source(ArrowError::CastError("invalid UTF-8 cast".into())),
        }
    }
}

/// A bound accumulator update, selected during lowering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateUpdateKind {
    CountRows,
    CountValues,
    CountDistinct,
    SumI64,
    SumU64,
    SumF64,
    Avg,
    CovarPop,
    Min,
    Max,
}

/// How one column is represented by variables in the current pipeline. Each
/// variant is a physical value type; the logical type (for example Date32
/// versus Int32) is the column's ColumnType. All variables belong to one domain.
///
/// `validity` is a Boolean, true for non-NULL rows, matching the Arrow validity
/// bitmap. None means the binding contains no NULL; a NULL-typed column always
/// has a validity (constant false).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnVariableBinding {
    Null {
        validity: VariableId,
    },
    Boolean {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Int8 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Int16 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Int32 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Int64 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Int128 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    UInt8 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    UInt16 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    UInt32 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    UInt64 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Float32 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Float64 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    /// Utf8, Binary and their Large variants. `ptr` is a BytePointer and `len`
    /// a Usize.
    Bytes {
        ptr: VariableId,
        len: VariableId,
        validity: Option<VariableId>,
    },
}

macro_rules! fixed_bindings {
    ($m:ident) => {
        $m!(
            Boolean, Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, Float32,
            Float64
        )
    };
}

impl ColumnVariableBinding {
    /// A fixed-width binding of the given value type.
    pub fn fixed(
        ty: VariableType,
        value: VariableId,
        validity: Option<VariableId>,
    ) -> Result<Self> {
        macro_rules! arms {
            ($($v:ident),*) => {
                match ty {
                    $(VariableType::$v => Self::$v { value, validity },)*
                    _ => {
                        return Err(Error::invalid_plan(format!(
                            "{ty:?} is not a column value type"
                        )));
                    }
                }
            };
        }
        Ok(fixed_bindings!(arms))
    }

    pub fn validity(&self) -> Option<VariableId> {
        macro_rules! arms {
            ($($v:ident),*) => {
                match self {
                    $(Self::$v { validity, .. })|* | Self::Bytes { validity, .. } => *validity,
                    Self::Null { validity } => Some(*validity),
                }
            };
        }
        fixed_bindings!(arms)
    }

    /// The value variables: none for NULL, `[ptr, len]` for bytes, otherwise one.
    pub fn values(&self) -> Vec<VariableId> {
        macro_rules! arms {
            ($($v:ident),*) => {
                match self {
                    $(Self::$v { value, .. })|* => vec![*value],
                    Self::Bytes { ptr, len, .. } => vec![*ptr, *len],
                    Self::Null { .. } => vec![],
                }
            };
        }
        fixed_bindings!(arms)
    }

    /// The single value variable of a fixed-width binding.
    pub fn value(&self) -> Option<VariableId> {
        match self.values().as_slice() {
            [value] => Some(*value),
            _ => None,
        }
    }

    pub fn variables(&self) -> Vec<VariableId> {
        let mut out = self.values();
        out.extend(self.validity());
        out
    }

    /// The same values with another validity. NULL bindings keep theirs.
    pub fn with_validity(&self, validity: Option<VariableId>) -> Self {
        let mut out = *self;
        macro_rules! arms {
            ($($v:ident),*) => {
                match &mut out {
                    $(Self::$v { validity: v, .. })|* | Self::Bytes { validity: v, .. } => {
                        *v = validity
                    }
                    Self::Null { .. } => {}
                }
            };
        }
        fixed_bindings!(arms);
        out
    }

    pub fn is_null_type(&self) -> bool {
        matches!(self, Self::Null { .. })
    }
}
