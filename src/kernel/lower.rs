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

//! Lowering helpers shared by expressions and operators. Validity is an
//! optional Boolean where None means "always valid".
use super::builder::is_bytes;
use super::*;
use crate::arrow::datatypes::DataType;
use crate::error::{Error, Result};
use crate::expr::scalar::CastMode;
use std::sync::Arc;

impl KernelBuilder<'_> {
    pub fn and(&mut self, a: VariableId, b: VariableId) -> Result<VariableId> {
        if a == b {
            return Ok(a);
        }
        Ok(self.call(ScalarFunction::And, vec![a, b])?[0])
    }

    pub fn or(&mut self, a: VariableId, b: VariableId) -> Result<VariableId> {
        if a == b {
            return Ok(a);
        }
        Ok(self.call(ScalarFunction::Or, vec![a, b])?[0])
    }

    pub fn not(&mut self, a: VariableId) -> Result<VariableId> {
        Ok(self.call(ScalarFunction::Not, vec![a])?[0])
    }

    /// AND of optional conditions, where None is true.
    pub fn and_validity(
        &mut self,
        a: Option<VariableId>,
        b: Option<VariableId>,
    ) -> Result<Option<VariableId>> {
        Ok(match (a, b) {
            (Some(a), Some(b)) => Some(self.and(a, b)?),
            (a, None) => a,
            (None, b) => b,
        })
    }

    /// The validity of a strict function: valid only when every input is.
    pub fn strict_validity(
        &mut self,
        inputs: &[ColumnVariableBinding],
    ) -> Result<Option<VariableId>> {
        let mut validity = None;
        for input in inputs {
            validity = self.and_validity(validity, input.validity())?;
        }
        Ok(validity)
    }

    /// A non-null Boolean that is true where the binding is true and valid.
    pub fn predicate(&mut self, binding: &ColumnVariableBinding) -> Result<VariableId> {
        match binding {
            ColumnVariableBinding::Null { .. } => self.boolean(false),
            ColumnVariableBinding::Boolean { value, validity } => Ok(match validity {
                Some(v) => self.and(*value, *v)?,
                None => *value,
            }),
            _ => Err(Error::invalid_plan("predicate must be Boolean".into())),
        }
    }

    pub fn materialize_validity(&mut self, validity: Option<VariableId>) -> Result<VariableId> {
        match validity {
            Some(v) => Ok(v),
            None => self.boolean(true),
        }
    }

    /// Masks every pending error with `cond`; None keeps them unchanged.
    pub fn guard_errors(
        &mut self,
        errors: PendingErrors,
        cond: Option<VariableId>,
    ) -> Result<PendingErrors> {
        let Some(cond) = cond else {
            return Ok(errors);
        };
        errors
            .into_iter()
            .map(|e| {
                Ok(PendingError {
                    flag: self.and(e.flag, cond)?,
                    ..e
                })
            })
            .collect()
    }

    /// A function's own error flag, suppressed on NULL rows.
    pub fn pending(
        &mut self,
        flag: VariableId,
        validity: Option<VariableId>,
        error: KernelError,
        args: Vec<VariableId>,
    ) -> Result<PendingError> {
        Ok(PendingError {
            flag: match validity {
                Some(v) => self.and(flag, v)?,
                None => flag,
            },
            error,
            args,
        })
    }

    /// One RaiseIf per pending error, in the current domain.
    pub fn raise(&mut self, errors: PendingErrors) -> Result<()> {
        for e in errors {
            self.raise_if(e.flag, e.error, e.args)?;
        }
        Ok(())
    }

    /// The safe default of a NULL value of a logical type.
    pub fn null_binding(&mut self, data_type: &DataType) -> Result<ColumnVariableBinding> {
        let validity = self.boolean(false)?;
        if *data_type == DataType::Null {
            return Ok(ColumnVariableBinding::Null { validity });
        }
        self.zero_binding(data_type, Some(validity))
    }

    fn zero_binding(
        &mut self,
        data_type: &DataType,
        validity: Option<VariableId>,
    ) -> Result<ColumnVariableBinding> {
        if is_bytes(data_type) {
            return Ok(ColumnVariableBinding::Bytes {
                ptr: self.constant(ConstantValue::BytePointer(Arc::from(&[][..])))?,
                len: self.constant(ConstantValue::Usize(0))?,
                validity,
            });
        }
        let ty = VariableType::of(data_type)
            .ok_or_else(|| Error::unsupported(format!("unsupported type: {data_type}")))?;
        let value = self.constant(ConstantValue::zero(ty))?;
        ColumnVariableBinding::fixed(ty, value, validity)
    }

    /// Row-wise choice between two bindings of the same logical type.
    pub fn select_binding(
        &mut self,
        cond: VariableId,
        then: &ColumnVariableBinding,
        otherwise: &ColumnVariableBinding,
    ) -> Result<ColumnVariableBinding> {
        use ColumnVariableBinding as B;
        let validity = match (then.validity(), otherwise.validity()) {
            (None, None) => None,
            (a, b) => {
                let a = self.materialize_validity(a)?;
                let b = self.materialize_validity(b)?;
                Some(self.select(cond, a, b)?)
            }
        };
        Ok(match (then, otherwise) {
            (B::Null { .. }, B::Null { .. }) => B::Null {
                validity: validity.expect("NULL bindings carry validity"),
            },
            (
                B::Bytes {
                    ptr: p1, len: l1, ..
                },
                B::Bytes {
                    ptr: p2, len: l2, ..
                },
            ) => B::Bytes {
                ptr: self.select(cond, *p1, *p2)?,
                len: self.select(cond, *l1, *l2)?,
                validity,
            },
            _ => {
                let (Some(a), Some(b)) = (then.value(), otherwise.value()) else {
                    return Err(Error::invalid_plan("CASE branch types differ".into()));
                };
                let ty = self.variable_type(a)?;
                if self.variable_type(b)? != ty {
                    return Err(Error::invalid_plan("CASE branch types differ".into()));
                }
                let value = self.select(cond, a, b)?;
                B::fixed(ty, value, validity)?
            }
        })
    }

    /// Converts a binding between logical types. Try casts turn invalid rows
    /// into NULL; strict casts return a pending error.
    pub fn cast_binding(
        &mut self,
        input: &ColumnVariableBinding,
        from: &DataType,
        to: &DataType,
        mode: CastMode,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        if from == to {
            return Ok((*input, vec![]));
        }
        if *from == DataType::Null {
            return Ok((self.null_binding(to)?, vec![]));
        }
        let validity = input.validity();
        let unsupported = || Error::unsupported(format!("unsupported cast from {from} to {to}"));
        // Returns the output validity and errors for an `invalid` flag.
        let checked = |b: &mut Self,
                       invalid: VariableId,
                       args: Vec<VariableId>,
                       error: KernelError|
         -> Result<(Option<VariableId>, PendingErrors)> {
            match mode {
                CastMode::Try => {
                    let valid = b.not(invalid)?;
                    Ok((b.and_validity(validity, Some(valid))?, vec![]))
                }
                CastMode::Strict => {
                    Ok((validity, vec![b.pending(invalid, validity, error, args)?]))
                }
            }
        };
        if is_bytes(from) && is_bytes(to) {
            let ColumnVariableBinding::Bytes { ptr, len, .. } = *input else {
                return Err(unsupported());
            };
            if matches!(from, DataType::Binary | DataType::LargeBinary)
                && matches!(to, DataType::Utf8 | DataType::LargeUtf8)
            {
                let invalid = self.call(ScalarFunction::ValidateUtf8, vec![ptr, len])?[0];
                let (validity, errors) = checked(self, invalid, vec![], KernelError::InvalidUtf8)?;
                return Ok((ColumnVariableBinding::Bytes { ptr, len, validity }, errors));
            }
            return Ok((*input, vec![]));
        }
        if is_bytes(from) {
            let ColumnVariableBinding::Bytes { ptr, len, .. } = *input else {
                return Err(unsupported());
            };
            let target = scalar_type(to).ok_or_else(unsupported)?;
            let out = self.call(ScalarFunction::ParseText { to: target }, vec![ptr, len])?;
            let (validity, errors) =
                checked(self, out[1], vec![ptr, len], KernelError::InvalidCast)?;
            return Ok((
                ColumnVariableBinding::fixed(target, out[0], validity)?,
                errors,
            ));
        }
        let source = scalar_type(from).ok_or_else(unsupported)?;
        let value = input.value().ok_or_else(unsupported)?;
        if is_bytes(to) {
            let out = self.call(ScalarFunction::FormatText, vec![value])?;
            return Ok((
                ColumnVariableBinding::Bytes {
                    ptr: out[0],
                    len: out[1],
                    validity,
                },
                vec![],
            ));
        }
        let target = scalar_type(to).ok_or_else(unsupported)?;
        let out = self.call(ScalarFunction::Cast { to: target }, vec![value])?;
        if !cast_can_fail(source, target) {
            return Ok((
                ColumnVariableBinding::fixed(target, out[0], validity)?,
                vec![],
            ));
        }
        let (validity, errors) = checked(self, out[1], vec![], KernelError::InvalidCast)?;
        Ok((
            ColumnVariableBinding::fixed(target, out[0], validity)?,
            errors,
        ))
    }
}

/// Numeric and Boolean types that Cast / ParseText / FormatText accept.
/// Temporal types only cast to themselves.
fn scalar_type(t: &DataType) -> Option<VariableType> {
    match t {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => None,
        _ => VariableType::of(t),
    }
}

/// Whether some input value is out of range for the target.
pub(crate) fn cast_can_fail(from: VariableType, to: VariableType) -> bool {
    use VariableType as T;
    if to == T::Boolean || to.is_float() {
        return false;
    }
    if from == T::Boolean {
        return false;
    }
    if from.is_float() {
        return true;
    }
    match (from.is_signed(), to.is_signed()) {
        (true, false) => true,
        (false, true) => from.bits() >= to.bits(),
        _ => from.bits() > to.bits(),
    }
}

/// Whether a logical cast is supported, checked before code generation.
pub(crate) fn cast_supported(from: &DataType, to: &DataType) -> bool {
    let value = |t: &DataType| scalar_type(t).is_some() || is_bytes(t);
    from == to || *from == DataType::Null || (value(from) && value(to))
}
