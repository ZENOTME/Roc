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

//! Decimal semantics as Int128 kernels over unscaled values. They follow
//! Arrow's decimal arithmetic and casts: results are exact or fail; scales
//! grow by multiplication and shrink by division rounding halves away from
//! zero; a cast checks the target precision.
use super::*;
use crate::arrow::datatypes::DataType;
use crate::error::{Error, Result};

pub(crate) const MAX_PRECISION: u8 = 38;

/// `10^k` for `k <= 38`.
pub(crate) fn pow10(k: u32) -> i128 {
    10i128.pow(k)
}

/// The precision and scale of a Decimal128 type.
pub(crate) fn decimal(t: &DataType) -> Option<(u8, i8)> {
    match t {
        DataType::Decimal128(p, s) => Some((*p, *s)),
        _ => None,
    }
}

/// A value and the flags of rows where computing it failed.
pub(crate) struct Checked {
    pub value: VariableId,
    pub flags: Vec<(VariableId, KernelError)>,
}

impl KernelBuilder<'_> {
    fn wide(&mut self, v: i128) -> Result<VariableId> {
        self.constant(ConstantValue::Int128(v))
    }

    /// `value * factor` for a factor above one; overflow is flagged.
    fn scale_up(
        &mut self,
        value: VariableId,
        factor: i128,
        flags: &mut Vec<(VariableId, KernelError)>,
    ) -> Result<VariableId> {
        if factor == 1 {
            return Ok(value);
        }
        let factor = self.wide(factor)?;
        let out = self.call(ScalarFunction::Multiply, vec![value, factor])?;
        flags.push((out[1], KernelError::ArithmeticOverflow));
        Ok(out[0])
    }

    /// Moves an unscaled value from scale `from` to scale `to`; overflow of
    /// the 128-bit value is flagged with `error`.
    pub(crate) fn rescale(
        &mut self,
        value: VariableId,
        from: i8,
        to: i8,
        error: KernelError,
    ) -> Result<Checked> {
        let mut flags = vec![];
        let value = if to >= from {
            let up = (to - from) as u32;
            if up > MAX_PRECISION as u32 {
                return Err(Error::unsupported(format!(
                    "decimal scale {from} cannot grow to {to}"
                )));
            }
            let v = self.scale_up(value, pow10(up), &mut flags)?;
            for f in &mut flags {
                f.1 = error;
            }
            v
        } else {
            let down = (from - to) as u32;
            if down > MAX_PRECISION as u32 {
                // Every value rounds to zero.
                self.wide(0)?
            } else {
                let d = self.wide(pow10(down))?;
                self.call(ScalarFunction::DivideRound, vec![value, d])?[0]
            }
        };
        Ok(Checked { value, flags })
    }

    /// True where an unscaled value has more than `precision` digits.
    pub(crate) fn exceeds_precision(
        &mut self,
        value: VariableId,
        precision: u8,
    ) -> Result<VariableId> {
        let bound = pow10(precision as u32);
        let max = self.wide(bound - 1)?;
        let min = self.wide(1 - bound)?;
        let high = self.call(ScalarFunction::GreaterThan, vec![value, max])?[0];
        let low = self.call(ScalarFunction::LessThan, vec![value, min])?[0];
        self.or(high, low)
    }

    /// `+ - * /` of two decimals, at Arrow's result scale; the caller converts
    /// to the declared result type.
    pub(crate) fn decimal_arithmetic(
        &mut self,
        function: ScalarFunction,
        (l, s1): (VariableId, i8),
        (r, s2): (VariableId, i8),
    ) -> Result<(Checked, i8)> {
        use ScalarFunction as S;
        let overflow = KernelError::ArithmeticOverflow;
        let mut flags = vec![];
        let (out, scale) = match function {
            S::Add | S::Subtract => {
                let scale = s1.max(s2);
                let l = self.rescale(l, s1, scale, overflow)?;
                let r = self.rescale(r, s2, scale, overflow)?;
                flags.extend(l.flags);
                flags.extend(r.flags);
                let out = self.call(function, vec![l.value, r.value])?;
                flags.push((out[1], overflow));
                (out[0], scale)
            }
            S::Multiply => {
                let scale = s1 + s2;
                if scale > MAX_PRECISION as i8 {
                    return Err(Error::invalid_plan(format!(
                        "decimal product scale {scale} exceeds {MAX_PRECISION}"
                    )));
                }
                let out = self.call(S::Multiply, vec![l, r])?;
                flags.push((out[1], overflow));
                (out[0], scale)
            }
            S::Divide => {
                let scale = (s1 + 4).min(MAX_PRECISION as i8);
                let shift = scale - s1 + s2;
                let (l, r) = if shift >= 0 {
                    (self.scale_up(l, pow10(shift as u32), &mut flags)?, r)
                } else {
                    (
                        l,
                        self.scale_up(r, pow10(shift.unsigned_abs() as u32), &mut flags)?,
                    )
                };
                let out = self.call(S::Divide, vec![l, r])?;
                flags.push((out[1], KernelError::DivideByZero));
                flags.push((out[2], overflow));
                (out[0], scale)
            }
            _ => {
                return Err(Error::unsupported(format!(
                    "unsupported decimal function {function:?}"
                )));
            }
        };
        Ok((Checked { value: out, flags }, scale))
    }
}
