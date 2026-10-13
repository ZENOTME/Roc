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

//! Numeric and Boolean conversions. Returns `(value, invalid)`; invalid marks
//! out-of-range and NaN inputs, and the value is then unspecified but total.
use super::*;
use cranelift_codegen::ir::condcodes::FloatCC;

pub(super) fn emit(
    c: &mut Compiler<'_, '_>,
    v: Value,
    from: VariableType,
    to: VariableType,
) -> (Value, Value) {
    let ok = c.b.ins().iconst(types::I8, 0);
    if from == to {
        return (v, ok);
    }
    let target = clif_type(to);
    if to == VariableType::Boolean {
        let bit = if from.is_float() {
            let zero = if from == VariableType::Float32 {
                c.b.ins().f32const(0.0)
            } else {
                c.b.ins().f64const(0.0)
            };
            c.b.ins().fcmp(FloatCC::NotEqual, v, zero)
        } else {
            c.b.ins().icmp_imm(IntCC::NotEqual, v, 0)
        };
        return (bit, ok);
    }
    if to.is_float() {
        let value = if from.is_float() {
            if from == VariableType::Float32 {
                c.b.ins().fpromote(types::F64, v)
            } else {
                c.b.ins().fdemote(types::F32, v)
            }
        } else {
            let raw = c.bits(v, from);
            if from.is_signed() {
                c.b.ins().fcvt_from_sint(target, raw)
            } else {
                c.b.ins().fcvt_from_uint(target, raw)
            }
        };
        return (value, ok);
    }
    let bits = to.bits();
    let signed = to.is_signed();
    if from.is_float() {
        let wide = if from == VariableType::Float32 {
            c.b.ins().fpromote(types::F64, v)
        } else {
            v
        };
        let lo = if signed {
            -(2f64).powi(bits as i32 - 1)
        } else {
            0.0
        };
        let hi = (2f64).powi(bits as i32 - i32::from(signed));
        let truncated = c.b.ins().trunc(wide);
        let lo = c.b.ins().f64const(lo);
        let hi = c.b.ins().f64const(hi);
        let low = c.b.ins().fcmp(FloatCC::LessThan, truncated, lo);
        let high = c.b.ins().fcmp(FloatCC::GreaterThanOrEqual, truncated, hi);
        let nan = c.b.ins().fcmp(FloatCC::Unordered, wide, wide);
        let bad = c.b.ins().bor(low, high);
        let bad = c.b.ins().bor(bad, nan);
        let value = if signed {
            c.b.ins().fcvt_to_sint_sat(types::I64, v)
        } else {
            c.b.ins().fcvt_to_uint_sat(types::I64, v)
        };
        let value = if bits < 64 {
            c.b.ins().ireduce(target, value)
        } else {
            value
        };
        return (value, bad);
    }
    let raw = c.bits(v, from);
    let bad = if signed {
        let min = i64::MIN >> (64 - bits);
        let max = i64::MAX >> (64 - bits);
        let cc = if from.is_signed() {
            IntCC::SignedGreaterThan
        } else {
            IntCC::UnsignedGreaterThan
        };
        let hi = c.b.ins().icmp_imm(cc, raw, max);
        if from.is_signed() {
            let lo = c.b.ins().icmp_imm(IntCC::SignedLessThan, raw, min);
            c.b.ins().bor(hi, lo)
        } else {
            hi
        }
    } else {
        let max = if bits == 64 {
            -1
        } else {
            ((1u64 << bits) - 1) as i64
        };
        let hi = c.b.ins().icmp_imm(IntCC::UnsignedGreaterThan, raw, max);
        if from.is_signed() {
            let lo = c.b.ins().icmp_imm(IntCC::SignedLessThan, raw, 0);
            c.b.ins().bor(hi, lo)
        } else {
            hi
        }
    };
    let value = if bits < 64 {
        c.b.ins().ireduce(target, raw)
    } else {
        raw
    };
    (value, bad)
}
