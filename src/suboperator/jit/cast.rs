//! All type/mode decisions in this module run during compilation. Generated
//! numeric conversions contain only typed IR; text conversions call a bound
//! monomorphized scalar helper. No DataType reaches the generated function.
use super::*;
use crate::suboperator::runtime;
use cranelift_codegen::ir::condcodes::FloatCC;
pub(super) fn emit(
    c: &mut Compiler<'_, '_>,
    a: Scalar,
    from: &DataType,
    to: &DataType,
    mode: CastMode,
) -> Result<Scalar> {
    if from == to {
        return Ok(a);
    }
    if scalar::bytes_type(from) && scalar::bytes_type(to) {
        if matches!(from, DataType::Binary | DataType::LargeBinary)
            && matches!(to, DataType::Utf8 | DataType::LargeUtf8)
        {
            let address = if mode == CastMode::Try {
                runtime::validate_utf8::<true> as *const () as usize
            } else {
                runtime::validate_utf8::<false> as *const () as usize
            };
            let input = c.stack(&[a]);
            let out = c.stack(&[]);
            c.helper(address, &[c.args[0], input, out])?;
            return Ok(c.load(out, to));
        }
        return Ok(a);
    }
    if *from == DataType::Null {
        return Ok(c.null_typed(to));
    }
    if scalar::bytes_type(from) || scalar::bytes_type(to) {
        macro_rules! helper {
            ($t:ty) => {
                if scalar::bytes_type(from) {
                    if mode == CastMode::Try {
                        runtime::parse::<$t, true> as *const () as usize
                    } else {
                        runtime::parse::<$t, false> as *const () as usize
                    }
                } else {
                    runtime::format::<$t> as *const () as usize
                }
            };
        }
        let numeric = if scalar::bytes_type(from) { to } else { from };
        let address = match numeric {
            DataType::Int8 => helper!(i8),
            DataType::Int16 => helper!(i16),
            DataType::Int32 => helper!(i32),
            DataType::Int64 => helper!(i64),
            DataType::UInt8 => helper!(u8),
            DataType::UInt16 => helper!(u16),
            DataType::UInt32 => helper!(u32),
            DataType::UInt64 => helper!(u64),
            DataType::Float32 => helper!(f32),
            DataType::Float64 => helper!(f64),
            DataType::Boolean => helper!(bool),
            _ => return Err(Error::InvalidPlan("unsupported scalar cast".into())),
        };
        let input = c.stack(&[a]);
        let out = c.stack(&[]);
        c.helper(address, &[c.args[0], input, out])?;
        return Ok(c.load(out, to));
    }
    let v = a.value;
    let raw = c.pack_bits(a);
    let float_in = matches!(from, DataType::Float32 | DataType::Float64);
    let float_out = matches!(to, DataType::Float32 | DataType::Float64);
    let target = value_type(to);
    let mut valid = a.valid;
    let v = if *to == DataType::Boolean {
        let bit = if float_in {
            let zero = if *from == DataType::Float32 {
                c.b.ins().f32const(0.0)
            } else {
                c.b.ins().f64const(0.0)
            };
            c.b.ins().fcmp(FloatCC::NotEqual, v, zero)
        } else {
            c.b.ins().icmp_imm(IntCC::NotEqual, v, 0)
        };
        c.b.ins().uextend(types::I64, bit)
    } else if float_out {
        if float_in {
            if *from == DataType::Float32 {
                c.b.ins().fpromote(types::F64, v)
            } else {
                c.b.ins().fdemote(types::F32, v)
            }
        } else if scalar::signed(from) {
            c.b.ins().fcvt_from_sint(target, raw)
        } else {
            c.b.ins().fcvt_from_uint(target, raw)
        }
    } else if float_in {
        let wide = if *from == DataType::Float32 {
            c.b.ins().fpromote(types::F64, v)
        } else {
            v
        };
        let bits = scalar::width(to);
        let signed = scalar::signed(to);
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
        valid = cast_valid(c, bad, valid, mode);
        let v = if signed {
            c.b.ins().fcvt_to_sint_sat(types::I64, v)
        } else {
            c.b.ins().fcvt_to_uint_sat(types::I64, v)
        };
        if bits < 64 {
            c.b.ins().ireduce(target, v)
        } else {
            v
        }
    } else {
        let bits = scalar::width(to);
        let signed = scalar::signed(to);
        let bad = if signed {
            let min = i64::MIN >> (64 - bits);
            let max = i64::MAX >> (64 - bits);
            let hi = c.b.ins().icmp_imm(
                if scalar::signed(from) {
                    IntCC::SignedGreaterThan
                } else {
                    IntCC::UnsignedGreaterThan
                },
                raw,
                max,
            );
            if scalar::signed(from) {
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
            if scalar::signed(from) {
                let lo = c.b.ins().icmp_imm(IntCC::SignedLessThan, raw, 0);
                c.b.ins().bor(hi, lo)
            } else {
                hi
            }
        };
        valid = cast_valid(c, bad, valid, mode);
        if bits < 64 {
            c.b.ins().ireduce(target, raw)
        } else {
            raw
        }
    };
    let mut result = c.scalar(v, valid);
    result.signed = scalar::signed(to);
    Ok(result)
}
fn cast_valid(c: &mut Compiler<'_, '_>, bad: Value, valid: Value, mode: CastMode) -> Value {
    if mode == CastMode::Try {
        let good = c.b.ins().bxor_imm(bad, 1);
        let good = c.b.ins().uextend(types::I64, good);
        c.b.ins().band(valid, good)
    } else {
        let live = c.b.ins().icmp_imm(IntCC::NotEqual, valid, 0);
        let bad = c.b.ins().band(bad, live);
        c.check_code(bad, 4);
        valid
    }
}
