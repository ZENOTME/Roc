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

//! Instruction selection for scalar kernels.
use super::*;
use crate::expr::agg::accumulator::{self as agg, Group};

impl Compiler<'_, '_> {
    pub(super) fn kernel(&mut self, k: usize) -> Result<()> {
        let kernel = match &self.graph.kernels[k].1 {
            Kernel::Scalar(s) => s.clone(),
            // A Filter opens its region lazily when an item of its domain is emitted.
            Kernel::Domain(_) => return Ok(()),
        };
        match kernel {
            ScalarKernel::Constant { value, output } => {
                let v = self.constant(&value);
                self.set(output, v);
            }
            ScalarKernel::Call {
                function,
                inputs,
                outputs,
            } => {
                let args = inputs.iter().map(|v| self.get(*v)).collect::<Vec<_>>();
                let types = inputs.iter().map(|v| self.ty(*v)).collect::<Vec<_>>();
                let out_types = outputs.iter().map(|v| self.ty(*v)).collect::<Vec<_>>();
                let values = self.function(function, &args, &types, &out_types)?;
                for (v, value) in outputs.iter().zip(values) {
                    self.set(*v, value);
                }
            }
            ScalarKernel::Select {
                cond,
                then,
                otherwise,
                output,
            } => {
                let c = self.get(cond);
                let a = self.get(then);
                let b = self.get(otherwise);
                let v = self.b.ins().select(c, a, b);
                self.set(output, v);
            }
            ScalarKernel::RaiseIf { cond, error, args } => {
                let c = self.get(cond);
                let values = args
                    .iter()
                    .map(|v| {
                        let x = self.get(*v);
                        self.bits(x, self.ty(*v))
                    })
                    .collect::<Vec<_>>();
                let cold = self.b.create_block();
                let next = self.b.create_block();
                self.b.set_cold_block(cold);
                self.b.ins().brif(c, cold, &[], next, &[]);
                self.b.switch_to_block(cold);
                if !values.is_empty() {
                    let ptr = self.stack(values.len() * 8);
                    for (i, v) in values.iter().enumerate() {
                        self.b.ins().store(MemFlags::new(), *v, ptr, (i * 8) as i32);
                    }
                    let n = self.word(values.len() as i64);
                    self.call(
                        runtime::raise_args as *const () as usize,
                        &[self.args[0], ptr, n],
                        &[],
                    );
                }
                let site = self.sites.len();
                self.sites.push(error);
                let status = self.b.ins().iconst(types::I32, 2 + site as i64);
                self.b.ins().jump(self.fail, &[status.into()]);
                self.b.switch_to_block(next);
            }
            ScalarKernel::GroupLookup {
                state,
                keys,
                output,
            } => {
                let id = self.group_lookup(state, &keys);
                self.set(output, id);
            }
            ScalarKernel::AggregateUpdate {
                state,
                kind,
                group,
                arguments,
            } => self.aggregate_update(state, kind, group, &arguments),
        }
        Ok(())
    }

    fn constant(&mut self, value: &ConstantValue) -> Value {
        match value {
            ConstantValue::Float32(v) => self.b.ins().f32const(*v),
            ConstantValue::Float64(v) => self.b.ins().f64const(*v),
            ConstantValue::BytePointer(bytes) => {
                self.constants.push(bytes.clone());
                self.word(bytes.as_ptr() as i64)
            }
            _ => {
                let t = clif_type(value.ty());
                let bits = value.bits();
                let bits = if t.bits() < 64 {
                    bits & ((1u64 << t.bits()) - 1)
                } else {
                    bits
                };
                self.b.ins().iconst(t, bits as i64)
            }
        }
    }

    /// The value as a 64-bit word: sign-extended for signed integers, raw bits
    /// for floats, zero-extended otherwise.
    pub(super) fn bits(&mut self, v: Value, ty: VariableType) -> Value {
        let v = match ty {
            VariableType::Float32 => self.b.ins().bitcast(types::I32, MemFlags::new(), v),
            VariableType::Float64 => self.b.ins().bitcast(types::I64, MemFlags::new(), v),
            _ => v,
        };
        if self.b.func.dfg.value_type(v).bits() == 64 {
            v
        } else if ty.is_signed() {
            self.b.ins().sextend(types::I64, v)
        } else {
            self.b.ins().uextend(types::I64, v)
        }
    }

    /// Reverses `bits`.
    pub(super) fn decode(&mut self, bits: Value, ty: VariableType) -> Value {
        let t = clif_type(ty);
        match ty {
            VariableType::Float32 => {
                let v = self.b.ins().ireduce(types::I32, bits);
                self.b.ins().bitcast(t, MemFlags::new(), v)
            }
            VariableType::Float64 => self.b.ins().bitcast(t, MemFlags::new(), bits),
            _ if t.bits() < 64 => self.b.ins().ireduce(t, bits),
            _ => bits,
        }
    }

    fn function(
        &mut self,
        f: ScalarFunction,
        a: &[Value],
        types: &[VariableType],
        outputs: &[VariableType],
    ) -> Result<Vec<Value>> {
        use ScalarFunction::*;
        let t = types.first().copied().unwrap_or(VariableType::Boolean);
        Ok(match f {
            Add | Subtract | Multiply if t.is_float() => vec![match f {
                Add => self.b.ins().fadd(a[0], a[1]),
                Subtract => self.b.ins().fsub(a[0], a[1]),
                _ => self.b.ins().fmul(a[0], a[1]),
            }],
            Add | Subtract | Multiply | Negate if t.is_integer() => {
                let (v, overflow) = match (f, t.is_signed()) {
                    (Add, true) => self.b.ins().sadd_overflow(a[0], a[1]),
                    (Add, false) => self.b.ins().uadd_overflow(a[0], a[1]),
                    (Subtract, true) => self.b.ins().ssub_overflow(a[0], a[1]),
                    (Subtract, false) => self.b.ins().usub_overflow(a[0], a[1]),
                    (Multiply, true) => self.b.ins().smul_overflow(a[0], a[1]),
                    (Multiply, false) => self.b.ins().umul_overflow(a[0], a[1]),
                    _ => {
                        let zero = self.b.ins().iconst(clif_type(t), 0);
                        self.b.ins().ssub_overflow(zero, a[0])
                    }
                };
                vec![v, overflow]
            }
            Negate => vec![self.b.ins().fneg(a[0])],
            Divide if t.is_float() => vec![self.b.ins().fdiv(a[0], a[1])],
            Remainder if t.is_float() => {
                let helper = if t == VariableType::Float32 {
                    runtime::remainder_f32 as *const () as usize
                } else {
                    runtime::remainder_f64 as *const () as usize
                };
                self.call(helper, a, &[clif_type(t)])
            }
            Divide | Remainder => {
                let ct = clif_type(t);
                let zero = self.b.ins().icmp_imm(IntCC::Equal, a[1], 0);
                let one = self.b.ins().iconst(ct, 1);
                let divisor = self.b.ins().select(zero, one, a[1]);
                if t.is_signed() {
                    let min = i64::MIN >> (64 - t.bits());
                    let is_min = self.b.ins().icmp_imm(IntCC::Equal, a[0], min);
                    let is_neg = self.b.ins().icmp_imm(IntCC::Equal, a[1], -1);
                    let overflow = self.b.ins().band(is_min, is_neg);
                    // MIN / -1 traps; MIN % -1 is 0, which MIN % 1 also yields.
                    let divisor = self.b.ins().select(overflow, one, divisor);
                    if f == Divide {
                        vec![self.b.ins().sdiv(a[0], divisor), zero, overflow]
                    } else {
                        vec![self.b.ins().srem(a[0], divisor), zero]
                    }
                } else if f == Divide {
                    let no = self.b.ins().iconst(types::I8, 0);
                    vec![self.b.ins().udiv(a[0], divisor), zero, no]
                } else {
                    vec![self.b.ins().urem(a[0], divisor), zero]
                }
            }
            Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
                let (l, r, signed) = if a.len() == 4 {
                    let v = self.call(
                        runtime::compare_bytes as *const () as usize,
                        a,
                        &[types::I64],
                    )[0];
                    (v, self.word(0), true)
                } else if t.is_float() {
                    (self.float_key(a[0]), self.float_key(a[1]), true)
                } else {
                    (a[0], a[1], t.is_signed())
                };
                let cc = match (f, signed) {
                    (Equal, _) => IntCC::Equal,
                    (NotEqual, _) => IntCC::NotEqual,
                    (LessThan, true) => IntCC::SignedLessThan,
                    (LessThan, false) => IntCC::UnsignedLessThan,
                    (LessThanOrEqual, true) => IntCC::SignedLessThanOrEqual,
                    (LessThanOrEqual, false) => IntCC::UnsignedLessThanOrEqual,
                    (GreaterThan, true) => IntCC::SignedGreaterThan,
                    (GreaterThan, false) => IntCC::UnsignedGreaterThan,
                    (GreaterThanOrEqual, true) => IntCC::SignedGreaterThanOrEqual,
                    (_, false) => IntCC::UnsignedGreaterThanOrEqual,
                    _ => unreachable!(),
                };
                vec![self.b.ins().icmp(cc, l, r)]
            }
            And => vec![self.b.ins().band(a[0], a[1])],
            Or => vec![self.b.ins().bor(a[0], a[1])],
            Not => vec![self.b.ins().bxor_imm(a[0], 1)],
            Cast { to } => {
                let (v, invalid) = cast::emit(self, a[0], t, to);
                vec![v, invalid]
            }
            ParseText { to } => {
                let out = self.stack(8);
                let invalid = self.call(parse_helper(to)?, &[a[0], a[1], out], &[types::I8])[0];
                let bits = self.b.ins().load(types::I64, MemFlags::new(), out, 0);
                let v = if to == VariableType::Boolean {
                    self.b.ins().icmp_imm(IntCC::NotEqual, bits, 0)
                } else {
                    self.decode(bits, to)
                };
                vec![v, invalid]
            }
            FormatText => {
                let out = self.stack(16);
                let bits = self.bits(a[0], t);
                let status =
                    self.call(format_helper(t)?, &[self.args[0], bits, out], &[types::I32])[0];
                self.check_status(status);
                let p = self.b.ins().load(types::I64, MemFlags::new(), out, 0);
                let n = self.b.ins().load(types::I64, MemFlags::new(), out, 8);
                vec![p, n]
            }
            ValidateUtf8 => self.call(
                runtime::validate_utf8 as *const () as usize,
                a,
                &[types::I8],
            ),
            _ => {
                return Err(Error::InvalidPlan(format!(
                    "unsupported kernel signature: {f:?}({types:?}) -> {outputs:?}"
                )));
            }
        })
    }

    /// Maps IEEE floats to integers ordered like `total_cmp`.
    fn float_key(&mut self, v: Value) -> Value {
        let t = if self.b.func.dfg.value_type(v) == types::F32 {
            types::I32
        } else {
            types::I64
        };
        let bits = self.b.ins().bitcast(t, MemFlags::new(), v);
        let sign = self.b.ins().sshr_imm(bits, t.bits() as i64 - 1);
        let mask = self.b.ins().ushr_imm(sign, 1);
        self.b.ins().bxor(bits, mask)
    }

    /// `(bits, len, valid)` of a binding as runtime Cell words.
    fn cell(&mut self, binding: &ColumnVariableBinding) -> [Value; 3] {
        let valid = match binding.validity() {
            Some(v) => {
                let v = self.get(v);
                self.b.ins().uextend(types::I64, v)
            }
            None => self.word(1),
        };
        match *binding {
            ColumnVariableBinding::Null { .. } => {
                let zero = self.word(0);
                [zero, zero, zero]
            }
            ColumnVariableBinding::Bytes { ptr, len, .. } => [self.get(ptr), self.get(len), valid],
            _ => {
                let value = binding.value().unwrap();
                let v = self.get(value);
                let bits = self.bits(v, self.ty(value));
                [bits, self.word(0), valid]
            }
        }
    }

    fn cells(&mut self, bindings: &[ColumnVariableBinding]) -> Value {
        let cells = bindings.iter().map(|b| self.cell(b)).collect::<Vec<_>>();
        let ptr = self.stack(cells.len() * 24);
        for (i, cell) in cells.iter().enumerate() {
            for (j, word) in cell.iter().enumerate() {
                self.b
                    .ins()
                    .store(MemFlags::new(), *word, ptr, (i * 24 + j * 8) as i32);
            }
        }
        ptr
    }

    fn group_lookup(&mut self, state: StateId, keys: &[ColumnVariableBinding]) -> Value {
        if keys.is_empty() {
            return self.word(0);
        }
        let state = self.descriptor(4, (state.0 * 8) as i32);
        let out = self.stack(8);
        let word_key = matches!(
            keys,
            [ColumnVariableBinding::Boolean { .. }
                | ColumnVariableBinding::Int8 { .. }
                | ColumnVariableBinding::Int16 { .. }
                | ColumnVariableBinding::Int32 { .. }
                | ColumnVariableBinding::Int64 { .. }
                | ColumnVariableBinding::UInt8 { .. }
                | ColumnVariableBinding::UInt16 { .. }
                | ColumnVariableBinding::UInt32 { .. }
                | ColumnVariableBinding::UInt64 { .. }]
        );
        if !word_key {
            let ptr = self.cells(keys);
            let n = self.word(keys.len() as i64);
            let status = self.call(
                runtime::group_lookup as *const () as usize,
                &[self.args[0], state, ptr, n, out],
                &[types::I32],
            )[0];
            self.check_status(status);
            return self.b.ins().load(types::I64, MemFlags::new(), out, 0);
        }
        let [bits, _, valid] = self.cell(&keys[0]);
        let zero = self.word(0);
        let key = self.b.ins().select(valid, bits, zero);
        let slot = self.b.ins().band_imm(key, 1023);
        let offset = self.b.ins().imul_imm(slot, 24);
        // GroupState and GroupIndex are repr(C) with the word cache first;
        // Option<Box<_>> has the nullable pointer ABI and is Some for word keys.
        let cache = self.b.ins().load(types::I64, MemFlags::new(), state, 0);
        let cached = self.b.ins().iadd(cache, offset);
        let old_key = self.b.ins().load(types::I64, MemFlags::new(), cached, 0);
        let old_tag = self.b.ins().load(types::I64, MemFlags::new(), cached, 16);
        let tag = self.b.ins().iadd_imm(valid, 1);
        let key_eq = self.b.ins().icmp(IntCC::Equal, old_key, key);
        let tag_eq = self.b.ins().icmp(IntCC::Equal, old_tag, tag);
        let hit = self.b.ins().band(key_eq, tag_eq);
        let fast = self.b.create_block();
        let slow = self.b.create_block();
        let ready = self.b.create_block();
        self.b.append_block_param(ready, types::I64);
        self.b.ins().brif(hit, fast, &[], slow, &[]);
        self.b.switch_to_block(fast);
        let id = self.b.ins().load(types::I64, MemFlags::new(), cached, 8);
        self.b.ins().jump(ready, &[id.into()]);
        self.b.switch_to_block(slow);
        let status = self.call(
            runtime::group_lookup_word as *const () as usize,
            &[self.args[0], state, bits, valid, out],
            &[types::I32],
        )[0];
        self.check_status(status);
        let id = self.b.ins().load(types::I64, MemFlags::new(), out, 0);
        self.b.ins().jump(ready, &[id.into()]);
        self.b.switch_to_block(ready);
        self.b.block_params(ready)[0]
    }

    fn aggregate_update(
        &mut self,
        state: StateId,
        kind: AggregateUpdateKind,
        group: VariableId,
        arguments: &[ColumnVariableBinding],
    ) {
        use AggregateUpdateKind as K;
        let id = self.get(group);
        let state = self.descriptor(4, (state.0 * 8) as i32);
        let inline = match kind {
            K::CountRows => true,
            K::CountValues => arguments.len() == 1,
            K::SumI64 => matches!(arguments, [ColumnVariableBinding::Int64 { .. }]),
            K::SumU64 => matches!(arguments, [ColumnVariableBinding::UInt64 { .. }]),
            K::SumF64 => matches!(arguments, [ColumnVariableBinding::Float64 { .. }]),
            _ => false,
        };
        if inline {
            return self.aggregate_inline(state, kind, id, arguments.first());
        }
        let address = match kind {
            K::CountRows => runtime::aggregate_update::<agg::CountRows> as *const () as usize,
            K::CountValues => runtime::aggregate_update::<agg::CountValues> as *const () as usize,
            K::CountDistinct => {
                runtime::aggregate_update::<agg::CountDistinct> as *const () as usize
            }
            K::SumI64 | K::SumU64 => {
                runtime::aggregate_update::<agg::SumInteger> as *const () as usize
            }
            K::SumF64 => runtime::aggregate_update::<agg::SumFloat> as *const () as usize,
            K::Avg => runtime::aggregate_update::<agg::Average> as *const () as usize,
            K::CovarPop => runtime::aggregate_update::<agg::CovarianceUpdate> as *const () as usize,
            K::Min => runtime::aggregate_update::<agg::Minimum> as *const () as usize,
            K::Max => runtime::aggregate_update::<agg::Maximum> as *const () as usize,
        };
        let ptr = self.cells(arguments);
        let n = self.word(arguments.len() as i64);
        let status = self.call(address, &[self.args[0], state, id, ptr, n], &[types::I32])[0];
        self.check_status(status);
    }

    /// Count and Sum update the accumulator's group slots directly.
    fn aggregate_inline(
        &mut self,
        state: Value,
        kind: AggregateUpdateKind,
        id: Value,
        argument: Option<&ColumnVariableBinding>,
    ) {
        use AggregateUpdateKind as K;
        let len = self.b.ins().load(types::I64, MemFlags::new(), state, 8);
        let ready = self.b.create_block();
        let grow = self.b.create_block();
        let exists = self.b.ins().icmp(IntCC::UnsignedLessThan, id, len);
        self.b.ins().brif(exists, ready, &[], grow, &[]);
        self.b.switch_to_block(grow);
        let len = self.b.ins().iadd_imm(id, 1);
        let status = self.call(
            runtime::ensure_groups as *const () as usize,
            &[self.args[0], state, len],
            &[types::I32],
        )[0];
        self.check_status(status);
        self.b.ins().jump(ready, &[]);
        self.b.switch_to_block(ready);
        let base = self.b.ins().load(types::I64, MemFlags::new(), state, 0);
        let offset = self
            .b
            .ins()
            .imul_imm(id, std::mem::size_of::<Group>() as i64);
        let ptr = self.b.ins().iadd(base, offset);
        let valid = match argument.and_then(|a| a.validity()) {
            Some(v) => {
                let v = self.get(v);
                self.b.ins().uextend(types::I64, v)
            }
            None => self.word(1),
        };
        match kind {
            K::CountRows | K::CountValues => {
                let valid = if kind == K::CountRows {
                    self.word(1)
                } else {
                    valid
                };
                let offset = std::mem::offset_of!(Group, count) as i32;
                let old = self.b.ins().load(types::I64, MemFlags::new(), ptr, offset);
                let (new, bad) = self.b.ins().sadd_overflow(old, valid);
                let overflow = self.b.create_block();
                let next = self.b.create_block();
                self.b.set_cold_block(overflow);
                self.b.ins().brif(bad, overflow, &[], next, &[]);
                self.b.switch_to_block(overflow);
                let status = self.call(
                    runtime::aggregate_overflow as *const () as usize,
                    &[self.args[0]],
                    &[types::I32],
                )[0];
                self.b.ins().jump(self.fail, &[status.into()]);
                self.b.switch_to_block(next);
                self.b.ins().store(MemFlags::new(), new, ptr, offset);
            }
            _ => {
                let offset = std::mem::offset_of!(Group, sum) as i32;
                let value = argument.unwrap().value().unwrap();
                let v = self.get(value);
                let done = self.b.create_block();
                if kind == K::SumF64 {
                    let update = self.b.create_block();
                    self.b.ins().brif(valid, update, &[], done, &[]);
                    self.b.switch_to_block(update);
                    let old = self.b.ins().load(types::F64, MemFlags::new(), ptr, offset);
                    let seen = self
                        .b
                        .ins()
                        .load(types::I64, MemFlags::new(), ptr, offset + 16);
                    let sum = self.b.ins().fadd(old, v);
                    let sum = self.b.ins().select(seen, sum, v);
                    self.b.ins().store(MemFlags::new(), sum, ptr, offset);
                } else {
                    let old = self.b.ins().load(types::I64, MemFlags::new(), ptr, offset);
                    let zero = self.word(0);
                    let v = self.b.ins().select(valid, v, zero);
                    let sum = self.b.ins().iadd(old, v);
                    self.b.ins().store(MemFlags::new(), sum, ptr, offset);
                }
                let old = self
                    .b
                    .ins()
                    .load(types::I64, MemFlags::new(), ptr, offset + 16);
                let seen = self.b.ins().bor(old, valid);
                self.b.ins().store(MemFlags::new(), seen, ptr, offset + 16);
                self.b.ins().jump(done, &[]);
                self.b.switch_to_block(done);
            }
        }
    }
}

macro_rules! numeric_helper {
    ($name:ident, $helper:ident) => {
        fn $name(t: VariableType) -> Result<usize> {
            Ok(match t {
                VariableType::Boolean => runtime::$helper::<bool> as *const () as usize,
                VariableType::Int8 => runtime::$helper::<i8> as *const () as usize,
                VariableType::Int16 => runtime::$helper::<i16> as *const () as usize,
                VariableType::Int32 => runtime::$helper::<i32> as *const () as usize,
                VariableType::Int64 => runtime::$helper::<i64> as *const () as usize,
                VariableType::UInt8 => runtime::$helper::<u8> as *const () as usize,
                VariableType::UInt16 => runtime::$helper::<u16> as *const () as usize,
                VariableType::UInt32 => runtime::$helper::<u32> as *const () as usize,
                VariableType::UInt64 => runtime::$helper::<u64> as *const () as usize,
                VariableType::Float32 => runtime::$helper::<f32> as *const () as usize,
                VariableType::Float64 => runtime::$helper::<f64> as *const () as usize,
                _ => return Err(Error::InvalidPlan(format!("no text conversion for {t:?}"))),
            })
        }
    };
}
numeric_helper!(parse_helper, parse_text);
numeric_helper!(format_helper, format_text);
