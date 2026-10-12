//! Ordinary nodes can emit scalar instructions and balanced local branches.
//! Persistent scope entry/exit is exclusively owned by the scope walker.
use super::*;
use crate::suboperator::runtime;
pub(super) struct LocalEmitContext<'c, 'a, 'f> {
    pub(super) compiler: &'c mut Compiler<'a, 'f>,
}
impl LocalEmitContext<'_, '_, '_> {
    pub(super) fn emit(&mut self, op: &Suboperator) -> Result<()> {
        let c = &mut self.compiler;
        let value = match op {
            Suboperator::SourceColumn {
                source,
                column,
                output,
            } => {
                if let Some(Some(view)) = c.source_views.get(source.0) {
                    let id = *view.get(*column).ok_or_else(|| {
                        Error::InvalidPlan("source view column out of bounds".into())
                    })?;
                    let value = c.value(id)?;
                    c.origins[output.index] = c.origins[id.index];
                    value
                } else {
                    // Reuse a visible source value even when distinct expression
                    // objects refer to the same physical column.
                    if let Some(value) = c.origins.iter().enumerate().find_map(|(i, origin)| {
                        (*origin == Some(*column)
                            && c.scope.variables[i].ty == c.scope.variables[output.index].ty)
                            .then(|| c.values[i])
                            .flatten()
                    }) {
                        c.origins[output.index] = Some(*column);
                        c.values[output.index] = Some(value);
                        return Ok(());
                    }
                    let slot = c.inputs.len();
                    let ty = c.scope.variables[output.index].ty.data_type().clone();
                    if !scalar::supported(&ty) {
                        return Err(Error::InvalidPlan(format!("unsupported source type: {ty}")));
                    }
                    c.origins[output.index] = Some(*column);
                    c.inputs.push(Input {
                        column: Some(*column),
                        ty: ty.clone(),
                        literal: None,
                        lazy: c.local_depth > 0 || c.scope_depth > 0,
                        nullable: c.scope.variables[output.index].ty.is_nullable(),
                    });
                    let ptr = c.descriptor(1, slot * 24);
                    if c.local_depth > 0 || c.scope_depth > 0 {
                        let absent = c.b.ins().icmp_imm(IntCC::Equal, ptr, 0);
                        let error = c.b.create_block();
                        let valid = c.b.create_block();
                        c.b.ins().brif(absent, error, &[], valid, &[]);
                        c.b.switch_to_block(error);
                        let id = c.word(slot as i64);
                        c.call(
                            runtime::input_error as *const () as usize,
                            &[c.args[0], id],
                            &[],
                        )?;
                        let status = c.b.ins().iconst(types::I32, 1);
                        c.b.ins().jump(c.fail, &[status.into()]);
                        c.b.switch_to_block(valid);
                    }
                    c.load_column(
                        slot,
                        c.row,
                        &ty,
                        ptr,
                        c.scope.variables[output.index].ty.is_nullable(),
                    )
                }
            }
            Suboperator::Constant { value, .. } => {
                if !scalar::supported(value.data_type()) {
                    return Err(Error::InvalidPlan(format!(
                        "unsupported constant type: {}",
                        value.data_type()
                    )));
                }
                let cell = scalar::decode(value)?[0];
                if scalar::bytes_type(value.data_type()) {
                    // Retain literal storage in the compiled object; its address is stable.
                    c.inputs.push(Input {
                        column: None,
                        ty: value.data_type().clone(),
                        literal: Some(value.clone()),
                        lazy: false,
                        nullable: value.null_count() != 0,
                    });
                }
                let bits = c.word(cell.bits as i64);
                let valid = c.word(cell.valid as i64);
                let mut result = c.scalar(bits, valid);
                result.len = c.word(cell.len as i64);
                let ty = value_type(value.data_type());
                if ty.bits() < 64 {
                    result.value =
                        c.b.ins()
                            .ireduce(Type::int(ty.bits() as u16).unwrap(), result.value);
                }
                if ty.is_float() {
                    result.value = c.b.ins().bitcast(ty, MemFlags::new(), result.value);
                }
                result
            }
            Suboperator::Call {
                function, inputs, ..
            } => {
                let a = c.value(inputs[0])?;
                let r = if inputs.len() == 2 {
                    c.value(inputs[1])?
                } else {
                    a
                };
                let ty = c.scope.variables[inputs[0].index].ty.data_type().clone();
                self.function(*function, a, r, &ty)?
            }
            Suboperator::Cast {
                input,
                mode,
                output,
            } => {
                let a = c.value(*input)?;
                let from = c.scope.variables[input.index].ty.data_type().clone();
                let to = c.scope.variables[output.index].ty.data_type().clone();
                self.cast(a, &from, &to, *mode)?
            }
            Suboperator::Not { input, .. } => {
                let a = c.value(*input)?;
                let bits = c.b.ins().bxor_imm(a.value, 1);
                c.scalar(bits, a.valid)
            }
            Suboperator::Boolean { and, inputs, .. } => {
                let bits = c.word(i64::from(*and));
                let yes = c.word(1);
                let mut a = c.scalar(bits, yes);
                for input in inputs {
                    let r = c.value(*input)?;
                    let (lbit, rbit) = if *and {
                        (
                            c.b.ins().bxor_imm(a.value, 1),
                            c.b.ins().bxor_imm(r.value, 1),
                        )
                    } else {
                        (a.value, r.value)
                    };
                    let l = c.b.ins().band(lbit, a.valid);
                    let rdec = c.b.ins().band(rbit, r.valid);
                    let decisive = c.b.ins().bor(l, rdec);
                    let both = c.b.ins().band(a.valid, r.valid);
                    let valid = c.b.ins().bor(both, decisive);
                    let bits = if *and {
                        c.b.ins().band(a.value, r.value)
                    } else {
                        c.b.ins().bor(a.value, r.value)
                    };
                    a = c.scalar(bits, valid);
                }
                a
            }
            Suboperator::Case {
                branches,
                otherwise,
                ..
            } => self.case(branches, *otherwise, op.outputs()[0])?,
            Suboperator::Coalesce { inputs, .. } => self.coalesce(inputs, op.outputs()[0])?,
            Suboperator::GroupLookup { state, inputs, .. } => {
                let values = inputs
                    .iter()
                    .map(|v| c.value(*v))
                    .collect::<Result<Vec<_>>>()?;
                if values.is_empty() {
                    let zero = c.word(0);
                    let yes = c.word(1);
                    let value = c.scalar(zero, yes);
                    c.values[op.outputs()[0].index] = Some(value);
                    return Ok(());
                }
                let state = c.descriptor(4, state.index * 8);
                let out = c.stack(&[]);
                let ty = c.scope.variables[inputs[0].index].ty.data_type();
                if values.len() == 1
                    && !scalar::bytes_type(ty)
                    && !matches!(ty, DataType::Float32 | DataType::Float64 | DataType::Null)
                {
                    let bits = c.pack_bits(values[0]);
                    let zero = c.word(0);
                    let key = c.b.ins().select(values[0].valid, bits, zero);
                    let slot = c.b.ins().band_imm(key, 1023);
                    let offset = c.b.ins().imul_imm(slot, 24);
                    // Option<Box<T>> has the nullable pointer ABI; the compiler
                    // only takes this path for a word-key state with a cache.
                    let cache = c.b.ins().load(types::I64, MemFlags::new(), state, 0);
                    let cached = c.b.ins().iadd(cache, offset);
                    let old_key = c.b.ins().load(types::I64, MemFlags::new(), cached, 0);
                    let old_tag = c.b.ins().load(types::I64, MemFlags::new(), cached, 16);
                    let tag = c.b.ins().iadd_imm(values[0].valid, 1);
                    let key_eq = c.b.ins().icmp(IntCC::Equal, old_key, key);
                    let tag_eq = c.b.ins().icmp(IntCC::Equal, old_tag, tag);
                    let hit = c.b.ins().band(key_eq, tag_eq);
                    let fast = c.b.create_block();
                    let slow = c.b.create_block();
                    let ready = c.b.create_block();
                    c.b.append_block_param(ready, types::I64);
                    c.b.ins().brif(hit, fast, &[], slow, &[]);
                    c.b.switch_to_block(fast);
                    let id = c.b.ins().load(types::I64, MemFlags::new(), cached, 8);
                    c.b.ins().jump(ready, &[id.into()]);
                    c.b.switch_to_block(slow);
                    c.helper(
                        runtime::group_lookup_word as *const () as usize,
                        &[c.args[0], state, bits, values[0].valid, out],
                    )?;
                    let id = c.load(out, &DataType::UInt64).value;
                    c.b.ins().jump(ready, &[id.into()]);
                    c.b.switch_to_block(ready);
                    let id = c.b.block_params(ready)[0];
                    let yes = c.word(1);
                    let value = c.scalar(id, yes);
                    c.values[op.outputs()[0].index] = Some(value);
                    return Ok(());
                } else {
                    let ptr = c.stack(&values);
                    let n = c.word(values.len() as i64);
                    c.helper(
                        runtime::group_lookup as *const () as usize,
                        &[c.args[0], state, ptr, n, out],
                    )?;
                }
                c.load(out, &DataType::UInt64)
            }
            Suboperator::AggregateUpdate {
                states,
                inputs,
                kernel,
            } => {
                use crate::expr::agg::accumulator as agg;
                if matches!(
                    kernel,
                    AggregateKernel::CountRows
                        | AggregateKernel::CountValues
                        | AggregateKernel::SumI64
                        | AggregateKernel::SumU64
                        | AggregateKernel::SumF64
                ) {
                    return self.aggregate_inline(states[1], inputs, *kernel);
                }
                let address = match kernel {
                    AggregateKernel::CountRows => {
                        runtime::aggregate_update::<agg::CountRows> as *const () as usize
                    }
                    AggregateKernel::CountValues => {
                        runtime::aggregate_update::<agg::CountValues> as *const () as usize
                    }
                    AggregateKernel::CountDistinct => {
                        runtime::aggregate_update::<agg::CountDistinct> as *const () as usize
                    }
                    AggregateKernel::SumI64 | AggregateKernel::SumU64 => {
                        runtime::aggregate_update::<agg::SumInteger> as *const () as usize
                    }
                    AggregateKernel::SumF64 => {
                        runtime::aggregate_update::<agg::SumFloat> as *const () as usize
                    }
                    AggregateKernel::Avg => {
                        runtime::aggregate_update::<agg::Average> as *const () as usize
                    }
                    AggregateKernel::CovarPop => {
                        runtime::aggregate_update::<agg::CovarianceUpdate> as *const () as usize
                    }
                    AggregateKernel::Min => {
                        runtime::aggregate_update::<agg::Minimum> as *const () as usize
                    }
                    AggregateKernel::Max => {
                        runtime::aggregate_update::<agg::Maximum> as *const () as usize
                    }
                };
                let id = c.value(inputs[0])?;
                let values = inputs[1..]
                    .iter()
                    .map(|v| c.value(*v))
                    .collect::<Result<Vec<_>>>()?;
                let ptr = c.stack(&values);
                let n = c.word(values.len() as i64);
                let state = c.b.ins().load(
                    types::I64,
                    MemFlags::new(),
                    c.args[4],
                    (states[1].index * 8) as i32,
                );
                c.helper(address, &[c.args[0], state, id.value, ptr, n])?;
                return Ok(());
            }
            Suboperator::Materialize { .. } | Suboperator::BatchUpdate { .. } => unreachable!(),
        };
        let mut value = value;
        value.signed = scalar::signed(
            self.compiler.scope.variables[op.outputs()[0].index]
                .ty
                .data_type(),
        );
        self.compiler.values[op.outputs()[0].index] = Some(value);
        Ok(())
    }
    fn aggregate_inline(
        &mut self,
        state: StateId,
        inputs: &[VariableId],
        kernel: AggregateKernel,
    ) -> Result<()> {
        use crate::expr::agg::accumulator::Group;
        let c = &mut self.compiler;
        let id = c.value(inputs[0])?.value;
        let value = inputs.get(1).map(|v| c.value(*v)).transpose()?;
        let state = c.descriptor(4, state.index * 8);
        let len = c.b.ins().load(types::I64, MemFlags::new(), state, 8);
        let ready = c.b.create_block();
        let grow = c.b.create_block();
        let exists = c.b.ins().icmp(IntCC::UnsignedLessThan, id, len);
        c.b.ins().brif(exists, ready, &[], grow, &[]);
        c.b.switch_to_block(grow);
        let len = c.b.ins().iadd_imm(id, 1);
        c.helper(
            runtime::ensure_groups as *const () as usize,
            &[c.args[0], state, len],
        )?;
        c.b.ins().jump(ready, &[]);
        c.b.switch_to_block(ready);
        let base = c.b.ins().load(types::I64, MemFlags::new(), state, 0);
        let offset = c.b.ins().imul_imm(id, std::mem::size_of::<Group>() as i64);
        let ptr = c.b.ins().iadd(base, offset);
        let valid = value.map_or_else(|| c.word(1), |v| v.valid);
        let float_sum = matches!(kernel, AggregateKernel::SumF64);
        let done = c.b.create_block();
        if float_sum {
            let update = c.b.create_block();
            c.b.ins().brif(valid, update, &[], done, &[]);
            c.b.switch_to_block(update);
        }
        match kernel {
            AggregateKernel::CountRows | AggregateKernel::CountValues => {
                let offset = std::mem::offset_of!(Group, count) as i32;
                let old = c.b.ins().load(types::I64, MemFlags::new(), ptr, offset);
                let (new, bad) = c.b.ins().sadd_overflow(old, valid);
                c.check(bad);
                c.b.ins().store(MemFlags::new(), new, ptr, offset);
            }
            _ => {
                let offset = std::mem::offset_of!(Group, sum) as i32;
                let v = value.unwrap();
                if matches!(kernel, AggregateKernel::SumF64) {
                    let old = c.b.ins().load(types::F64, MemFlags::new(), ptr, offset);
                    let valid =
                        c.b.ins()
                            .load(types::I64, MemFlags::new(), ptr, offset + 16);
                    let sum = c.b.ins().fadd(old, v.value);
                    let sum = c.b.ins().select(valid, sum, v.value);
                    c.b.ins().store(MemFlags::new(), sum, ptr, offset);
                } else {
                    let old = c.b.ins().load(types::I64, MemFlags::new(), ptr, offset);
                    let zero = c.word(0);
                    let value = c.b.ins().select(valid, v.value, zero);
                    let sum = c.b.ins().iadd(old, value);
                    c.b.ins().store(MemFlags::new(), sum, ptr, offset);
                }
                let old =
                    c.b.ins()
                        .load(types::I64, MemFlags::new(), ptr, offset + 16);
                let seen = c.b.ins().bor(old, valid);
                c.b.ins().store(MemFlags::new(), seen, ptr, offset + 16);
            }
        }
        c.b.ins().jump(done, &[]);
        c.b.switch_to_block(done);
        Ok(())
    }
    fn function(&mut self, f: FunctionKind, a: Scalar, r: Scalar, t: &DataType) -> Result<Scalar> {
        use FunctionKind::*;
        if matches!(f, IsNull | IsNotNull) {
            let c = &mut self.compiler;
            let bits = if f == IsNull {
                c.b.ins().bxor_imm(a.valid, 1)
            } else {
                a.valid
            };
            let yes = c.word(1);
            return Ok(c.scalar(bits, yes));
        }
        let n = self.compiler.b.ins().band(a.valid, r.valid);
        if matches!(
            f,
            Equal
                | NotEqual
                | LessThan
                | LessThanOrEqual
                | GreaterThan
                | GreaterThanOrEqual
                | IsDistinctFrom
                | IsNotDistinctFrom
        ) {
            let (l, rval, unsigned) = if scalar::bytes_type(t) {
                let c = &mut self.compiler;
                let v = c.call(
                    runtime::compare_bytes as *const () as usize,
                    &[a.value, a.len, r.value, r.len],
                    &[types::I64],
                )?[0];
                (v, c.word(0), false)
            } else {
                let l = a.value;
                let r = r.value;
                if matches!(t, DataType::Float32 | DataType::Float64) {
                    (self.float_key(l), self.float_key(r), false)
                } else {
                    (l, r, !scalar::signed(t))
                }
            };
            let cc = match f {
                Equal | IsNotDistinctFrom => IntCC::Equal,
                NotEqual | IsDistinctFrom => IntCC::NotEqual,
                LessThan => {
                    if unsigned {
                        IntCC::UnsignedLessThan
                    } else {
                        IntCC::SignedLessThan
                    }
                }
                LessThanOrEqual => {
                    if unsigned {
                        IntCC::UnsignedLessThanOrEqual
                    } else {
                        IntCC::SignedLessThanOrEqual
                    }
                }
                GreaterThan => {
                    if unsigned {
                        IntCC::UnsignedGreaterThan
                    } else {
                        IntCC::SignedGreaterThan
                    }
                }
                GreaterThanOrEqual => {
                    if unsigned {
                        IntCC::UnsignedGreaterThanOrEqual
                    } else {
                        IntCC::SignedGreaterThanOrEqual
                    }
                }
                _ => unreachable!(),
            };
            let c = &mut self.compiler;
            let cmp = c.b.ins().icmp(cc, l, rval);
            let cmp = c.b.ins().uextend(types::I64, cmp);
            let (bits, valid) = if matches!(f, IsDistinctFrom | IsNotDistinctFrom) {
                let diff = c.b.ins().bxor(a.valid, r.valid);
                let cmp = if f == IsNotDistinctFrom {
                    c.b.ins().bxor_imm(cmp, 1)
                } else {
                    cmp
                };
                let cmp = c.b.ins().band(n, cmp);
                let diff = c.b.ins().bor(diff, cmp);
                let bits = if f == IsNotDistinctFrom {
                    c.b.ins().bxor_imm(diff, 1)
                } else {
                    diff
                };
                (bits, c.word(1))
            } else {
                (cmp, n)
            };
            return Ok(c.scalar(bits, valid));
        }
        let left = a.value;
        let right = r.value;
        let c = &mut self.compiler;
        let float = matches!(t, DataType::Float32 | DataType::Float64);
        let result = if float {
            match f {
                Add => c.b.ins().fadd(left, right),
                Subtract => c.b.ins().fsub(left, right),
                Multiply => c.b.ins().fmul(left, right),
                Divide => c.b.ins().fdiv(left, right),
                Negate => c.b.ins().fneg(left),
                Remainder => {
                    let helper = if *t == DataType::Float32 {
                        runtime::remainder_f32 as *const () as usize
                    } else {
                        runtime::remainder_f64 as *const () as usize
                    };
                    c.call(helper, &[left, right], &[c.b.func.dfg.value_type(left)])?[0]
                }
                _ => unreachable!(),
            }
        } else if matches!(f, Divide | Remainder) {
            let zero = c.b.ins().icmp_imm(IntCC::Equal, right, 0);
            let live = c.b.ins().icmp_imm(IntCC::NotEqual, n, 0);
            let bad = c.b.ins().band(zero, live);
            c.check_code(bad, 3);
            let ty = c.b.func.dfg.value_type(left);
            let one = c.b.ins().iconst(ty, 1);
            let mut divisor = c.b.ins().select(live, right, one);
            if scalar::signed(t) {
                let min =
                    c.b.ins()
                        .icmp_imm(IntCC::Equal, left, i64::MIN >> (64 - scalar::width(t)));
                let neg = c.b.ins().icmp_imm(IntCC::Equal, right, -1);
                let overflow = c.b.ins().band(min, neg);
                if f == Divide {
                    let bad = c.b.ins().band(overflow, live);
                    c.check(bad);
                } else {
                    divisor = c.b.ins().select(overflow, one, divisor);
                }
                if f == Divide {
                    c.b.ins().sdiv(left, divisor)
                } else {
                    c.b.ins().srem(left, divisor)
                }
            } else if f == Divide {
                c.b.ins().udiv(left, divisor)
            } else {
                c.b.ins().urem(left, divisor)
            }
        } else {
            let (v, overflow) = match (f, scalar::signed(t)) {
                (Add, true) => c.b.ins().sadd_overflow(left, right),
                (Add, false) => c.b.ins().uadd_overflow(left, right),
                (Subtract, true) => c.b.ins().ssub_overflow(left, right),
                (Subtract, false) => c.b.ins().usub_overflow(left, right),
                (Multiply, true) => c.b.ins().smul_overflow(left, right),
                (Multiply, false) => c.b.ins().umul_overflow(left, right),
                (Negate, true) => {
                    let t = c.b.func.dfg.value_type(left);
                    let z = c.b.ins().iconst(t, 0);
                    c.b.ins().ssub_overflow(z, left)
                }
                _ => unreachable!(),
            };
            let live = c.b.ins().icmp_imm(IntCC::NotEqual, n, 0);
            let bad = c.b.ins().band(overflow, live);
            c.check(bad);
            v
        };
        let bits = result;
        Ok(self.compiler.scalar(bits, n))
    }
    fn float_key(&mut self, v: Value) -> Value {
        let c = &mut self.compiler;
        let t = if c.b.func.dfg.value_type(v) == types::F32 {
            types::I32
        } else {
            types::I64
        };
        let bits = c.b.ins().bitcast(t, MemFlags::new(), v);
        let sign = c.b.ins().sshr_imm(bits, t.bits() as i64 - 1);
        let mask = c.b.ins().ushr_imm(sign, 1);
        c.b.ins().bxor(bits, mask)
    }
    fn merge_block(&mut self, ty: &DataType) -> Block {
        let c = &mut self.compiler;
        let end = c.b.create_block();
        c.b.append_block_param(end, value_type(ty));
        for _ in 0..2 {
            c.b.append_block_param(end, types::I64);
        }
        end
    }
    fn jump(&mut self, end: Block, v: Scalar) {
        self.compiler
            .b
            .ins()
            .jump(end, &[v.value.into(), v.len.into(), v.valid.into()]);
    }
    fn merged(&mut self, end: Block) -> Scalar {
        let c = &mut self.compiler;
        c.b.switch_to_block(end);
        let p = c.b.block_params(end);
        Scalar {
            value: p[0],
            len: p[1],
            valid: p[2],
            signed: false,
        }
    }
    fn case(
        &mut self,
        branches: &[(VariableId, VariableId)],
        otherwise: VariableId,
        output: VariableId,
    ) -> Result<Scalar> {
        let ty = self.compiler.scope.variables[output.index]
            .ty
            .data_type()
            .clone();
        self.compiler.local_depth += 1;
        let end = self.merge_block(&ty);
        let saved = self.compiler.values.clone();
        let emitted = self.compiler.emitted.clone();
        for (cond, value) in branches {
            let c = &mut self.compiler;
            let cond = c.value(*cond)?;
            let pass = c.b.ins().band(cond.value, cond.valid);
            let yes = c.b.create_block();
            let no = c.b.create_block();
            c.b.ins().brif(pass, yes, &[], no, &[]);
            c.b.switch_to_block(yes);
            let v = c.value(*value)?;
            self.jump(end, v);
            let c = &mut self.compiler;
            c.b.switch_to_block(no);
            c.values = saved.clone();
            c.emitted = emitted.clone();
        }
        let v = self.compiler.value(otherwise)?;
        self.jump(end, v);
        self.compiler.values = saved;
        self.compiler.emitted = emitted;
        self.compiler.local_depth -= 1;
        Ok(self.merged(end))
    }
    fn coalesce(&mut self, inputs: &[VariableId], output: VariableId) -> Result<Scalar> {
        let ty = self.compiler.scope.variables[output.index]
            .ty
            .data_type()
            .clone();
        self.compiler.local_depth += 1;
        let end = self.merge_block(&ty);
        let saved = self.compiler.values.clone();
        let emitted = self.compiler.emitted.clone();
        for input in inputs {
            let c = &mut self.compiler;
            let v = c.value(*input)?;
            let yes = c.b.create_block();
            let no = c.b.create_block();
            c.b.ins().brif(v.valid, yes, &[], no, &[]);
            c.b.switch_to_block(yes);
            self.jump(end, v);
            self.compiler.b.switch_to_block(no);
        }
        let v = self.compiler.null_typed(&ty);
        self.jump(end, v);
        self.compiler.values = saved;
        self.compiler.emitted = emitted;
        self.compiler.local_depth -= 1;
        Ok(self.merged(end))
    }
    fn cast(
        &mut self,
        a: Scalar,
        from: &DataType,
        to: &DataType,
        mode: CastMode,
    ) -> Result<Scalar> {
        super::cast::emit(self.compiler, a, from, to, mode)
    }
}
