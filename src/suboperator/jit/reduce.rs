//! Vector lowering for global integer reductions over one Arrow column.
//! The eligibility check is structural; query names and input values are irrelevant.
use super::*;
use cranelift_module::FuncId;

pub(super) fn compile(
    module: &mut JITModule,
    scope: &SuboperatorScope,
    inputs: &[Input],
) -> Result<Option<(FuncId, String)>> {
    if !scope.boundaries.is_empty() || !scope.outputs.is_empty() {
        return Ok(None);
    }
    let mut columns = vec![None; scope.variables.len()];
    let mut global = vec![false; scope.variables.len()];
    let mut aggregates = vec![];
    let mut column = None;
    for op in &scope.suboperators {
        match op {
            Suboperator::SourceColumn {
                source,
                column: index,
                output,
            } if source.0 == 0 => {
                let ty = scope.variables[output.index].ty.data_type();
                if !matches!(ty, DataType::Int64 | DataType::UInt64) {
                    return Ok(None);
                }
                columns[output.index] = Some(*index);
            }
            Suboperator::Cast { input, output, .. }
                if scope.variables[input.index].ty.data_type()
                    == scope.variables[output.index].ty.data_type() =>
            {
                columns[output.index] = columns[input.index];
            }
            Suboperator::GroupLookup { inputs, output, .. } if inputs.is_empty() => {
                global[output.index] = true;
            }
            Suboperator::AggregateUpdate {
                inputs,
                states,
                kernel,
            } if matches!(
                kernel,
                AggregateKernel::SumI64
                    | AggregateKernel::SumU64
                    | AggregateKernel::CountValues
                    | AggregateKernel::CountRows
            ) && global[inputs[0].index] =>
            {
                if let Some(input) = inputs.get(1) {
                    let Some(index) = columns[input.index] else {
                        return Ok(None);
                    };
                    if column.is_some_and(|c| c != index) {
                        return Ok(None);
                    }
                    column = Some(index);
                }
                aggregates.push((states[1], *kernel));
            }
            _ => return Ok(None),
        }
    }
    if aggregates.is_empty() {
        return Ok(None);
    }
    let Some(column) = column else {
        return Ok(None);
    };
    let Some(slot) = inputs.iter().position(|i| i.column == Some(column)) else {
        return Ok(None);
    };
    let ptr = types::I64;
    let mut ctx = module.make_context();
    ctx.func.signature.params = vec![AbiParam::new(ptr); 7];
    ctx.func.signature.returns = vec![AbiParam::new(types::I32)];
    let id = module
        .declare_function("roc_global_reduce", Linkage::Local, &ctx.func.signature)
        .map_err(error)?;
    let mut fb = FunctionBuilderContext::new();
    let mut b = FunctionBuilder::new(&mut ctx.func, &mut fb);
    let entry = b.create_block();
    b.append_block_params_for_function_params(entry);
    b.switch_to_block(entry);
    let args = b.block_params(entry).to_vec();
    let zero = b.ins().iconst(ptr, 0);
    let one = b.ins().iconst(ptr, 1);
    let fail = b.create_block();
    b.append_block_param(fail, types::I32);
    // Allocate the one global state before entering any hot loop.
    let mut sig = module.make_signature();
    sig.params = vec![AbiParam::new(ptr); 3];
    sig.returns = vec![AbiParam::new(types::I32)];
    let sig = b.import_signature(sig);
    let address = b
        .ins()
        .iconst(ptr, runtime::ensure_groups as *const () as usize as i64);
    let mut state_ptrs = vec![];
    for (state, _) in &aggregates {
        let state = b
            .ins()
            .load(ptr, MemFlags::new(), args[4], (state.index * 8) as i32);
        let call = b.ins().call_indirect(sig, address, &[args[0], state, one]);
        let status = b.inst_results(call)[0];
        let ready = b.create_block();
        b.ins().brif(status, fail, &[status.into()], ready, &[]);
        b.switch_to_block(ready);
        state_ptrs.push(b.ins().load(ptr, MemFlags::new(), state, 0));
    }
    let data = b
        .ins()
        .load(ptr, MemFlags::new(), args[1], (slot * 24) as i32);
    let bitmap = b
        .ins()
        .load(ptr, MemFlags::new(), args[1], (slot * 24 + 8) as i32);
    let bitmap_offset = b
        .ins()
        .load(ptr, MemFlags::new(), args[1], (slot * 24 + 16) as i32);
    let vt = types::I64X2;
    let vz = b.ins().splat(vt, zero);
    let head = b.create_block();
    let body = b.create_block();
    let tail = b.create_block();
    for t in [ptr, ptr, vt, vt, vt, vt] {
        b.append_block_param(head, t);
    }
    b.ins().jump(
        head,
        &[
            zero.into(),
            zero.into(),
            vz.into(),
            vz.into(),
            vz.into(),
            vz.into(),
        ],
    );
    b.switch_to_block(head);
    let params = b.block_params(head).to_vec();
    let row = params[0];
    let count = params[1];
    let limit = b.ins().iadd_imm(row, 64);
    let more = b.ins().icmp(IntCC::UnsignedLessThanOrEqual, limit, args[5]);
    b.ins().brif(more, body, &[], tail, &[]);
    b.switch_to_block(body);
    let tick = b.ins().band_imm(row, 4095);
    let due = b.ins().icmp_imm(IntCC::Equal, tick, 0);
    let poll = b.create_block();
    let work = b.create_block();
    b.ins().brif(due, poll, &[], work, &[]);
    b.switch_to_block(poll);
    let mut poll_sig = module.make_signature();
    poll_sig.params = vec![AbiParam::new(ptr)];
    poll_sig.returns = vec![AbiParam::new(types::I32)];
    let poll_sig = b.import_signature(poll_sig);
    let address = b
        .ins()
        .iconst(ptr, runtime::poll as *const () as usize as i64);
    let call = b.ins().call_indirect(poll_sig, address, &[args[0]]);
    let status = b.inst_results(call)[0];
    b.ins().brif(status, fail, &[status.into()], work, &[]);
    b.switch_to_block(work);
    let full = b.ins().iconst(ptr, -1);
    let bits = validity(&mut b, bitmap, bitmap_offset, row, 64, full);
    let live = b.ins().popcnt(bits);
    let count = b.ins().iadd(count, live);
    let offset = b.ins().ishl_imm(row, 3);
    let at = b.ins().iadd(data, offset);
    let sparse = b.create_block();
    let dense = b.create_block();
    let accumulated = b.create_block();
    for _ in 0..4 {
        b.append_block_param(accumulated, vt);
    }
    let few_nulls = b
        .ins()
        .icmp_imm(IntCC::UnsignedGreaterThanOrEqual, live, 56);
    b.ins().brif(few_nulls, sparse, &[], dense, &[]);
    b.switch_to_block(dense);
    let bits_vec = b.ins().splat(vt, bits);
    let mut sums = params[2..].to_vec();
    for i in 0..32 {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&(1u64 << (i * 2)).to_le_bytes());
        bytes[8..].copy_from_slice(&(2u64 << (i * 2)).to_le_bytes());
        let constant = b.func.dfg.constants.insert(bytes.to_vec().into());
        let mask = b.ins().vconst(vt, constant);
        let flag = b.ins().band(bits_vec, mask);
        let valid = b.ins().icmp(IntCC::NotEqual, flag, vz);
        let value = b.ins().load(vt, MemFlags::new(), at, (i * 16) as i32);
        let value = b.ins().band(value, valid);
        sums[i % 4] = b.ins().iadd(sums[i % 4], value);
    }
    b.ins().jump(
        accumulated,
        &[
            sums[0].into(),
            sums[1].into(),
            sums[2].into(),
            sums[3].into(),
        ],
    );
    b.switch_to_block(sparse);
    let mut sums = params[2..].to_vec();
    for i in 0..32 {
        let value = b.ins().load(vt, MemFlags::new(), at, (i * 16) as i32);
        sums[i % 4] = b.ins().iadd(sums[i % 4], value);
    }
    // Wrapping integer sums permit summing all payloads and subtracting the
    // few invalid payloads. NULL payloads are arbitrary, not assumed to be zero.
    let missing = b.ins().bnot(bits);
    let skip = b.create_block();
    let subtract = b.create_block();
    let corrected = b.create_block();
    b.append_block_param(skip, ptr);
    b.append_block_param(skip, ptr);
    b.ins().jump(skip, &[missing.into(), zero.into()]);
    b.switch_to_block(skip);
    let missing = b.block_params(skip)[0];
    let correction = b.block_params(skip)[1];
    b.ins().brif(missing, subtract, &[], corrected, &[]);
    b.switch_to_block(subtract);
    let index = b.ins().ctz(missing);
    let offset = b.ins().ishl_imm(index, 3);
    let address = b.ins().iadd(at, offset);
    let value = b.ins().load(ptr, MemFlags::new(), address, 0);
    let correction = b.ins().iadd(correction, value);
    let less = b.ins().iadd_imm(missing, -1);
    let missing = b.ins().band(missing, less);
    b.ins().jump(skip, &[missing.into(), correction.into()]);
    b.switch_to_block(corrected);
    let correction = b.block_params(skip)[1];
    let lane = b.ins().extractlane(sums[0], 0);
    let lane = b.ins().isub(lane, correction);
    sums[0] = b.ins().insertlane(sums[0], lane, 0);
    b.ins().jump(
        accumulated,
        &[
            sums[0].into(),
            sums[1].into(),
            sums[2].into(),
            sums[3].into(),
        ],
    );
    b.switch_to_block(accumulated);
    let sums = b.block_params(accumulated).to_vec();
    b.ins().jump(
        head,
        &[
            limit.into(),
            count.into(),
            sums[0].into(),
            sums[1].into(),
            sums[2].into(),
            sums[3].into(),
        ],
    );
    b.switch_to_block(tail);
    let sum01 = b.ins().iadd(params[2], params[3]);
    let sum23 = b.ins().iadd(params[4], params[5]);
    let sum = b.ins().iadd(sum01, sum23);
    let a = b.ins().extractlane(sum, 0);
    let z = b.ins().extractlane(sum, 1);
    let sum = b.ins().iadd(a, z);
    let rest = b.create_block();
    let item = b.create_block();
    let done = b.create_block();
    for t in [ptr, ptr, ptr] {
        b.append_block_param(rest, t);
    }
    b.ins()
        .jump(rest, &[row.into(), params[1].into(), sum.into()]);
    b.switch_to_block(rest);
    let p = b.block_params(rest).to_vec();
    let more = b.ins().icmp(IntCC::UnsignedLessThan, p[0], args[5]);
    b.ins().brif(more, item, &[], done, &[]);
    b.switch_to_block(item);
    let valid = validity(&mut b, bitmap, bitmap_offset, p[0], 1, one);
    let offset = b.ins().ishl_imm(p[0], 3);
    let at = b.ins().iadd(data, offset);
    let value = b.ins().load(ptr, MemFlags::new(), at, 0);
    let value = b.ins().select(valid, value, zero);
    let sum = b.ins().iadd(p[2], value);
    let count = b.ins().iadd(p[1], valid);
    let row = b.ins().iadd_imm(p[0], 1);
    b.ins().jump(rest, &[row.into(), count.into(), sum.into()]);
    b.switch_to_block(done);
    for ((_, kernel), state) in aggregates.iter().zip(state_ptrs) {
        use crate::expr::agg::accumulator::Group;
        if matches!(
            kernel,
            AggregateKernel::CountValues | AggregateKernel::CountRows
        ) {
            let offset = std::mem::offset_of!(Group, count) as i32;
            let old = b.ins().load(ptr, MemFlags::new(), state, offset);
            let count = if matches!(kernel, AggregateKernel::CountRows) {
                args[5]
            } else {
                p[1]
            };
            let (sum, bad) = b.ins().sadd_overflow(old, count);
            let okay = b.create_block();
            let code = b.ins().iconst(types::I32, 2);
            b.ins().brif(bad, fail, &[code.into()], okay, &[]);
            b.switch_to_block(okay);
            b.ins().store(MemFlags::new(), sum, state, offset);
        } else {
            let offset = std::mem::offset_of!(Group, sum) as i32;
            let old = b.ins().load(ptr, MemFlags::new(), state, offset);
            let sum = b.ins().iadd(old, p[2]);
            b.ins().store(MemFlags::new(), sum, state, offset);
            let old = b.ins().load(ptr, MemFlags::new(), state, offset + 16);
            let any = b.ins().icmp_imm(IntCC::NotEqual, p[1], 0);
            let any = b.ins().uextend(ptr, any);
            let valid = b.ins().bor(old, any);
            b.ins().store(MemFlags::new(), valid, state, offset + 16);
        }
    }
    let okay = b.ins().iconst(types::I32, 0);
    b.ins().return_(&[okay]);
    b.switch_to_block(fail);
    let status = b.block_params(fail)[0];
    b.ins().return_(&[status]);
    b.seal_all_blocks();
    b.finalize();
    let ir = ctx.func.display().to_string();
    module.define_function(id, &mut ctx).map_err(error)?;
    Ok(Some((id, ir)))
}
fn validity(
    b: &mut FunctionBuilder<'_>,
    bitmap: Value,
    offset: Value,
    row: Value,
    width: u8,
    full: Value,
) -> Value {
    let nullable = b.create_block();
    let done = b.create_block();
    b.append_block_param(done, types::I64);
    b.ins().brif(bitmap, nullable, &[], done, &[full.into()]);
    b.switch_to_block(nullable);
    let bit = b.ins().iadd(offset, row);
    let byte = b.ins().ushr_imm(bit, 3);
    let at = b.ins().iadd(bitmap, byte);
    let shift = b.ins().band_imm(bit, 7);
    let bits = if width == 64 {
        let first = b.ins().load(types::I64, MemFlags::new(), at, 0);
        let aligned = b.create_block();
        let extra = b.create_block();
        b.append_block_param(aligned, types::I64);
        b.ins().brif(shift, extra, &[], aligned, &[first.into()]);
        b.switch_to_block(extra);
        let last = b.ins().uload8(types::I64, MemFlags::new(), at, 8);
        let inverse = b.ins().irsub_imm(shift, 64);
        let last = b.ins().ishl(last, inverse);
        let first = b.ins().ushr(first, shift);
        let both = b.ins().bor(first, last);
        b.ins().jump(aligned, &[both.into()]);
        b.switch_to_block(aligned);
        b.block_params(aligned)[0]
    } else {
        let first = b.ins().uload8(types::I64, MemFlags::new(), at, 0);
        let bits = b.ins().ushr(first, shift);
        b.ins().band(bits, full)
    };
    b.ins().jump(done, &[bits.into()]);
    b.switch_to_block(done);
    b.block_params(done)[0]
}
