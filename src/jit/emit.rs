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

use super::{CodeMemory, Expr, PredicateKernel, ProjectKernel};
use crate::{
    error::{Error, Result},
    expr::scalar::FunctionKind,
};
use cranelift_codegen::ir::{
    AbiParam, Block, InstBuilder, MemFlagsData, Value, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};

fn compile_error(error: impl std::fmt::Display) -> Error {
    Error::Execution(format!("Cranelift compilation: {error}"))
}

pub(super) struct CompiledKernels {
    pub memory: CodeMemory,
    pub predicate: PredicateKernel,
    pub project: ProjectKernel,
    pub clif: String,
    pub disassembly: Option<String>,
}

pub(super) fn compile(
    predicate: &Expr,
    outputs: &[Expr],
    references: &[usize],
    disassemble: bool,
) -> Result<CompiledKernels> {
    let builder = JITBuilder::with_flags(&[("opt_level", "speed")], default_libcall_names())
        .map_err(compile_error)?;
    let mut memory = CodeMemory(Some(JITModule::new(builder)));
    let module = memory.0.as_mut().unwrap();
    if module.target_config().pointer_type() != types::I64 {
        return Err(Error::Execution(
            "bitmap JIT requires a 64-bit target".into(),
        ));
    }
    let mut compile_function = |name: &str, params: usize, predicate_kernel: bool| -> Result<_> {
        let mut ctx = module.make_context();
        ctx.set_disasm(disassemble);
        ctx.func.signature.params = vec![AbiParam::new(types::I64); params];
        ctx.func.signature.returns.push(AbiParam::new(types::I64));
        let id = module
            .declare_function(name, Linkage::Local, &ctx.func.signature)
            .map_err(compile_error)?;
        let mut frontend = FunctionBuilderContext::new();
        let mut b = FunctionBuilder::new(&mut ctx.func, &mut frontend);
        if predicate_kernel {
            emit_predicate(&mut b, predicate, references)?;
        } else {
            emit_project(&mut b, outputs, references)?;
        }
        b.seal_all_blocks();
        b.finalize(module.target_config());
        let clif = ctx.func.display().to_string();
        module
            .define_function(id, &mut ctx)
            .map_err(compile_error)?;
        let assembly = ctx
            .compiled_code()
            .and_then(|code| code.vcode.as_deref())
            .unwrap_or("")
            .to_owned();
        Ok((id, clif, assembly))
    };
    let (predicate_id, predicate_clif, predicate_asm) =
        compile_function("bitmap_predicate", 3, true)?;
    let (project_id, project_clif, project_asm) = compile_function("bitmap_project", 4, false)?;
    module.finalize_definitions().map_err(compile_error)?;
    // SAFETY: native C signatures match the aliases. Both code allocations are
    // private and retained by the returned owner until all synchronous calls end.
    let predicate = unsafe {
        std::mem::transmute::<*const u8, PredicateKernel>(
            module.get_finalized_function(predicate_id),
        )
    };
    let project = unsafe {
        std::mem::transmute::<*const u8, ProjectKernel>(module.get_finalized_function(project_id))
    };
    Ok(CompiledKernels {
        memory,
        predicate,
        project,
        clif: format!("{predicate_clif}\n{project_clif}"),
        disassembly: disassemble.then(|| {
            format!("; bitmap_predicate\n{predicate_asm}\n; bitmap_project\n{project_asm}")
        }),
    })
}

fn block(b: &mut FunctionBuilder<'_>, params: usize) -> Block {
    let block = b.create_block();
    for _ in 0..params {
        b.append_block_param(block, types::I64);
    }
    block
}

fn emit_predicate(
    b: &mut FunctionBuilder<'_>,
    predicate: &Expr,
    references: &[usize],
) -> Result<()> {
    let entry = b.create_block();
    let full = block(b, 2); // base, selected
    let pack_full = b.create_block();
    let tail_check = b.create_block();
    let tail = block(b, 2); // row, packed mask
    let pack_tail = b.create_block();
    let store_tail = b.create_block();
    let done = b.create_block();
    let overflow = b.create_block();
    b.set_cold_block(overflow);
    b.append_block_params_for_function_params(entry);
    b.switch_to_block(entry);
    let args = b.block_params(entry).to_vec();
    let bases = load_pointers(b, args[0], references.len(), types::I64)?;
    let zero = b.ins().iconst(types::I64, 0);
    b.ins().jump(full, &[zero.into(), zero.into()]);

    b.switch_to_block(full);
    let base = b.block_params(full)[0];
    let selected = b.block_params(full)[1];
    let remaining = b.ins().isub(args[1], base);
    let has_full = b
        .ins()
        .icmp_imm_s(IntCC::UnsignedGreaterThanOrEqual, remaining, 64);
    b.ins().brif(has_full, pack_full, &[], tail_check, &[]);

    b.switch_to_block(pack_full);
    let byte_base = b.ins().ishl_imm_u(base, 3);
    let mask = if let Some(mask) = emit_simd_mask(b, predicate, references, &bases, byte_base) {
        mask
    } else {
        let mut mask = zero;
        for lane in 0..64 {
            let offset = b.ins().iadd_imm_u(byte_base, lane * 8);
            let pass = emit_expr(b, predicate, references, &bases, offset, overflow);
            let bit = b.ins().uextend(types::I64, pass);
            let bit = b.ins().ishl_imm_u(bit, lane);
            mask = b.ins().bor(mask, bit);
        }
        mask
    };
    let mask_offset = b.ins().ushr_imm_u(base, 3); // (base / 64) * 8
    let address = b.ins().iadd(args[2], mask_offset);
    b.ins().store(MemFlagsData::new(), mask, address, 0);
    let count = b.ins().popcnt(mask);
    let selected_next = b.ins().iadd(selected, count);
    let base_next = b.ins().iadd_imm_u(base, 64);
    b.ins()
        .jump(full, &[base_next.into(), selected_next.into()]);

    b.switch_to_block(tail_check);
    let has_tail = b.ins().icmp_imm_s(IntCC::NotEqual, remaining, 0);
    b.ins()
        .brif(has_tail, tail, &[base.into(), zero.into()], done, &[]);
    b.switch_to_block(tail);
    let row = b.block_params(tail)[0];
    let mask = b.block_params(tail)[1];
    let has_row = b.ins().icmp(IntCC::UnsignedLessThan, row, args[1]);
    b.ins().brif(has_row, pack_tail, &[], store_tail, &[]);
    b.switch_to_block(pack_tail);
    let offset = b.ins().ishl_imm_u(row, 3);
    let pass = emit_expr(b, predicate, references, &bases, offset, overflow);
    let bit = b.ins().uextend(types::I64, pass);
    let lane = b.ins().isub(row, base);
    let bit = b.ins().ishl(bit, lane);
    let next_mask = b.ins().bor(mask, bit);
    let next_row = b.ins().iadd_imm_u(row, 1);
    b.ins().jump(tail, &[next_row.into(), next_mask.into()]);
    b.switch_to_block(store_tail);
    let mask_offset = b.ins().ushr_imm_u(base, 3);
    let address = b.ins().iadd(args[2], mask_offset);
    b.ins().store(MemFlagsData::new(), mask, address, 0);
    let count = b.ins().popcnt(mask);
    let total = b.ins().iadd(selected, count);
    b.ins().return_(&[total]);
    b.switch_to_block(done);
    b.ins().return_(&[selected]);
    emit_overflow(b, overflow);
    Ok(())
}

/// Pack the same 64 row predicates into one bitmap, using NEON-friendly IR.
/// Arithmetic expressions retain scalar checked evaluation; SIMD is only used
/// for direct Int64 column/constant comparisons on the measured native target.
fn emit_simd_mask(
    b: &mut FunctionBuilder<'_>,
    predicate: &Expr,
    references: &[usize],
    bases: &[Value],
    byte_base: Value,
) -> Option<Value> {
    if !cfg!(all(target_arch = "aarch64", target_endian = "little")) {
        return None;
    }
    let Expr::Binary(kind, left, right) = predicate else {
        return None;
    };
    let condition = comparison(*kind)?;
    if !matches!(&**left, Expr::Column(_) | Expr::Int(_))
        || !matches!(&**right, Expr::Column(_) | Expr::Int(_))
    {
        return None;
    }
    // Only reached after the full-chunk bounds check. Vector loads are
    // unaligned and never read past the 64 live rows (including sliced arrays).
    let operand = |b: &mut FunctionBuilder<'_>, expr: &Expr, lane: i32| match expr {
        Expr::Column(index) => {
            let base = bases[references.binary_search(index).unwrap()];
            let address = b.ins().iadd(base, byte_base);
            b.ins()
                .load(types::I64X2, MemFlagsData::new(), address, lane * 8)
        }
        Expr::Int(value) => {
            let scalar = b.ins().iconst(types::I64, *value);
            b.ins().splat(types::I64X2, scalar)
        }
        _ => unreachable!("validated SIMD operand"),
    };
    let mut parts = Vec::with_capacity(32);
    for lane in (0..64).step_by(2) {
        let lhs = operand(b, left, lane);
        let rhs = operand(b, right, lane);
        let cmp = b.ins().icmp(condition, lhs, rhs);
        let bytes = b.ins().bitcast(
            types::I8X16,
            MemFlagsData::new().with_endianness(cranelift_codegen::ir::Endianness::Little),
            cmp,
        );
        parts.push(bytes);
    }
    // Each comparison lane is all zeros or all ones, so discarding its upper
    // half preserves the Boolean exactly. Pairwise shuffles compact 64-bit
    // masks to 32-, 16-, then 8-bit masks (AArch64 UZP1), retaining row order.
    // Unlike saturating narrowing, each shuffle needs just one instruction.
    for width in [4, 2, 1] {
        let indices: Vec<u8> = (0..32)
            .step_by(width * 2)
            .flat_map(|base| (base..base + width).map(|i| i as u8))
            .collect();
        let shuffle = b.func.dfg.immediates.push(indices.into());
        parts = parts
            .chunks_exact(2)
            .map(|pair| b.ins().shuffle(pair[0], pair[1], shuffle))
            .collect();
    }
    // Extract four 16-bit groups, rather than extracting every two rows.
    let mut mask = b.ins().iconst(types::I64, 0);
    for (i, part) in parts.into_iter().enumerate() {
        let bits = b.ins().vhigh_bits(types::I16, part);
        let bits = b.ins().uextend(types::I64, bits);
        let bits = b.ins().ishl_imm_u(bits, (i * 16) as i64);
        mask = b.ins().bor(mask, bits);
    }
    Some(mask)
}

fn comparison(kind: FunctionKind) -> Option<IntCC> {
    use FunctionKind::*;
    Some(match kind {
        Equal => IntCC::Equal,
        NotEqual => IntCC::NotEqual,
        LessThan => IntCC::SignedLessThan,
        LessThanOrEqual => IntCC::SignedLessThanOrEqual,
        GreaterThan => IntCC::SignedGreaterThan,
        GreaterThanOrEqual => IntCC::SignedGreaterThanOrEqual,
        _ => return None,
    })
}

fn emit_project(b: &mut FunctionBuilder<'_>, outputs: &[Expr], references: &[usize]) -> Result<()> {
    let entry = b.create_block();
    let chunks = block(b, 2); // base, output count
    let dispatch = b.create_block();
    let nonempty = b.create_block();
    let dense = block(b, 2); // row, output count
    let dense_body = b.create_block();
    let dense_four = b.create_block();
    let dense_tail = b.create_block();
    let sparse = block(b, 2); // remaining bits, output count
    let sparse_body = b.create_block();
    let advance = block(b, 1);
    let done = b.create_block();
    let overflow = b.create_block();
    b.set_cold_block(overflow);
    b.append_block_params_for_function_params(entry);
    b.switch_to_block(entry);
    let args = b.block_params(entry).to_vec();
    let bases = load_pointers(b, args[0], references.len(), types::I64)?;
    let out_bases = load_pointers(b, args[3], outputs.len(), types::I64)?;
    let zero = b.ins().iconst(types::I64, 0);
    let sixty_four = b.ins().iconst(types::I64, 64);
    let all = b.ins().iconst(types::I64, -1);
    b.ins().jump(chunks, &[zero.into(), zero.into()]);
    b.switch_to_block(chunks);
    let base = b.block_params(chunks)[0];
    let count = b.block_params(chunks)[1];
    let has_chunk = b.ins().icmp(IntCC::UnsignedLessThan, base, args[1]);
    b.ins().brif(has_chunk, dispatch, &[], done, &[]);
    b.switch_to_block(dispatch);
    let remaining = b.ins().isub(args[1], base);
    let width = b.ins().umin(remaining, sixty_four);
    let end = b.ins().iadd(base, width);
    let mask_offset = b.ins().ushr_imm_u(base, 3);
    let address = b.ins().iadd(args[2], mask_offset);
    let mask = b.ins().load(types::I64, MemFlagsData::new(), address, 0);
    let empty = b.ins().icmp_imm_s(IntCC::Equal, mask, 0);
    b.ins().brif(empty, advance, &[count.into()], nonempty, &[]);
    b.switch_to_block(nonempty);
    let shift = b.ins().isub(sixty_four, width); // 0..63, including tail
    let full_mask = b.ins().ushr(all, shift);
    let is_full = b.ins().icmp(IntCC::Equal, mask, full_mask);
    b.ins().brif(
        is_full,
        dense,
        &[base.into(), count.into()],
        sparse,
        &[mask.into(), count.into()],
    );

    b.switch_to_block(dense);
    let row = b.block_params(dense)[0];
    let out = b.block_params(dense)[1];
    // Four rows share loop bookkeeping. Each expression still checks overflow
    // before its store; the scalar remainder handles short final chunks.
    let remaining_dense = b.ins().isub(end, row);
    let has_four = b
        .ins()
        .icmp_imm_s(IntCC::UnsignedGreaterThanOrEqual, remaining_dense, 4);
    b.ins().brif(has_four, dense_four, &[], dense_tail, &[]);
    b.switch_to_block(dense_four);
    for lane in 0..4 {
        let lane_row = b.ins().iadd_imm_u(row, lane);
        let lane_out = b.ins().iadd_imm_u(out, lane);
        emit_outputs(
            b, outputs, references, &bases, &out_bases, lane_row, lane_out, overflow,
        );
    }
    let next_row = b.ins().iadd_imm_u(row, 4);
    let next_out = b.ins().iadd_imm_u(out, 4);
    b.ins().jump(dense, &[next_row.into(), next_out.into()]);
    b.switch_to_block(dense_tail);
    let has_row = b.ins().icmp(IntCC::UnsignedLessThan, row, end);
    b.ins()
        .brif(has_row, dense_body, &[], advance, &[out.into()]);
    b.switch_to_block(dense_body);
    emit_outputs(
        b, outputs, references, &bases, &out_bases, row, out, overflow,
    );
    let next_row = b.ins().iadd_imm_u(row, 1);
    let next_out = b.ins().iadd_imm_u(out, 1);
    b.ins().jump(dense, &[next_row.into(), next_out.into()]);

    b.switch_to_block(sparse);
    let bits = b.block_params(sparse)[0];
    let out = b.block_params(sparse)[1];
    let has_bit = b.ins().icmp_imm_s(IntCC::NotEqual, bits, 0);
    b.ins()
        .brif(has_bit, sparse_body, &[], advance, &[out.into()]);
    b.switch_to_block(sparse_body);
    let lane = b.ins().ctz(bits);
    let row = b.ins().iadd(base, lane);
    emit_outputs(
        b, outputs, references, &bases, &out_bases, row, out, overflow,
    );
    let minus_one = b.ins().iadd_imm_s(bits, -1);
    let next_bits = b.ins().band(bits, minus_one);
    let next_out = b.ins().iadd_imm_u(out, 1);
    b.ins().jump(sparse, &[next_bits.into(), next_out.into()]);

    b.switch_to_block(advance);
    let next_out = b.block_params(advance)[0];
    b.ins().jump(chunks, &[end.into(), next_out.into()]);
    b.switch_to_block(done);
    b.ins().return_(&[count]);
    emit_overflow(b, overflow);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_outputs(
    b: &mut FunctionBuilder<'_>,
    outputs: &[Expr],
    references: &[usize],
    bases: &[Value],
    out_bases: &[Value],
    row: Value,
    count: Value,
    overflow: Block,
) {
    let offset = b.ins().ishl_imm_u(row, 3);
    let output_offset = b.ins().ishl_imm_u(count, 3);
    for (expr, &base) in outputs.iter().zip(out_bases) {
        let value = emit_expr(b, expr, references, bases, offset, overflow);
        let address = b.ins().iadd(base, output_offset);
        b.ins().store(MemFlagsData::new(), value, address, 0);
    }
}

fn emit_overflow(b: &mut FunctionBuilder<'_>, overflow: Block) {
    b.switch_to_block(overflow);
    let error = b.ins().iconst(types::I64, -1);
    b.ins().return_(&[error]);
}

fn load_pointers(
    b: &mut FunctionBuilder<'_>,
    table: Value,
    len: usize,
    pointer: cranelift_codegen::ir::Type,
) -> Result<Vec<Value>> {
    (0..len)
        .map(|i| {
            let offset = i
                .checked_mul(pointer.bytes() as usize)
                .and_then(|n| i32::try_from(n).ok())
                .ok_or_else(|| Error::InvalidPlan("JIT pointer table too large".into()))?;
            Ok(b.ins().load(pointer, MemFlagsData::new(), table, offset))
        })
        .collect()
}

fn emit_expr(
    b: &mut FunctionBuilder<'_>,
    expr: &Expr,
    references: &[usize],
    bases: &[Value],
    offset: Value,
    overflow: Block,
) -> Value {
    match expr {
        Expr::Column(index) => {
            let base = bases[references.binary_search(index).unwrap()];
            let address = b.ins().iadd(base, offset);
            b.ins().load(types::I64, MemFlagsData::new(), address, 0)
        }
        Expr::Int(value) => b.ins().iconst(types::I64, *value),
        Expr::Bool(value) => b.ins().iconst(types::I8, i64::from(*value)),
        Expr::Binary(kind, left, right) => {
            let left = emit_expr(b, left, references, bases, offset, overflow);
            let right = emit_expr(b, right, references, bases, offset, overflow);
            use FunctionKind::*;
            let arithmetic = match kind {
                Add => Some(b.ins().sadd_overflow(left, right)),
                Subtract => Some(b.ins().ssub_overflow(left, right)),
                Multiply => {
                    // The signed 128-bit product fits in i64 exactly when its
                    // high half equals the sign extension of its low half.
                    // Keeping this as an explicit comparison lets AArch64
                    // branch on flags without materializing an overflow byte.
                    let value = b.ins().imul(left, right);
                    let high = b.ins().smulhi(left, right);
                    let sign = b.ins().sshr_imm_u(value, 63);
                    let failed = b.ins().icmp(IntCC::NotEqual, high, sign);
                    Some((value, failed))
                }
                _ => None,
            };
            if let Some((value, failed)) = arithmetic {
                let next = b.create_block();
                b.ins().brif(failed, overflow, &[], next, &[]);
                b.switch_to_block(next);
                value
            } else {
                let condition = comparison(*kind).expect("validated expression");
                b.ins().icmp(condition, left, right)
            }
        }
    }
}
