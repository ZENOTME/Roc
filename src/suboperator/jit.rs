//! Direct Cranelift lowering. Supported scalar regions become one row loop.
//! Other regions retain their native programs; no C text or runtime compiler in native mode.
use super::{FunctionKind, Suboperator, SuboperatorContext, VariableId};
use crate::error::{Error, Result};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray},
    datatypes::*,
};
use cranelift_codegen::ir::{AbiParam, InstBuilder, MemFlags, Value, condcodes::IntCC, types};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};
use std::sync::{Arc, Mutex};

// Slots address primitive Arrow values at their native width (Boolean uses
// bytes). Validity bytes keep Arrow bit offsets outside the generated ABI.
// Output storage is aligned to u64 but uses each output type’s actual stride.
type Entry = unsafe extern "C" fn(
    *const *const u64,
    *const *const u8,
    *const *mut u64,
    *const *mut u8,
    usize,
) -> i32;
pub(super) struct CompiledProgram {
    entry: Entry,
    module: Mutex<Option<JITModule>>,
    inputs: Vec<(usize, DataType)>,
    outputs: Vec<DataType>,
}
impl std::fmt::Debug for CompiledProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledProgram")
            .field("inputs", &self.inputs)
            .field("outputs", &self.outputs)
            .finish_non_exhaustive()
    }
}
impl Drop for CompiledProgram {
    fn drop(&mut self) {
        if let Some(module) = self
            .module
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            // The only callable pointer is private and execute borrows self.
            // Exclusive destruction therefore cannot overlap an invocation.
            unsafe {
                module.free_memory();
            }
        }
    }
}
fn compilation_error(e: impl std::fmt::Display) -> Error {
    Error::Execution(format!("Cranelift: {e}"))
}
impl CompiledProgram {
    pub fn compile(context: &SuboperatorContext, outputs: &[VariableId]) -> Result<Option<Self>> {
        use FunctionKind::*;
        if context
            .suboperators
            .iter()
            .any(|op| op.outputs().is_empty())
        {
            return Ok(None);
        }
        let mut needed = vec![false; context.variables.len()];
        for o in outputs.iter().chain(&context.roots) {
            needed[o.index] = true;
        }
        for op in context.suboperators.iter().rev() {
            if needed[op.outputs()[0].index] {
                for i in op.inputs() {
                    needed[i.index] = true;
                }
            }
        }
        let ops = context
            .suboperators
            .iter()
            .filter(|op| needed[op.outputs()[0].index])
            .collect::<Vec<_>>();
        let mut inputs = vec![];
        for op in &ops {
            if !supports(context, op) {
                return Ok(None);
            }
            if let Suboperator::SourceColumn { column, output, .. } = op {
                inputs.push((
                    *column,
                    context.variables[output.index].ty.data_type().clone(),
                ));
            }
        }
        if ops.is_empty() {
            return Ok(None);
        }
        let mut module = JITModule::new(
            JITBuilder::with_flags(
                &[("opt_level", "speed")],
                cranelift_module::default_libcall_names(),
            )
            .map_err(compilation_error)?,
        );
        let ptr = module.target_config().pointer_type();
        let mut ctx = module.make_context();
        ctx.func.signature.params = vec![AbiParam::new(ptr); 5];
        ctx.func.signature.returns.push(AbiParam::new(types::I32));
        let id = module
            .declare_function("vector_program", Linkage::Local, &ctx.func.signature)
            .map_err(compilation_error)?;
        let mut fbctx = FunctionBuilderContext::new();
        {
            let mut b = FunctionBuilder::new(&mut ctx.func, &mut fbctx);
            let entry = b.create_block();
            let header = b.create_block();
            let body = b.create_block();
            let done = b.create_block();
            let fail = b.create_block();
            b.append_block_params_for_function_params(entry);
            b.append_block_param(header, ptr);
            b.switch_to_block(entry);
            let args = b.block_params(entry).to_vec();
            let zero = b.ins().iconst(ptr, 0);
            b.ins().jump(header, &[zero.into()]);
            b.switch_to_block(header);
            let row = b.block_params(header)[0];
            let more = b.ins().icmp(IntCC::UnsignedLessThan, row, args[4]);
            b.ins().brif(more, body, &[], done, &[]);
            b.switch_to_block(body);
            let yes = b.ins().iconst(types::I8, 1);
            let no = b.ins().iconst(types::I8, 0);
            let mut values: Vec<Option<(Value, Value)>> = vec![None; context.variables.len()];
            let mut slot = 0;
            for op in ops {
                let output = op.outputs()[0];
                let ty = context.variables[output.index].ty.data_type();
                let pair = match op {
                    Suboperator::SourceColumn { .. } => {
                        let vp =
                            b.ins()
                                .load(ptr, MemFlags::new(), args[0], slot * ptr.bytes() as i32);
                        let np =
                            b.ins()
                                .load(ptr, MemFlags::new(), args[1], slot * ptr.bytes() as i32);
                        let offset = b.ins().imul_imm(row, cl_type(ty).unwrap().bytes() as i64);
                        let vp = b.ins().iadd(vp, offset);
                        let np = b.ins().iadd(np, row);
                        let v = b.ins().load(cl_type(ty).unwrap(), MemFlags::new(), vp, 0);
                        let n = b.ins().load(types::I8, MemFlags::new(), np, 0);
                        slot += 1;
                        (v, n)
                    }
                    Suboperator::Constant { value: a, .. } => {
                        let valid = a.is_valid(0);
                        let t = cl_type(ty).unwrap();
                        let v = match ty {
                            DataType::Float32 => b.ins().f32const(if valid {
                                a.as_primitive::<Float32Type>().value(0)
                            } else {
                                0.0
                            }),
                            DataType::Float64 => b.ins().f64const(if valid {
                                a.as_primitive::<Float64Type>().value(0)
                            } else {
                                0.0
                            }),
                            _ => b.ins().iconst(t, if valid { constant_bits(a) } else { 0 }),
                        };
                        (v, if valid { yes } else { no })
                    }
                    Suboperator::Call {
                        function, inputs, ..
                    } => {
                        let (a, an) = values[inputs[0].index].unwrap();
                        let (v, n) = if matches!(function, IsNull | IsNotNull) {
                            let v = if *function == IsNull {
                                b.ins().icmp_imm(IntCC::Equal, an, 0)
                            } else {
                                an
                            };
                            (v, yes)
                        } else {
                            let input_type = context.variables[inputs[0].index].ty.data_type();
                            let float = matches!(input_type, DataType::Float32 | DataType::Float64);
                            let (r, rn) = if inputs.len() == 2 {
                                values[inputs[1].index].unwrap()
                            } else {
                                (a, an)
                            };
                            let n = b.ins().band(an, rn);
                            let v = if matches!(
                                function,
                                Equal
                                    | NotEqual
                                    | LessThan
                                    | LessThanOrEqual
                                    | GreaterThan
                                    | GreaterThanOrEqual
                                    | IsDistinctFrom
                                    | IsNotDistinctFrom
                            ) {
                                let (a, r) = if float {
                                    (float_order_key(&mut b, a), float_order_key(&mut b, r))
                                } else {
                                    (a, r)
                                };
                                let unsigned = matches!(
                                    input_type,
                                    DataType::UInt8
                                        | DataType::UInt16
                                        | DataType::UInt32
                                        | DataType::UInt64
                                        | DataType::Boolean
                                );
                                let cc = match function {
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
                                let result = b.ins().icmp(cc, a, r);
                                if matches!(function, IsDistinctFrom | IsNotDistinctFrom) {
                                    let different_null = b.ins().bxor(an, rn);
                                    let comparison = if *function == IsDistinctFrom {
                                        result
                                    } else {
                                        b.ins().bxor_imm(result, 1)
                                    };
                                    let both_valid_differ = b.ins().band(n, comparison);
                                    let distinct = b.ins().bor(different_null, both_valid_differ);
                                    if *function == IsDistinctFrom {
                                        distinct
                                    } else {
                                        b.ins().bxor_imm(distinct, 1)
                                    }
                                } else {
                                    result
                                }
                            } else if float {
                                match function {
                                    Add => b.ins().fadd(a, r),
                                    Subtract => b.ins().fsub(a, r),
                                    Multiply => b.ins().fmul(a, r),
                                    Divide => b.ins().fdiv(a, r),
                                    Negate => b.ins().fneg(a),
                                    _ => unreachable!(),
                                }
                            } else {
                                let unsigned = matches!(
                                    input_type,
                                    DataType::UInt8
                                        | DataType::UInt16
                                        | DataType::UInt32
                                        | DataType::UInt64
                                );
                                let t = cl_type(input_type).unwrap();
                                let (v, overflow) = match (function, unsigned) {
                                    (Add, false) => b.ins().sadd_overflow(a, r),
                                    (Add, true) => b.ins().uadd_overflow(a, r),
                                    (Subtract, false) => b.ins().ssub_overflow(a, r),
                                    (Subtract, true) => b.ins().usub_overflow(a, r),
                                    (Multiply, false) => b.ins().smul_overflow(a, r),
                                    (Multiply, true) => b.ins().umul_overflow(a, r),
                                    (Negate, false) => {
                                        let z = b.ins().iconst(t, 0);
                                        b.ins().ssub_overflow(z, a)
                                    }
                                    _ => unreachable!(),
                                };
                                let bad = b.ins().band(overflow, n);
                                let next = b.create_block();
                                b.ins().brif(bad, fail, &[], next, &[]);
                                b.switch_to_block(next);
                                v
                            };
                            let n = if matches!(function, IsDistinctFrom | IsNotDistinctFrom) {
                                yes
                            } else {
                                n
                            };
                            (v, n)
                        };
                        (v, n)
                    }
                    Suboperator::Not { input, .. } => {
                        let (v, n) = values[input.index].unwrap();
                        (b.ins().bxor_imm(v, 1), n)
                    }
                    Suboperator::Boolean { and, inputs, .. } => {
                        let mut v = if *and { yes } else { no };
                        let mut n = yes;
                        for input in inputs {
                            let (r, rn) = values[input.index].unwrap();
                            let decisive = if *and {
                                let lfalse = b.ins().bxor_imm(v, 1);
                                let rfalse = b.ins().bxor_imm(r, 1);
                                let l = b.ins().band(lfalse, n);
                                let r = b.ins().band(rfalse, rn);
                                b.ins().bor(l, r)
                            } else {
                                let l = b.ins().band(v, n);
                                let r = b.ins().band(r, rn);
                                b.ins().bor(l, r)
                            };
                            let both = b.ins().band(n, rn);
                            n = b.ins().bor(both, decisive);
                            v = if *and {
                                b.ins().band(v, r)
                            } else {
                                b.ins().bor(v, r)
                            };
                        }
                        (v, n)
                    }
                    _ => unreachable!(),
                };
                values[output.index] = Some(pair);
            }
            for (slot, o) in outputs.iter().enumerate() {
                let (v, n) = values[o.index].unwrap();
                let vp = b.ins().load(
                    ptr,
                    MemFlags::new(),
                    args[2],
                    (slot * ptr.bytes() as usize) as i32,
                );
                let np = b.ins().load(
                    ptr,
                    MemFlags::new(),
                    args[3],
                    (slot * ptr.bytes() as usize) as i32,
                );
                let offset = b.ins().imul_imm(
                    row,
                    cl_type(context.variables[o.index].ty.data_type())
                        .unwrap()
                        .bytes() as i64,
                );
                let vp = b.ins().iadd(vp, offset);
                let np = b.ins().iadd(np, row);
                b.ins().store(MemFlags::new(), v, vp, 0);
                b.ins().store(MemFlags::new(), n, np, 0);
            }
            let next = b.ins().iadd_imm(row, 1);
            b.ins().jump(header, &[next.into()]);
            b.switch_to_block(done);
            let status = b.ins().iconst(types::I32, 0);
            b.ins().return_(&[status]);
            b.switch_to_block(fail);
            let status = b.ins().iconst(types::I32, 1);
            b.ins().return_(&[status]);
            b.seal_all_blocks();
            b.finalize();
        }
        module
            .define_function(id, &mut ctx)
            .map_err(compilation_error)?;
        module.finalize_definitions().map_err(compilation_error)?;
        // The generated signature above exactly matches Entry on the native target.
        let entry =
            unsafe { std::mem::transmute::<*const u8, Entry>(module.get_finalized_function(id)) };
        Ok(Some(Self {
            entry,
            module: Mutex::new(Some(module)),
            inputs,
            outputs: outputs
                .iter()
                .map(|v| context.variables[v.index].ty.data_type().clone())
                .collect(),
        }))
    }
    pub fn execute(&self, columns: &[ArrayRef], rows: usize) -> Result<Vec<ArrayRef>> {
        let mut values = vec![];
        let mut validity = vec![];
        let mut bool_storage = Vec::new();
        for (column, ty) in &self.inputs {
            let a = columns
                .get(*column)
                .ok_or_else(|| Error::Execution(format!("column index {column} out of bounds")))?;
            if a.data_type() != ty || a.len() != rows {
                return Err(Error::Execution(
                    "source column differs from compiled variable".into(),
                ));
            }
            let pointer = if *ty == DataType::Boolean {
                bool_storage.push(
                    a.as_boolean()
                        .values()
                        .iter()
                        .map(u8::from)
                        .collect::<Vec<_>>(),
                );
                bool_storage.last().unwrap().as_ptr()
            } else {
                primitive_pointer(a)
            };
            values.push(pointer.cast::<u64>());
            validity.push(
                (0..rows)
                    .map(|i| u8::from(a.is_valid(i)))
                    .collect::<Vec<_>>(),
            );
        }
        let null_ptrs = validity.iter().map(|v| v.as_ptr()).collect::<Vec<_>>();
        let mut out = self
            .outputs
            .iter()
            .map(|ty| vec![0u64; (rows * cl_type(ty).unwrap().bytes() as usize).div_ceil(8)])
            .collect::<Vec<_>>();
        let mut valid = vec![vec![0u8; rows]; self.outputs.len()];
        let out_ptrs = out.iter_mut().map(|v| v.as_mut_ptr()).collect::<Vec<_>>();
        let valid_ptrs = valid.iter_mut().map(|v| v.as_mut_ptr()).collect::<Vec<_>>();
        // All owners outlive this synchronous call, lengths/types were checked,
        // outputs do not alias inputs, and no pointer escapes generated code.
        let status = unsafe {
            (self.entry)(
                values.as_ptr(),
                null_ptrs.as_ptr(),
                out_ptrs.as_ptr(),
                valid_ptrs.as_ptr(),
                rows,
            )
        };
        if status != 0 {
            return Err(arrow::error::ArrowError::ArithmeticOverflow(
                "compiled integer arithmetic overflow".into(),
            )
            .into());
        }
        Ok(self
            .outputs
            .iter()
            .zip(out)
            .zip(valid)
            .map(|((ty, v), n)| output_array(ty, v, n, rows))
            .collect())
    }
}

fn nulls(valid: Vec<u8>) -> Option<NullBuffer> {
    if valid.iter().all(|&v| v != 0) {
        None
    } else {
        Some(NullBuffer::from(
            valid.into_iter().map(|v| v != 0).collect::<Vec<_>>(),
        ))
    }
}

fn cl_type(ty: &DataType) -> Option<cranelift_codegen::ir::Type> {
    Some(match ty {
        DataType::Int8 | DataType::UInt8 | DataType::Boolean => types::I8,
        DataType::Int16 | DataType::UInt16 => types::I16,
        DataType::Int32 | DataType::UInt32 => types::I32,
        DataType::Int64 | DataType::UInt64 => types::I64,
        DataType::Float32 => types::F32,
        DataType::Float64 => types::F64,
        _ => return None,
    })
}
fn constant_bits(a: &ArrayRef) -> i64 {
    macro_rules! value {
        ($t:ty) => {
            a.as_primitive::<$t>().value(0) as i64
        };
    }
    match a.data_type() {
        DataType::Int8 => value!(Int8Type),
        DataType::Int16 => value!(Int16Type),
        DataType::Int32 => value!(Int32Type),
        DataType::Int64 => value!(Int64Type),
        DataType::UInt8 => value!(UInt8Type),
        DataType::UInt16 => value!(UInt16Type),
        DataType::UInt32 => value!(UInt32Type),
        DataType::UInt64 => value!(UInt64Type),
        DataType::Boolean => i64::from(a.as_boolean().value(0)),
        _ => unreachable!(),
    }
}
fn primitive_pointer(a: &ArrayRef) -> *const u8 {
    macro_rules! pointer {
        ($t:ty) => {
            a.as_primitive::<$t>().values().as_ptr().cast::<u8>()
        };
    }
    match a.data_type() {
        DataType::Int8 => pointer!(Int8Type),
        DataType::Int16 => pointer!(Int16Type),
        DataType::Int32 => pointer!(Int32Type),
        DataType::Int64 => pointer!(Int64Type),
        DataType::UInt8 => pointer!(UInt8Type),
        DataType::UInt16 => pointer!(UInt16Type),
        DataType::UInt32 => pointer!(UInt32Type),
        DataType::UInt64 => pointer!(UInt64Type),
        DataType::Float32 => pointer!(Float32Type),
        DataType::Float64 => pointer!(Float64Type),
        _ => unreachable!(),
    }
}
fn output_array(ty: &DataType, values: Vec<u64>, valid: Vec<u8>, rows: usize) -> ArrayRef {
    let buffer = Buffer::from_vec(values);
    if *ty == DataType::Boolean {
        return Arc::new(BooleanArray::from_iter(
            buffer
                .as_slice()
                .iter()
                .take(rows)
                .zip(valid)
                .map(|(&v, n)| (n != 0).then_some(v != 0)),
        ));
    }
    let nulls = nulls(valid);
    macro_rules! array {
        ($t:ty) => {
            Arc::new(PrimitiveArray::<$t>::new(
                ScalarBuffer::new(buffer, 0, rows),
                nulls,
            )) as ArrayRef
        };
    }
    match ty {
        DataType::Int8 => array!(Int8Type),
        DataType::Int16 => array!(Int16Type),
        DataType::Int32 => array!(Int32Type),
        DataType::Int64 => array!(Int64Type),
        DataType::UInt8 => array!(UInt8Type),
        DataType::UInt16 => array!(UInt16Type),
        DataType::UInt32 => array!(UInt32Type),
        DataType::UInt64 => array!(UInt64Type),
        DataType::Float32 => array!(Float32Type),
        DataType::Float64 => array!(Float64Type),
        _ => unreachable!(),
    }
}
// Arrow comparisons use IEEE total ordering, including signed zero and NaN
// payloads. Transform the bits exactly as Rust’s total_cmp does.
fn float_order_key(b: &mut FunctionBuilder<'_>, v: Value) -> Value {
    let t = if b.func.dfg.value_type(v) == types::F32 {
        types::I32
    } else {
        types::I64
    };
    let bits = b.ins().bitcast(t, MemFlags::new(), v);
    let sign = b.ins().sshr_imm(bits, i64::from(t.bits() - 1));
    let mask = b.ins().ushr_imm(sign, 1);
    b.ins().bxor(bits, mask)
}
pub(super) fn supports(context: &SuboperatorContext, op: &Suboperator) -> bool {
    use FunctionKind::*;
    match op {
        Suboperator::SourceColumn { output, .. } | Suboperator::Constant { output, .. } => {
            cl_type(context.variables[output.index].ty.data_type()).is_some()
        }
        Suboperator::Not { .. } | Suboperator::Boolean { .. } => true,
        Suboperator::Call {
            function, inputs, ..
        } => {
            let ty = context.variables[inputs[0].index].ty.data_type();
            if cl_type(ty).is_none() {
                return false;
            }
            match function {
                Add | Subtract | Multiply | Negate => *ty != DataType::Boolean,
                Divide => matches!(ty, DataType::Float32 | DataType::Float64),
                Remainder => false,
                _ => true,
            }
        }
        _ => false,
    }
}
