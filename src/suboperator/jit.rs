//! A scope compiles to one executable row loop. There is no vector fallback.
use super::{context::Producer, runtime::Runtime, scalar, *};
use crate::{
    arrow::{array::ArrayRef, datatypes::*},
    error::{Error, Result},
};
use cranelift_codegen::ir::{
    AbiParam, Block, InstBuilder, MemFlags, StackSlotData, StackSlotKind, Type, Value,
    condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};
use std::sync::Mutex;
mod cast;
mod emit;
mod reduce;
use emit::LocalEmitContext;

pub(super) type Entry = unsafe extern "C" fn(
    *mut Runtime,
    *const super::column::Column,
    *const super::column::Column,
    *mut usize,
    *const *mut (),
    usize,
    usize,
) -> i32;
#[derive(Debug)]
pub(super) struct Input {
    pub column: Option<usize>,
    pub ty: DataType,
    pub literal: Option<ArrayRef>,
    pub lazy: bool,
    pub nullable: bool,
}
#[derive(Debug)]
pub(super) struct OutputPort {
    pub types: Vec<DataType>,
    pub schema: Option<crate::arrow::datatypes::SchemaRef>,
    pub sink: Option<(StateId, BatchUpdate)>,
    pub passthrough: bool,
    pub aliases: Vec<Option<usize>>,
}
pub(super) struct CompiledProgram {
    pub entry: Entry,
    pub reduction: Option<Entry>,
    module: Mutex<Option<JITModule>>,
    pub inputs: Vec<Input>,
    pub ports: Vec<OutputPort>,
    pub ir: String,
    pub schedule: Vec<super::program::Program>,
    pub passthrough: bool,
}
impl std::fmt::Debug for CompiledProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledProgram")
            .field("inputs", &self.inputs)
            .field("ports", &self.ports)
            .finish_non_exhaustive()
    }
}
impl Drop for CompiledProgram {
    fn drop(&mut self) {
        if let Some(m) = self
            .module
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            unsafe {
                m.free_memory();
            }
        }
    }
}
fn error(e: impl std::fmt::Display) -> Error {
    Error::Execution(format!("Cranelift: {e}"))
}
#[derive(Clone, Copy)]
pub(super) struct Scalar {
    value: Value,
    len: Value,
    valid: Value,
    signed: bool,
}
struct Compiler<'a, 'f> {
    b: FunctionBuilder<'f>,
    module: &'a mut JITModule,
    scope: &'a SuboperatorScope,
    producers: Vec<Option<Producer>>,
    values: Vec<Option<Scalar>>,
    emitted: Vec<bool>,
    args: Vec<Value>,
    row: Value,
    fail: Block,
    setup: Block,
    inputs: Vec<Input>,
    ports: Vec<OutputPort>,
    output_slots: usize,
    source_views: Vec<Option<Vec<VariableId>>>,
    view: Option<Vec<VariableId>>,
    schema: Option<SchemaRef>,
    schedule: Vec<super::program::Program>,
    origins: Vec<Option<usize>>,
    local_depth: usize,
    scope_depth: usize,
}
impl CompiledProgram {
    pub fn compile(
        scope: &SuboperatorScope,
        producers: Vec<Option<Producer>>,
        empty_projection: bool,
    ) -> Result<Self> {
        let mut module = JITModule::new(
            JITBuilder::with_flags(
                &[("opt_level", "speed")],
                cranelift_module::default_libcall_names(),
            )
            .map_err(error)?,
        );
        let mut ctx = module.make_context();
        let ptr = module.target_config().pointer_type();
        if ptr != types::I64 {
            return Err(Error::InvalidPlan(
                "generated scalar ABI requires a 64-bit target".into(),
            ));
        }
        ctx.func.signature.params = vec![AbiParam::new(ptr); 7];
        ctx.func.signature.returns = vec![AbiParam::new(types::I32)];
        let id = module
            .declare_function("roc_pipeline", Linkage::Export, &ctx.func.signature)
            .map_err(error)?;
        let mut fb = FunctionBuilderContext::new();
        let mut b = FunctionBuilder::new(&mut ctx.func, &mut fb);
        let entry = b.create_block();
        b.append_block_params_for_function_params(entry);
        b.switch_to_block(entry);
        let args = b.block_params(entry).to_vec();
        let zero = b.ins().iconst(types::I64, 0);
        let head = b.create_block();
        let body = b.create_block();
        let next = b.create_block();
        let done = b.create_block();
        let fail = b.create_block();
        b.append_block_param(fail, types::I32);
        b.append_block_param(head, types::I64);
        b.append_block_param(next, types::I64);
        let setup = b.create_block();
        b.ins().jump(setup, &[]);
        b.switch_to_block(head);
        let row = b.block_params(head)[0];
        let more = b.ins().icmp(IntCC::UnsignedLessThan, row, args[5]);
        b.ins().brif(more, body, &[], done, &[]);
        b.switch_to_block(body);
        let n = scope.variables.len();
        let mut c = Compiler {
            b,
            module: &mut module,
            scope,
            producers,
            values: vec![None; n],
            emitted: vec![false; scope.suboperators.len()],
            args,
            row,
            fail,
            setup,
            inputs: vec![],
            ports: vec![],
            output_slots: 0,
            source_views: vec![None],
            view: None,
            schema: None,
            schedule: vec![],
            origins: vec![None; n],
            local_depth: 0,
            scope_depth: 0,
        };
        let poll = c.b.create_block();
        let work = c.b.create_block();
        let tick = c.b.ins().band_imm(row, 1023);
        let due = c.b.ins().icmp_imm(IntCC::Equal, tick, 0);
        c.b.ins().brif(due, poll, &[], work, &[]);
        c.b.switch_to_block(poll);
        c.helper(super::runtime::poll as *const () as usize, &[c.args[0]])?;
        c.b.ins().jump(work, &[]);
        c.b.switch_to_block(work);
        let mut required = vec![false; n];
        for v in scope.roots.iter().chain(&scope.outputs) {
            required[v.index] = true;
        }
        // Values crossing a persistent scope boundary are completed in their
        // defining scope, even if the consumer uses them in a local branch.
        for (i, op) in scope.suboperators.iter().enumerate() {
            let owner = scope.boundaries.partition_point(|b| b.end <= i);
            for v in op.inputs() {
                if let Some(Producer::Node(p)) = c.producers[v.index]
                    && scope.boundaries.partition_point(|b| b.end <= p) < owner
                {
                    required[v.index] = true;
                }
            }
        }
        for boundary in &scope.boundaries {
            for v in boundary.operator.inputs() {
                required[v.index] = true;
            }
        }
        let mut dependencies = scope
            .dependencies
            .values()
            .flatten()
            .map(|d| d.producer)
            .collect::<Vec<_>>();
        dependencies.sort_by_key(|p| scope.rank(*p));
        dependencies.dedup();
        let mut scopes = Vec::new();
        let mut boundary = 0;
        for i in 0..=scope.suboperators.len() {
            while boundary < scope.boundaries.len() && scope.boundaries[boundary].end == i {
                let p = Producer::Scope(boundary);
                c.dependencies(p)?;
                let ScopeSuboperator::Filter {
                    predicate,
                    body_inputs,
                    body_outputs,
                } = &scope.boundaries[boundary].operator;
                let pred = c.value(*predicate)?;
                let bindings = body_inputs
                    .iter()
                    .map(|v| c.value(*v))
                    .collect::<Result<Vec<_>>>()?;
                let opened = ScopeEmitContext { compiler: &mut c }.open_filter(pred);
                for (v, value) in body_outputs.iter().zip(bindings) {
                    c.values[v.index] = Some(value);
                }
                c.scope_depth += 1;
                scopes.push(opened);
                boundary += 1;
            }
            let Some(op) = scope.suboperators.get(i) else {
                break;
            };
            if op.outputs().iter().any(|v| required[v.index])
                || dependencies.contains(&Producer::Node(i))
                || matches!(
                    op,
                    Suboperator::AggregateUpdate { .. }
                        | Suboperator::GroupLookup { .. }
                        | Suboperator::Materialize { .. }
                        | Suboperator::BatchUpdate { .. }
                )
            {
                c.node(i)?;
            }
        }
        let outputs = if !scope.outputs.is_empty() {
            Some(scope.outputs.clone())
        } else if empty_projection {
            Some(vec![])
        } else {
            c.view.clone()
        };
        let collect = c.b.create_block();
        let collected = c.b.create_block();
        c.b.ins().brif(c.args[6], collect, &[], collected, &[]);
        c.b.switch_to_block(collect);
        c.output(outputs, c.schema.clone(), None)?;
        c.b.ins().jump(collected, &[]);
        c.b.switch_to_block(collected);
        for opened in scopes.into_iter().rev() {
            ScopeEmitContext { compiler: &mut c }.close(opened);
        }
        c.b.ins().jump(next, &[row.into()]);
        c.b.switch_to_block(next);
        let row = c.b.block_params(next)[0];
        let row = c.b.ins().iadd_imm(row, 1);
        c.b.ins().jump(head, &[row.into()]);
        c.b.switch_to_block(done);
        let status = c.b.ins().iconst(types::I32, 0);
        c.b.ins().return_(&[status]);
        c.b.switch_to_block(fail);
        let status = c.b.block_params(fail)[0];
        c.b.ins().return_(&[status]);
        c.b.switch_to_block(c.setup);
        c.b.ins().jump(head, &[zero.into()]);
        c.b.seal_all_blocks();
        let inputs = std::mem::take(&mut c.inputs);
        let ports = std::mem::take(&mut c.ports);
        let schedule = std::mem::take(&mut c.schedule);
        c.b.finalize();
        let mut ir = ctx.func.display().to_string();
        module.define_function(id, &mut ctx).map_err(error)?;
        let reduction = reduce::compile(&mut module, scope, &inputs)?.map(|(id, lowered)| {
            ir.push_str(
                "
; Specialized global reduction
",
            );
            ir.push_str(&lowered);
            id
        });
        module.finalize_definitions().map_err(error)?;
        let reduction = reduction.map(|id| unsafe {
            std::mem::transmute::<*const u8, Entry>(module.get_finalized_function(id))
        });
        let entry =
            unsafe { std::mem::transmute::<*const u8, Entry>(module.get_finalized_function(id)) };
        Ok(Self {
            entry,
            reduction,
            module: Mutex::new(Some(module)),
            inputs,
            ports,
            ir,
            schedule,
            passthrough: scope.boundaries.is_empty()
                && scope
                    .suboperators
                    .iter()
                    .all(|op| matches!(op, Suboperator::BatchUpdate { .. })),
        })
    }
}
struct OpenedScope {
    end: Block,
}
struct ScopeEmitContext<'c, 'a, 'f> {
    compiler: &'c mut Compiler<'a, 'f>,
}
impl ScopeEmitContext<'_, '_, '_> {
    fn open_filter(&mut self, p: Scalar) -> OpenedScope {
        let c = &mut self.compiler;
        let body = c.b.create_block();
        let end = c.b.create_block();
        let value = c.b.ins().icmp_imm(IntCC::NotEqual, p.value, 0);
        let valid = c.b.ins().icmp_imm(IntCC::NotEqual, p.valid, 0);
        let pass = c.b.ins().band(value, valid);
        c.b.ins().brif(pass, body, &[], end, &[]);
        c.b.switch_to_block(body);
        OpenedScope { end }
    }
    fn close(&mut self, s: OpenedScope) {
        let c = &mut self.compiler;
        c.b.ins().jump(s.end, &[]);
        c.b.switch_to_block(s.end);
    }
}
impl Compiler<'_, '_> {
    fn descriptor(&mut self, argument: usize, offset: usize) -> Value {
        // Insert loop-invariant loads without changing FunctionBuilder's current
        // (possibly unfinished) block. The preheader is terminated after lowering.
        use cranelift_codegen::cursor::{Cursor, FuncCursor};
        if !self.b.func.layout.is_block_inserted(self.setup) {
            self.b.func.layout.append_block(self.setup);
        }
        let arg = self.args[argument];
        FuncCursor::new(self.b.func)
            .at_bottom(self.setup)
            .ins()
            .load(types::I64, MemFlags::new(), arg, offset as i32)
    }
    fn word(&mut self, x: i64) -> Value {
        self.b.ins().iconst(types::I64, x)
    }
    fn scalar(&mut self, bits: Value, valid: Value) -> Scalar {
        let len = self.word(0);
        Scalar {
            value: bits,
            len,
            valid,
            signed: false,
        }
    }
    fn value(&mut self, id: VariableId) -> Result<Scalar> {
        if let Some(v) = self.values[id.index] {
            return Ok(v);
        }
        match self.producers[id.index] {
            Some(Producer::Node(i)) => self.node(i)?,
            _ => {
                return Err(Error::InvalidPlan(
                    "scope output used outside its body".into(),
                ));
            }
        }
        self.values[id.index]
            .ok_or_else(|| Error::InvalidPlan("node failed to define its output".into()))
    }
    fn dependencies(&mut self, p: Producer) -> Result<()> {
        if let Some(deps) = self.scope.dependencies.get(&p) {
            for dep in deps {
                if let Producer::Node(i) = dep.producer {
                    self.node(i)?;
                }
            }
        }
        Ok(())
    }
    fn node(&mut self, i: usize) -> Result<()> {
        if self.emitted[i] {
            return Ok(());
        }
        self.dependencies(Producer::Node(i))?;
        let op = &self.scope.suboperators[i];
        match op {
            Suboperator::Materialize { inputs, schema } => {
                for v in inputs {
                    self.value(*v)?;
                }
                self.view = Some(inputs.clone());
                self.source_views.push(self.view.clone());
                self.schema = schema.clone();
            }
            Suboperator::BatchUpdate { state, call } => {
                self.output(
                    self.view.clone(),
                    self.schema.clone(),
                    Some((*state, *call)),
                )?;
            }
            _ => LocalEmitContext { compiler: self }.emit(op)?,
        }
        self.emitted[i] = true;
        self.schedule.push(super::program::Program::new(op));
        Ok(())
    }
    fn output(
        &mut self,
        outputs: Option<Vec<VariableId>>,
        schema: Option<SchemaRef>,
        sink: Option<(StateId, BatchUpdate)>,
    ) -> Result<()> {
        let passthrough = outputs.is_none();
        if let Some(outputs) = &outputs {
            for &v in outputs {
                self.value(v)?;
            }
        }
        let aliases = outputs.as_ref().map_or(vec![None], |out| {
            out.iter().map(|v| self.origins[v.index]).collect()
        });
        let types = outputs.as_ref().map_or(vec![DataType::UInt64], |out| {
            out.iter()
                .map(|v| self.scope.variables[v.index].ty.data_type().clone())
                .collect()
        });
        let values = if let Some(outputs) = outputs {
            outputs
                .iter()
                .map(|v| self.value(*v))
                .collect::<Result<Vec<_>>>()?
        } else {
            let yes = self.word(1);
            vec![self.scalar(self.row, yes)]
        };
        let port = self.ports.len();
        let count = self
            .b
            .ins()
            .load(types::I64, MemFlags::new(), self.args[3], (port * 8) as i32);
        for (v, ty) in values.into_iter().zip(&types) {
            let ptr = self.b.ins().load(
                types::I64,
                MemFlags::new(),
                self.args[2],
                (self.output_slots * 24) as i32,
            );
            if super::column::direct_output(ty) {
                let offset = self.b.ins().ishl_imm(count, 3);
                let ptr = self.b.ins().iadd(ptr, offset);
                let bits = self.pack_bits(v);
                self.b.ins().store(MemFlags::new(), bits, ptr, 0);
                let bitmap = self.b.ins().load(
                    types::I64,
                    MemFlags::new(),
                    self.args[2],
                    (self.output_slots * 24 + 8) as i32,
                );
                let byte = self.b.ins().ushr_imm(count, 3);
                let at = self.b.ins().iadd(bitmap, byte);
                let shift = self.b.ins().band_imm(count, 7);
                let valid = self.b.ins().ireduce(types::I8, v.valid);
                let bit = self.b.ins().ishl(valid, shift);
                let old = self.b.ins().load(types::I8, MemFlags::new(), at, 0);
                let bits = self.b.ins().bor(old, bit);
                self.b.ins().store(MemFlags::new(), bits, at, 0);
            } else {
                let offset = self.b.ins().imul_imm(count, 24);
                let ptr = self.b.ins().iadd(ptr, offset);
                self.store(ptr, v);
            }
            self.output_slots += 1;
        }
        let next = self.b.ins().iadd_imm(count, 1);
        self.b
            .ins()
            .store(MemFlags::new(), next, self.args[3], (port * 8) as i32);
        self.ports.push(OutputPort {
            types,
            schema,
            sink,
            passthrough,
            aliases,
        });
        Ok(())
    }
    fn pack_bits(&mut self, v: Scalar) -> Value {
        let ty = self.b.func.dfg.value_type(v.value);
        let bits = match ty {
            types::F32 => self.b.ins().bitcast(types::I32, MemFlags::new(), v.value),
            types::F64 => self.b.ins().bitcast(types::I64, MemFlags::new(), v.value),
            _ => v.value,
        };
        if self.b.func.dfg.value_type(bits).bits() < 64 {
            if v.signed {
                self.b.ins().sextend(types::I64, bits)
            } else {
                self.b.ins().uextend(types::I64, bits)
            }
        } else {
            bits
        }
    }
    fn store(&mut self, ptr: Value, v: Scalar) {
        let bits = self.pack_bits(v);
        self.b.ins().store(MemFlags::new(), bits, ptr, 0);
        self.b.ins().store(MemFlags::new(), v.len, ptr, 8);
        self.b.ins().store(MemFlags::new(), v.valid, ptr, 16);
    }
    fn load(&mut self, ptr: Value, ty: &DataType) -> Scalar {
        let bits = self.b.ins().load(types::I64, MemFlags::new(), ptr, 0);
        let len = self.b.ins().load(types::I64, MemFlags::new(), ptr, 8);
        let valid = self.b.ins().load(types::I64, MemFlags::new(), ptr, 16);
        let t = value_type(ty);
        let bits = if t.bits() < 64 {
            self.b
                .ins()
                .ireduce(Type::int(t.bits() as u16).unwrap(), bits)
        } else {
            bits
        };
        let bits = if t.is_float() {
            self.b.ins().bitcast(t, MemFlags::new(), bits)
        } else {
            bits
        };
        Scalar {
            value: bits,
            len,
            valid,
            signed: scalar::signed(ty),
        }
    }
    fn load_column(
        &mut self,
        slot: usize,
        row: Value,
        ty: &DataType,
        ptr: Value,
        nullable: bool,
    ) -> Scalar {
        if !super::column::direct(ty) {
            let offset = self.b.ins().imul_imm(row, 24);
            let ptr = self.b.ins().iadd(ptr, offset);
            return self.load(ptr, ty);
        }
        let offset = self.b.ins().imul_imm(row, (scalar::width(ty) / 8) as i64);
        let at = self.b.ins().iadd(ptr, offset);
        let value = self.b.ins().load(value_type(ty), MemFlags::new(), at, 0);
        if !nullable {
            let len = self.word(0);
            let valid = self.word(1);
            return Scalar {
                value,
                len,
                valid,
                signed: scalar::signed(ty),
            };
        }
        let bitmap = self.descriptor(1, slot * 24 + 8);
        let valid = self.b.create_block();
        let nullable = self.b.create_block();
        self.b.append_block_param(valid, types::I64);
        let yes = self.word(1);
        self.b
            .ins()
            .brif(bitmap, nullable, &[], valid, &[yes.into()]);
        self.b.switch_to_block(nullable);
        let base = self.descriptor(1, slot * 24 + 16);
        let bit = self.b.ins().iadd(base, row);
        let byte = self.b.ins().ushr_imm(bit, 3);
        let at = self.b.ins().iadd(bitmap, byte);
        let bits = self.b.ins().uload8(types::I64, MemFlags::new(), at, 0);
        let shift = self.b.ins().band_imm(bit, 7);
        let bits = self.b.ins().ushr(bits, shift);
        let bits = self.b.ins().band_imm(bits, 1);
        self.b.ins().jump(valid, &[bits.into()]);
        self.b.switch_to_block(valid);
        let valid = self.b.block_params(valid)[0];
        let len = self.word(0);
        Scalar {
            value,
            len,
            valid,
            signed: scalar::signed(ty),
        }
    }
    fn null_typed(&mut self, ty: &DataType) -> Scalar {
        let t = value_type(ty);
        let bits = match t {
            types::F32 => self.b.ins().f32const(0.0),
            types::F64 => self.b.ins().f64const(0.0),
            _ => self.b.ins().iconst(t, 0),
        };
        let zero = self.word(0);
        Scalar {
            value: bits,
            len: zero,
            valid: zero,
            signed: scalar::signed(ty),
        }
    }
    fn stack(&mut self, values: &[Scalar]) -> Value {
        let slot = self.b.create_sized_stack_slot(StackSlotData::new(
            StackSlotKind::ExplicitSlot,
            (values.len().max(1) * 24) as u32,
            3,
        ));
        let ptr = self.b.ins().stack_addr(types::I64, slot, 0);
        for (i, v) in values.iter().enumerate() {
            let p = self.b.ins().iadd_imm(ptr, (i * 24) as i64);
            self.store(p, *v);
        }
        ptr
    }
    fn call(&mut self, address: usize, args: &[Value], returns: &[Type]) -> Result<Vec<Value>> {
        let mut sig = self.module.make_signature();
        sig.params = args
            .iter()
            .map(|v| AbiParam::new(self.b.func.dfg.value_type(*v)))
            .collect();
        sig.returns = returns.iter().map(|t| AbiParam::new(*t)).collect();
        let sig = self.b.import_signature(sig);
        let address = self.word(address as i64);
        let inst = self.b.ins().call_indirect(sig, address, args);
        Ok(self.b.inst_results(inst).to_vec())
    }
    fn check(&mut self, bad: Value) {
        self.check_code(bad, 2)
    }
    fn check_code(&mut self, bad: Value, code: i64) {
        let next = self.b.create_block();
        let code = self.b.ins().iconst(types::I32, code);
        self.b.ins().brif(bad, self.fail, &[code.into()], next, &[]);
        self.b.switch_to_block(next);
    }
    fn helper(&mut self, address: usize, args: &[Value]) -> Result<()> {
        let status = self.call(address, args, &[types::I32])?[0];
        self.check_code(status, 1);
        Ok(())
    }
}

fn value_type(t: &DataType) -> Type {
    match t {
        DataType::Float32 => types::F32,
        DataType::Float64 => types::F64,
        DataType::Boolean => types::I64,
        _ => Type::int(scalar::width(t) as u16).unwrap(),
    }
}
