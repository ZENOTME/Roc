//! LLVM emission shared by fused programs, cached kernels, and SIMD reductions.
//! Mutable graph variables are entry allocas; LLVM's O3 pipeline promotes them
//! to SSA. Explicit loop/block arguments are LLVM phi nodes from the outset.
use inkwell::{
    AddressSpace, FloatPredicate, IntPredicate, OptimizationLevel,
    basic_block::BasicBlock,
    builder::Builder,
    context::Context,
    execution_engine::ExecutionEngine,
    intrinsics::Intrinsic,
    module::Module,
    passes::PassBuilderOptions,
    targets::{CodeModel, InitializationConfig, RelocMode, Target, TargetMachine},
    types::{BasicType, BasicTypeEnum, FunctionType},
    values::{BasicValue, BasicValueEnum, FunctionValue, PhiValue, PointerValue},
};
use std::{collections::HashMap, sync::OnceLock};

pub type Value = BasicValueEnum<'static>;
pub type FuncId = FunctionValue<'static>;
pub type Block = BasicBlock<'static>;
pub type Var = usize;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    I64X2,
}
pub mod types {
    pub use super::Type::*;
}
impl Type {
    pub fn bits(self) -> u32 {
        match self {
            Self::I8 => 8,
            Self::I16 => 16,
            Self::I32 | Self::F32 => 32,
            Self::I64 | Self::F64 => 64,
            Self::I64X2 => 128,
        }
    }
    pub fn bytes(self) -> u32 {
        self.bits() / 8
    }
    fn llvm(self, c: &'static Context) -> BasicTypeEnum<'static> {
        match self {
            Self::I8 => c.i8_type().into(),
            Self::I16 => c.i16_type().into(),
            Self::I32 => c.i32_type().into(),
            Self::I64 => c.i64_type().into(),
            Self::F32 => c.f32_type().into(),
            Self::F64 => c.f64_type().into(),
            Self::I64X2 => c.i64_type().vec_type(2).into(),
        }
    }
}
#[derive(Clone, Copy)]
pub enum IntCC {
    Equal,
    NotEqual,
    SignedLessThan,
    SignedLessThanOrEqual,
    SignedGreaterThan,
    SignedGreaterThanOrEqual,
    UnsignedLessThan,
    UnsignedLessThanOrEqual,
    UnsignedGreaterThan,
    UnsignedGreaterThanOrEqual,
}
impl IntCC {
    fn llvm(self) -> IntPredicate {
        match self {
            Self::Equal => IntPredicate::EQ,
            Self::NotEqual => IntPredicate::NE,
            Self::SignedLessThan => IntPredicate::SLT,
            Self::SignedLessThanOrEqual => IntPredicate::SLE,
            Self::SignedGreaterThan => IntPredicate::SGT,
            Self::SignedGreaterThanOrEqual => IntPredicate::SGE,
            Self::UnsignedLessThan => IntPredicate::ULT,
            Self::UnsignedLessThanOrEqual => IntPredicate::ULE,
            Self::UnsignedGreaterThan => IntPredicate::UGT,
            Self::UnsignedGreaterThanOrEqual => IntPredicate::UGE,
        }
    }
}
pub enum FloatCC {
    NotEqual,
    LessThan,
    GreaterThanOrEqual,
    Unordered,
}
#[derive(Clone, Copy)]
pub struct MemFlags;
impl MemFlags {
    pub fn new() -> Self {
        Self
    }
}
#[derive(Clone)]
pub struct AbiParam(Type);
impl AbiParam {
    pub fn new(t: Type) -> Self {
        Self(t)
    }
}
#[derive(Default, Clone)]
pub struct Signature {
    pub params: Vec<AbiParam>,
    pub returns: Vec<AbiParam>,
}
impl Signature {
    fn llvm(&self, c: &'static Context) -> FunctionType<'static> {
        let p = self
            .params
            .iter()
            .map(|p| p.0.llvm(c).into())
            .collect::<Vec<_>>();
        match self.returns.as_slice() {
            [] => c.void_type().fn_type(&p, false),
            [r] => r.0.llvm(c).fn_type(&p, false),
            _ => panic!("multiple ABI returns"),
        }
    }
}

/// Owns the context, IR, and executable memory as one unit. No LLVM handle is
/// exposed outside this private backend. The context allocation stays at a
/// stable address until both module and execution engine have been dropped.
pub struct JitModule {
    context: *mut Context,
    pub module: Option<Module<'static>>,
    engine: Option<ExecutionEngine<'static>>,
    machine: TargetMachine,
}
// SAFETY: a module has one owner; compilation never shares its LLVM context.
// After finalization only machine-code pointers escape. Moving or destroying
// this owner cannot overlap IR access, and Compiled serializes destruction.
unsafe impl Send for JitModule {}
impl Drop for JitModule {
    fn drop(&mut self) {
        drop(self.module.take());
        drop(self.engine.take());
        // SAFETY: all context-dependent owners are gone and no handles escape.
        unsafe {
            drop(Box::from_raw(self.context));
        }
    }
}
impl JitModule {
    pub fn new() -> super::Result<Self> {
        static INIT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
        INIT.get_or_init(|| Target::initialize_native(&InitializationConfig::default()))
            .clone()
            .map_err(super::error)?;
        if usize::BITS != 64 {
            return Err(super::error("generated code requires a 64-bit target"));
        }
        let triple = TargetMachine::get_default_triple();
        let target = Target::from_triple(&triple).map_err(super::error)?;
        let cpu = TargetMachine::get_host_cpu_name();
        let features = TargetMachine::get_host_cpu_features();
        let machine = target
            .create_target_machine(
                &triple,
                cpu.to_str().unwrap(),
                features.to_str().unwrap(),
                OptimizationLevel::Aggressive,
                RelocMode::Default,
                CodeModel::JITDefault,
            )
            .ok_or_else(|| super::error("cannot create target machine"))?;
        let context = Box::into_raw(Box::new(Context::create()));
        // SAFETY: this allocation is owned above and disposed after the module.
        let c: &'static Context = unsafe { &*context };
        let module = c.create_module("roc");
        module.set_triple(&triple);
        module.set_data_layout(&machine.get_target_data().get_data_layout());
        Ok(Self {
            context,
            module: Some(module),
            engine: None,
            machine,
        })
    }
    fn context(&self) -> &'static Context {
        unsafe { &*self.context }
    }
    pub fn make_signature(&self) -> Signature {
        Signature::default()
    }
    pub fn function(&self, name: &str, n: usize) -> FuncId {
        let c = self.context();
        let f = self.module.as_ref().unwrap().add_function(
            name,
            c.i32_type().fn_type(&vec![c.i64_type().into(); n], false),
            None,
        );
        // O3 and MCJIT must agree on the native ISA available to this process.
        let cpu = TargetMachine::get_host_cpu_name();
        let features = TargetMachine::get_host_cpu_features();
        f.add_attribute(
            inkwell::attributes::AttributeLoc::Function,
            c.create_string_attribute("target-cpu", cpu.to_str().unwrap()),
        );
        f.add_attribute(
            inkwell::attributes::AttributeLoc::Function,
            c.create_string_attribute("target-features", features.to_str().unwrap()),
        );
        f
    }
    pub fn builder(&self, f: FuncId) -> FunctionBuilder<'_> {
        FunctionBuilder::new(self.context(), self.module.as_ref().unwrap(), f)
    }
    pub fn finalize(&mut self) -> super::Result<()> {
        let m = self.module.as_ref().unwrap();
        m.verify().map_err(super::error)?;
        m.run_passes("default<O3>", &self.machine, PassBuilderOptions::create())
            .map_err(super::error)?;
        m.verify().map_err(super::error)?;
        self.engine = Some(
            m.create_jit_execution_engine(OptimizationLevel::Aggressive)
                .map_err(super::error)?,
        );
        Ok(())
    }
    pub fn address(&self, f: FuncId) -> super::Result<usize> {
        self.engine
            .as_ref()
            .unwrap()
            .get_function_address(f.get_name().to_str().unwrap())
            .map_err(super::error)
    }
    pub fn ir(&self) -> String {
        self.module.as_ref().unwrap().print_to_string().to_string()
    }
}

pub struct FunctionBuilder<'a> {
    c: &'static Context,
    m: &'a Module<'static>,
    f: FuncId,
    b: Builder<'static>,
    params: HashMap<Block, Vec<Value>>,
    phis: HashMap<Block, Vec<PhiValue<'static>>>,
    vars: Vec<(Type, PointerValue<'static>)>,
    calls: Vec<Vec<Value>>,
    signatures: Vec<FunctionType<'static>>,
}
macro_rules! int_binary {
    ($($name:ident => $op:ident),* $(,)?)=>{$(pub fn $name(&mut self,a:Value,b:Value)->Value { match (a,b) { (Value::IntValue(a),Value::IntValue(b))=>self.b.$op(a,b,"").unwrap().into(), (Value::VectorValue(a),Value::VectorValue(b))=>self.b.$op(a,b,"").unwrap().into(), _=>panic!("integer operands") } })*};
}
macro_rules! float_binary { ($($name:ident=>$intr:literal),* $(,)?)=>{$(pub fn $name(&mut self,a:Value,b:Value)->Value {self.strict_float($intr,a,b)})*}; }

macro_rules! immediate { ($($name:ident=>$op:ident),* $(,)?)=>{$(pub fn $name(&mut self,a:Value,b:i64)->Value {let v=self.iconst(self.value_type(a),b);self.$op(a,v)})*}; }
macro_rules! overflow { ($($name:ident=>$intr:literal),* $(,)?)=>{$(pub fn $name(&mut self,a:Value,b:Value)->(Value,Value) {self.overflow($intr,a,b)})*}; }
impl<'a> FunctionBuilder<'a> {
    fn new(c: &'static Context, m: &'a Module<'static>, f: FuncId) -> Self {
        Self {
            c,
            m,
            f,
            b: c.create_builder(),
            params: HashMap::new(),
            phis: HashMap::new(),
            vars: vec![],
            calls: vec![],
            signatures: vec![],
        }
    }
    pub fn ins(&mut self) -> &mut Self {
        self
    }
    pub fn value_type(&self, v: Value) -> Type {
        match v.get_type() {
            BasicTypeEnum::IntType(t) => match t.get_bit_width() {
                8 => Type::I8,
                16 => Type::I16,
                32 => Type::I32,
                64 => Type::I64,
                _ => panic!("noncanonical integer"),
            },
            BasicTypeEnum::FloatType(t) => {
                if t == self.c.f32_type() {
                    Type::F32
                } else {
                    Type::F64
                }
            }
            BasicTypeEnum::VectorType(_) => Type::I64X2,
            _ => panic!("unexpected value type"),
        }
    }
    pub fn create_block(&mut self) -> Block {
        self.c.append_basic_block(self.f, "")
    }
    pub fn switch_to_block(&mut self, b: Block) {
        self.b.position_at_end(b)
    }
    pub fn append_block_params_for_function_params(&mut self, b: Block) {
        self.params.insert(b, self.f.get_param_iter().collect());
    }
    pub fn append_block_param(&mut self, b: Block, t: Type) {
        let builder = self.c.create_builder();
        builder.position_at_end(b);
        let p = builder.build_phi(t.llvm(self.c), "").unwrap();
        self.phis.entry(b).or_default().push(p);
        self.params.entry(b).or_default().push(p.as_basic_value());
    }
    pub fn block_params(&self, b: Block) -> &[Value] {
        self.params.get(&b).map_or(&[], Vec::as_slice)
    }
    fn incoming(&mut self, b: Block, args: &[Value]) {
        let from = self.b.get_insert_block().unwrap();
        let phis = self.phis.get(&b).map_or(&[][..], Vec::as_slice);
        assert_eq!(phis.len(), args.len());
        for (p, v) in phis.iter().zip(args) {
            p.add_incoming(&[(v, from)]);
        }
    }
    pub fn jump(&mut self, b: Block, args: &[Value]) {
        self.incoming(b, args);
        self.b.build_unconditional_branch(b).unwrap();
    }
    pub fn brif(&mut self, v: Value, a: Block, av: &[Value], b: Block, bv: &[Value]) {
        self.incoming(a, av);
        self.incoming(b, bv);
        let bit = self.truth(v);
        self.b.build_conditional_branch(bit, a, b).unwrap();
    }
    fn truth(&self, v: Value) -> inkwell::values::IntValue<'static> {
        let i = v.into_int_value();
        self.b
            .build_int_compare(IntPredicate::NE, i, i.get_type().const_zero(), "")
            .unwrap()
    }
    pub fn return_(&mut self, v: &[Value]) {
        assert_eq!(v.len(), 1);
        self.b.build_return(Some(&v[0])).unwrap();
    }
    pub fn set_cold_block(&mut self, _: Block) {}
    pub fn seal_all_blocks(&mut self) {}
    pub fn finalize(self) {}
    fn alloca(&self, t: BasicTypeEnum<'static>) -> PointerValue<'static> {
        let b = self.c.create_builder();
        let entry = self.f.get_first_basic_block().unwrap();
        if let Some(i) = entry.get_first_instruction() {
            b.position_before(&i)
        } else {
            b.position_at_end(entry)
        };
        b.build_alloca(t, "").unwrap()
    }
    pub fn declare_var(&mut self, t: Type) -> Var {
        let p = self.alloca(t.llvm(self.c));
        let id = self.vars.len();
        self.vars.push((t, p));
        id
    }
    pub fn def_var(&mut self, v: Var, x: Value) {
        self.b.build_store(self.vars[v].1, x).unwrap();
    }
    pub fn use_var(&mut self, v: Var) -> Value {
        let (t, p) = self.vars[v];
        self.b.build_load(t.llvm(self.c), p, "").unwrap()
    }
    pub fn stack(&mut self, bytes: usize) -> Value {
        let p = self.alloca(self.c.i8_type().array_type(bytes.max(8) as u32).into());
        p.as_instruction_value().unwrap().set_alignment(8).unwrap();
        self.b
            .build_ptr_to_int(p, self.c.i64_type(), "")
            .unwrap()
            .into()
    }
    pub fn hoisted_load(&mut self, block: Block, arg: Value, offset: i32) -> Value {
        let current = self.b.get_insert_block().unwrap();
        self.b.position_at_end(block);
        let v = self.load(Type::I64, MemFlags, arg, offset);
        self.b.position_at_end(current);
        v
    }
    pub fn iconst(&mut self, t: Type, v: i64) -> Value {
        t.llvm(self.c)
            .into_int_type()
            .const_int(v as u64, true)
            .into()
    }
    pub fn f32const(&mut self, v: f32) -> Value {
        self.c.f32_type().const_float(v as f64).into()
    }
    pub fn f64const(&mut self, v: f64) -> Value {
        self.c.f64_type().const_float(v).into()
    }
    fn ptr(&mut self, base: Value, offset: i32) -> PointerValue<'static> {
        let pointer = match base {
            Value::PointerValue(pointer) => pointer,
            _ => self
                .b
                .build_int_to_ptr(
                    base.into_int_value(),
                    self.c.ptr_type(AddressSpace::default()),
                    "",
                )
                .unwrap(),
        };
        if offset == 0 {
            pointer
        } else {
            // GEP without inbounds preserves wrapping address arithmetic.
            unsafe {
                self.b
                    .build_gep(
                        self.c.i8_type(),
                        pointer,
                        &[self.c.i64_type().const_int(offset as u64, true)],
                        "",
                    )
                    .unwrap()
            }
        }
    }

    /// Preserve pointer provenance and affine offsets in the vector loop IR.
    /// Integer addition followed by inttoptr hides array accesses from LLVM's
    /// dependence analysis, even when the scalar operation is fully typed.
    pub fn address(&mut self, base: Value, offset: Value) -> Value {
        let pointer = self.ptr(base, 0);
        unsafe {
            self.b
                .build_gep(self.c.i8_type(), pointer, &[offset.into_int_value()], "")
                .unwrap()
                .into()
        }
    }
    pub fn load(&mut self, t: Type, _: MemFlags, base: Value, offset: i32) -> Value {
        let p = self.ptr(base, offset);
        let v = self.b.build_load(t.llvm(self.c), p, "").unwrap();
        v.as_instruction_value().unwrap().set_alignment(1).unwrap();
        v
    }
    pub fn store(&mut self, _: MemFlags, v: Value, base: Value, offset: i32) {
        let p = self.ptr(base, offset);
        self.b.build_store(p, v).unwrap().set_alignment(1).unwrap();
    }
    pub fn uload8(&mut self, t: Type, f: MemFlags, b: Value, o: i32) -> Value {
        let v = self.load(Type::I8, f, b, o);
        self.uextend(t, v)
    }
    pub fn uload32(&mut self, f: MemFlags, b: Value, o: i32) -> Value {
        let v = self.load(Type::I32, f, b, o);
        self.uextend(Type::I64, v)
    }
    pub fn import_signature(&mut self, s: Signature) -> usize {
        let i = self.signatures.len();
        self.signatures.push(s.llvm(self.c));
        i
    }
    pub fn call_indirect(&mut self, s: usize, address: Value, args: &[Value]) -> usize {
        let p = self.ptr(address, 0);
        let args = args.iter().map(|v| (*v).into()).collect::<Vec<_>>();
        let call = self
            .b
            .build_indirect_call(self.signatures[s], p, &args, "")
            .unwrap();
        let values = call.try_as_basic_value().basic().into_iter().collect();
        let id = self.calls.len();
        self.calls.push(values);
        id
    }
    pub fn inst_results(&self, i: usize) -> &[Value] {
        &self.calls[i]
    }
    int_binary! {iadd=>build_int_add,isub=>build_int_sub,imul=>build_int_mul,band=>build_and,bor=>build_or,bxor=>build_xor,sdiv=>build_int_signed_div,udiv=>build_int_unsigned_div,srem=>build_int_signed_rem,urem=>build_int_unsigned_rem}
    float_binary! {fadd=>"llvm.experimental.constrained.fadd",fsub=>"llvm.experimental.constrained.fsub",fmul=>"llvm.experimental.constrained.fmul",fdiv=>"llvm.experimental.constrained.fdiv"}
    immediate! {iadd_imm=>iadd,imul_imm=>imul,band_imm=>band,bxor_imm=>bxor,ishl_imm=>ishl,ushr_imm=>ushr,sshr_imm=>sshr}
    pub fn irsub_imm(&mut self, a: Value, b: i64) -> Value {
        let v = self.iconst(self.value_type(a), b);
        self.isub(v, a)
    }
    pub fn bnot(&mut self, a: Value) -> Value {
        self.b.build_not(a.into_int_value(), "").unwrap().into()
    }
    fn shift_count(&mut self, a: Value, b: Value) -> Value {
        let ty = self.value_type(a);
        let bt = self.value_type(b);
        let b = if bt.bits() > ty.bits() {
            self.ireduce(ty, b)
        } else if bt.bits() < ty.bits() {
            self.uextend(ty, b)
        } else {
            b
        };
        self.band_imm(b, (ty.bits() - 1) as i64)
    }
    pub fn ishl(&mut self, a: Value, b: Value) -> Value {
        let b = self.shift_count(a, b);
        self.b
            .build_left_shift(a.into_int_value(), b.into_int_value(), "")
            .unwrap()
            .into()
    }
    pub fn ushr(&mut self, a: Value, b: Value) -> Value {
        let b = self.shift_count(a, b);
        self.b
            .build_right_shift(a.into_int_value(), b.into_int_value(), false, "")
            .unwrap()
            .into()
    }
    pub fn sshr(&mut self, a: Value, b: Value) -> Value {
        let b = self.shift_count(a, b);
        self.b
            .build_right_shift(a.into_int_value(), b.into_int_value(), true, "")
            .unwrap()
            .into()
    }
    pub fn icmp(&mut self, cc: IntCC, a: Value, b: Value) -> Value {
        match (a, b) {
            (Value::IntValue(a), Value::IntValue(b)) => {
                let v = self.b.build_int_compare(cc.llvm(), a, b, "").unwrap();
                self.b
                    .build_int_z_extend(v, self.c.i8_type(), "")
                    .unwrap()
                    .into()
            }
            (Value::VectorValue(a), Value::VectorValue(b)) => {
                let v = self.b.build_int_compare(cc.llvm(), a, b, "").unwrap();
                self.b
                    .build_int_s_extend(v, self.c.i64_type().vec_type(2), "")
                    .unwrap()
                    .into()
            }
            _ => panic!("compare operands"),
        }
    }
    pub fn icmp_imm(&mut self, c: IntCC, a: Value, b: i64) -> Value {
        let b = self.iconst(self.value_type(a), b);
        self.icmp(c, a, b)
    }
    pub fn fcmp(&mut self, cc: FloatCC, a: Value, b: Value) -> Value {
        let p = match cc {
            FloatCC::NotEqual => FloatPredicate::UNE,
            FloatCC::LessThan => FloatPredicate::OLT,
            FloatCC::GreaterThanOrEqual => FloatPredicate::OGE,
            FloatCC::Unordered => FloatPredicate::UNO,
        };
        let v = self
            .b
            .build_float_compare(p, a.into_float_value(), b.into_float_value(), "")
            .unwrap();
        self.b
            .build_int_z_extend(v, self.c.i8_type(), "")
            .unwrap()
            .into()
    }
    pub fn select(&mut self, c: Value, a: Value, b: Value) -> Value {
        self.b.build_select(self.truth(c), a, b, "").unwrap()
    }
    pub fn bitcast(&mut self, t: Type, _: MemFlags, v: Value) -> Value {
        self.b.build_bit_cast(v, t.llvm(self.c), "").unwrap()
    }
    pub fn ireduce(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_int_truncate(v.into_int_value(), t.llvm(self.c).into_int_type(), "")
            .unwrap()
            .into()
    }
    pub fn uextend(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_int_z_extend(v.into_int_value(), t.llvm(self.c).into_int_type(), "")
            .unwrap()
            .into()
    }
    pub fn sextend(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_int_s_extend(v.into_int_value(), t.llvm(self.c).into_int_type(), "")
            .unwrap()
            .into()
    }
    pub fn fneg(&mut self, v: Value) -> Value {
        self.b
            .build_float_neg(v.into_float_value(), "")
            .unwrap()
            .into()
    }
    pub fn fpromote(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_float_ext(v.into_float_value(), t.llvm(self.c).into_float_type(), "")
            .unwrap()
            .into()
    }
    pub fn fdemote(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_float_trunc(v.into_float_value(), t.llvm(self.c).into_float_type(), "")
            .unwrap()
            .into()
    }
    pub fn fcvt_from_sint(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_signed_int_to_float(v.into_int_value(), t.llvm(self.c).into_float_type(), "")
            .unwrap()
            .into()
    }
    pub fn fcvt_from_uint(&mut self, t: Type, v: Value) -> Value {
        self.b
            .build_unsigned_int_to_float(v.into_int_value(), t.llvm(self.c).into_float_type(), "")
            .unwrap()
            .into()
    }
    // Keep actual FP operations even with constant operands: folding -0 - x
    // into fneg changes NaN sign bits, observable in Arrow's bitwise equality.
    fn strict_float(&mut self, name: &str, a: Value, b: Value) -> Value {
        use inkwell::attributes::{Attribute, AttributeLoc};
        let attr = self
            .c
            .create_enum_attribute(Attribute::get_named_enum_kind_id("strictfp"), 0);
        self.f.add_attribute(AttributeLoc::Function, attr);
        let f = Intrinsic::find(name)
            .unwrap()
            .get_declaration(self.m, &[a.get_type()])
            .unwrap();
        let args = [
            a.into(),
            b.into(),
            self.c.metadata_string("round.dynamic").into(),
            self.c.metadata_string("fpexcept.strict").into(),
        ];
        let call = self.b.build_call(f, &args, "").unwrap();
        call.add_attribute(AttributeLoc::Function, attr);
        call.try_as_basic_value().unwrap_basic()
    }
    fn intrinsic(&mut self, name: &str, types: &[BasicTypeEnum<'static>], args: &[Value]) -> Value {
        let f = Intrinsic::find(name)
            .unwrap()
            .get_declaration(self.m, types)
            .unwrap();
        let args = args.iter().map(|v| (*v).into()).collect::<Vec<_>>();
        self.b
            .build_call(f, &args, "")
            .unwrap()
            .try_as_basic_value()
            .unwrap_basic()
    }
    fn overflow(&mut self, name: &str, a: Value, b: Value) -> (Value, Value) {
        let out = self
            .intrinsic(name, &[a.get_type()], &[a, b])
            .into_struct_value();
        let v = self.b.build_extract_value(out, 0, "").unwrap();
        let flag = self
            .b
            .build_extract_value(out, 1, "")
            .unwrap()
            .into_int_value();
        let flag = self
            .b
            .build_int_z_extend(flag, self.c.i8_type(), "")
            .unwrap()
            .into();
        (v, flag)
    }
    // Express the two scalar results independently. LLVM 19's loop vectorizer
    // cannot widen the struct-returning checked-arithmetic intrinsic; the
    // equivalent sign-bit algebra can be lifted to any SIMD width normally.
    pub fn sadd_overflow(&mut self, a: Value, b: Value) -> (Value, Value) {
        let value = self.iadd(a, b);
        let x = self.bxor(value, a);
        let y = self.bxor(value, b);
        let sign = self.band(x, y);
        let overflow = self.icmp_imm(IntCC::SignedLessThan, sign, 0);
        (value, overflow)
    }
    pub fn uadd_overflow(&mut self, a: Value, b: Value) -> (Value, Value) {
        let value = self.iadd(a, b);
        let overflow = self.icmp(IntCC::UnsignedLessThan, value, a);
        (value, overflow)
    }
    pub fn ssub_overflow(&mut self, a: Value, b: Value) -> (Value, Value) {
        let value = self.isub(a, b);
        let x = self.bxor(a, b);
        let y = self.bxor(a, value);
        let sign = self.band(x, y);
        let overflow = self.icmp_imm(IntCC::SignedLessThan, sign, 0);
        (value, overflow)
    }
    pub fn usub_overflow(&mut self, a: Value, b: Value) -> (Value, Value) {
        let value = self.isub(a, b);
        let overflow = self.icmp(IntCC::UnsignedLessThan, a, b);
        (value, overflow)
    }
    overflow! {smul_overflow=>"llvm.smul.with.overflow",umul_overflow=>"llvm.umul.with.overflow"}
    pub fn trunc(&mut self, v: Value) -> Value {
        self.intrinsic("llvm.trunc", &[v.get_type()], &[v])
    }
    pub fn fcvt_to_sint_sat(&mut self, t: Type, v: Value) -> Value {
        self.intrinsic("llvm.fptosi.sat", &[t.llvm(self.c), v.get_type()], &[v])
    }
    pub fn fcvt_to_uint_sat(&mut self, t: Type, v: Value) -> Value {
        self.intrinsic("llvm.fptoui.sat", &[t.llvm(self.c), v.get_type()], &[v])
    }
    pub fn popcnt(&mut self, v: Value) -> Value {
        self.intrinsic("llvm.ctpop", &[v.get_type()], &[v])
    }
    pub fn ctz(&mut self, v: Value) -> Value {
        let flag = self.c.bool_type().const_zero().into();
        self.intrinsic("llvm.cttz", &[v.get_type()], &[v, flag])
    }
    pub fn splat(&mut self, t: Type, v: Value) -> Value {
        let mut out = t.llvm(self.c).into_vector_type().get_undef();
        for i in 0..2 {
            out = self
                .b
                .build_insert_element(out, v, self.c.i32_type().const_int(i, false), "")
                .unwrap();
        }
        out.into()
    }
    pub fn vconst(&mut self, bytes: [u8; 16]) -> Value {
        let a = self
            .c
            .i64_type()
            .const_int(u64::from_le_bytes(bytes[..8].try_into().unwrap()), false);
        let b = self
            .c
            .i64_type()
            .const_int(u64::from_le_bytes(bytes[8..].try_into().unwrap()), false);
        inkwell::types::VectorType::const_vector(&[a, b]).into()
    }
    pub fn extractlane(&mut self, v: Value, i: u8) -> Value {
        self.b
            .build_extract_element(
                v.into_vector_value(),
                self.c.i32_type().const_int(i as u64, false),
                "",
            )
            .unwrap()
    }
    pub fn insertlane(&mut self, v: Value, x: Value, i: u8) -> Value {
        self.b
            .build_insert_element(
                v.into_vector_value(),
                x,
                self.c.i32_type().const_int(i as u64, false),
                "",
            )
            .unwrap()
            .into()
    }
}
