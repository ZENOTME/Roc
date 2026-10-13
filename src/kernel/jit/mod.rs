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

//! Compiles a pipeline kernel graph into one fused row loop.
//!
//! LLVM promotes entry-allocated graph variables to SSA, so values defined inside a
//! Filter region and used after it is reopened are carried by SSA phis. A
//! Filter domain is a region guarded by its predicate; regions open lazily and
//! close when an item of a non-descendant domain is emitted.
use super::io::{INPUT_WORDS, InputDescriptor, OUTPUT_WORDS};
use super::runtime::{self, Runtime};
use super::*;
use crate::arrow::datatypes::DataType;
use crate::error::{Error, Result};
mod llvm;
use llvm::{AbiParam, Block, FunctionBuilder, IntCC, JitModule, MemFlags, Type, Value, Var, types};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod cast;
mod emit;
mod kernel;
mod reduce;

pub use kernel::prewarm_kernels;
pub(crate) use kernel::{KernelEntry, generated};

use super::plan::{Item, Plan};

pub(crate) type Entry = unsafe extern "C" fn(
    *mut Runtime,
    *const InputDescriptor,
    *const [*mut u8; OUTPUT_WORDS],
    *mut usize,
    *const *mut (),
    usize,
) -> i32;

pub(crate) struct Compiled {
    /// None when the program has no generated work: every output aliases an
    /// input in the root domain and no kernel is live.
    pub entry: Option<Entry>,
    /// A specialized global reduction, used instead of `entry` when present.
    pub reduction: Option<Entry>,
    _module: Mutex<Option<JitModule>>,
    pub ir: String,
    pub sites: Vec<KernelError>,
    _constants: Vec<Arc<[u8]>>,
}

impl std::fmt::Debug for Compiled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compiled")
            .field("sites", &self.sites)
            .finish_non_exhaustive()
    }
}

fn error(e: impl std::fmt::Display) -> Error {
    Error::Execution(format!("LLVM: {e}"))
}

pub(super) fn llvm_type(t: VariableType) -> Type {
    match t {
        VariableType::Boolean | VariableType::Int8 | VariableType::UInt8 => types::I8,
        VariableType::Int16 | VariableType::UInt16 => types::I16,
        VariableType::Int32 | VariableType::UInt32 => types::I32,
        VariableType::Float32 => types::F32,
        VariableType::Float64 => types::F64,
        _ => types::I64,
    }
}

fn large_offsets(t: &DataType) -> bool {
    matches!(t, DataType::LargeUtf8 | DataType::LargeBinary)
}

impl Compiled {
    pub fn compile(graph: &KernelGraph, plan: &Plan) -> Result<Self> {
        if plan.trivial(graph) {
            return Ok(Self {
                entry: None,
                reduction: None,
                _module: Mutex::new(None),
                ir: String::new(),
                sites: vec![],
                _constants: vec![],
            });
        }
        let mut module = JitModule::new()?;
        let id = module.function("roc_pipeline", 6);
        let mut b = module.builder(id);
        let entry = b.create_block();
        b.append_block_params_for_function_params(entry);
        b.switch_to_block(entry);
        let args = b.block_params(entry).to_vec();
        let count = b.declare_var(types::I64);
        let zero = b.ins().iconst(types::I64, 0);
        b.def_var(count, zero);
        let setup = b.create_block();
        let head = b.create_block();
        let body = b.create_block();
        let done = b.create_block();
        let fail = b.create_block();
        b.append_block_param(head, types::I64);
        b.append_block_param(fail, types::I32);
        b.ins().jump(setup, &[]);
        b.switch_to_block(head);
        let row = b.block_params(head)[0];
        let more = b.ins().icmp(IntCC::UnsignedLessThan, row, args[5]);
        b.ins().brif(more, body, &[], done, &[]);
        b.switch_to_block(body);
        let mut c = Compiler {
            b,
            module: &module,
            graph,
            plan,
            vars: vec![None; graph.variables.len()],
            args,
            row,
            setup,
            fail,
            hoisted: HashMap::new(),
            regions: vec![],
            count,
            sites: vec![],
            constants: vec![],
            aliases: &plan.aliases,
        };
        let poll = c.b.create_block();
        let work = c.b.create_block();
        let tick = c.b.ins().band_imm(row, 1023);
        let due = c.b.ins().icmp_imm(IntCC::Equal, tick, 0);
        c.b.ins().brif(due, poll, &[], work, &[]);
        c.b.switch_to_block(poll);
        let status = c.call(
            runtime::poll as *const () as usize,
            &[c.args[0]],
            &[types::I32],
        )[0];
        c.check_status(status);
        c.b.ins().jump(work, &[]);
        c.b.switch_to_block(work);
        for (item, place) in &plan.items {
            c.goto(*place);
            match *item {
                Item::Input(i) => c.input(i),
                Item::Kernel(k) => c.kernel(k)?,
                Item::Output => c.output(),
            }
        }
        c.goto(DomainId::ROOT);
        let next = c.b.ins().iadd_imm(row, 1);
        c.b.ins().jump(head, &[next.into()]);
        c.b.switch_to_block(done);
        let n = c.b.use_var(count);
        c.b.ins().store(MemFlags::new(), n, c.args[3], 0);
        let ok = c.b.ins().iconst(types::I32, 0);
        c.b.ins().return_(&[ok]);
        c.b.switch_to_block(fail);
        let status = c.b.block_params(fail)[0];
        c.b.ins().return_(&[status]);
        c.b.switch_to_block(setup);
        c.b.ins().jump(head, &[zero.into()]);
        c.b.seal_all_blocks();
        let sites = std::mem::take(&mut c.sites);
        let constants = std::mem::take(&mut c.constants);
        c.b.finalize();
        let reduction = reduce::compile(&module, graph, plan)?;
        module.finalize()?;
        let ir = module.ir();
        let reduction = reduction
            .map(|id| {
                module
                    .address(id)
                    .map(|p| unsafe { std::mem::transmute::<usize, Entry>(p) })
            })
            .transpose()?;
        let entry = unsafe { std::mem::transmute::<usize, Entry>(module.address(id)?) };
        Ok(Self {
            entry: Some(entry),
            reduction,
            _module: Mutex::new(Some(module)),
            ir,
            sites,
            _constants: constants,
        })
    }
}

pub(super) struct Compiler<'a, 'f> {
    pub b: FunctionBuilder<'f>,
    module: &'a JitModule,
    pub graph: &'a KernelGraph,
    plan: &'a Plan,
    vars: Vec<Option<Var>>,
    pub args: Vec<Value>,
    row: Value,
    setup: Block,
    pub fail: Block,
    hoisted: HashMap<(usize, i32), Value>,
    /// Open Filter regions below the root: `(domain, end block)`.
    regions: Vec<(DomainId, Block)>,
    count: Var,
    pub sites: Vec<KernelError>,
    pub constants: Vec<Arc<[u8]>>,
    aliases: &'a [Option<usize>],
}

impl Compiler<'_, '_> {
    fn var(&mut self, v: VariableId) -> Var {
        if let Some(var) = self.vars[v.0] {
            return var;
        }
        let var = self.b.declare_var(llvm_type(self.graph.variables[v.0].ty));
        self.vars[v.0] = Some(var);
        var
    }
    pub fn get(&mut self, v: VariableId) -> Value {
        let var = self.var(v);
        self.b.use_var(var)
    }
    pub fn set(&mut self, v: VariableId, value: Value) {
        let var = self.var(v);
        self.b.def_var(var, value);
    }
    pub fn ty(&self, v: VariableId) -> VariableType {
        self.graph.variables[v.0].ty
    }

    /// A loop-invariant load from an entry argument, hoisted to the preheader
    /// without changing the builder's current block.
    pub fn descriptor(&mut self, argument: usize, offset: i32) -> Value {
        if let Some(v) = self.hoisted.get(&(argument, offset)) {
            return *v;
        }
        let v = self.b.hoisted_load(self.setup, self.args[argument], offset);
        self.hoisted.insert((argument, offset), v);
        v
    }
    pub fn word(&mut self, x: i64) -> Value {
        self.b.ins().iconst(types::I64, x)
    }
    pub fn call(&mut self, address: usize, args: &[Value], returns: &[Type]) -> Vec<Value> {
        let mut sig = self.module.make_signature();
        sig.params = args
            .iter()
            .map(|v| AbiParam::new(self.b.value_type(*v)))
            .collect();
        sig.returns = returns.iter().map(|t| AbiParam::new(*t)).collect();
        let sig = self.b.import_signature(sig);
        let address = self.word(address as i64);
        let inst = self.b.ins().call_indirect(sig, address, args);
        self.b.inst_results(inst).to_vec()
    }
    /// Leaves with a nonzero I32 status.
    pub fn check_status(&mut self, status: Value) {
        let next = self.b.create_block();
        self.b
            .ins()
            .brif(status, self.fail, &[status.into()], next, &[]);
        self.b.switch_to_block(next);
    }
    pub fn stack(&mut self, bytes: usize) -> Value {
        self.b.stack(bytes)
    }

    fn goto(&mut self, target: DomainId) {
        let path = self.plan.path(target);
        let keep = self
            .regions
            .iter()
            .zip(&path[1..])
            .take_while(|((d, _), p)| d == *p)
            .count();
        while self.regions.len() > keep {
            let (_, end) = self.regions.pop().unwrap();
            self.b.ins().jump(end, &[]);
            self.b.switch_to_block(end);
        }
        for &domain in &path[1 + keep..] {
            let predicate = self.plan.predicates[domain.0].expect("domain without a filter");
            let p = self.get(predicate);
            let body = self.b.create_block();
            let end = self.b.create_block();
            self.b.ins().brif(p, body, &[], end, &[]);
            self.b.switch_to_block(body);
            self.regions.push((domain, end));
        }
    }

    fn input(&mut self, i: usize) {
        let input = &self.graph.inputs[i];
        let live = self.plan.input_vars[i].clone();
        let base = (i * INPUT_WORDS * 8) as i32;
        let row = self.row;
        let binding = input.binding;
        let data_type = input.ty.data_type.clone();
        match binding {
            ColumnVariableBinding::Null { .. } => {}
            ColumnVariableBinding::Bytes { ptr, len, .. } => {
                if live.contains(&ptr) || live.contains(&len) {
                    let data = self.descriptor(1, base);
                    let offsets = self.descriptor(1, base + 8);
                    let (start, end) = if large_offsets(&data_type) {
                        let at = self.b.ins().ishl_imm(row, 3);
                        let at = self.b.ins().iadd(offsets, at);
                        let start = self.b.ins().load(types::I64, MemFlags::new(), at, 0);
                        let end = self.b.ins().load(types::I64, MemFlags::new(), at, 8);
                        (start, end)
                    } else {
                        let at = self.b.ins().ishl_imm(row, 2);
                        let at = self.b.ins().iadd(offsets, at);
                        let start = self.b.ins().uload32(MemFlags::new(), at, 0);
                        let end = self.b.ins().uload32(MemFlags::new(), at, 4);
                        (start, end)
                    };
                    let p = self.b.ins().iadd(data, start);
                    let n = self.b.ins().isub(end, start);
                    self.set(ptr, p);
                    self.set(len, n);
                }
            }
            _ => {
                let value = binding.value().unwrap();
                if live.contains(&value) {
                    let data = self.descriptor(1, base);
                    let v = if self.ty(value) == VariableType::Boolean {
                        let offset = self.descriptor(1, base + 32);
                        self.bit(data, offset, row)
                    } else {
                        let t = llvm_type(self.ty(value));
                        let at = self.b.ins().imul_imm(row, i64::from(t.bytes()));
                        let at = self.b.ins().iadd(data, at);
                        self.b.ins().load(t, MemFlags::new(), at, 0)
                    };
                    self.set(value, v);
                }
            }
        }
        if let Some(validity) = binding.validity()
            && !binding.is_null_type()
            && live.contains(&validity)
        {
            // The bitmap pointer is null when the array has no NULLs.
            let bitmap = self.descriptor(1, base + 16);
            let offset = self.descriptor(1, base + 24);
            let nullable = self.b.create_block();
            let ready = self.b.create_block();
            self.b.append_block_param(ready, types::I8);
            let yes = self.b.ins().iconst(types::I8, 1);
            self.b
                .ins()
                .brif(bitmap, nullable, &[], ready, &[yes.into()]);
            self.b.switch_to_block(nullable);
            let bit = self.bit(bitmap, offset, row);
            self.b.ins().jump(ready, &[bit.into()]);
            self.b.switch_to_block(ready);
            let v = self.b.block_params(ready)[0];
            self.set(validity, v);
        }
    }

    /// Bit `offset + row` of a bitmap as an I8 0/1.
    fn bit(&mut self, bitmap: Value, offset: Value, row: Value) -> Value {
        let bit = self.b.ins().iadd(offset, row);
        let byte = self.b.ins().ushr_imm(bit, 3);
        let at = self.b.ins().iadd(bitmap, byte);
        let bits = self.b.ins().uload8(types::I64, MemFlags::new(), at, 0);
        let shift = self.b.ins().band_imm(bit, 7);
        let bits = self.b.ins().ushr(bits, shift);
        let bits = self.b.ins().band_imm(bits, 1);
        self.b.ins().ireduce(types::I8, bits)
    }

    /// ORs an I8 0/1 into bit `index` of a zeroed bitmap.
    fn set_bit(&mut self, bitmap: Value, index: Value, flag: Value) {
        let byte = self.b.ins().ushr_imm(index, 3);
        let at = self.b.ins().iadd(bitmap, byte);
        let shift = self.b.ins().band_imm(index, 7);
        let bit = self.b.ins().ishl(flag, shift);
        let old = self.b.ins().load(types::I8, MemFlags::new(), at, 0);
        let new = self.b.ins().bor(old, bit);
        self.b.ins().store(MemFlags::new(), new, at, 0);
    }

    fn output(&mut self) {
        let output = self.graph.output.as_ref().unwrap();
        let count = self.b.use_var(self.count);
        for (j, binding) in output.bindings.iter().enumerate() {
            if self.aliases[j].is_some() {
                continue;
            }
            let base = (j * OUTPUT_WORDS * 8) as i32;
            match *binding {
                ColumnVariableBinding::Null { .. } => continue,
                ColumnVariableBinding::Bytes { ptr, len, .. } => {
                    let data = self.descriptor(2, base);
                    let aux = self.descriptor(2, base + 8);
                    let at = self.b.ins().ishl_imm(count, 3);
                    let p = self.get(ptr);
                    let n = self.get(len);
                    let d = self.b.ins().iadd(data, at);
                    self.b.ins().store(MemFlags::new(), p, d, 0);
                    let a = self.b.ins().iadd(aux, at);
                    self.b.ins().store(MemFlags::new(), n, a, 0);
                }
                ColumnVariableBinding::Boolean { value, .. } => {
                    let data = self.descriptor(2, base);
                    let v = self.get(value);
                    self.set_bit(data, count, v);
                }
                _ => {
                    let value = binding.value().unwrap();
                    let data = self.descriptor(2, base);
                    let t = llvm_type(self.ty(value));
                    let at = self.b.ins().imul_imm(count, i64::from(t.bytes()));
                    let at = self.b.ins().iadd(data, at);
                    let v = self.get(value);
                    self.b.ins().store(MemFlags::new(), v, at, 0);
                }
            }
            if let Some(validity) = binding.validity() {
                let bitmap = self.descriptor(2, base + 16);
                let v = self.get(validity);
                self.set_bit(bitmap, count, v);
            }
        }
        let next = self.b.ins().iadd_imm(count, 1);
        self.b.def_var(self.count, next);
    }
}
