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

//! `Compile` programs: each run of consecutive kernels, Boolean steps and
//! RaiseIfs becomes one generated function, compiled with LLVM MCJIT by the
//! emitter of the precompiled library. It keeps every value in registers
//! and writes only the variables read after the run. Its raises only report
//! that some row may fail; the window then runs the replaced steps, which
//! find the failing row.
use super::*;
use crate::kernel::codegen::{
    self, emit,
    ir::{Arg, Group, Op, Src},
};
use inkwell::{
    OptimizationLevel,
    context::Context,
    execution_engine::ExecutionEngine,
    module::Module,
    targets::{RelocMode, TargetMachine},
};
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::sync::OnceLock;

/// The generated code of one program, freed with it.
pub(crate) struct Jit {
    /// Owned by the engine; kept to print its IR on request.
    module: ManuallyDrop<Module<'static>>,
    engine: ManuallyDrop<ExecutionEngine<'static>>,
    context: ManuallyDrop<Box<Context>>,
    ir: OnceLock<String>,
}

impl Jit {
    pub fn ir(&self) -> &str {
        self.ir
            .get_or_init(|| self.module.print_to_string().to_string())
    }
}

// SAFETY: the engine and context are only used to free the code, by the
// owner; the functions themselves are pure code called from any thread.
unsafe impl Send for Jit {}
unsafe impl Sync for Jit {}

impl Drop for Jit {
    fn drop(&mut self) {
        // SAFETY: the engine owns the module and code; the context outlives both.
        unsafe {
            ManuallyDrop::drop(&mut self.module);
            ManuallyDrop::drop(&mut self.engine);
            ManuallyDrop::drop(&mut self.context);
        }
    }
}

fn error(e: impl std::fmt::Display) -> Error {
    Error::internal(format!("LLVM code generation failed: {e}"))
}

thread_local! {
    /// The host target machine, created once per thread that compiles.
    static MACHINE: std::cell::RefCell<Option<TargetMachine>> = const { std::cell::RefCell::new(None) };
}

fn host() -> Result<&'static codegen::Isa> {
    static HOST: OnceLock<std::result::Result<codegen::Isa, String>> = OnceLock::new();
    HOST.get_or_init(|| {
        codegen::initialize()?;
        let cpu = TargetMachine::get_host_cpu_name().to_string();
        let features = TargetMachine::get_host_cpu_features().to_string();
        let vector_bits = if features.split(',').any(|f| f == "+avx2") {
            256
        } else {
            128
        };
        Ok(codegen::Isa {
            cpu,
            features,
            vector_bits,
        })
    })
    .as_ref()
    .map_err(error)
}

fn fusable(step: &Step) -> bool {
    matches!(
        step,
        Step::Kernel { .. } | Step::Bits { .. } | Step::SelectBits { .. } | Step::RaiseIf { .. }
    )
}

/// One run as a group: its arguments and reported Booleans.
struct Run {
    group: Group,
    args: Vec<Operand>,
    reports: Vec<VariableId>,
}

fn group(steps: &Steps, run: &[Step], later: &[bool]) -> Run {
    let mut g = Group::default();
    let mut args = vec![];
    let mut srcs: HashMap<VariableId, Src> = HashMap::new();
    let mut domains: HashMap<DomainId, Src> = HashMap::new();
    let mut defined = vec![];
    let src = |srcs: &mut HashMap<VariableId, Src>,
               g: &mut Group,
               args: &mut Vec<Operand>,
               v: VariableId|
     -> Src {
        if let Some(s) = srcs.get(&v) {
            return *s;
        }
        let var = steps.vars[v.0];
        // Float constants stay operands: LLVM folds float arithmetic with
        // known operands in ways that change NaN bits, which the precompiled
        // kernels must match.
        let s = match var.place {
            Place::Constant(i) if !var.ty.is_float() && var.ty != Ty::I128 => {
                Src::Imm(var.ty, steps.pool[i])
            }
            Place::Constant(_) => {
                args.push(Operand::Var(v));
                g.arg(Arg::Scalar(var.ty))
            }
            _ => {
                args.push(Operand::Var(v));
                g.arg(if var.ty == Ty::Bool {
                    Arg::Bitmap
                } else {
                    Arg::Column(var.ty)
                })
            }
        };
        srcs.insert(v, s);
        s
    };
    for step in run {
        match step {
            Step::Kernel { call, .. } => {
                let inputs = call
                    .inputs
                    .iter()
                    .map(|v| src(&mut srcs, &mut g, &mut args, *v))
                    .collect::<Vec<_>>();
                let results = codegen::ir::lower(&mut g, call.func, &inputs);
                for (v, r) in call.outputs.iter().zip(results) {
                    srcs.insert(*v, r);
                    defined.push(*v);
                }
            }
            Step::Bits { op, a, b, output } => {
                let a = src(&mut srcs, &mut g, &mut args, *a);
                let r = match op {
                    BitOp::Not => g.node(Op::Not, vec![a]),
                    BitOp::And => {
                        let b = src(&mut srcs, &mut g, &mut args, *b);
                        g.node(Op::And, vec![a, b])
                    }
                    BitOp::Or => {
                        let b = src(&mut srcs, &mut g, &mut args, *b);
                        g.node(Op::Or, vec![a, b])
                    }
                };
                srcs.insert(*output, r);
                defined.push(*output);
            }
            Step::SelectBits {
                cond,
                then,
                otherwise,
                output,
            } => {
                let operands = [*cond, *then, *otherwise]
                    .iter()
                    .map(|v| src(&mut srcs, &mut g, &mut args, *v))
                    .collect();
                let r = g.node(Op::Select, operands);
                srcs.insert(*output, r);
                defined.push(*output);
            }
            Step::RaiseIf { cond, domain, .. } => {
                let c = src(&mut srcs, &mut g, &mut args, *cond);
                let mask = (*domain != DomainId::ROOT).then(|| {
                    *domains.entry(*domain).or_insert_with(|| {
                        args.push(Operand::Domain(*domain));
                        g.arg(Arg::Bitmap)
                    })
                });
                g.raises.push((c, mask));
            }
            _ => unreachable!("only fusable steps"),
        }
    }
    let mut reports = vec![];
    for v in defined {
        let var = steps.vars[v.0];
        if !later[v.0] || matches!(var.place, Place::Constant(_)) {
            continue;
        }
        let s = srcs[&v];
        args.push(Operand::Var(v));
        g.store(s);
        if var.ty == Ty::Bool {
            g.reports.push(s);
            reports.push(v);
        }
    }
    Run {
        group: g,
        args,
        reports,
    }
}

/// The slot of a variable, if it has one: `(is a bitmap slot, index)`.
fn slot(place: Place) -> Option<(bool, usize)> {
    match place {
        Place::Value(s) | Place::InputBytes { slot: s, .. } => Some((false, s)),
        Place::Bits(s) | Place::InputBits { slot: s, .. } => Some((true, s)),
        _ => None,
    }
}

/// Moves outputs of a run that share a slot with one of its inputs to new
/// slots. Steps run one at a time may share them: an input is dead once a
/// later step writes the output. A fused function, though, stores outputs
/// chunk by chunk while it still reads inputs, and the steps rerun after a
/// possible raise read the inputs again.
fn separate(steps: &mut Steps, run: &Run) {
    let operands = run.group.args.iter().zip(&run.args);
    let read = operands
        .clone()
        .filter_map(|(arg, op)| match (arg, op) {
            (Arg::Column(_) | Arg::Bitmap, Operand::Var(v)) => slot(steps.vars[v.0].place),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (arg, op) in operands {
        let (Arg::Output(_) | Arg::OutputBitmap, Operand::Var(v)) = (arg, op) else {
            continue;
        };
        let place = &mut steps.vars[v.0].place;
        match *place {
            Place::Value(s) if read.contains(&(false, s)) => {
                *place = Place::Value(steps.value_slots);
                steps.value_slots += 1;
            }
            Place::Bits(s) if read.contains(&(true, s)) => {
                *place = Place::Bits(steps.bit_slots);
                steps.bit_slots += 1;
            }
            _ => {}
        }
    }
}

pub(super) fn fuse(steps: &mut Steps) -> Result<()> {
    // Runs of fusable steps within each block.
    let mut runs = vec![];
    for (b, block) in steps.blocks.iter().enumerate() {
        let len = block.steps.len();
        let mut s = 0;
        while s < len {
            let e = (s..len).find(|&e| !fusable(&block.steps[e])).unwrap_or(len);
            let kernels = block.steps[s..e]
                .iter()
                .filter(|s| matches!(s, Step::Kernel { .. }))
                .count();
            if kernels >= 1 {
                runs.push((b, s..e));
            }
            s = e + 1;
        }
    }
    if runs.is_empty() {
        return Ok(());
    }
    // Variables read outside the steps of their block: the output and
    // reductions, run after the block's steps.
    let mut outside = steps
        .output_bindings
        .iter()
        .flat_map(|b| b.variables())
        .collect::<Vec<_>>();
    for block in &steps.blocks {
        for step in &block.steps {
            if let Step::Expand(e) = step {
                outside.extend(e.reduces.iter().flat_map(|r| r.any.iter().map(|(v, _)| *v)));
            }
        }
    }
    let mut groups = vec![];
    for (b, run) in &runs {
        let mut later = vec![false; steps.vars.len()];
        for step in &steps.blocks[*b].steps[run.end..] {
            access(step).0.iter().for_each(|v| later[v.0] = true);
        }
        outside.iter().for_each(|v| later[v.0] = true);
        groups.push(group(steps, &steps.blocks[*b].steps[run.clone()], &later));
    }

    for run in &groups {
        separate(steps, run);
    }

    let isa = host()?;
    let context = Box::new(Context::create());
    // SAFETY: the context is boxed, so its address is stable, and is freed
    // only after the engine and module referring to it.
    let cx: &'static Context = unsafe { &*(context.as_ref() as *const Context) };
    let module = cx.create_module("roc_program");
    for (i, run) in groups.iter().enumerate() {
        emit::function(cx, &module, &format!("roc_fused_{i}"), &run.group, isa).map_err(error)?;
    }
    MACHINE
        .with(|machine| {
            let mut machine = machine.borrow_mut();
            if machine.is_none() {
                let triple = TargetMachine::get_default_triple();
                *machine = Some(codegen::machine(&triple, isa, RelocMode::Default)?);
            }
            // The emitted code is already explicit and vectorized: common
            // subexpressions are all that is left, and compile time matters here.
            codegen::optimize(&module, machine.as_ref().unwrap(), "function(early-cse)")
        })
        .map_err(error)?;
    let engine = module
        .create_jit_execution_engine(OptimizationLevel::Aggressive)
        .map_err(error)?;
    let mut functions = vec![];
    for i in 0..groups.len() {
        let address = engine
            .get_function_address(&format!("roc_fused_{i}"))
            .map_err(error)?;
        // SAFETY: emitted with the kernel signature.
        functions.push(unsafe { std::mem::transmute::<usize, KernelFn>(address) });
    }

    let mut fused = runs
        .into_iter()
        .zip(groups.into_iter().zip(functions))
        .peekable();
    for b in 0..steps.blocks.len() {
        let mut old = std::mem::take(&mut steps.blocks[b].steps)
            .into_iter()
            .enumerate()
            .peekable();
        while let Some((i, step)) = old.next() {
            match fused.peek() {
                Some(((block, range), _)) if *block == b && range.start == i => {
                    let ((_, range), (run, f)) = fused.next().unwrap();
                    let begin = steps.fallback.len();
                    steps.fallback.push(step);
                    for _ in range.start + 1..range.end {
                        steps.fallback.push(old.next().unwrap().1);
                    }
                    steps.blocks[b].steps.push(Step::Fused {
                        f,
                        args: run.args,
                        reports: run.reports,
                        fallback: begin..steps.fallback.len(),
                    });
                }
                _ => steps.blocks[b].steps.push(step),
            }
        }
    }
    steps.jit = Some(Jit {
        module: ManuallyDrop::new(module),
        engine: ManuallyDrop::new(engine),
        context: ManuallyDrop::new(context),
        ir: OnceLock::new(),
    });
    Ok(())
}
