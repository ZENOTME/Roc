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

//! Execution of a kernel plan over windows of `WINDOW` rows.
//!
//! Values keep their Arrow width: input columns are read in place, root
//! outputs are written in place and other variables live in window slots,
//! reused after their last read. Booleans are window bitmaps with a
//! `BitState`; constants are one broadcast value. A Filter domain is a mask
//! of the window's rows: pure kernels compute every row, which is safe as
//! they are total, while effects (RaiseIf, GroupLookup, AggregateUpdate and
//! the output) only see the rows of their domain.
//!
//! Each step runs over the whole window, in plan order. Kernels call the
//! precompiled library; `Compile` programs replace runs of kernels by one
//! fused function (see `fuse`), which defers to the steps it replaces in a
//! window where it may raise.
mod aggregate;
pub(crate) mod bits;
mod fuse;
mod output;
mod run;

pub(crate) use run::Buffers;

use super::codegen::{
    catalog::{Key, Kind},
    ir::{self, Func, Pred, Ty},
};
use super::library::{self, KernelFn};
use super::plan::{Item, Plan};
use super::*;
use crate::error::{Error, Result};
use std::ops::Range;
use std::sync::Arc;

pub(crate) const WINDOW: usize = 1024;
pub(crate) const WORDS: usize = WINDOW / 64;

/// The lane type of a variable type.
pub(crate) fn lane(t: VariableType) -> Ty {
    match t {
        VariableType::Boolean => Ty::Bool,
        VariableType::Int8 => Ty::I8,
        VariableType::Int16 => Ty::I16,
        VariableType::Int32 => Ty::I32,
        VariableType::Int64 => Ty::I64,
        VariableType::UInt8 => Ty::U8,
        VariableType::UInt16 => Ty::U16,
        VariableType::UInt32 => Ty::U32,
        VariableType::UInt64 | VariableType::BytePointer | VariableType::Usize => Ty::U64,
        VariableType::Float32 => Ty::F32,
        VariableType::Float64 => Ty::F64,
    }
}

/// Where the window values of a variable live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Place {
    Unused,
    /// The fixed-width values of an input, read in place.
    Input(usize),
    /// An input bitmap: the validity, or the values of a Boolean input.
    /// Read in place when word-aligned, else copied into the slot.
    InputBits {
        input: usize,
        validity: bool,
        slot: usize,
    },
    /// The row pointers or lengths of a byte input.
    InputBytes {
        input: usize,
        len: bool,
        slot: usize,
    },
    Value(usize),
    Bits(usize),
    /// An output column of the root domain, written in place.
    Output(usize),
    /// A pool entry: the value, or 0 / 1 for a Boolean.
    Constant(usize),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Var {
    pub ty: Ty,
    pub vt: VariableType,
    pub place: Place,
}

impl Var {
    pub fn scalar(&self) -> bool {
        matches!(self.place, Place::Constant(_))
    }
}

/// A kernel argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operand {
    Var(VariableId),
    Pool(usize),
    /// The row mask of a domain.
    Domain(DomainId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BitOp {
    And,
    Or,
    Not,
}

/// The function and variables of a kernel, kept for fusion.
#[derive(Clone, Debug)]
pub(crate) struct Call {
    pub func: Func,
    pub inputs: Vec<VariableId>,
    pub outputs: Vec<VariableId>,
}

#[derive(Clone, Debug)]
pub(crate) enum Text {
    Compare {
        pred: Pred,
        inputs: [VariableId; 4],
        output: VariableId,
    },
    Parse {
        to: VariableType,
        ptr: VariableId,
        len: VariableId,
        value: VariableId,
        invalid: VariableId,
    },
    Format {
        input: VariableId,
        ptr: VariableId,
        len: VariableId,
    },
    Validate {
        ptr: VariableId,
        len: VariableId,
        invalid: VariableId,
    },
}

/// How an AggregateUpdate runs, decided when it is prepared.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Update {
    pub kind: AggregateUpdateKind,
    /// The rows to update need this to be true: the argument validity of
    /// updates that skip NULL arguments.
    pub validity: Option<VariableId>,
    /// The column of an integer sum.
    pub sum: Option<VariableId>,
    /// COUNT of a NULL-typed argument: nothing to do.
    pub none: bool,
}

impl Update {
    fn new(kind: AggregateUpdateKind, arguments: &[ColumnVariableBinding], vars: &[Var]) -> Self {
        use AggregateUpdateKind as K;
        use ColumnVariableBinding as B;
        let valid = match arguments {
            [a] if !a.is_null_type() => a.validity(),
            _ => None,
        };
        let sum = match (kind, arguments) {
            (K::SumI64, [B::Int64 { value, .. }]) | (K::SumU64, [B::UInt64 { value, .. }])
                if !vars[value.0].scalar() =>
            {
                Some(*value)
            }
            _ => None,
        };
        Self {
            kind,
            validity: match kind {
                K::CountValues => valid,
                _ if sum.is_some() => valid,
                _ => None,
            },
            sum,
            none: kind == K::CountValues && matches!(arguments, [a] if a.is_null_type()),
        }
    }
}

/// One update of an Aggregates step.
#[derive(Clone, Debug)]
pub(crate) struct Aggregate {
    pub state: StateId,
    pub update: Update,
    pub arguments: Vec<ColumnVariableBinding>,
}

#[derive(Clone, Debug)]
pub(crate) enum Step {
    Kernel {
        f: KernelFn,
        args: Vec<Operand>,
        /// The Boolean outputs, in report order.
        reports: Vec<VariableId>,
        call: Call,
    },
    Bits {
        op: BitOp,
        a: VariableId,
        b: VariableId,
        output: VariableId,
    },
    SelectBits {
        cond: VariableId,
        then: VariableId,
        otherwise: VariableId,
        output: VariableId,
    },
    Text {
        text: Text,
        domain: DomainId,
    },
    Filter {
        predicate: VariableId,
        parent: DomainId,
        domain: DomainId,
    },
    RaiseIf {
        cond: VariableId,
        error: KernelError,
        args: Vec<VariableId>,
        domain: DomainId,
    },
    GroupLookup {
        state: StateId,
        keys: Vec<ColumnVariableBinding>,
        output: VariableId,
        domain: DomainId,
    },
    /// Consecutive AggregateUpdates of one group ID over the same rows.
    Aggregates {
        group: VariableId,
        domain: DomainId,
        /// The rows to update need this to be true.
        validity: Option<VariableId>,
        updates: Vec<Aggregate>,
    },
    Output {
        domain: DomainId,
    },
    /// Generated code for `fallback`, a range of `Steps::fallback`.
    Fused {
        f: KernelFn,
        args: Vec<Operand>,
        reports: Vec<VariableId>,
        fallback: Range<usize>,
    },
}

/// The prepared steps of one program.
pub(crate) struct Steps {
    steps: Vec<Step>,
    fallback: Vec<Step>,
    vars: Vec<Var>,
    pool: Box<[u64]>,
    value_slots: usize,
    bit_slots: usize,
    /// Per domain: the bitmap slot of its mask; None for the root.
    masks: Vec<Option<usize>>,
    /// Per input: whether a step reads it.
    read: Vec<bool>,
    /// The variables whose window pointers change with the window.
    windowed: Vec<VariableId>,
    _bytes: Vec<Arc<[u8]>>,
    jit: Option<fuse::Jit>,
}

impl std::fmt::Debug for Steps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Steps")
            .field("steps", &self.steps.len())
            .field("fused", &self.jit.is_some())
            .finish_non_exhaustive()
    }
}

fn lowered(f: ScalarFunction) -> Option<Func> {
    use ScalarFunction as S;
    Some(match f {
        S::Add => Func::Add,
        S::Subtract => Func::Sub,
        S::Multiply => Func::Mul,
        S::Negate => Func::Neg,
        S::Divide => Func::Div,
        S::Remainder => Func::Rem,
        S::Equal => Func::Compare(Pred::Eq),
        S::NotEqual => Func::Compare(Pred::Ne),
        S::LessThan => Func::Compare(Pred::Lt),
        S::LessThanOrEqual => Func::Compare(Pred::Le),
        S::GreaterThan => Func::Compare(Pred::Gt),
        S::GreaterThanOrEqual => Func::Compare(Pred::Ge),
        S::Cast { to } => Func::Cast(lane(to)),
        _ => return None,
    })
}

/// The steps under construction.
struct Builder {
    vars: Vec<Var>,
    pool: Vec<u64>,
    bytes: Vec<Arc<[u8]>>,
    steps: Vec<Step>,
}

impl Builder {
    fn constant(&mut self, v: VariableId, bits: u64) {
        self.pool.push(bits);
        self.vars[v.0].place = Place::Constant(self.pool.len() - 1);
    }
    fn pool(&mut self, bits: u64) -> Operand {
        self.pool.push(bits);
        Operand::Pool(self.pool.len() - 1)
    }
    fn value(&self, v: VariableId) -> Option<u64> {
        match self.vars[v.0].place {
            Place::Constant(i) => Some(self.pool[i]),
            _ => None,
        }
    }

    /// Computes a kernel of constant operands now.
    fn fold(&mut self, key: Key, inputs: &[VariableId], outputs: &[VariableId]) -> Result<()> {
        let f = library::kernel(&Key { scalars: 0, ..key })?;
        let mut values = inputs
            .iter()
            .map(|v| self.value(*v).unwrap())
            .collect::<Vec<_>>();
        let mut results = vec![0u64; outputs.len()];
        let mut args: Vec<*mut u8> = vec![];
        for (v, value) in inputs.iter().zip(values.iter_mut()) {
            args.push(if self.vars[v.0].ty == Ty::Bool {
                bits::BitState::constant(*value != 0).bitmap().unwrap()
            } else {
                (value as *mut u64).cast()
            });
        }
        for r in results.iter_mut() {
            args.push((r as *mut u64).cast());
        }
        // SAFETY: every argument is one row: a value or a bitmap word.
        unsafe { f(args.as_ptr(), 1) };
        for (v, bits) in outputs.iter().zip(results) {
            let bits = if self.vars[v.0].ty == Ty::Bool {
                bits & 1
            } else {
                bits
            };
            self.constant(*v, bits);
        }
        Ok(())
    }

    fn call(
        &mut self,
        function: ScalarFunction,
        inputs: &[VariableId],
        outputs: &[VariableId],
        domain: DomainId,
    ) -> Result<()> {
        use ScalarFunction as S;
        let ty = |v: &VariableId| self.vars[v.0].ty;
        let constant = inputs.iter().all(|v| self.value(*v).is_some());
        match function {
            S::And | S::Or | S::Not => {
                let op = match function {
                    S::And => BitOp::And,
                    S::Or => BitOp::Or,
                    _ => BitOp::Not,
                };
                if constant {
                    let a = self.value(inputs[0]).unwrap() != 0;
                    let b = inputs.get(1).is_some_and(|v| self.value(*v).unwrap() != 0);
                    let r = match op {
                        BitOp::And => a && b,
                        BitOp::Or => a || b,
                        BitOp::Not => !a,
                    };
                    self.constant(outputs[0], r as u64);
                } else {
                    self.steps.push(Step::Bits {
                        op,
                        a: inputs[0],
                        b: *inputs.get(1).unwrap_or(&inputs[0]),
                        output: outputs[0],
                    });
                }
                return Ok(());
            }
            S::ParseText { to } => {
                self.steps.push(Step::Text {
                    text: Text::Parse {
                        to,
                        ptr: inputs[0],
                        len: inputs[1],
                        value: outputs[0],
                        invalid: outputs[1],
                    },
                    domain,
                });
                return Ok(());
            }
            S::FormatText => {
                self.steps.push(Step::Text {
                    text: Text::Format {
                        input: inputs[0],
                        ptr: outputs[0],
                        len: outputs[1],
                    },
                    domain,
                });
                return Ok(());
            }
            S::ValidateUtf8 => {
                self.steps.push(Step::Text {
                    text: Text::Validate {
                        ptr: inputs[0],
                        len: inputs[1],
                        invalid: outputs[0],
                    },
                    domain,
                });
                return Ok(());
            }
            _ if inputs.len() == 4 => {
                let Some(Func::Compare(pred)) = lowered(function) else {
                    return Err(Error::unsupported(format!("unsupported {function:?}")));
                };
                self.steps.push(Step::Text {
                    text: Text::Compare {
                        pred,
                        inputs: [inputs[0], inputs[1], inputs[2], inputs[3]],
                        output: outputs[0],
                    },
                    domain,
                });
                return Ok(());
            }
            _ => {}
        }
        let func = lowered(function)
            .ok_or_else(|| Error::unsupported(format!("unsupported {function:?}")))?;
        let t = ty(&inputs[0]);
        let scalars = inputs
            .iter()
            .enumerate()
            .filter(|(_, v)| self.value(**v).is_some() && ty(v) != Ty::Bool)
            .fold(0u8, |s, (i, _)| s | 1 << i);
        let key = Key {
            kind: Kind::Func(func),
            ty: t,
            scalars,
        };
        if constant {
            return self.fold(key, inputs, outputs);
        }
        if let (Func::Div | Func::Rem, Some(d)) = (func, inputs.get(1).and_then(|v| self.value(*v)))
            && let Some(m) = ir::magic(t, d)
        {
            let kind = if func == Func::Div {
                Kind::DivMagic
            } else {
                Kind::RemMagic
            };
            let mut args = vec![Operand::Var(inputs[0]), Operand::Var(inputs[1])];
            for c in m {
                args.push(self.pool(c));
            }
            args.push(Operand::Var(outputs[0]));
            for flag in &outputs[1..] {
                self.constant(*flag, 0);
            }
            self.steps.push(Step::Kernel {
                f: library::kernel(&Key {
                    kind,
                    ty: t,
                    scalars: 0,
                })?,
                args,
                reports: vec![],
                call: Call {
                    func,
                    inputs: inputs.to_vec(),
                    outputs: outputs.to_vec(),
                },
            });
            return Ok(());
        }
        self.kernel(key, inputs, outputs, func)
    }

    fn kernel(
        &mut self,
        key: Key,
        inputs: &[VariableId],
        outputs: &[VariableId],
        func: Func,
    ) -> Result<()> {
        let mut args = inputs.iter().map(|v| Operand::Var(*v)).collect::<Vec<_>>();
        args.extend(outputs.iter().map(|v| Operand::Var(*v)));
        let reports = outputs
            .iter()
            .copied()
            .filter(|v| self.vars[v.0].ty == Ty::Bool)
            .collect();
        self.steps.push(Step::Kernel {
            f: library::kernel(&key)?,
            args,
            reports,
            call: Call {
                func,
                inputs: inputs.to_vec(),
                outputs: outputs.to_vec(),
            },
        });
        Ok(())
    }

    fn select(
        &mut self,
        cond: VariableId,
        then: VariableId,
        otherwise: VariableId,
        output: VariableId,
    ) -> Result<()> {
        let inputs = [cond, then, otherwise];
        if self.vars[then.0].ty == Ty::Bool {
            if inputs.iter().all(|v| self.value(*v).is_some()) {
                let c = self.value(cond).unwrap() != 0;
                let v = self.value(if c { then } else { otherwise }).unwrap();
                self.constant(output, v);
            } else {
                self.steps.push(Step::SelectBits {
                    cond,
                    then,
                    otherwise,
                    output,
                });
            }
            return Ok(());
        }
        let scalars =
            (self.value(then).is_some() as u8) | (self.value(otherwise).is_some() as u8) << 1;
        let key = Key {
            kind: Kind::Func(Func::Select),
            ty: self.vars[then.0].ty,
            scalars,
        };
        if inputs.iter().all(|v| self.value(*v).is_some()) {
            return self.fold(key, &inputs, &[output]);
        }
        self.kernel(key, &inputs, &[output], Func::Select)
    }
}

/// Variables a step reads and writes.
fn access(step: &Step) -> (Vec<VariableId>, Vec<VariableId>) {
    let vars = |ops: &[Operand]| {
        ops.iter()
            .filter_map(|o| match o {
                Operand::Var(v) => Some(*v),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    match step {
        Step::Kernel { args, call, .. } => {
            let mut reads = call.inputs.clone();
            reads.retain(|v| vars(args).contains(v));
            (
                reads,
                call.outputs
                    .iter()
                    .copied()
                    .filter(|v| vars(args).contains(v))
                    .collect(),
            )
        }
        Step::Bits { a, b, output, .. } => (vec![*a, *b], vec![*output]),
        Step::SelectBits {
            cond,
            then,
            otherwise,
            output,
        } => (vec![*cond, *then, *otherwise], vec![*output]),
        Step::Text { text, .. } => match text {
            Text::Compare { inputs, output, .. } => (inputs.to_vec(), vec![*output]),
            Text::Parse {
                ptr,
                len,
                value,
                invalid,
                ..
            } => (vec![*ptr, *len], vec![*value, *invalid]),
            Text::Format { input, ptr, len } => (vec![*input], vec![*ptr, *len]),
            Text::Validate { ptr, len, invalid } => (vec![*ptr, *len], vec![*invalid]),
        },
        Step::Filter { predicate, .. } => (vec![*predicate], vec![]),
        Step::RaiseIf { cond, args, .. } => (
            std::iter::once(*cond).chain(args.iter().copied()).collect(),
            vec![],
        ),
        Step::GroupLookup { keys, output, .. } => (
            keys.iter().flat_map(|k| k.variables()).collect(),
            vec![*output],
        ),
        Step::Aggregates { group, updates, .. } => (
            std::iter::once(*group)
                .chain(
                    updates
                        .iter()
                        .flat_map(|u| u.arguments.iter().flat_map(|a| a.variables())),
                )
                .collect(),
            vec![],
        ),
        Step::Output { .. } | Step::Fused { .. } => (vec![], vec![]),
    }
}

impl Steps {
    pub fn prepare(graph: &KernelGraph, plan: &Plan, compile: bool) -> Result<Self> {
        let mut b = Builder {
            vars: graph
                .variables
                .iter()
                .map(|v| Var {
                    ty: lane(v.ty),
                    vt: v.ty,
                    place: Place::Unused,
                })
                .collect(),
            pool: vec![],
            bytes: vec![],
            steps: vec![],
        };
        let mut output_reads = vec![];
        for &(item, domain) in &plan.items {
            match item {
                Item::Output => {
                    let output = graph.output.as_ref().unwrap();
                    for (j, binding) in output.bindings.iter().enumerate() {
                        if plan.aliases.get(j).copied().flatten().is_none() {
                            output_reads.extend(binding.variables());
                        }
                    }
                    b.steps.push(Step::Output { domain });
                }
                Item::Kernel(k) => match &graph.kernels[k].1 {
                    Kernel::Domain(DomainKernel::Filter { predicate, output }) => {
                        b.steps.push(Step::Filter {
                            predicate: *predicate,
                            parent: domain,
                            domain: *output,
                        })
                    }
                    Kernel::Domain(_) => {
                        return Err(Error::invalid_plan(
                            "only Filter domain kernels are supported".into(),
                        ));
                    }
                    Kernel::Scalar(kernel) => match kernel {
                        ScalarKernel::Constant { value, output } => {
                            if let ConstantValue::BytePointer(bytes) = value {
                                b.bytes.push(bytes.clone());
                            }
                            b.constant(*output, value.bits());
                        }
                        ScalarKernel::Call {
                            function,
                            inputs,
                            outputs,
                        } => b.call(*function, inputs, outputs, domain)?,
                        ScalarKernel::Select {
                            cond,
                            then,
                            otherwise,
                            output,
                        } => b.select(*cond, *then, *otherwise, *output)?,
                        ScalarKernel::RaiseIf { cond, error, args } => {
                            if b.value(*cond) != Some(0) {
                                b.steps.push(Step::RaiseIf {
                                    cond: *cond,
                                    error: *error,
                                    args: args.clone(),
                                    domain,
                                })
                            }
                        }
                        ScalarKernel::GroupLookup {
                            state,
                            keys,
                            output,
                        } => {
                            if keys.is_empty() {
                                b.constant(*output, 0);
                            } else {
                                b.steps.push(Step::GroupLookup {
                                    state: *state,
                                    keys: keys.clone(),
                                    output: *output,
                                    domain,
                                })
                            }
                        }
                        ScalarKernel::AggregateUpdate {
                            state,
                            kind,
                            group,
                            arguments,
                        } => {
                            let update = Update::new(*kind, arguments, &b.vars);
                            let aggregate = Aggregate {
                                state: *state,
                                update,
                                arguments: arguments.clone(),
                            };
                            match b.steps.last_mut() {
                                Some(Step::Aggregates {
                                    group: g,
                                    domain: d,
                                    validity: v,
                                    updates,
                                }) if *g == *group && *d == domain && *v == update.validity => {
                                    updates.push(aggregate)
                                }
                                _ => b.steps.push(Step::Aggregates {
                                    group: *group,
                                    domain,
                                    validity: update.validity,
                                    updates: vec![aggregate],
                                }),
                            }
                        }
                    },
                },
            }
        }
        let Builder {
            mut vars,
            pool,
            bytes,
            steps,
            ..
        } = b;

        // Input variables that some step reads.
        let mut read_var = vec![false; vars.len()];
        for step in &steps {
            access(step).0.iter().for_each(|v| read_var[v.0] = true);
        }
        output_reads.iter().for_each(|v| read_var[v.0] = true);
        let mut read = vec![false; graph.inputs.len()];
        let mut bit_slots = 0;
        let mut value_slots = 0;
        for (i, input) in graph.inputs.iter().enumerate() {
            let binding = input.binding;
            for v in binding.variables() {
                if !read_var[v.0] || vars[v.0].place != Place::Unused {
                    continue;
                }
                read[i] = true;
                vars[v.0].place = match binding {
                    _ if Some(v) == binding.validity() => {
                        bit_slots += 1;
                        Place::InputBits {
                            input: i,
                            validity: true,
                            slot: bit_slots - 1,
                        }
                    }
                    ColumnVariableBinding::Boolean { .. } => {
                        bit_slots += 1;
                        Place::InputBits {
                            input: i,
                            validity: false,
                            slot: bit_slots - 1,
                        }
                    }
                    ColumnVariableBinding::Bytes { len, .. } => {
                        value_slots += 1;
                        Place::InputBytes {
                            input: i,
                            len: v == len,
                            slot: value_slots - 1,
                        }
                    }
                    _ => Place::Input(i),
                };
            }
        }

        // Root outputs are written in place by the step defining them.
        let mut defined = vec![false; vars.len()];
        for step in &steps {
            if matches!(
                step,
                Step::Kernel { .. }
                    | Step::Text {
                        text: Text::Parse { .. },
                        ..
                    }
            ) {
                access(step).1.iter().for_each(|v| defined[v.0] = true);
            }
        }
        if let Some(output) = &graph.output
            && output.domain == DomainId::ROOT
        {
            for (j, binding) in output.bindings.iter().enumerate() {
                if plan.aliases.get(j).copied().flatten().is_some() {
                    continue;
                }
                if let Some(v) = binding.value()
                    && vars[v.0].ty != Ty::Bool
                    && defined[v.0]
                    && vars[v.0].place == Place::Unused
                {
                    vars[v.0].place = Place::Output(j);
                }
            }
        }

        // Slots, free after the last read.
        let mut last = vec![None; vars.len()];
        for (s, step) in steps.iter().enumerate() {
            access(step).0.iter().for_each(|v| last[v.0] = Some(s));
            if let Step::Output { .. } = step {
                output_reads.iter().for_each(|v| last[v.0] = Some(s));
            }
        }
        let mut free_values = vec![];
        let mut free_bits = vec![];
        let mut release: Vec<Vec<VariableId>> = vec![vec![]; steps.len()];
        for (s, step) in steps.iter().enumerate() {
            for v in access(step).1 {
                if vars[v.0].place != Place::Unused {
                    continue;
                }
                vars[v.0].place = if vars[v.0].ty == Ty::Bool {
                    Place::Bits(free_bits.pop().unwrap_or_else(|| {
                        bit_slots += 1;
                        bit_slots - 1
                    }))
                } else {
                    Place::Value(free_values.pop().unwrap_or_else(|| {
                        value_slots += 1;
                        value_slots - 1
                    }))
                };
                release[last[v.0].unwrap_or(s).max(s)].push(v);
            }
            for v in std::mem::take(&mut release[s]) {
                match vars[v.0].place {
                    Place::Bits(slot) => free_bits.push(slot),
                    Place::Value(slot) => free_values.push(slot),
                    _ => {}
                }
            }
        }
        let masks = graph
            .domains
            .iter()
            .enumerate()
            .map(|(d, _)| {
                (d != 0).then(|| {
                    bit_slots += 1;
                    bit_slots - 1
                })
            })
            .collect();
        let windowed = (0..vars.len())
            .map(VariableId)
            .filter(|v| {
                matches!(
                    vars[v.0].place,
                    Place::Bits(_)
                        | Place::Input(_)
                        | Place::Output(_)
                        | Place::InputBits { .. }
                        | Place::InputBytes { .. }
                )
            })
            .collect();
        let mut prepared = Self {
            windowed,
            steps,
            fallback: vec![],
            vars,
            pool: pool.into_boxed_slice(),
            value_slots,
            bit_slots,
            masks,
            read,
            _bytes: bytes,
            jit: None,
        };
        if compile {
            fuse::fuse(&mut prepared, graph)?;
        }
        Ok(prepared)
    }

    /// Whether the only step is the output: the program computes nothing.
    pub fn is_output_only(&self) -> bool {
        self.steps.iter().all(|s| matches!(s, Step::Output { .. }))
    }

    /// The IR of the fused functions; empty without any.
    pub fn ir(&self) -> &str {
        self.jit.as_ref().map_or("", |j| j.ir())
    }
}
