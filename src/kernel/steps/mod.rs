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
mod expand;
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
use std::collections::HashMap;
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
        VariableType::Int128 => Ty::I128,
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
    /// Substring, Upper, Concat and Like, with a LIKE pattern compiled when
    /// it is a constant.
    Strings {
        function: ScalarFunction,
        inputs: Vec<VariableId>,
        outputs: Vec<VariableId>,
        pattern: Option<Arc<crate::kernel::text::Pattern>>,
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
    /// A TableGather.
    Gather {
        state: StateId,
        row: VariableId,
        column: usize,
        output: ColumnVariableBinding,
    },
    Expand(Box<ExpandStep>),
    /// Generated code for `fallback`, a range of `Steps::fallback`.
    Fused {
        f: KernelFn,
        args: Vec<Operand>,
        reports: Vec<VariableId>,
        fallback: Range<usize>,
    },
}

/// An Expand: the rows it produces for a window of the enclosing block run
/// `block`, in windows of their own.
#[derive(Clone, Debug)]
pub(crate) struct ExpandStep {
    pub kind: ExpandStepKind,
    /// The domain whose rows expand, in the enclosing block.
    pub input: DomainId,
    pub row: VariableId,
    pub block: usize,
    /// Enclosing-block variables the block reads, with their copies.
    pub imports: Vec<(VariableId, VariableId)>,
    pub reduces: Vec<ReduceStep>,
}

#[derive(Clone, Debug)]
pub(crate) enum ExpandStepKind {
    Lookup {
        state: StateId,
        keys: Vec<ColumnVariableBinding>,
        nulls_equal: bool,
        outer: bool,
    },
    Product {
        state: StateId,
    },
}

impl ExpandStepKind {
    pub fn state(&self) -> StateId {
        match self {
            Self::Lookup { state, .. } | Self::Product { state } => *state,
        }
    }
}

/// A Reduce, run by the Expand of its descendant.
#[derive(Clone, Debug)]
pub(crate) struct ReduceStep {
    /// The domain produced: the rows of the expansion's input.
    pub domain: DomainId,
    pub descendant: DomainId,
    /// Booleans of the block and the outputs they are reduced into.
    pub any: Vec<(VariableId, VariableId)>,
}

/// The steps of one row space, run over its windows.
pub(crate) struct Block {
    pub steps: Vec<Step>,
    /// The domain of every row of the block's windows.
    pub dense: DomainId,
    /// The other domains of the space; their masks start empty.
    pub domains: Vec<DomainId>,
    /// The variables whose window pointers change with the window.
    pub windowed: Vec<VariableId>,
}

/// The prepared steps of one program.
pub(crate) struct Steps {
    /// The root block first.
    blocks: Vec<Block>,
    fallback: Vec<Step>,
    vars: Vec<Var>,
    pool: Box<[u64]>,
    value_slots: usize,
    bit_slots: usize,
    /// Per domain: the bitmap slot of its mask; None for the root.
    masks: Vec<Option<usize>>,
    /// Per input: whether a step reads it.
    read: Vec<bool>,
    /// The output bindings, as variables of the output step's block.
    output_bindings: Vec<ColumnVariableBinding>,
    _bytes: Vec<Arc<[u8]>>,
    jit: Option<fuse::Jit>,
}

impl std::fmt::Debug for Steps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Steps")
            .field("blocks", &self.blocks.len())
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
        S::DivideRound => Func::DivRound,
        S::Abs => Func::Abs,
        S::Round => Func::Round,
        S::BitAnd => Func::BitAnd,
        S::BitOr => Func::BitOr,
        S::ShiftRight => Func::ShiftRight,
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
    /// A constant of `v` given its low word; a 128-bit value sign-extends it.
    fn constant(&mut self, v: VariableId, bits: u64) {
        let high = if (bits as i64) < 0 { u64::MAX } else { 0 };
        self.constant_words(v, [bits, high]);
    }
    /// Pool entries hold one word, or two for a 128-bit lane.
    fn constant_words(&mut self, v: VariableId, words: [u64; 2]) {
        self.pool.push(words[0]);
        if self.vars[v.0].ty == Ty::I128 {
            self.pool.push(words[1]);
        }
        self.vars[v.0].place =
            Place::Constant(self.pool.len() - 1 - (self.vars[v.0].ty == Ty::I128) as usize);
    }
    fn words(&self, v: VariableId) -> Option<[u64; 2]> {
        match self.vars[v.0].place {
            Place::Constant(i) => Some([self.pool[i], *self.pool.get(i + 1).unwrap_or(&0)]),
            _ => None,
        }
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
            .map(|v| self.words(*v).unwrap())
            .collect::<Vec<_>>();
        let mut results = vec![[0u64; 2]; outputs.len()];
        let mut args: Vec<*mut u8> = vec![];
        for (v, value) in inputs.iter().zip(values.iter_mut()) {
            args.push(if self.vars[v.0].ty == Ty::Bool {
                bits::BitState::constant(value[0] != 0).bitmap().unwrap()
            } else {
                value.as_mut_ptr().cast()
            });
        }
        for r in results.iter_mut() {
            args.push(r.as_mut_ptr().cast());
        }
        // SAFETY: every argument is one row: a value of up to two words or a
        // bitmap word.
        unsafe { f(args.as_ptr(), 1) };
        for (v, words) in outputs.iter().zip(results) {
            match self.vars[v.0].ty {
                Ty::Bool => self.constant(*v, words[0] & 1),
                Ty::I128 => self.constant_words(*v, words),
                _ => self.constant(*v, words[0]),
            }
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
            S::Substring | S::Upper | S::Concat | S::Like { .. } => {
                // A constant pattern compiles once.
                let pattern = match function {
                    S::Like { escape } => match (self.value(inputs[2]), self.value(inputs[3])) {
                        (Some(ptr), Some(len)) => {
                            // SAFETY: byte constants are retained by the program.
                            let bytes = unsafe {
                                std::slice::from_raw_parts(ptr as *const u8, len as usize)
                            };
                            let text = std::str::from_utf8(bytes).map_err(|source| {
                                Error::invalid_plan("LIKE pattern is not UTF-8".into())
                                    .with_source(source)
                            })?;
                            let escape = (escape != 0).then_some(escape as char);
                            Some(Arc::new(crate::kernel::text::Pattern::new(text, escape)))
                        }
                        _ => None,
                    },
                    _ => None,
                };
                self.steps.push(Step::Text {
                    text: Text::Strings {
                        function,
                        inputs: inputs.to_vec(),
                        outputs: outputs.to_vec(),
                        pattern,
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

impl Builder {
    /// Adds the steps of one kernel item in `domain`. A Reduce adds none: its
    /// expansion runs it.
    fn item(&mut self, kernel: &Kernel, domain: DomainId) -> Result<()> {
        match kernel {
            Kernel::Domain(DomainKernel::Filter { predicate, output }) => {
                self.steps.push(Step::Filter {
                    predicate: *predicate,
                    parent: domain,
                    domain: *output,
                })
            }
            Kernel::Domain(DomainKernel::Expand {
                input,
                kind,
                outputs,
                ..
            }) => {
                let kind = match kind {
                    ExpandKind::Lookup {
                        state,
                        keys,
                        nulls_equal,
                        outer,
                    } => ExpandStepKind::Lookup {
                        state: *state,
                        keys: keys.clone(),
                        nulls_equal: *nulls_equal,
                        outer: *outer,
                    },
                    ExpandKind::Product { state } => ExpandStepKind::Product { state: *state },
                };
                self.steps.push(Step::Expand(Box::new(ExpandStep {
                    kind,
                    input: *input,
                    row: outputs[0],
                    block: usize::MAX,
                    imports: vec![],
                    reduces: vec![],
                })))
            }
            Kernel::Domain(DomainKernel::Reduce { .. }) => {}
            Kernel::Domain(DomainKernel::Union { .. }) => {
                return Err(Error::unsupported("union domains are not supported".into()));
            }
            Kernel::Scalar(kernel) => match kernel {
                ScalarKernel::Constant { value, output } => {
                    if let ConstantValue::BytePointer(bytes) = value {
                        self.bytes.push(bytes.clone());
                    }
                    self.constant_words(*output, value.words());
                }
                ScalarKernel::Call {
                    function,
                    inputs,
                    outputs,
                } => self.call(*function, inputs, outputs, domain)?,
                ScalarKernel::Select {
                    cond,
                    then,
                    otherwise,
                    output,
                } => self.select(*cond, *then, *otherwise, *output)?,
                ScalarKernel::RaiseIf { cond, error, args } => {
                    if self.value(*cond) != Some(0) {
                        self.steps.push(Step::RaiseIf {
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
                        self.constant(*output, 0);
                    } else {
                        self.steps.push(Step::GroupLookup {
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
                    let update = Update::new(*kind, arguments, &self.vars);
                    let aggregate = Aggregate {
                        state: *state,
                        update,
                        arguments: arguments.clone(),
                    };
                    match self.steps.last_mut() {
                        Some(Step::Aggregates {
                            group: g,
                            domain: d,
                            validity: v,
                            updates,
                        }) if *g == *group && *d == domain && *v == update.validity => {
                            updates.push(aggregate)
                        }
                        _ => self.steps.push(Step::Aggregates {
                            group: *group,
                            domain,
                            validity: update.validity,
                            updates: vec![aggregate],
                        }),
                    }
                }
                ScalarKernel::TableGather {
                    state,
                    row,
                    column,
                    output,
                } => self.steps.push(Step::Gather {
                    state: *state,
                    row: *row,
                    column: *column,
                    output: *output,
                }),
            },
        }
        Ok(())
    }
}

/// The variables a step reads and writes. An Expand reads its keys and the
/// variables its block imports, and writes the outputs of its reductions.
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
            Text::Strings {
                inputs, outputs, ..
            } => (inputs.clone(), outputs.clone()),
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
        Step::Gather { row, output, .. } => (vec![*row], output.variables()),
        Step::Expand(e) => {
            let mut reads = match &e.kind {
                ExpandStepKind::Lookup { keys, .. } => {
                    keys.iter().flat_map(|k| k.variables()).collect()
                }
                ExpandStepKind::Product { .. } => vec![],
            };
            reads.extend(e.imports.iter().map(|(from, _)| *from));
            let writes = e
                .reduces
                .iter()
                .flat_map(|r| r.any.iter().map(|(_, out)| *out))
                .collect();
            (reads, writes)
        }
        Step::Output { .. } | Step::Fused { .. } => (vec![], vec![]),
    }
}

fn rename_binding(b: &mut ColumnVariableBinding, map: &HashMap<VariableId, VariableId>) {
    let r = |v: &mut VariableId| {
        if let Some(n) = map.get(v) {
            *v = *n;
        }
    };
    use ColumnVariableBinding as B;
    match b {
        B::Null { validity } => r(validity),
        B::Bytes { ptr, len, validity } => {
            r(ptr);
            r(len);
            validity.iter_mut().for_each(r);
        }
        B::Boolean { value, validity }
        | B::Int8 { value, validity }
        | B::Int16 { value, validity }
        | B::Int32 { value, validity }
        | B::Int64 { value, validity }
        | B::Int128 { value, validity }
        | B::UInt8 { value, validity }
        | B::UInt16 { value, validity }
        | B::UInt32 { value, validity }
        | B::UInt64 { value, validity }
        | B::Float32 { value, validity }
        | B::Float64 { value, validity } => {
            r(value);
            validity.iter_mut().for_each(r);
        }
    }
}

/// Replaces the variables a step reads by their copies in `map`. Imported
/// variables are never written by the steps that read the copies.
fn rename(step: &mut Step, map: &HashMap<VariableId, VariableId>) {
    let r = |v: &mut VariableId| {
        if let Some(n) = map.get(v) {
            *v = *n;
        }
    };
    match step {
        Step::Kernel { args, call, .. } => {
            for a in args {
                if let Operand::Var(v) = a {
                    r(v);
                }
            }
            call.inputs.iter_mut().for_each(r);
        }
        Step::Bits { a, b, .. } => {
            r(a);
            r(b);
        }
        Step::SelectBits {
            cond,
            then,
            otherwise,
            ..
        } => [cond, then, otherwise].into_iter().for_each(r),
        Step::Text { text, .. } => match text {
            Text::Compare { inputs, .. } => inputs.iter_mut().for_each(r),
            Text::Parse { ptr, len, .. } | Text::Validate { ptr, len, .. } => {
                r(ptr);
                r(len);
            }
            Text::Format { input, .. } => r(input),
            Text::Strings { inputs, .. } => inputs.iter_mut().for_each(r),
        },
        Step::Filter { predicate, .. } => r(predicate),
        Step::RaiseIf { cond, args, .. } => {
            r(cond);
            args.iter_mut().for_each(r);
        }
        Step::GroupLookup { keys, .. } => keys.iter_mut().for_each(|k| rename_binding(k, map)),
        Step::Aggregates {
            group,
            validity,
            updates,
            ..
        } => {
            r(group);
            validity.iter_mut().for_each(r);
            for u in updates {
                u.update.validity.iter_mut().for_each(r);
                u.update.sum.iter_mut().for_each(r);
                u.arguments.iter_mut().for_each(|a| rename_binding(a, map));
            }
        }
        Step::Gather { row, .. } => r(row),
        Step::Expand(e) => {
            if let ExpandStepKind::Lookup { keys, .. } = &mut e.kind {
                keys.iter_mut().for_each(|k| rename_binding(k, map));
            }
            e.imports.iter_mut().for_each(|(from, _)| r(from));
        }
        Step::Output { .. } | Step::Fused { .. } => {}
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
        // One block per row space, enclosing blocks first.
        let mut block_of = HashMap::new();
        let mut blocks: Vec<Block> = vec![];
        for (d, space) in plan.spaces.iter().enumerate() {
            if space.0 == d {
                block_of.insert(*space, blocks.len());
                blocks.push(Block {
                    steps: vec![],
                    dense: *space,
                    domains: vec![],
                    windowed: vec![],
                });
            } else {
                blocks[block_of[space]].domains.push(DomainId(d));
            }
        }
        let block = |d: DomainId| block_of[&plan.spaces[d.0]];
        // The Expand step of each child block: its block and position.
        let mut expands: HashMap<usize, (usize, usize)> = HashMap::new();
        let mut reduces = vec![];
        let mut output_reads = vec![];
        let mut output_block = 0;
        for &(item, domain) in &plan.items {
            let target = block(domain);
            std::mem::swap(&mut b.steps, &mut blocks[target].steps);
            let result = (|| -> Result<()> {
                match item {
                    Item::Output => {
                        let output = graph.output.as_ref().unwrap();
                        for (j, binding) in output.bindings.iter().enumerate() {
                            if plan.aliases.get(j).copied().flatten().is_none() {
                                output_reads.extend(binding.variables());
                            }
                        }
                        output_block = target;
                        b.steps.push(Step::Output { domain });
                    }
                    Item::Kernel(k) => b.item(&graph.kernels[k].1, domain)?,
                }
                Ok(())
            })();
            if let Item::Kernel(k) = item {
                match &graph.kernels[k].1 {
                    Kernel::Domain(DomainKernel::Expand { output, .. }) => {
                        let child = block(*output);
                        if let Some(Step::Expand(e)) = b.steps.last_mut() {
                            e.block = child;
                        }
                        expands.insert(child, (target, b.steps.len() - 1));
                    }
                    Kernel::Domain(DomainKernel::Reduce {
                        descendant,
                        aggregates,
                        outputs,
                        output,
                        ..
                    }) => reduces.push((
                        block(*descendant),
                        ReduceStep {
                            domain: *output,
                            descendant: *descendant,
                            any: aggregates
                                .iter()
                                .map(|(_, v)| *v)
                                .zip(outputs.iter().copied())
                                .collect(),
                        },
                    )),
                    _ => {}
                }
            }
            std::mem::swap(&mut b.steps, &mut blocks[target].steps);
            result?;
        }
        for (child, reduce) in reduces {
            let (parent, at) = expands[&child];
            match &mut blocks[parent].steps[at] {
                Step::Expand(e) => e.reduces.push(reduce),
                _ => unreachable!("expands index their steps"),
            }
        }
        let Builder {
            mut vars,
            pool,
            bytes,
            ..
        } = b;
        let mut var_block = graph
            .variables
            .iter()
            .map(|v| block(v.domain))
            .collect::<Vec<_>>();

        // Each child block reads enclosing-block variables through copies,
        // gathered by row when a window starts. Inner blocks first: their
        // imports are reads of the enclosing block.
        let mut output_bindings = graph.output.as_ref().map_or(vec![], |o| o.bindings.clone());
        for k in (1..blocks.len()).rev() {
            let (parent, at) = expands[&k];
            let mut reads = blocks[k]
                .steps
                .iter()
                .flat_map(|s| access(s).0)
                .collect::<Vec<_>>();
            if output_block == k {
                reads.extend(output_bindings.iter().flat_map(|b| b.variables()));
            }
            if let Step::Expand(e) = &blocks[parent].steps[at] {
                reads.extend(e.reduces.iter().flat_map(|r| r.any.iter().map(|(v, _)| *v)));
            }
            reads.sort();
            reads.dedup();
            let mut imports = vec![];
            let mut map = HashMap::new();
            for v in reads {
                if var_block[v.0] == k || matches!(vars[v.0].place, Place::Constant(_)) {
                    continue;
                }
                let copy = VariableId(vars.len());
                vars.push(Var {
                    place: Place::Unused,
                    ..vars[v.0]
                });
                var_block.push(k);
                imports.push((v, copy));
                map.insert(v, copy);
            }
            for step in &mut blocks[k].steps {
                rename(step, &map);
            }
            if output_block == k {
                output_bindings
                    .iter_mut()
                    .for_each(|b| rename_binding(b, &map));
                output_reads.iter_mut().for_each(|v| {
                    if let Some(n) = map.get(v) {
                        *v = *n;
                    }
                });
            }
            if let Step::Expand(e) = &mut blocks[parent].steps[at] {
                for r in &mut e.reduces {
                    for (v, _) in &mut r.any {
                        if let Some(n) = map.get(v) {
                            *v = *n;
                        }
                    }
                }
                e.imports = imports;
            }
        }

        // Input variables that some step reads.
        let mut read_var = vec![false; vars.len()];
        for block in &blocks {
            for step in &block.steps {
                access(step).0.iter().for_each(|v| read_var[v.0] = true);
            }
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
        if let Some(output) = &graph.output
            && output.domain == DomainId::ROOT
        {
            let mut defined = vec![false; vars.len()];
            for step in &blocks[0].steps {
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

        // Slots per block, free after the last read; blocks never share
        // slots, since an enclosing window stays live while its expansion
        // runs.
        let mut allocate = |vars: &mut Vec<Var>, v: VariableId, free: &mut Free| {
            if vars[v.0].place != Place::Unused {
                return false;
            }
            vars[v.0].place = if vars[v.0].ty == Ty::Bool {
                Place::Bits(free.bits.pop().unwrap_or_else(|| {
                    bit_slots += 1;
                    bit_slots - 1
                }))
            } else if vars[v.0].ty == Ty::I128 {
                // Two consecutive value slots.
                Place::Value(free.wide.pop().unwrap_or_else(|| {
                    value_slots += 2;
                    value_slots - 2
                }))
            } else {
                Place::Value(free.values.pop().unwrap_or_else(|| {
                    value_slots += 1;
                    value_slots - 1
                }))
            };
            true
        };
        let release = |vars: &Vec<Var>, v: VariableId, free: &mut Free| match vars[v.0].place {
            Place::Bits(slot) => free.bits.push(slot),
            Place::Value(slot) if vars[v.0].ty == Ty::I128 => free.wide.push(slot),
            Place::Value(slot) => free.values.push(slot),
            _ => {}
        };
        for k in 0..blocks.len() {
            let steps = &blocks[k].steps;
            let mut last = vec![None; vars.len()];
            for (s, step) in steps.iter().enumerate() {
                access(step).0.iter().for_each(|v| last[v.0] = Some(s));
                if let Step::Output { .. } = step {
                    output_reads.iter().for_each(|v| last[v.0] = Some(s));
                }
            }
            // The rows and imports of a child block exist from its start;
            // reductions read their Booleans after its last step.
            let mut initial = vec![];
            if k > 0 {
                let (parent, at) = expands[&k];
                if let Step::Expand(e) = &blocks[parent].steps[at] {
                    initial.push(e.row);
                    initial.extend(e.imports.iter().map(|(_, copy)| *copy));
                    for r in &e.reduces {
                        for (v, _) in &r.any {
                            last[v.0] = Some(steps.len());
                        }
                    }
                }
            }
            let mut free = Free::default();
            let mut releases: Vec<Vec<VariableId>> = vec![vec![]; steps.len() + 1];
            for v in initial {
                if allocate(&mut vars, v, &mut free) {
                    releases[last[v.0].unwrap_or(0)].push(v);
                }
            }
            for (s, step) in steps.iter().enumerate() {
                for v in access(step).1 {
                    if allocate(&mut vars, v, &mut free) {
                        releases[last[v.0].unwrap_or(s).max(s)].push(v);
                    }
                }
                for v in std::mem::take(&mut releases[s]) {
                    release(&vars, v, &mut free);
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
        for (v, var) in vars.iter().enumerate() {
            if matches!(
                var.place,
                Place::Bits(_)
                    | Place::Input(_)
                    | Place::Output(_)
                    | Place::InputBits { .. }
                    | Place::InputBytes { .. }
            ) {
                blocks[var_block[v]].windowed.push(VariableId(v));
            }
        }
        let mut prepared = Self {
            blocks,
            fallback: vec![],
            vars,
            pool: pool.into_boxed_slice(),
            value_slots,
            bit_slots,
            masks,
            read,
            output_bindings,
            _bytes: bytes,
            jit: None,
        };
        if compile {
            fuse::fuse(&mut prepared)?;
        }
        Ok(prepared)
    }

    /// Whether the only step is the output: the program computes nothing.
    pub fn is_output_only(&self) -> bool {
        self.blocks.len() == 1
            && self.blocks[0]
                .steps
                .iter()
                .all(|s| matches!(s, Step::Output { .. }))
    }

    /// The IR of the fused functions; empty without any.
    pub fn ir(&self) -> &str {
        self.jit.as_ref().map_or("", |j| j.ir())
    }

    /// Every step, including those of nested blocks and fallbacks.
    pub(crate) fn all_steps(&self) -> impl Iterator<Item = &Step> {
        self.blocks
            .iter()
            .flat_map(|b| b.steps.iter())
            .chain(&self.fallback)
    }
}

#[derive(Default)]
struct Free {
    values: Vec<usize>,
    wide: Vec<usize>,
    bits: Vec<usize>,
}
