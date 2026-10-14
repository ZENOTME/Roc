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

//! The precompiled kernel library: one function per function, input type and
//! operand shape. Arguments are the inputs, then one output per result in
//! `lower` order.
use super::ir::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Func(Func),
    /// `[a, d, magic, shift, more]` to `[a / d]`, see `ir::magic`.
    DivMagic,
    RemMagic,
    /// `[values, mask]`, or `[values]` with `scalars` 1 to sum every row:
    /// returns the wrapping sum.
    Sum,
}

/// A precompiled kernel. Bit `i` of `scalars` is set when value operand `i`
/// is one broadcast value instead of a column; Boolean operands are bitmaps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    pub kind: Kind,
    pub ty: Ty,
    pub scalars: u8,
}

impl Key {
    pub fn name(&self) -> String {
        let shape = |n: usize| -> String {
            (0..n)
                .map(|i| if self.scalars >> i & 1 != 0 { 's' } else { 'c' })
                .collect()
        };
        let t = self.ty.name();
        match self.kind {
            Kind::Func(f) => match f {
                Func::Add => format!("add_{t}_{}", shape(2)),
                Func::Sub => format!("sub_{t}_{}", shape(2)),
                Func::Mul => format!("mul_{t}_{}", shape(2)),
                Func::Div => format!("div_{t}_{}", shape(2)),
                Func::Rem => format!("rem_{t}_{}", shape(2)),
                Func::Neg => format!("neg_{t}"),
                Func::Compare(p) => format!("cmp_{}_{t}_{}", p.name(), shape(2)),
                Func::Cast(to) => format!("cast_{t}_{}", to.name()),
                Func::Select => format!("select_{t}_{}", shape(2)),
                Func::DivRound => format!("divround_{t}_{}", shape(2)),
                Func::Abs => format!("abs_{t}"),
                Func::Round => format!("round_{t}"),
                Func::BitAnd => format!("bitand_{t}_{}", shape(2)),
                Func::BitOr => format!("bitor_{t}_{}", shape(2)),
                Func::ShiftRight => format!("shr_{t}_{}", shape(2)),
            },
            Kind::DivMagic => format!("divmagic_{t}"),
            Kind::RemMagic => format!("remmagic_{t}"),
            Kind::Sum if self.scalars != 0 => format!("sum_{t}_all"),
            Kind::Sum => format!("sum_{t}"),
        }
    }

    pub fn group(&self) -> Group {
        let mut g = Group::default();
        let t = self.ty;
        let value = |g: &mut Group, i: usize| {
            if t == Ty::Bool {
                g.arg(Arg::Bitmap)
            } else if self.scalars >> i & 1 != 0 {
                g.arg(Arg::Scalar(t))
            } else {
                g.arg(Arg::Column(t))
            }
        };
        let results = match self.kind {
            Kind::Func(f) => {
                let args = match f {
                    Func::Neg | Func::Cast(_) | Func::Abs | Func::Round => {
                        vec![value(&mut g, 0)]
                    }
                    Func::Select => {
                        let cond = g.arg(Arg::Bitmap);
                        vec![cond, value(&mut g, 0), value(&mut g, 1)]
                    }
                    _ => vec![value(&mut g, 0), value(&mut g, 1)],
                };
                lower(&mut g, f, &args)
            }
            Kind::DivMagic | Kind::RemMagic => {
                let mut args = vec![g.arg(Arg::Column(t)), g.arg(Arg::Scalar(t))];
                args.extend((0..3).map(|_| g.arg(Arg::Scalar(Ty::U64))));
                let op = if self.kind == Kind::DivMagic {
                    Op::DivMagic
                } else {
                    Op::RemMagic
                };
                vec![g.node(op, args)]
            }
            Kind::Sum => {
                let values = g.arg(Arg::Column(t));
                let mask = if self.scalars != 0 {
                    Src::Imm(Ty::Bool, 1)
                } else {
                    g.arg(Arg::Bitmap)
                };
                g.sum = Some((values, mask));
                vec![]
            }
        };
        // Error flags follow the value and are rarely set.
        for (i, r) in results.into_iter().enumerate() {
            g.store(r);
            if g.ty(r) == Ty::Bool {
                g.reports.push(r);
                if i > 0 {
                    g.lazy.push(r);
                }
            }
        }
        g
    }
}

/// Every precompiled kernel.
pub fn all() -> Vec<Key> {
    let mut keys = vec![];
    let values = Ty::ALL.into_iter().filter(|t| *t != Ty::Bool);
    let binary = [0u8, 1, 2];
    let key = |kind, ty, scalars| Key { kind, ty, scalars };
    for t in values.clone() {
        for f in [Func::Add, Func::Sub, Func::Mul, Func::Div, Func::Rem] {
            keys.extend(binary.map(|s| key(Kind::Func(f), t, s)));
        }
        if t.is_signed() || t.is_float() {
            keys.push(key(Kind::Func(Func::Neg), t, 0));
        }
        for p in Pred::ALL {
            keys.extend(binary.map(|s| key(Kind::Func(Func::Compare(p)), t, s)));
        }
        keys.extend([0, 1, 2, 3].map(|s| key(Kind::Func(Func::Select), t, s)));
        keys.push(key(Kind::Func(Func::Abs), t, 0));
        if t.is_float() {
            keys.push(key(Kind::Func(Func::Round), t, 0));
        } else {
            for f in [Func::DivRound, Func::BitAnd, Func::BitOr, Func::ShiftRight] {
                keys.extend(binary.map(|s| key(Kind::Func(f), t, s)));
            }
        }
        if t.is_integer() && t.bits() <= 64 {
            keys.push(key(Kind::DivMagic, t, 0));
            keys.push(key(Kind::RemMagic, t, 0));
        }
    }
    for p in Pred::ALL {
        keys.push(key(Kind::Func(Func::Compare(p)), Ty::Bool, 0));
    }
    // Casts to the same lane type pass values through, such as UInt64 to Usize.
    for from in Ty::ALL {
        for to in Ty::ALL {
            keys.push(key(Kind::Func(Func::Cast(to)), from, 0));
        }
    }
    for t in [Ty::I64, Ty::U64] {
        keys.extend([0, 1].map(|s| key(Kind::Sum, t, s)));
    }
    keys
}
