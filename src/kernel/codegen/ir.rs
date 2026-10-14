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

//! Window functions as lane-generic node graphs.
//!
//! A `Group` computes its nodes for every row of a window and writes some of
//! them to output arguments. Values have their native width; Booleans are
//! bitmaps with bit `r % 64` of word `r / 64` for window row `r`.

/// Lane types. Byte pointers and lengths are U64.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Ty {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}

impl Ty {
    pub const ALL: [Ty; 11] = [
        Ty::Bool,
        Ty::I8,
        Ty::I16,
        Ty::I32,
        Ty::I64,
        Ty::U8,
        Ty::U16,
        Ty::U32,
        Ty::U64,
        Ty::F32,
        Ty::F64,
    ];

    /// Bits of a lane; a Boolean lane is one bit.
    pub fn bits(self) -> u32 {
        match self {
            Ty::Bool => 1,
            Ty::I8 | Ty::U8 => 8,
            Ty::I16 | Ty::U16 => 16,
            Ty::I32 | Ty::U32 | Ty::F32 => 32,
            _ => 64,
        }
    }
    /// Bytes of a value in a column; Booleans have no column.
    pub fn bytes(self) -> usize {
        (self.bits() as usize).div_ceil(8)
    }
    pub fn is_signed(self) -> bool {
        matches!(self, Ty::I8 | Ty::I16 | Ty::I32 | Ty::I64)
    }
    pub fn is_float(self) -> bool {
        matches!(self, Ty::F32 | Ty::F64)
    }
    pub fn is_integer(self) -> bool {
        !self.is_float() && self != Ty::Bool
    }
    pub fn name(self) -> &'static str {
        match self {
            Ty::Bool => "bool",
            Ty::I8 => "i8",
            Ty::I16 => "i16",
            Ty::I32 => "i32",
            Ty::I64 => "i64",
            Ty::U8 => "u8",
            Ty::U16 => "u16",
            Ty::U32 => "u32",
            Ty::U64 => "u64",
            Ty::F32 => "f32",
            Ty::F64 => "f64",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pred {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Pred {
    pub const ALL: [Pred; 6] = [Pred::Eq, Pred::Ne, Pred::Lt, Pred::Le, Pred::Gt, Pred::Ge];
    pub fn name(self) -> &'static str {
        match self {
            Pred::Eq => "eq",
            Pred::Ne => "ne",
            Pred::Lt => "lt",
            Pred::Le => "le",
            Pred::Gt => "gt",
            Pred::Ge => "ge",
        }
    }
}

/// Node operations. Each produces one lane value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// `[a, b]`; integers wrap.
    Add,
    Sub,
    Mul,
    /// Integer `[a, b, a op b]` to Bool.
    AddOverflow,
    SubOverflow,
    MulOverflow,
    /// `[a]`; integers wrap.
    Neg,
    /// Signed `[a]` to Bool.
    NegOverflow,
    /// `[a, b]`. An integer divisor that is zero, or -1 with a signed MIN
    /// dividend, is replaced by one.
    Div,
    Rem,
    /// Integer `[b]` to Bool.
    DivZero,
    /// Integer `[a, b]` to Bool: signed MIN / -1.
    DivOverflow,
    /// Integer `[a, d, magic, shift, more]` with the constants of `magic`:
    /// `a / d` (or `a % d`) for a divisor `d >= 2`.
    DivMagic,
    RemMagic,
    /// `[a, b]` to Bool; floats compare in their total order.
    Compare(Pred),
    And,
    Or,
    Not,
    /// `[cond, a, b]`.
    Select,
    /// `[a]` to the target type. Out-of-range and NaN inputs give an
    /// unspecified value and set the CastInvalid flag.
    Cast(Ty),
    CastInvalid(Ty),
}

/// An operand of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Src {
    /// An input argument.
    Arg(usize),
    Node(usize),
    /// A constant lane value: the bits of the value, zero-extended.
    Imm(Ty, u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub op: Op,
    pub args: Vec<Src>,
}

/// Arguments are pointers at the first row of the window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Arg {
    /// Values of one type, one per row.
    Column(Ty),
    /// One value, the same on every row.
    Scalar(Ty),
    /// A Boolean bitmap.
    Bitmap,
    /// Written: a column of values.
    Output(Ty),
    /// Written: a bitmap, whole words.
    OutputBitmap,
}

/// `extern "C" fn(args: *const *mut u8, rows: u64) -> u64`.
///
/// The result has bit `2j` set when `reports[j]` is true on some row and bit
/// `2j + 1` when it is true on every row; bit 63 when a raise condition is
/// true on some row. With `sum` set, the result is instead the wrapping sum.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Group {
    pub args: Vec<Arg>,
    pub nodes: Vec<Node>,
    /// A value or Boolean written to an output argument.
    pub stores: Vec<(Src, usize)>,
    /// Boolean conditions, each with an optional Boolean mask.
    pub raises: Vec<(Src, Option<Src>)>,
    pub reports: Vec<Src>,
    /// Reported Booleans whose stores may be skipped when they are false on
    /// every row: rare error flags, computed exactly only when one is set.
    pub lazy: Vec<Src>,
    /// An integer value summed over the rows of a Boolean mask.
    pub sum: Option<(Src, Src)>,
}

pub const MAX_REPORTS: usize = 31;
pub const RAISED: u64 = 1 << 63;

impl Group {
    pub fn ty(&self, src: Src) -> Ty {
        match src {
            Src::Imm(t, _) => t,
            Src::Arg(i) => match self.args[i] {
                Arg::Column(t) | Arg::Scalar(t) | Arg::Output(t) => t,
                Arg::Bitmap | Arg::OutputBitmap => Ty::Bool,
            },
            Src::Node(n) => {
                let node = &self.nodes[n];
                match node.op {
                    Op::AddOverflow
                    | Op::SubOverflow
                    | Op::MulOverflow
                    | Op::NegOverflow
                    | Op::DivZero
                    | Op::DivOverflow
                    | Op::Compare(_)
                    | Op::And
                    | Op::Or
                    | Op::Not
                    | Op::CastInvalid(_) => Ty::Bool,
                    Op::Cast(t) => t,
                    Op::Select => self.ty(node.args[1]),
                    _ => self.ty(node.args[0]),
                }
            }
        }
    }

    pub fn node(&mut self, op: Op, args: Vec<Src>) -> Src {
        self.nodes.push(Node { op, args });
        Src::Node(self.nodes.len() - 1)
    }

    pub fn arg(&mut self, arg: Arg) -> Src {
        self.args.push(arg);
        Src::Arg(self.args.len() - 1)
    }

    /// Stores `src` into a new output argument.
    pub fn store(&mut self, src: Src) {
        let ty = self.ty(src);
        let out = if ty == Ty::Bool {
            Arg::OutputBitmap
        } else {
            Arg::Output(ty)
        };
        self.args.push(out);
        self.stores.push((src, self.args.len() - 1));
    }
}

/// Row functions of the kernel model, as far as they lower to nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Func {
    Add,
    Sub,
    Mul,
    Neg,
    Div,
    Rem,
    Compare(Pred),
    Cast(Ty),
    Select,
}

/// The nodes of one function, one per output in the kernel model's output
/// order. The single definition of each function's semantics: precompiled
/// kernels and fused groups both lower through it.
pub fn lower(g: &mut Group, f: Func, args: &[Src]) -> Vec<Src> {
    let t = g.ty(args[0]);
    let no = Src::Imm(Ty::Bool, 0);
    match f {
        Func::Add | Func::Sub | Func::Mul => {
            let (op, overflow) = match f {
                Func::Add => (Op::Add, Op::AddOverflow),
                Func::Sub => (Op::Sub, Op::SubOverflow),
                _ => (Op::Mul, Op::MulOverflow),
            };
            let v = g.node(op, args.to_vec());
            if t.is_float() {
                vec![v]
            } else {
                vec![v, g.node(overflow, vec![args[0], args[1], v])]
            }
        }
        Func::Neg => {
            let v = g.node(Op::Neg, args.to_vec());
            if t.is_float() {
                vec![v]
            } else {
                vec![v, g.node(Op::NegOverflow, args.to_vec())]
            }
        }
        Func::Div | Func::Rem if t.is_float() => {
            let op = if f == Func::Div { Op::Div } else { Op::Rem };
            vec![g.node(op, args.to_vec())]
        }
        Func::Div | Func::Rem => {
            if let Src::Imm(_, bits) = args[1]
                && let Some(m) = magic(t, bits)
            {
                let mut operands = vec![args[0], Src::Imm(t, bits)];
                operands.extend(m.map(|c| Src::Imm(Ty::U64, c)));
                let op = if f == Func::Div {
                    Op::DivMagic
                } else {
                    Op::RemMagic
                };
                let v = g.node(op, operands);
                return if f == Func::Div {
                    vec![v, no, no]
                } else {
                    vec![v, no]
                };
            }
            let op = if f == Func::Div { Op::Div } else { Op::Rem };
            let v = g.node(op, args.to_vec());
            let zero = g.node(Op::DivZero, vec![args[1]]);
            if f == Func::Rem {
                return vec![v, zero];
            }
            let overflow = if t.is_signed() {
                g.node(Op::DivOverflow, args.to_vec())
            } else {
                no
            };
            vec![v, zero, overflow]
        }
        Func::Compare(p) => vec![g.node(Op::Compare(p), args.to_vec())],
        Func::Cast(to) => vec![
            g.node(Op::Cast(to), args.to_vec()),
            g.node(Op::CastInvalid(to), args.to_vec()),
        ],
        Func::Select => vec![g.node(Op::Select, args.to_vec())],
    }
}

/// `[magic, shift, more]` for dividing an integer type by a constant `d >= 2`
/// (given as its zero-extended bits), computed for 64-bit operands.
pub fn magic(t: Ty, bits: u64) -> Option<[u64; 3]> {
    if !t.is_integer() {
        return None;
    }
    let shift = 64 - t.bits();
    if t.is_signed() {
        let d = ((bits << shift) as i64) >> shift;
        (d >= 2).then(|| magic_signed(d))
    } else {
        let d = (bits << shift) >> shift;
        (d >= 2).then(|| magic_unsigned(d))
    }
}

/// Hacker's Delight 10-1 for `d >= 2`: `q = mulhs(m, n) + (more & n) >> s`,
/// plus one when `n < 0`.
fn magic_signed(d: i64) -> [u64; 3] {
    let two63 = 1u64 << 63;
    let ad = d as u64;
    let t = two63;
    let anc = t - 1 - t % ad;
    let mut p = 63u32;
    let (mut q1, mut r1) = (two63 / anc, two63 - (two63 / anc) * anc);
    let (mut q2, mut r2) = (two63 / ad, two63 - (two63 / ad) * ad);
    loop {
        p += 1;
        q1 = q1.wrapping_mul(2);
        r1 = r1.wrapping_mul(2);
        if r1 >= anc {
            q1 = q1.wrapping_add(1);
            r1 = r1.wrapping_sub(anc);
        }
        q2 = q2.wrapping_mul(2);
        r2 = r2.wrapping_mul(2);
        if r2 >= ad {
            q2 = q2.wrapping_add(1);
            r2 = r2.wrapping_sub(ad);
        }
        let delta = ad - r2;
        if !(q1 < delta || (q1 == delta && r1 == 0)) {
            break;
        }
    }
    let m = q2.wrapping_add(1);
    let more = if (m as i64) < 0 { u64::MAX } else { 0 };
    [m, (p - 64) as u64, more]
}

/// Hacker's Delight 10-2 for `d >= 2`: `t = mulhu(m, n)`, then `t >> s`, or
/// `((n - t) / 2 + t) >> (s - 1)` when `more`. The shift is `s` or `s - 1`.
fn magic_unsigned(d: u64) -> [u64; 3] {
    let two63 = 1u64 << 63;
    let mut add = false;
    let nc = u64::MAX - (0u64.wrapping_sub(d)) % d;
    let mut p = 63u32;
    let (mut q1, mut r1) = (two63 / nc, two63 - (two63 / nc) * nc);
    let (mut q2, mut r2) = ((two63 - 1) / d, (two63 - 1) - ((two63 - 1) / d) * d);
    loop {
        p += 1;
        if r1 >= nc - r1 {
            q1 = q1.wrapping_mul(2).wrapping_add(1);
            r1 = r1.wrapping_mul(2).wrapping_sub(nc);
        } else {
            q1 = q1.wrapping_mul(2);
            r1 = r1.wrapping_mul(2);
        }
        if r2 + 1 >= d - r2 {
            if q2 >= two63 - 1 {
                add = true;
            }
            q2 = q2.wrapping_mul(2).wrapping_add(1);
            r2 = r2.wrapping_mul(2).wrapping_add(1).wrapping_sub(d);
        } else {
            if q2 >= two63 {
                add = true;
            }
            q2 = q2.wrapping_mul(2);
            r2 = r2.wrapping_mul(2).wrapping_add(1);
        }
        let delta = d - 1 - r2;
        if !(p < 128 && (q1 < delta || (q1 == delta && r1 == 0))) {
            break;
        }
    }
    let s = (p - 64) as u64;
    if add {
        [q2.wrapping_add(1), s - 1, u64::MAX]
    } else {
        [q2.wrapping_add(1), s, 0]
    }
}

/// The reference arithmetic of `DivMagic` on 64-bit operands.
#[cfg(test)]
pub fn divide_magic(signed: bool, n: u64, [m, s, more]: [u64; 3]) -> u64 {
    if signed {
        let n = n as i64;
        let q = ((m as i64 as i128 * n as i128) >> 64) as i64;
        let q = q.wrapping_add(n & more as i64) >> s;
        q.wrapping_add((n as u64 >> 63) as i64) as u64
    } else {
        let t = ((m as u128 * n as u128) >> 64) as u64;
        if more != 0 {
            (((n - t) >> 1) + t) >> s
        } else {
            t >> s
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_division_divides_exactly() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut divisors = (2..300).collect::<Vec<u64>>();
        for k in 1..64 {
            divisors.extend([1u64 << k, (1u64 << k) + 1, (1u64 << k) - 1]);
        }
        for _ in 0..300 {
            divisors.extend([next(), next() >> (next() % 64)]);
        }
        for &d in &divisors {
            let mut dividends = vec![
                0,
                1,
                d.wrapping_sub(1),
                d,
                d.wrapping_add(1),
                u64::MAX,
                1 << 63,
            ];
            for _ in 0..100 {
                dividends.extend([next(), next() >> (next() % 64)]);
            }
            if let Some(m) = magic(Ty::U64, d) {
                for &n in &dividends {
                    assert_eq!(divide_magic(false, n, m), n / d, "{n} / {d}");
                }
            }
            if let Some(m) = magic(Ty::I64, d) {
                for &n in &dividends {
                    let (n, d) = (n as i64, d as i64);
                    assert_eq!(divide_magic(true, n as u64, m) as i64, n / d, "{n} / {d}");
                }
            }
        }
        // Narrow types divide their sign- or zero-extended values.
        for d in 2..=127i64 {
            let m = magic(Ty::I8, d as u64).unwrap();
            for n in -128..=127i64 {
                assert_eq!(divide_magic(true, n as u64, m) as i64, n / d);
            }
        }
        for d in 2..=255u64 {
            let m = magic(Ty::U8, d).unwrap();
            for n in 0..=255u64 {
                assert_eq!(divide_magic(false, n, m), n / d);
            }
        }
        assert!(magic(Ty::I8, 0xff).is_none());
        assert!(magic(Ty::U64, 1).is_none());
    }
}
