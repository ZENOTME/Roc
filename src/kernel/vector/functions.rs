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

//! Precompiled row functions, monomorphized per scalar function and value
//! type. Results match the generated instructions of `jit::emit` and
//! `jit::cast` bit for bit.
use super::super::runtime::{self, TextNumber};
use super::super::{ScalarFunction, VariableType};
use super::Rows;
use crate::error::{Error, Result};
use std::cmp::Ordering;
use std::marker::PhantomData;

#[derive(Clone, Copy)]
pub(super) enum Operand<'a> {
    Constant(u64),
    Column(&'a [u64]),
}

impl<'a> Operand<'a> {
    #[inline(always)]
    pub fn get(self, row: usize) -> u64 {
        match self {
            Self::Constant(c) => c,
            Self::Column(v) => v[row],
        }
    }
    /// A branch-free reader for per-row loops; `constant` holds the word of a
    /// constant operand.
    #[inline(always)]
    pub fn view(self, constant: &'a u64) -> View<'a> {
        match self {
            Self::Constant(_) => View {
                words: std::slice::from_ref(constant),
                mask: 0,
            },
            Self::Column(v) => View {
                words: v,
                mask: usize::MAX,
            },
        }
    }
    #[inline(always)]
    pub fn constant(self) -> u64 {
        match self {
            Self::Constant(c) => c,
            Self::Column(_) => 0,
        }
    }
}

/// Reads row r at `r & mask`, so a constant (mask 0) needs no branch.
#[derive(Clone, Copy)]
pub(super) struct View<'a> {
    words: &'a [u64],
    mask: usize,
}

impl View<'_> {
    #[inline(always)]
    pub fn at(self, row: usize) -> u64 {
        self.words[row & self.mask]
    }
}

/// Writes every output at every row of `Rows`; other rows are left as is.
pub(super) type CallFn = fn(&[Operand<'_>], Rows<'_>, &mut [Vec<u64>]);
pub(super) type FormatFn = fn(u64) -> String;

pub(super) enum Resolved {
    Call(CallFn),
    /// FormatText allocates in the invocation's string arena.
    Format(FormatFn),
}

pub(super) trait RowFn<const N: usize, const M: usize> {
    fn row(x: [u64; N]) -> [u64; M];
}

pub(super) fn call<const N: usize, const M: usize, R: RowFn<N, M>>(
    inputs: &[Operand<'_>],
    rows: Rows<'_>,
    outputs: &mut [Vec<u64>],
) {
    let outputs: &mut [Vec<u64>; M] = outputs.try_into().expect("kernel output count");
    match *inputs {
        [a] if N == 1 => run1(rows, a, outputs, |x| R::row([x; N])),
        [a, b] if N == 2 => run2(rows, a, b, outputs, |x, y| {
            R::row(std::array::from_fn(|i| if i == 0 { x } else { y }))
        }),
        _ => {
            let inputs: [Operand<'_>; N] = inputs.try_into().expect("kernel input count");
            rows.for_each(|r| {
                let y = R::row(std::array::from_fn(|i| inputs[i].get(r)));
                for j in 0..M {
                    outputs[j][r] = y[j];
                }
            });
        }
    }
}

#[inline(always)]
fn run1<const M: usize>(
    rows: Rows<'_>,
    a: Operand<'_>,
    outputs: &mut [Vec<u64>; M],
    f: impl Fn(u64) -> [u64; M],
) {
    match (rows, a) {
        (Rows::Range(start, end), Operand::Column(a)) => {
            let a = &a[start..end];
            let o = outputs.each_mut().map(|o| &mut o[start..end]);
            for (r, &x) in a.iter().enumerate() {
                let y = f(x);
                for j in 0..M {
                    // SAFETY: every output slice has n words.
                    unsafe { *o[j].get_unchecked_mut(r) = y[j] };
                }
            }
        }
        (rows, a) => rows.for_each(|r| {
            let y = f(a.get(r));
            for j in 0..M {
                outputs[j][r] = y[j];
            }
        }),
    }
}

#[inline(always)]
fn run2<const M: usize>(
    rows: Rows<'_>,
    a: Operand<'_>,
    b: Operand<'_>,
    outputs: &mut [Vec<u64>; M],
    f: impl Fn(u64, u64) -> [u64; M],
) {
    macro_rules! dense {
        ($start:expr, $end:expr, $x:expr, $y:expr) => {{
            let o = outputs.each_mut().map(|o| &mut o[$start..$end]);
            for r in 0..$end - $start {
                let y = f($x(r), $y(r));
                for j in 0..M {
                    // SAFETY: every output slice has `end - start` words.
                    unsafe { *o[j].get_unchecked_mut(r) = y[j] };
                }
            }
        }};
    }
    match (rows, a, b) {
        (Rows::Range(s, e), Operand::Column(a), Operand::Column(b)) => {
            let (a, b) = (&a[s..e], &b[s..e]);
            dense!(s, e, |r| a[r], |r| b[r])
        }
        (Rows::Range(s, e), Operand::Column(a), Operand::Constant(b)) => {
            let a = &a[s..e];
            dense!(s, e, |r| a[r], |_| b)
        }
        (Rows::Range(s, e), Operand::Constant(a), Operand::Column(b)) => {
            let b = &b[s..e];
            dense!(s, e, |_| a, |r| b[r])
        }
        (rows, a, b) => rows.for_each(|r| {
            let y = f(a.get(r), b.get(r));
            for j in 0..M {
                outputs[j][r] = y[j];
            }
        }),
    }
}

/// A value type and its canonical 64-bit word.
pub(super) trait Word: Copy {
    fn get(bits: u64) -> Self;
    fn put(self) -> u64;
}

macro_rules! words {
    ($($t:ty: $wide:ty),*) => {$(
        impl Word for $t {
            #[inline(always)]
            fn get(bits: u64) -> Self {
                bits as $t
            }
            #[inline(always)]
            fn put(self) -> u64 {
                self as $wide as u64
            }
        }
    )*};
}
words!(i8: i64, i16: i64, i32: i64, i64: i64, u8: u64, u16: u64, u32: u64, u64: u64);

impl Word for f32 {
    #[inline(always)]
    fn get(bits: u64) -> Self {
        f32::from_bits(bits as u32)
    }
    #[inline(always)]
    fn put(self) -> u64 {
        self.to_bits() as u64
    }
}
impl Word for f64 {
    #[inline(always)]
    fn get(bits: u64) -> Self {
        f64::from_bits(bits)
    }
    #[inline(always)]
    fn put(self) -> u64 {
        self.to_bits()
    }
}
impl Word for bool {
    #[inline(always)]
    fn get(bits: u64) -> Self {
        bits & 1 != 0
    }
    #[inline(always)]
    fn put(self) -> u64 {
        self as u64
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Class {
    Bool,
    Signed,
    Unsigned,
    Float,
}

/// Arithmetic, ordering and conversions of one value type.
pub(super) trait Value: Word {
    const CLASS: Class;
    const BITS: u32;
    const LO: i128;
    const HI: i128;
    fn add(self, b: Self) -> (Self, bool);
    fn sub(self, b: Self) -> (Self, bool);
    fn mul(self, b: Self) -> (Self, bool);
    fn div(self, b: Self) -> Self;
    fn rem(self, b: Self) -> Self;
    fn is_zero(self) -> bool;
    /// Signed MIN / -1.
    fn div_overflow(self, b: Self) -> bool;
    fn one() -> Self;
    fn order(self, b: Self) -> Ordering;
    fn integer(self) -> i128;
    fn float(self) -> f64;
    fn from_integer(v: i128) -> Self;
    fn from_f64(v: f64) -> Self;
    fn from_i64(v: i64) -> Self;
    fn from_u64(v: u64) -> Self;
}

macro_rules! integers {
    ($($t:ty: $class:ident),*) => {$(
        impl Value for $t {
            const CLASS: Class = Class::$class;
            const BITS: u32 = <$t>::BITS;
            const LO: i128 = <$t>::MIN as i128;
            const HI: i128 = <$t>::MAX as i128;
            // Overflow from the sign bits rather than overflowing_add, which
            // LLVM does not vectorize.
            #[inline(always)]
            fn add(self, b: Self) -> (Self, bool) {
                let v = self.wrapping_add(b);
                let overflow = if <$t>::MIN == 0 {
                    v < self
                } else {
                    (self ^ v) & (b ^ v) < 0 as $t
                };
                (v, overflow)
            }
            #[inline(always)]
            fn sub(self, b: Self) -> (Self, bool) {
                let v = self.wrapping_sub(b);
                let overflow = if <$t>::MIN == 0 {
                    self < b
                } else {
                    (self ^ b) & (self ^ v) < 0 as $t
                };
                (v, overflow)
            }
            #[inline(always)]
            fn mul(self, b: Self) -> (Self, bool) {
                self.overflowing_mul(b)
            }
            #[inline(always)]
            fn div(self, b: Self) -> Self {
                self.wrapping_div(b)
            }
            #[inline(always)]
            fn rem(self, b: Self) -> Self {
                self.wrapping_rem(b)
            }
            #[inline(always)]
            fn is_zero(self) -> bool {
                self == 0
            }
            #[inline(always)]
            fn div_overflow(self, b: Self) -> bool {
                Self::CLASS == Class::Signed && self == <$t>::MIN && b == (0 as $t).wrapping_sub(1)
            }
            #[inline(always)]
            fn one() -> Self {
                1
            }
            #[inline(always)]
            fn order(self, b: Self) -> Ordering {
                self.cmp(&b)
            }
            #[inline(always)]
            fn integer(self) -> i128 {
                self as i128
            }
            #[inline(always)]
            fn float(self) -> f64 {
                self as f64
            }
            #[inline(always)]
            fn from_integer(v: i128) -> Self {
                v as $t
            }
            #[inline(always)]
            fn from_f64(v: f64) -> Self {
                v as $t
            }
            #[inline(always)]
            fn from_i64(v: i64) -> Self {
                v as $t
            }
            #[inline(always)]
            fn from_u64(v: u64) -> Self {
                v as $t
            }
        }
    )*};
}
integers!(
    i8: Signed, i16: Signed, i32: Signed, i64: Signed,
    u8: Unsigned, u16: Unsigned, u32: Unsigned, u64: Unsigned
);

macro_rules! floats {
    ($($t:ty),*) => {$(
        impl Value for $t {
            const CLASS: Class = Class::Float;
            const BITS: u32 = <$t>::MANTISSA_DIGITS;
            const LO: i128 = 0;
            const HI: i128 = 0;
            #[inline(always)]
            fn add(self, b: Self) -> (Self, bool) {
                (self + b, false)
            }
            #[inline(always)]
            fn sub(self, b: Self) -> (Self, bool) {
                (self - b, false)
            }
            #[inline(always)]
            fn mul(self, b: Self) -> (Self, bool) {
                (self * b, false)
            }
            #[inline(always)]
            fn div(self, b: Self) -> Self {
                self / b
            }
            #[inline(always)]
            fn rem(self, b: Self) -> Self {
                self % b
            }
            #[inline(always)]
            fn is_zero(self) -> bool {
                self == 0.0
            }
            #[inline(always)]
            fn div_overflow(self, _: Self) -> bool {
                false
            }
            #[inline(always)]
            fn one() -> Self {
                1.0
            }
            #[inline(always)]
            fn order(self, b: Self) -> Ordering {
                self.total_cmp(&b)
            }
            #[inline(always)]
            fn integer(self) -> i128 {
                0
            }
            #[inline(always)]
            fn float(self) -> f64 {
                self as f64
            }
            #[inline(always)]
            fn from_integer(v: i128) -> Self {
                v as $t
            }
            #[inline(always)]
            fn from_f64(v: f64) -> Self {
                v as $t
            }
            #[inline(always)]
            fn from_i64(v: i64) -> Self {
                v as $t
            }
            #[inline(always)]
            fn from_u64(v: u64) -> Self {
                v as $t
            }
        }
    )*};
}
floats!(f32, f64);

impl Value for bool {
    const CLASS: Class = Class::Bool;
    const BITS: u32 = 8;
    const LO: i128 = 0;
    const HI: i128 = 1;
    fn add(self, b: Self) -> (Self, bool) {
        (self ^ b, false)
    }
    fn sub(self, b: Self) -> (Self, bool) {
        (self ^ b, false)
    }
    fn mul(self, b: Self) -> (Self, bool) {
        (self & b, false)
    }
    fn div(self, _: Self) -> Self {
        self
    }
    fn rem(self, _: Self) -> Self {
        false
    }
    fn is_zero(self) -> bool {
        !self
    }
    fn div_overflow(self, _: Self) -> bool {
        false
    }
    fn one() -> Self {
        true
    }
    #[inline(always)]
    fn order(self, b: Self) -> Ordering {
        self.cmp(&b)
    }
    #[inline(always)]
    fn integer(self) -> i128 {
        self as i128
    }
    #[inline(always)]
    fn float(self) -> f64 {
        self as u8 as f64
    }
    fn from_integer(v: i128) -> Self {
        v != 0
    }
    fn from_f64(v: f64) -> Self {
        v != 0.0
    }
    fn from_i64(v: i64) -> Self {
        v != 0
    }
    fn from_u64(v: u64) -> Self {
        v != 0
    }
}

const ADD: u8 = 0;
const SUB: u8 = 1;
const MUL: u8 = 2;
const DIV: u8 = 3;
const REM: u8 = 4;

/// Integers: `[value, overflow]`.
pub(super) struct IntArith<T, const OP: u8>(PhantomData<T>);
impl<T: Value, const OP: u8> RowFn<2, 2> for IntArith<T, OP> {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 2] {
        let (a, b) = (T::get(a), T::get(b));
        let (v, overflow) = match OP {
            ADD => a.add(b),
            SUB => a.sub(b),
            _ => a.mul(b),
        };
        [v.put(), overflow as u64]
    }
}

/// Floats: `[value]`.
pub(super) struct FloatArith<T, const OP: u8>(PhantomData<T>);
impl<T: Value, const OP: u8> RowFn<2, 1> for FloatArith<T, OP> {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 1] {
        let (a, b) = (T::get(a), T::get(b));
        [match OP {
            ADD => a.add(b).0,
            SUB => a.sub(b).0,
            MUL => a.mul(b).0,
            DIV => a.div(b),
            _ => a.rem(b),
        }
        .put()]
    }
}

pub(super) struct IntNegate<T>(PhantomData<T>);
impl<T: Value> RowFn<1, 2> for IntNegate<T> {
    #[inline(always)]
    fn row([a]: [u64; 1]) -> [u64; 2] {
        let (v, overflow) = T::from_integer(0).sub(T::get(a));
        [v.put(), overflow as u64]
    }
}

pub(super) struct FloatNegate<T>(PhantomData<T>);
impl<T: Value + std::ops::Neg<Output = T>> RowFn<1, 1> for FloatNegate<T> {
    #[inline(always)]
    fn row([a]: [u64; 1]) -> [u64; 1] {
        [(-T::get(a)).put()]
    }
}

/// `[value, divide_by_zero, overflow]`; a zero or overflowing divisor is
/// replaced by one.
pub(super) struct IntDivide<T>(PhantomData<T>);
impl<T: Value> RowFn<2, 3> for IntDivide<T> {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 3] {
        let (a, b) = (T::get(a), T::get(b));
        let zero = b.is_zero();
        let overflow = a.div_overflow(b);
        let divisor = if zero || overflow { T::one() } else { b };
        [a.div(divisor).put(), zero as u64, overflow as u64]
    }
}

/// `[value, divide_by_zero]`; MIN % -1 is 0.
pub(super) struct IntRemainder<T>(PhantomData<T>);
impl<T: Value> RowFn<2, 2> for IntRemainder<T> {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 2] {
        let (a, b) = (T::get(a), T::get(b));
        let zero = b.is_zero();
        let divisor = if zero || a.div_overflow(b) {
            T::one()
        } else {
            b
        };
        [a.rem(divisor).put(), zero as u64]
    }
}

const EQ: u8 = 0;
const NE: u8 = 1;
const LT: u8 = 2;
const LE: u8 = 3;
const GT: u8 = 4;
const GE: u8 = 5;

#[inline(always)]
fn test<const OP: u8>(o: Ordering) -> u64 {
    (match OP {
        EQ => o.is_eq(),
        NE => o.is_ne(),
        LT => o.is_lt(),
        LE => o.is_le(),
        GT => o.is_gt(),
        _ => o.is_ge(),
    }) as u64
}

/// Floats use a total order.
pub(super) struct Compare<T, const OP: u8>(PhantomData<T>);
impl<T: Value, const OP: u8> RowFn<2, 1> for Compare<T, OP> {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 1] {
        [test::<OP>(T::get(a).order(T::get(b)))]
    }
}

pub(super) struct CompareBytes<const OP: u8>;
impl<const OP: u8> RowFn<4, 1> for CompareBytes<OP> {
    #[inline(always)]
    fn row([a, an, b, bn]: [u64; 4]) -> [u64; 1] {
        [test::<OP>(runtime::compare_bytes(a, an, b, bn).cmp(&0))]
    }
}

pub(super) struct BoolAnd;
impl RowFn<2, 1> for BoolAnd {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 1] {
        [a & b]
    }
}
pub(super) struct BoolOr;
impl RowFn<2, 1> for BoolOr {
    #[inline(always)]
    fn row([a, b]: [u64; 2]) -> [u64; 1] {
        [a | b]
    }
}
pub(super) struct BoolNot;
impl RowFn<1, 1> for BoolNot {
    #[inline(always)]
    fn row([a]: [u64; 1]) -> [u64; 1] {
        [a ^ 1]
    }
}
pub(super) struct Select;
impl RowFn<3, 1> for Select {
    #[inline(always)]
    fn row([c, a, b]: [u64; 3]) -> [u64; 1] {
        [if c != 0 { a } else { b }]
    }
}

/// `[value, invalid]`: invalid marks out-of-range and NaN inputs.
pub(super) struct CastValue<F, T>(PhantomData<(F, T)>);
impl<F: Value, T: Value> RowFn<1, 2> for CastValue<F, T> {
    #[inline(always)]
    fn row([a]: [u64; 1]) -> [u64; 2] {
        if F::CLASS == T::CLASS && F::BITS == T::BITS {
            return [a, 0];
        }
        let v = F::get(a);
        match T::CLASS {
            Class::Bool => {
                let bit = if F::CLASS == Class::Float {
                    v.float() != 0.0
                } else {
                    v.integer() != 0
                };
                [bit as u64, 0]
            }
            Class::Float => {
                let x = match F::CLASS {
                    Class::Float => T::from_f64(v.float()),
                    Class::Signed => T::from_i64(v.integer() as i64),
                    _ => T::from_u64(v.integer() as u64),
                };
                [x.put(), 0]
            }
            _ if F::CLASS == Class::Float => {
                let wide = v.float();
                let signed = T::CLASS == Class::Signed;
                let bits = T::BITS as i32;
                let lo = if signed { -(2f64).powi(bits - 1) } else { 0.0 };
                let hi = (2f64).powi(bits - signed as i32);
                let t = wide.trunc();
                let bad = t < lo || t >= hi || wide.is_nan();
                let raw = if signed {
                    wide as i64 as i128
                } else {
                    wide as u64 as i128
                };
                [T::from_integer(raw).put(), bad as u64]
            }
            _ => {
                let raw = v.integer();
                [
                    T::from_integer(raw).put(),
                    (raw < T::LO || raw > T::HI) as u64,
                ]
            }
        }
    }
}

/// `[ptr, len]` to `[value, invalid]`.
pub(super) struct ParseValue<T>(PhantomData<T>);
impl<T: TextNumber> RowFn<2, 2> for ParseValue<T> {
    #[inline(always)]
    fn row([p, n]: [u64; 2]) -> [u64; 2] {
        let mut out = 0;
        // SAFETY: byte pointers are valid on every row of their domain.
        let invalid = unsafe { runtime::parse_text::<T>(p, n, &mut out) };
        [out, invalid as u64]
    }
}

pub(super) struct CheckUtf8;
impl RowFn<2, 1> for CheckUtf8 {
    #[inline(always)]
    fn row([p, n]: [u64; 2]) -> [u64; 1] {
        [runtime::validate_utf8(p, n) as u64]
    }
}

fn format<T: TextNumber>(bits: u64) -> String {
    T::from_bits(bits).to_string()
}

macro_rules! with_int {
    ($t:expr, $T:ident => $e:expr, $err:expr) => {
        match $t {
            VariableType::Int8 => {
                type $T = i8;
                $e
            }
            VariableType::Int16 => {
                type $T = i16;
                $e
            }
            VariableType::Int32 => {
                type $T = i32;
                $e
            }
            VariableType::Int64 => {
                type $T = i64;
                $e
            }
            VariableType::UInt8 => {
                type $T = u8;
                $e
            }
            VariableType::UInt16 => {
                type $T = u16;
                $e
            }
            VariableType::UInt32 => {
                type $T = u32;
                $e
            }
            VariableType::UInt64 => {
                type $T = u64;
                $e
            }
            _ => return Err($err),
        }
    };
}
macro_rules! with_float {
    ($t:expr, $T:ident => $e:expr, $err:expr) => {
        match $t {
            VariableType::Float32 => {
                type $T = f32;
                $e
            }
            VariableType::Float64 => {
                type $T = f64;
                $e
            }
            _ => return Err($err),
        }
    };
}
macro_rules! with_value {
    ($t:expr, $T:ident => $e:expr, $err:expr) => {
        match $t {
            VariableType::Boolean => { type $T = bool; $e }
            VariableType::Float32 => { type $T = f32; $e }
            VariableType::Float64 => { type $T = f64; $e }
            t => with_int!(t, $T => $e, $err),
        }
    };
}

fn compare<T: Value + 'static>(f: ScalarFunction) -> CallFn {
    use ScalarFunction::*;
    match f {
        Equal => call::<2, 1, Compare<T, EQ>>,
        NotEqual => call::<2, 1, Compare<T, NE>>,
        LessThan => call::<2, 1, Compare<T, LT>>,
        LessThanOrEqual => call::<2, 1, Compare<T, LE>>,
        GreaterThan => call::<2, 1, Compare<T, GT>>,
        _ => call::<2, 1, Compare<T, GE>>,
    }
}

pub(super) fn select() -> CallFn {
    call::<3, 1, Select>
}

/// Selects the precompiled function of a Call kernel from its input types.
pub(super) fn resolve(f: ScalarFunction, inputs: &[VariableType]) -> Result<Resolved> {
    use ScalarFunction::*;
    let err = || Error::InvalidPlan(format!("unsupported kernel signature: {f:?}({inputs:?})"));
    let t = inputs.first().copied().unwrap_or(VariableType::Boolean);
    let call: CallFn = match f {
        Add | Subtract | Multiply | Divide | Remainder if t.is_float() => {
            with_float!(t, T => match f {
            Add => call::<2, 1, FloatArith<T, ADD>>,
            Subtract => call::<2, 1, FloatArith<T, SUB>>,
            Multiply => call::<2, 1, FloatArith<T, MUL>>,
            Divide => call::<2, 1, FloatArith<T, DIV>>,
            _ => call::<2, 1, FloatArith<T, REM>>,
        }, err())
        }
        Add | Subtract | Multiply => with_int!(t, T => match f {
            Add => call::<2, 2, IntArith<T, ADD>>,
            Subtract => call::<2, 2, IntArith<T, SUB>>,
            _ => call::<2, 2, IntArith<T, MUL>>,
        }, err()),
        Negate if t.is_float() => with_float!(t, T => call::<1, 1, FloatNegate<T>>, err()),
        Negate if t.is_signed() => with_int!(t, T => call::<1, 2, IntNegate<T>>, err()),
        Divide => with_int!(t, T => call::<2, 3, IntDivide<T>>, err()),
        Remainder => with_int!(t, T => call::<2, 2, IntRemainder<T>>, err()),
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual
            if inputs.len() == 4 =>
        {
            match f {
                Equal => call::<4, 1, CompareBytes<EQ>>,
                NotEqual => call::<4, 1, CompareBytes<NE>>,
                LessThan => call::<4, 1, CompareBytes<LT>>,
                LessThanOrEqual => call::<4, 1, CompareBytes<LE>>,
                GreaterThan => call::<4, 1, CompareBytes<GT>>,
                _ => call::<4, 1, CompareBytes<GE>>,
            }
        }
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
            with_value!(t, T => compare::<T>(f), err())
        }
        And => call::<2, 1, BoolAnd>,
        Or => call::<2, 1, BoolOr>,
        Not => call::<1, 1, BoolNot>,
        Cast { to } => {
            with_value!(t, F => with_value!(to, T => call::<1, 2, CastValue<F, T>>, err()), err())
        }
        ParseText { to } => with_value!(to, T => call::<2, 2, ParseValue<T>>, err()),
        FormatText => {
            let format: FormatFn = with_value!(t, T => format::<T>, err());
            return Ok(Resolved::Format(format));
        }
        ValidateUtf8 => call::<2, 1, CheckUtf8>,
        _ => return Err(err()),
    };
    Ok(Resolved::Call(call))
}
