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

//! Scalar runtime functions called by generated code. None accepts an
//! operation tag, an expression tree or a vector kernel.
use super::aggregation::{AccumulatorState, GroupState};
use super::scalar::Cell;
use crate::error::{Error, Result};

/// Per-invocation runtime context.
pub(crate) struct Runtime {
    /// A system error (helper failure, panic or cancellation): status 1.
    pub error: Option<Error>,
    /// Owns strings produced by FormatText until the output is copied.
    pub strings: Vec<Box<[u8]>>,
    /// The row values passed by the RaiseIf that failed.
    pub raise_args: Vec<u64>,
    pub guard: *const asyncband::shutdown::ShutdownGuard,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            error: None,
            strings: vec![],
            raise_args: vec![],
            guard: std::ptr::null(),
        }
    }
}

// Each pointer comes from a live invocation and a compile-time validated state
// binding. No Rust panic may unwind through a generated frame.
fn protect(runtime: &mut Runtime, f: impl FnOnce() -> Result<()>) -> i32 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            runtime.error = Some(e);
            1
        }
        Err(_) => {
            runtime.error = Some(Error::Execution("scalar runtime helper panicked".into()));
            1
        }
    }
}

pub(crate) unsafe extern "C" fn group_lookup(
    runtime: *mut Runtime,
    state: *mut GroupState,
    values: *const Cell,
    len: usize,
    out: *mut u64,
) -> i32 {
    protect(unsafe { &mut *runtime }, || {
        let g = unsafe { &mut *state };
        let values = unsafe { std::slice::from_raw_parts(values, len) };
        unsafe { *out = g.index.intern_row(values) as u64 };
        Ok(())
    })
}

pub(crate) unsafe extern "C" fn group_lookup_word(
    runtime: *mut Runtime,
    state: *mut GroupState,
    bits: u64,
    valid: u64,
    out: *mut u64,
) -> i32 {
    protect(unsafe { &mut *runtime }, || {
        let id = unsafe { &mut *state }.index.intern_word(Cell {
            bits,
            len: 0,
            valid,
        });
        unsafe { *out = id as u64 };
        Ok(())
    })
}

pub(crate) unsafe extern "C" fn ensure_groups(
    runtime: *mut Runtime,
    state: *mut AccumulatorState,
    count: u64,
) -> i32 {
    protect(unsafe { &mut *runtime }, || {
        unsafe { &mut *state }.accumulator.resize(count as usize);
        Ok(())
    })
}

pub(crate) unsafe extern "C" fn aggregate_update<K: crate::expr::agg::accumulator::RowKernel>(
    runtime: *mut Runtime,
    state: *mut AccumulatorState,
    id: u64,
    values: *const Cell,
    len: usize,
) -> i32 {
    protect(unsafe { &mut *runtime }, || unsafe {
        let accumulator = &mut (&mut *state).accumulator;
        accumulator.resize(id as usize + 1);
        K::update(
            accumulator,
            id as usize,
            std::slice::from_raw_parts(values, len),
        )
    })
}

/// Reports an overflow of an inline aggregate counter.
pub(crate) unsafe extern "C" fn aggregate_overflow(runtime: *mut Runtime) -> i32 {
    unsafe { &mut *runtime }.error = Some(
        crate::arrow::error::ArrowError::ArithmeticOverflow(
            "generated aggregate arithmetic".into(),
        )
        .into(),
    );
    1
}

fn bytes<'a>(ptr: u64, len: u64) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: byte pointers reference input buffers, retained constants or
        // the invocation arena, valid on every row.
        unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
    }
}

pub(crate) extern "C" fn compare_bytes(a: u64, an: u64, b: u64, bn: u64) -> i64 {
    bytes(a, an).cmp(bytes(b, bn)) as i64
}

pub(crate) extern "C" fn remainder_f32(a: f32, b: f32) -> f32 {
    a % b
}

pub(crate) extern "C" fn remainder_f64(a: f64, b: f64) -> f64 {
    a % b
}

pub(crate) trait TextNumber: std::str::FromStr + std::fmt::Display {
    fn parse_text(s: &str) -> Option<Self>
    where
        Self: Sized,
    {
        s.parse().ok()
    }
    fn bits(self) -> u64;
    fn from_bits(bits: u64) -> Self;
}

macro_rules! integer {
    ($($t:ty),*) => {
        $(impl TextNumber for $t {
            fn bits(self) -> u64 {
                self as u64
            }
            fn from_bits(bits: u64) -> Self {
                bits as Self
            }
        })*
    };
}
integer!(i8, i16, i32, i64, u8, u16, u32, u64);

impl TextNumber for f32 {
    fn bits(self) -> u64 {
        self.to_bits() as u64
    }
    fn from_bits(b: u64) -> Self {
        Self::from_bits(b as u32)
    }
}

impl TextNumber for f64 {
    fn bits(self) -> u64 {
        self.to_bits()
    }
    fn from_bits(b: u64) -> Self {
        Self::from_bits(b)
    }
}

impl TextNumber for bool {
    fn parse_text(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Some(true),
            "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Some(false),
            _ => None,
        }
    }
    fn bits(self) -> u64 {
        self as u64
    }
    fn from_bits(b: u64) -> Self {
        b != 0
    }
}

/// Writes the parsed bits and returns 1 when the text is not a valid `T`.
pub(crate) unsafe extern "C" fn parse_text<T: TextNumber>(ptr: u64, len: u64, out: *mut u64) -> u8 {
    let parsed = std::str::from_utf8(bytes(ptr, len))
        .ok()
        .and_then(T::parse_text);
    let invalid = parsed.is_none() as u8;
    unsafe { *out = parsed.map_or(0, T::bits) };
    invalid
}

/// Formats `bits` as `T` into the invocation arena; writes `[ptr, len]`.
pub(crate) unsafe extern "C" fn format_text<T: TextNumber>(
    runtime: *mut Runtime,
    bits: u64,
    out: *mut u64,
) -> i32 {
    let r = unsafe { &mut *runtime };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        T::from_bits(bits).to_string()
    }));
    match result {
        Ok(text) => {
            r.strings.push(text.into_bytes().into_boxed_slice());
            let text = r.strings.last().unwrap();
            unsafe {
                *out = text.as_ptr() as u64;
                *out.add(1) = text.len() as u64;
            }
            0
        }
        Err(_) => {
            r.error = Some(Error::Execution("string conversion failed".into()));
            1
        }
    }
}

pub(crate) extern "C" fn validate_utf8(ptr: u64, len: u64) -> u8 {
    std::str::from_utf8(bytes(ptr, len)).is_err() as u8
}

pub(crate) unsafe extern "C" fn poll(runtime: *mut Runtime) -> i32 {
    let r = unsafe { &mut *runtime };
    if !r.guard.is_null() && unsafe { (*r.guard).is_shutdown_requested() } {
        r.error = Some(Error::Cancelled);
        1
    } else {
        0
    }
}

/// Records the row values of a failing RaiseIf.
pub(crate) unsafe extern "C" fn raise_args(runtime: *mut Runtime, args: *const u64, len: usize) {
    let r = unsafe { &mut *runtime };
    r.raise_args = unsafe { std::slice::from_raw_parts(args, len) }.to_vec();
}
