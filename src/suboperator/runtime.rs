//! Explicit scalar runtime functions. These are called by generated code;
//! none accepts an operation tag, expression tree, or vector kernel.
use super::{
    aggregation::{AccumulatorState, GroupState},
    scalar::Cell,
};
use crate::error::{Error, Result};
#[derive(Default)]
pub(crate) struct Runtime {
    pub error: Option<Error>,
    pub strings: Vec<Box<[u8]>>,
    pub input_errors: Vec<Option<Error>>,
    pub guard: *const asyncband::shutdown::ShutdownGuard,
}
impl Runtime {
    pub fn string(&mut self, value: String) -> Cell {
        self.strings.push(value.into_bytes().into_boxed_slice());
        Cell::bytes(self.strings.last().unwrap())
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
    out: *mut Cell,
) -> i32 {
    let runtime = unsafe { &mut *runtime };
    protect(runtime, || {
        let g = unsafe { &mut *state };
        let values = unsafe { std::slice::from_raw_parts(values, len) };
        unsafe {
            *out = Cell::word(g.index.intern_row(values) as u64);
        }
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
pub(crate) unsafe extern "C" fn group_lookup_word(
    runtime: *mut Runtime,
    state: *mut GroupState,
    bits: u64,
    valid: u64,
    out: *mut Cell,
) -> i32 {
    protect(unsafe { &mut *runtime }, || {
        let id = unsafe { &mut *state }.index.intern_word(Cell {
            bits,
            len: 0,
            valid,
        });
        unsafe {
            *out = Cell::word(id as u64);
        }
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
    let runtime = unsafe { &mut *runtime };
    protect(runtime, || unsafe {
        {
            let accumulator = &mut (&mut *state).accumulator;
            accumulator.resize(id as usize + 1);
            K::update(
                accumulator,
                id as usize,
                std::slice::from_raw_parts(values, len),
            )
        }
    })
}
pub(crate) unsafe extern "C" fn compare_bytes(a: u64, an: u64, b: u64, bn: u64) -> i64 {
    let a = Cell {
        bits: a,
        len: an,
        valid: 1,
    };
    let b = Cell {
        bits: b,
        len: bn,
        valid: 1,
    };
    unsafe { a.as_bytes().cmp(b.as_bytes()) as i64 }
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
macro_rules! integer { ($($t:ty),*) => { $(impl TextNumber for $t { fn bits(self)->u64 { self as u64 } fn from_bits(bits:u64)->Self { bits as Self } })* }; }
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
pub(crate) unsafe extern "C" fn parse<T: TextNumber, const TRY: bool>(
    runtime: *mut Runtime,
    input: *const Cell,
    output: *mut Cell,
) -> i32 {
    let r = unsafe { &mut *runtime };
    let c = unsafe { *input };
    let out = unsafe { &mut *output };
    *out = Cell::default();
    protect(r, || {
        if c.valid == 0 {
            return Ok(());
        }
        let parsed = std::str::from_utf8(unsafe { c.as_bytes() })
            .ok()
            .and_then(T::parse_text);
        match parsed {
            Some(v) => {
                *out = Cell::word(v.bits());
                Ok(())
            }
            None if TRY => Ok(()),
            None => Err(Error::Execution("invalid scalar cast".into())),
        }
    })
}
pub(crate) unsafe extern "C" fn format<T: TextNumber>(
    runtime: *mut Runtime,
    input: *const Cell,
    output: *mut Cell,
) -> i32 {
    let r = unsafe { &mut *runtime };
    let c = unsafe { *input };
    let out = unsafe { &mut *output };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        *out = if c.valid == 0 {
            Cell::default()
        } else {
            r.string(T::from_bits(c.bits).to_string())
        };
    }));
    if result.is_err() {
        r.error = Some(Error::Execution("string conversion failed".into()));
        1
    } else {
        0
    }
}

pub(crate) unsafe extern "C" fn input_error(runtime: *mut Runtime, index: usize) {
    let r = unsafe { &mut *runtime };
    r.error = r.input_errors[index].take();
}

pub(crate) unsafe extern "C" fn poll(runtime: *mut Runtime) -> i32 {
    let r = unsafe { &mut *runtime };
    if !r.guard.is_null() && unsafe { (&*r.guard).is_shutdown_requested() } {
        r.error = Some(Error::Cancelled);
        1
    } else {
        0
    }
}

pub(crate) unsafe extern "C" fn validate_utf8<const TRY: bool>(
    runtime: *mut Runtime,
    input: *const Cell,
    output: *mut Cell,
) -> i32 {
    let r = unsafe { &mut *runtime };
    let c = unsafe { *input };
    let out = unsafe { &mut *output };
    *out = Cell::default();
    protect(r, || {
        if c.valid == 0 {
            return Ok(());
        }
        if std::str::from_utf8(unsafe { c.as_bytes() }).is_ok() {
            *out = c;
            Ok(())
        } else if TRY {
            Ok(())
        } else {
            Err(Error::Execution("invalid UTF-8 cast".into()))
        }
    })
}
