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

//! Scalar text and byte-string functions of the kernel model. None accepts an
//! operation tag, an expression tree or a vector kernel.

fn bytes<'a>(ptr: u64, len: u64) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: byte pointers reference input buffers, retained constants or
        // the invocation arena, valid on every row.
        unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
    }
}

pub(crate) fn compare_bytes(a: u64, an: u64, b: u64, bn: u64) -> i64 {
    bytes(a, an).cmp(bytes(b, bn)) as i64
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

pub(crate) fn validate_utf8(ptr: u64, len: u64) -> u8 {
    std::str::from_utf8(bytes(ptr, len)).is_err() as u8
}
