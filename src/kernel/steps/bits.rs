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

//! Window bitmaps: bit `r % 64` of word `r / 64` for window row `r`. Bits at
//! and beyond the window's row count are unspecified.
use super::{WINDOW, WORDS};

/// What is known about a Boolean over the rows of a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BitState {
    AllFalse,
    AllTrue,
    Mixed,
}

#[repr(align(64))]
pub(crate) struct Bitmap(pub [u64; WORDS]);

pub(crate) static ONES: Bitmap = Bitmap([u64::MAX; WORDS]);
pub(crate) static ZEROS: Bitmap = Bitmap([0; WORDS]);

impl BitState {
    pub fn constant(value: bool) -> Self {
        if value { Self::AllTrue } else { Self::AllFalse }
    }
    /// The static bitmap of a known state.
    pub fn bitmap(self) -> Option<*mut u8> {
        match self {
            Self::AllTrue => Some(ONES.0.as_ptr() as *mut u8),
            Self::AllFalse => Some(ZEROS.0.as_ptr() as *mut u8),
            Self::Mixed => None,
        }
    }
    /// From kernel result bits `2j` (some row) and `2j + 1` (every row).
    pub fn reported(result: u64, j: usize) -> Self {
        if j >= crate::kernel::codegen::ir::MAX_REPORTS {
            Self::Mixed
        } else if result >> (2 * j + 1) & 1 != 0 {
            Self::AllTrue
        } else if result >> (2 * j) & 1 == 0 {
            Self::AllFalse
        } else {
            Self::Mixed
        }
    }
}

/// The valid bits of word `w` of a window of `n` rows.
#[inline]
pub(crate) fn tail_mask(w: usize, n: usize) -> u64 {
    let rows = n.saturating_sub(w * 64);
    if rows >= 64 {
        u64::MAX
    } else {
        (1u64 << rows) - 1
    }
}

pub(crate) fn words(n: usize) -> usize {
    n.div_ceil(64)
}

pub(crate) fn state(bits: &[u64], n: usize) -> BitState {
    let (mut any, mut all) = (0u64, u64::MAX);
    for (w, &word) in bits[..words(n)].iter().enumerate() {
        let mask = tail_mask(w, n);
        any |= word & mask;
        all &= word | !mask;
    }
    if any == 0 {
        BitState::AllFalse
    } else if all == u64::MAX {
        BitState::AllTrue
    } else {
        BitState::Mixed
    }
}

/// The first row below `n` set in every bitmap.
pub(crate) fn first(bitmaps: &[&[u64]], n: usize) -> Option<usize> {
    (0..words(n)).find_map(|w| {
        let word = bitmaps.iter().fold(tail_mask(w, n), |a, b| a & b[w]);
        (word != 0).then(|| w * 64 + word.trailing_zeros() as usize)
    })
}

/// Rows below `n` set in every bitmap.
pub(crate) fn count(bitmaps: &[&[u64]], n: usize) -> usize {
    (0..words(n))
        .map(|w| {
            bitmaps
                .iter()
                .fold(tail_mask(w, n), |a, b| a & b[w])
                .count_ones() as usize
        })
        .sum()
}

/// Copies `n` bits starting at bit `offset` of `bytes` into window words.
pub(crate) fn extract(bytes: &[u8], offset: usize, n: usize, out: &mut [u64; WORDS]) {
    debug_assert!(n <= WINDOW);
    let chunks = arrow_buffer::bit_chunk_iterator::BitChunks::new(bytes, offset, n);
    for (o, word) in out.iter_mut().zip(chunks.iter_padded()) {
        *o = word;
    }
}

/// A growing bitmap, appended to in windows.
#[derive(Default)]
pub(crate) struct BitBuilder {
    pub words: Vec<u64>,
    pub len: usize,
}

impl BitBuilder {
    pub fn with_capacity(bits: usize) -> Self {
        Self {
            words: Vec::with_capacity(bits.div_ceil(64) + 1),
            len: 0,
        }
    }

    /// Appends the low `n` bits of `bits`, `n <= 64`.
    #[inline]
    pub fn push(&mut self, bits: u64, n: usize) {
        if n == 0 {
            return;
        }
        let bits = if n == 64 {
            bits
        } else {
            bits & ((1u64 << n) - 1)
        };
        let used = self.len % 64;
        if used == 0 {
            self.words.push(bits);
        } else {
            *self.words.last_mut().unwrap() |= bits << used;
            if used + n > 64 {
                self.words.push(bits >> (64 - used));
            }
        }
        self.len += n;
    }

    pub fn fill(&mut self, value: bool, mut n: usize) {
        let word = if value { u64::MAX } else { 0 };
        while n > 0 {
            let k = n.min(64);
            self.push(word, k);
            n -= k;
        }
    }

    /// Appends the bits of `bits` at the rows below `n` selected by `mask`.
    pub fn select(&mut self, bits: &[u64], mask: &[u64], n: usize) {
        for w in 0..words(n) {
            let m = mask[w] & tail_mask(w, n);
            if m == u64::MAX {
                self.push(bits[w], 64);
            } else if m != 0 {
                self.push(compress(bits[w], m), m.count_ones() as usize);
            }
        }
    }

    pub fn finish(self) -> arrow_buffer::BooleanBuffer {
        let len = self.len;
        arrow_buffer::BooleanBuffer::new(arrow_buffer::Buffer::from_vec(self.words), 0, len)
    }
}

/// The bits of `bits` at the set bits of `mask`, packed downwards: Hacker's
/// Delight 7-4, branch-free whatever the mask's density.
#[inline]
pub(crate) fn compress(bits: u64, mut mask: u64) -> u64 {
    let mut x = bits & mask;
    let mut mk = !mask << 1;
    for i in 0..6 {
        let mut mp = mk ^ (mk << 1);
        mp ^= mp << 2;
        mp ^= mp << 4;
        mp ^= mp << 8;
        mp ^= mp << 16;
        mp ^= mp << 32;
        let mv = mp & mask;
        mask = (mask ^ mv) | (mv >> (1 << i));
        let t = x & mv;
        x = (x ^ t) | (t >> (1 << i));
        mk &= !mp;
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_appends_unaligned_runs() {
        let mut b = BitBuilder::default();
        b.push(0b101, 3);
        b.push(u64::MAX, 64);
        b.push(0, 2);
        assert_eq!(b.len, 69);
        assert_eq!(b.words, vec![0b101 | (u64::MAX << 3), 0b111]);
        let mut s = BitBuilder::default();
        s.select(&[0b1100, 0], &[0b1010, 0], 70);
        assert_eq!((s.len, s.words[0]), (2, 0b10));
    }

    #[test]
    fn compress_packs_the_masked_bits() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..10_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let (bits, mask) = (x, x.rotate_left(29) ^ x.wrapping_mul(31));
            let mut expected = 0;
            let mut k = 0;
            for i in 0..64 {
                if mask >> i & 1 != 0 {
                    expected |= (bits >> i & 1) << k;
                    k += 1;
                }
            }
            assert_eq!(compress(bits, mask), expected);
        }
        assert_eq!(compress(u64::MAX, 0), 0);
        assert_eq!(compress(0b1010, u64::MAX), 0b1010);
    }

    #[test]
    fn state_ignores_rows_beyond_the_window() {
        let mut bits = [0u64; WORDS];
        bits[0] = 0b111 | (1 << 40);
        assert_eq!(state(&bits, 3), BitState::AllTrue);
        assert_eq!(state(&bits, 4), BitState::Mixed);
        assert_eq!(first(&[&bits, &ONES.0], 64), Some(0));
        assert_eq!(count(&[&bits], 41), 4);
    }
}
