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

//! The precompiled kernel library, built into the binary by the build script
//! from `codegen`. Each CPU runs the widest variant it supports.
use super::codegen::catalog::Key;
use crate::error::{Error, Result};
use std::sync::LazyLock;

/// See `codegen::ir::Group`.
pub(crate) type KernelFn = unsafe extern "C" fn(*const *mut u8, u64) -> u64;

mod table {
    use super::KernelFn;
    include!(concat!(env!("OUT_DIR"), "/kernels.rs"));
}

/// The variant this CPU runs, in `table::VARIANTS` order.
static VARIANT: LazyLock<usize> = LazyLock::new(|| {
    let supported = |name: &str| match name {
        #[cfg(target_arch = "x86_64")]
        "avx2" => {
            std::arch::is_x86_feature_detected!("avx2")
                && std::arch::is_x86_feature_detected!("bmi1")
                && std::arch::is_x86_feature_detected!("bmi2")
                && std::arch::is_x86_feature_detected!("fma")
                && std::arch::is_x86_feature_detected!("lzcnt")
                && std::arch::is_x86_feature_detected!("movbe")
                && std::arch::is_x86_feature_detected!("f16c")
        }
        #[cfg(target_arch = "x86_64")]
        "sse2" => true,
        _ => !cfg!(target_arch = "x86_64"),
    };
    table::VARIANTS
        .iter()
        .position(|v| supported(v))
        .unwrap_or(table::VARIANTS.len() - 1)
});

pub(crate) fn kernel(key: &Key) -> Result<KernelFn> {
    let name = key.name();
    let i = table::NAMES
        .binary_search(&name.as_str())
        .map_err(|_| Error::InvalidPlan(format!("no precompiled kernel {name}")))?;
    Ok(table::TABLES[*VARIANT][i])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::codegen::{
        catalog::{self, Kind},
        ir::{self, Arg},
    };

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        /// Mostly small values, so that some rows overflow and some do not;
        /// every bit pattern now and then.
        fn value(&mut self) -> u64 {
            match self.next() % 4 {
                0 => self.next(),
                1 => self.next() >> (self.next() % 64),
                _ => self.next() % 7,
            }
        }
    }

    /// The inputs of one kernel over a window: 64-bit words per argument,
    /// with the constants of magic division derived from its divisor.
    fn inputs(key: &Key, args: &[Arg], rng: &mut Rng) -> Vec<Vec<u64>> {
        let mut inputs = args
            .iter()
            .map(|a| match a {
                Arg::Output(_) | Arg::OutputBitmap => vec![0u64; 1024],
                _ => (0..1024).map(|_| rng.value()).collect(),
            })
            .collect::<Vec<_>>();
        if matches!(key.kind, Kind::DivMagic | Kind::RemMagic) {
            let magic = loop {
                let d = rng.value() >> (rng.next() % 64);
                if let Some(m) = ir::magic(key.ty, d) {
                    inputs[1][0] = d;
                    break m;
                }
            };
            for (i, c) in magic.into_iter().enumerate() {
                inputs[2 + i][0] = c;
            }
        }
        inputs
    }

    /// Runs a kernel; returns its result and the bytes of its outputs.
    fn run(f: KernelFn, args: &[Arg], inputs: &[Vec<u64>], n: usize) -> (u64, Vec<Vec<u8>>) {
        let mut buffers = inputs.to_vec();
        let pointers = buffers.iter_mut().map(|b| b.as_mut_ptr().cast()).collect::<Vec<_>>();
        // SAFETY: every argument has room for a window of 64-bit values or a
        // window bitmap.
        let result = unsafe { f(pointers.as_ptr(), n as u64) };
        let outputs = args
            .iter()
            .zip(&buffers)
            .filter_map(|(a, b)| {
                let bytes = match a {
                    Arg::Output(t) => n * t.bytes(),
                    Arg::OutputBitmap => n.div_ceil(8),
                    _ => return None,
                };
                // SAFETY: the buffer has 8192 bytes.
                let mut out = unsafe { std::slice::from_raw_parts(b.as_ptr().cast::<u8>(), bytes) }.to_vec();
                if let (Arg::OutputBitmap, Some(last)) = (a, out.last_mut())
                    && !n.is_multiple_of(8)
                {
                    *last &= (1 << (n % 8)) - 1;
                }
                Some(out)
            })
            .collect();
        (result, outputs)
    }

    /// The inputs of window row `r` as row 0 of new arguments.
    fn row(args: &[Arg], inputs: &[Vec<u64>], r: usize) -> Vec<Vec<u64>> {
        args.iter()
            .zip(inputs)
            .map(|(a, input)| {
                let mut one = vec![0u64; 1024];
                match a {
                    Arg::Column(t) => {
                        let w = t.bytes();
                        // SAFETY: both buffers have 8192 bytes.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                input.as_ptr().cast::<u8>().add(r * w),
                                one.as_mut_ptr().cast::<u8>(),
                                w,
                            )
                        }
                    }
                    Arg::Bitmap => one[0] = input[r / 64] >> (r % 64) & 1,
                    Arg::Scalar(_) => one[0] = input[0],
                    Arg::Output(_) | Arg::OutputBitmap => {}
                }
                one
            })
            .collect()
    }

    /// The values and flags of a whole window, computed in vectors, equal
    /// those of each row computed alone by the one-lane loop; and every
    /// variant agrees (on x86_64, the SSE2 kernels a CPU with AVX2 never
    /// selects).
    #[test]
    fn every_kernel_agrees_across_widths_and_variants() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for key in catalog::all() {
            let g = key.group();
            let i = table::NAMES.binary_search(&key.name().as_str()).unwrap();
            for n in [1usize, 17, 63, 64, 100, 1024] {
                let inputs = inputs(&key, &g.args, &mut rng);
                let (result, outputs) = run(table::TABLES[*VARIANT][i], &g.args, &inputs, n);
                for variant in 0..table::VARIANTS.len() {
                    let other = run(table::TABLES[variant][i], &g.args, &inputs, n);
                    assert!(
                        other == (result, outputs.clone()),
                        "{} differs in {} at {n} rows",
                        key.name(),
                        table::VARIANTS[variant]
                    );
                }
                if n != 1024 {
                    continue;
                }
                let mut any = 0u64;
                for r in 0..n {
                    let (one_result, one_outputs) = run(table::TABLES[*VARIANT][i], &g.args, &row(&g.args, &inputs, r), 1);
                    for ((arg, whole), one) in g
                        .args
                        .iter()
                        .filter(|a| matches!(a, Arg::Output(_) | Arg::OutputBitmap))
                        .zip(&outputs)
                        .zip(&one_outputs)
                    {
                        let expected = match arg {
                            Arg::Output(t) => whole[r * t.bytes()..(r + 1) * t.bytes()].to_vec(),
                            _ => vec![whole[r / 8] >> (r % 8) & 1],
                        };
                        assert_eq!(*one, expected, "{} row {r}", key.name());
                    }
                    any |= one_result & 0x5555_5555_5555_5555;
                }
                if g.sum.is_none() {
                    assert_eq!(result & 0x5555_5555_5555_5555, any, "{} reports", key.name());
                }
            }
        }
    }
}
