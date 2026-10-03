# Add kernel benchmark

Compare two Array × Array addition paths in `add_kernel.rs`:

- `dynamic_add`: call Arrow's `numeric::add` on every evaluation.
- `bind_add`: select a typed function pointer once, then call `add_int64` or
  `add_float64`. The functions use Arrow's `try_binary` / `binary` kernels.

Run through Cargo's optimized benchmark profile with Criterion:

```sh
cargo bench --locked --bench add_kernel
```

The matrix covers Int64 and Float64, with and without NULLs, and batch lengths
1, 16, 64, 256, 2048, and 16384. Each nullable input has approximately 10% NULLs
at different positions. Both routes use an opaque function pointer and safe
`Any` downcasts. Each timed call allocates and drops its output. Input creation,
binding, and correctness checks are outside the timed region. Inputs are reused
across iterations, so this measures repeated calls with cached input arrays.

The checks compare outputs and exercise empty arrays, mismatched lengths,
integer overflow, and overflow hidden by a NULL. Integer arithmetic remains
checked in both routes. The benchmark does not cover scalar broadcasting or
complete expression/operator execution.

## Local results

Apple M4 Pro, arm64, Rust 1.95.0, Arrow 59.3.0. The CSV contains Criterion's
slope estimates and 95% confidence intervals in nanoseconds for each kernel call.
The full matrix used 50 samples, 200 ms warmup, and 1 s measurement. The 2048-row
cases were also repeated in bound-first order, then measured again with:

```sh
cargo bench --locked --bench add_kernel -- '/2048$' --noplot \
  --sample-size 100 --warm-up-time 1 --measurement-time 3
```

Latest 2048-row estimates:

| Type | NULLs | Dynamic | Bound | Time reduction |
| --- | --- | ---: | ---: | ---: |
| Int64 | No | 1119.80 ns | 858.84 ns | 23.3% |
| Int64 | Yes | 1265.91 ns | 1246.12 ns | 1.6% |
| Float64 | No | 313.10 ns | 302.60 ns | 3.4% |
| Float64 | Yes | 387.17 ns | 384.02 ns | 0.8% |

Across the three runs, non-null Int64 consistently reduced time by approximately
22–26%. Nullable Int64 showed little benefit; Float64 varied enough between runs
that these measurements do not establish a reliable improvement.

These are complete kernel timings, not an isolated cost of a `DataType` match.
Arrow dispatches once per batch before entering its typed loop. Specializing
the wrapper also changes the compiler's optimization opportunities; the measured
benefit cannot be attributed entirely to avoiding dispatch. These timings measure
isolated kernels, not the expression executor.

## Dynamic versus static in each arithmetic mode

`add_nullable.rs` measures dynamic/static pairs for all four Int64 modes, with
and without NULLs, at 64, 2048, and 16384 rows: 48 benchmarks in total.
Dynamic resolves the input types on every call; static selects the Int64 function
before timing. Both routes retain an opaque function-pointer call, safe downcasts,
output allocation, and destruction.

| Mode | Dynamic | Static | Positions evaluated |
| --- | --- | --- | --- |
| `checked` | Arrow `numeric::add` | `try_binary` + `add_checked` | Jointly valid only |
| `unchecked` | Custom integer type dispatch | `try_binary` + `Ok(unchecked_add)` | Jointly valid only |
| `unchecked_all` | Custom integer type dispatch | `binary` + `unchecked_add` | Every underlying slot |
| `wrapping` | Arrow `numeric::add_wrapping` | `binary` + `wrapping_add` | Every underlying slot |

Arrow has no `numeric::add_unchecked` API. The custom dispatcher matches both
input DataTypes against the eight integer types from Int8/UInt8 through
Int64/UInt64 and executes the corresponding typed Arrow loop. Only Int64 inputs
are timed. It does not implement Arrow's scalar broadcasting or other numeric
types, so unchecked results describe this custom dispatcher, not an Arrow API.

Before timing, every underlying value pair (including NULL payloads) is checked
to fit in Int64, and every variant's logical output is compared with checked
addition. Empty and all-NULL arrays are also validated. This makes the unsafe
calls valid for these inputs. Production NULL payloads cannot generally be
assumed to satisfy `unchecked_all`'s stronger precondition; wrapping addition
does not require that assumption, but changes valid-value overflow behavior.

Run the complete matrix:

```sh
cargo bench --locked --bench add_nullable -- --noplot
```

The same machine and versions as above were used, with 100 samples, 500 ms
warmup, and 2 s measurement per case. The periodic pattern has approximately 10%
NULLs in each input and 20% in the output. Inputs are cached and reused.
`add_dispatch_results.csv` records all 48 estimates and 95% confidence intervals.
Times below are microseconds per full kernel call, shown as dynamic → static:

| Kernel | Rows | No NULL: dynamic → static | With NULL: dynamic → static |
| --- | ---: | ---: | ---: |
| `checked` | 64 | 0.108 → 0.091 | 0.166 → 0.157 |
| `checked` | 2048 | 1.098 → 0.851 | 1.251 → 1.300 |
| `checked` | 16384 | 9.741 → 7.775 | 9.473 → 9.534 |
| `unchecked` | 64 | 0.074 → 0.076 | 0.153 → 0.154 |
| `unchecked` | 2048 | 0.305 → 0.307 | 1.177 → 1.180 |
| `unchecked` | 16384 | 3.380 → 3.404 | 8.670 → 8.728 |
| `unchecked_all` | 64 | 0.074 → 0.070 | 0.119 → 0.115 |
| `unchecked_all` | 2048 | 0.293 → 0.306 | 0.388 → 0.385 |
| `unchecked_all` | 16384 | 3.395 → 3.387 | 3.562 → 3.558 |
| `wrapping` | 64 | 0.079 → 0.074 | 0.124 → 0.117 |
| `wrapping` | 2048 | 0.318 → 0.308 | 0.374 → 0.370 |
| `wrapping` | 16384 | 3.400 → 3.512 | 3.528 → 3.554 |

This run showed approximately 20–23% less time for static checked addition on
large non-null batches. Nullable batches and the unchecked/wrapping modes
generally differed by only a few percent, sometimes favoring dynamic. These are
complete kernel measurements, including code generation differences, not the
isolated cost of a match. Dynamic dispatch still enters a typed loop and can
use SIMD. Results apply to this NULL pattern and input range; other densities,
clustered NULLs, or dispatch implementations may behave differently.


## x86_64 comparison on dev

The same benchmark source and Cargo.lock were transferred to a fresh temporary
directory on `ssh dev`. The host is a KVM guest with 32 logical CPUs on an AMD
EPYC 7K62, running Linux 6.6.119 and Rust 1.95.0 (LLVM 22.1.2). Both configurations
were pinned to guest CPU 2. Builds completed before measurement. Sample counts,
input arrays, NULL patterns, and validation were identical to the ARM64 run.

Each configuration completed all 48 cases and output correctness checks. The
first used default x86_64 compilation; the second used `target-cpu=native`, which
resolved to `znver2` with AVX2. Commands after copying the sources:

```sh
cargo +1.95.0 bench --locked --bench add_nullable --no-run -j4
taskset -c 2 cargo +1.95.0 bench --locked --bench add_nullable -- --noplot
RUSTFLAGS='-C target-cpu=native' CARGO_TARGET_DIR=target-native cargo +1.95.0 bench --locked --bench add_nullable --no-run -j4
RUSTFLAGS='-C target-cpu=native' CARGO_TARGET_DIR=target-native taskset -c 2 cargo +1.95.0 bench --locked --bench add_nullable -- --noplot
```

`add_dispatch_x86_default.csv` and `add_dispatch_x86_native.csv` contain the full
estimates and 95% confidence intervals. `add_dispatch_x86_metadata.json` records
the environment, source checksums, and compilation settings. Times below are
microseconds per full kernel call, shown as dynamic → static:

| Mode | Rows | Default, no NULL | Default, with NULL | Native, no NULL | Native, with NULL |
| --- | ---: | ---: | ---: | ---: | ---: |
| `checked` | 64 | 0.226 → 0.185 | 0.310 → 0.290 | 0.220 → 0.179 | 0.305 → 0.279 |
| `checked` | 2048 | 2.753 → 2.097 | 2.981 → 2.916 | 2.742 → 2.082 | 2.912 → 2.914 |
| `checked` | 16384 | 20.650 → 15.575 | 21.228 → 21.253 | 20.703 → 15.653 | 20.982 → 20.897 |
| `unchecked` | 64 | 0.146 → 0.141 | 0.279 → 0.273 | 0.136 → 0.128 | 0.270 → 0.262 |
| `unchecked` | 2048 | 0.705 → 0.667 | 2.385 → 2.322 | 0.646 → 0.714 | 2.292 → 2.285 |
| `unchecked` | 16384 | 4.277 → 4.257 | 16.726 → 16.756 | 4.293 → 4.333 | 16.452 → 16.488 |
| `unchecked_all` | 64 | 0.128 → 0.121 | 0.201 → 0.194 | 0.124 → 0.121 | 0.190 → 0.193 |
| `unchecked_all` | 2048 | 0.632 → 0.642 | 0.900 → 0.844 | 0.628 → 0.732 | 0.746 → 0.741 |
| `unchecked_all` | 16384 | 4.240 → 4.241 | 5.605 → 5.610 | 4.284 → 4.225 | 4.597 → 4.576 |
| `wrapping` | 64 | 0.148 → 0.121 | 0.218 → 0.196 | 0.141 → 0.122 | 0.209 → 0.187 |
| `wrapping` | 2048 | 0.800 → 0.612 | 0.858 → 0.844 | 0.641 → 0.617 | 0.762 → 0.706 |
| `wrapping` | 16384 | 4.267 → 4.205 | 5.742 → 5.526 | 4.247 → 4.286 | 4.696 → 4.636 |

For 2048 non-null rows, static checked addition took about 24% less time with
both compilation settings. With NULLs, checked dynamic/static were close.
Unchecked modes did not show a consistent static advantage: some native static
cases were slower. This is one complete run per configuration, and includes
allocation, downcasts, and code generation differences; these differences cannot
be attributed solely to the datatype match.

Disassembly of relevant typed `binary` and `try_binary_no_nulls` functions showed
128-bit SSE2 `paddq` with default settings and 256-bit AVX2 `vpaddq` with native
settings. Dynamic dispatch can also enter these vectorized typed loops. This
observation does not imply vectorization of checked early-error loops or the
NULL path that visits only valid indices. In native nullable 2048-row cases,
`unchecked_all` took about 0.74 µs versus 2.29 µs for `unchecked`, for both dispatch
variants: the choice of NULL-processing loop dominated the dispatch difference.

Absolute ARM64/x86 timings also differ in CPU, operating system, allocator, and
virtualization. Use the dynamic/static pairs within each host when assessing
binding benefits, rather than treating cross-host times as architecture-only
comparisons.


### Follow-up: the apparent native static regression did not reproduce

After inspecting the native binary, the Int64 dynamic and static unchecked
wrappers were found to call exactly the same typed `try_binary` function
(address `0x1f0db0`). Both unchecked_all wrappers likewise called the same typed
`binary` function (`0x1f42a0`). Their arithmetic loops therefore cannot explain
the difference through different SIMD code generation.

Two focused reruns used the unchanged native binary, CPU affinity, inputs, and
Criterion configuration, selecting only these four cases:

```sh
RUSTFLAGS='-C target-cpu=native' CARGO_TARGET_DIR=target-native taskset -c 2 cargo +1.95.0 bench --locked --bench add_nullable -- 'add_dispatch/Int64/nulls=false/unchecked.*/.*/2048' --noplot --save-baseline diagnosis1
```

The second used `--save-baseline diagnosis2`. Times are microseconds, dynamic →
static. `add_dispatch_x86_native_rechecks.csv` preserves all eight estimates and
95% confidence intervals:

| Mode, 2048 rows without NULLs | Original full matrix | Focused run 1 | Focused run 2 |
| --- | ---: | ---: | ---: |
| unchecked | 0.646 → 0.714 | 0.687 → 0.642 | 0.685 → 0.644 |
| unchecked_all | 0.628 → 0.732 | 0.633 → 0.620 | 0.632 → 0.620 |

The original slowdown reversed in both focused runs. Thus the full-matrix
numbers do not establish an intrinsic static-dispatch regression. Narrow
within-run confidence intervals do not capture changes in process/allocator
state or host conditions across benchmark runs. The timed paths include fresh
output allocation and destruction, and all cases run sequentially in a fixed
order. The responsible factor has not been isolated: allocation/cache state,
wrapper execution differences, and VM conditions remain possible contributors.
A controlled measurement of dispatch overhead would need a shared typed body,
interleaved/reversed execution order, and controlled output allocation.
