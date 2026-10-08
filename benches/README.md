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

## Cranelift query compilation experiment

[TUM 相关工作与 Roc 落地风险评估](query_compilation_research.md) covers the research lineage, implementation gaps and a staged rollout.

The primary comparison holds the algorithm fixed: **the same bitmap filter +
projection, ahead-of-time compiled by rustc/LLVM versus generated by Cranelift**.
The old scalar-fused-versus-Arrow comparison mixed algorithm and backend effects;
it is superseded here and cannot establish a JIT-versus-AOT performance claim.
The latest execution comparison is in [Selection-aware vector execution](#selection-aware-vector-execution-2026-10-08).
The JIT lowering is described in [Optimized SIMD lowering](#optimized-simd-lowering-2026-10-08);
the earlier scalar-IR tables below are retained as historical baselines.

```sh
cargo test --locked --all-targets --features jit
RUSTFLAGS="-C target-cpu=native" cargo bench --locked --features jit --bench query_compilation -- --noplot
# Reverse the within-case order to check measurement-order effects:
ROC_BITMAP_BENCH_REVERSE=1 RUSTFLAGS="-C target-cpu=native" cargo bench --locked --features jit --bench query_compilation -- selection_comparison --noplot
```

Cranelift selects the native ISA; `target-cpu=native` also lets rustc/LLVM target
this CPU. This aligns the target hardware, not the two compilers' instruction
selection or optimization passes. Both run at optimized settings (Cargo bench
profile and Cranelift `opt_level=speed`).

### What is held equal

For the equivalent of `SELECT x * 3 + y FROM input WHERE x < threshold`, both
implement exactly this algorithm:

1. Traverse full 64-row chunks. Compare the 64 rows and
   pack their Boolean results into a `u64`; count set bits. A scalar tail loop
   handles the final 0–63 rows, with unused high bits zero.
2. Materialize the bitmap (`ceil(n/64)` words). Allocate one zero-initialized
   output buffer sized to the **actual** selected row count. If the batch selects
   no rows, skip projection and allocate no output values.
3. Traverse bitmap chunks. Skip zero masks; process full masks with a contiguous
   loop; otherwise visit set bits in order using `trailing_zeros` and
   `bits &= bits - 1`. Compute checked `x * 3 + y` directly into final output.
4. Construct and eventually drop the output Arrow RecordBatch.

The Rust AOT kernels are in `tests/support/bitmap_aot.rs`. Thresholds -500, 0 and
500 are separate const-generic instantiations, and multiplier 3 is constant, just
as those constants are embedded in the JIT code. No expression interpreter or
per-row dynamic dispatch is introduced in the AOT path. Both kernels use the
same raw-pointer C ABI and unchecked accesses after common input validation.
Both return an error status for overflow, without panics/unwinding across the
ABI. The original baseline used scalar unrolled IR in both paths. The optimized
AArch64 JIT now explicitly emits SIMD comparisons and bitmap compaction, and
unrolls the dense projection loop four rows at a time. Rust AOT source is
unchanged and relies on LLVM vectorization. Algorithm, semantics, chunk size,
allocation and ABI remain equal; this compares the complete lowering/backend
paths, not two compilers receiving identical scalar IR.

Both backends run through **the very same `CompiledFilterProject::execute_batch`
implementation**. This holds column validation, pointer-table creation, mask
allocation, output allocation, indirect function calls, error handling, output
schema and Arrow materialization constant. The hidden unsafe `from_aot_kernels`
constructor exists only to supply the static benchmark functions to this shared
wrapper; it is not a stable extension API. The code-generating path still owns
its executable pages and frees them on drop.

Historical results use `bitmap_same_algorithm` for the paired AOT/JIT results
and `arrow_reference` for the materializing Arrow implementation. The current
`selection_comparison` group measures all four backends within each case:
`arrow`, `vector_selection`, `aot`, and `cranelift`. `ArrowReference` preserves
the original eager filtering plus dense expression evaluation, independently
of the changed `FilterExec`. It remains the untimed correctness oracle.
Only the AOT/JIT pair holds algorithm, allocation wrapper and ABI fixed;
vector/JIT comparisons measure complete execution strategies, not compiler
backend quality alone.

Inputs use a fixed pseudo-random seed, 64/2048/16384 rows, and approximately
0%/50%/100% selectivity. Inputs are reused: these are warm-cache microbenchmarks,
not storage or whole-query measurements. Each timed call includes allocations,
materialization and output destruction. JIT compilation is outside the warm
execution timings and measured separately by `jit_bitmap_startup`, including
module creation, lowering, CLIF formatting, code generation, finalization and
code-memory cleanup. This measures repeated compilations in a running process,
not cold process startup.

### API and scope

`roc::jit::CompiledFilterProject::compile(&predicate, &projection)` consumes the
existing scalar IR. Reuse `execute_batch(&batch)` across batches; `clif()` exposes
both generated kernel functions. `Ok(None)` means unsupported IR, allowing the
caller to retain Arrow executors; `Err` means compilation failed. There is no
automatic fallback after entering native execution and no automatic plan rewrite.

The subset supports non-null Int64 references/constants, signed comparisons,
checked addition/subtraction/multiplication, Boolean constant predicates and
multiple Int64 projections. Empty projections preserve row counts. Runtime
column types, null counts and lengths are checked before native loads. Arrow
slice offsets and bitmap tail bits are respected. Projection references address
the original input: remap references if a preceding filter reorders columns.
Successful values and overflow failure agree with the existing executor; exact
error messages/categories may differ. No partial output is returned on failure.

Nullable expressions, Boolean columns/projections, casts, division, floating
point, strings, CASE, conjunctions, aggregation and joins remain outside this
subset. Code is movable to a worker, but caching, shared code ownership and
adaptive compilation are not implemented. The generated bitmap kernels require
a 64-bit target and have been executed locally on aarch64 macOS. Batch boundaries
remain the host scheduler's cancellation points.

### Follow-on experiments

- Extend equivalent AOT/JIT kernels together for nullable SQL semantics and
  `Filter -> Project -> SUM/COUNT`. Keep selection, aggregation, allocation and
  materialization algorithms equal in each backend comparison.
- Measure predicate packing and projection separately, inspect generated machine
  code, and introduce the same SIMD/chunking strategy to both variants when
  investigating algorithm effects. Do not infer branch/vectorization causes
  from aggregate timings alone.
- Add explicit compiled-fragment lowering to pipeline construction. `Operator`
  is extensible and lacks a typed visitor/downcast hook, so automatic fusion
  requires a deliberate inspection/lowering interface. Compile once per query
  fragment and separate shared code ownership from worker-local aggregate state.
- Keep runtime allocation, hash tables and strings in precompiled helpers with
  a stable C ABI and explicit errors. Preserve source/exchange/blocking boundaries.
- A later execution policy can compare actual end-to-end Arrow and JIT costs,
  including compilation. That is a workload-policy comparison, distinct from
  the controlled backend experiment. Cache keys must include fragment structure,
  types, nullability, embedded constants, ABI and CPU features.

[HyPer's data-centric compilation](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf)
and [Umbra's Tidy Tuples / Flying Start](https://link.springer.com/article/10.1007/s00778-020-00643-4)
provide context for fusion and low-latency compilation. Cranelift is not
Flying Start/DirectEmit. Umbra's authors evaluated a Cranelift backend in
[Compile-Time Analysis of Compiler Frameworks for Query Compilation (2024)](https://home.cit.tum.de/~engelke/pubs/2403-cgo.pdf);
that is precedent for this backend, not a performance prediction. Cranelift
0.135.5 is compatible with this repository's Rust 1.95 toolchain.

### Original scalar-IR measurements (2026-10-08)

Apple M4 Pro, aarch64 macOS, Rust 1.95.0, Arrow 59.3.0, Cranelift 0.135.5.
Both final runs use `target-cpu=native` for AOT and native ISA for JIT; 30 samples,
200 ms warmup and 500 ms measurement per case. Test/build jobs finished before
timing. The second run reverses backend order within each case. Values below
are microseconds per batch. JIT compilation is excluded from these warm timings.

| Rows | Passing rows | AOT (AOT first) | JIT (AOT first) | AOT (JIT first) | JIT (JIT first) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 0% | 0.144 | 0.157 | 0.141 | 0.156 |
| 64 | ~50% | 0.217 | 0.217 | 0.211 | 0.215 |
| 64 | 100% | 0.216 | 0.226 | 0.229 | 0.231 |
| 2048 | 0% | 0.341 | 0.696 | 0.338 | 0.691 |
| 2048 | ~50% | 1.283 | 1.611 | 1.315 | 1.595 |
| 2048 | 100% | 1.498 | 2.065 | 1.479 | 2.129 |
| 16384 | 0% | 1.864 | 4.665 | 1.907 | 4.650 |
| 16384 | ~50% | 8.988 | 11.708 | 9.128 | 11.970 |
| 16384 | 100% | 9.861 | 15.030 | 10.146 | 14.695 |

The raw estimates and 95% confidence intervals are in
[`query_compilation_results.csv`](query_compilation_results.csv), with distinct
group and run columns. Small differences at 64 rows are not a stable speedup
claim. At 2048/16384 rows the AOT version is faster in both measurement orders;
16384 rows with zero selection costs about 1.86–1.91 us AOT versus 4.65–4.67 us
JIT (see the CSV for exact estimates). Unlike the previous comparison, these
gaps now compare the same algorithm, allocation policy and ABI. They motivate
inspection of generated code/optimization quality; timings alone do not prove
a particular SIMD or branch-prediction explanation.

Compile + drop is **916.7 us**; compile + one 2048-row/~50% batch + drop is
**921.5 us**. Two kernels and 64-way predicate expansion substantially increase
compilation work relative to the old single scalar loop (~66 us). This is a
prototype tradeoff, not an inherent bitmap/JIT cost. No finite amortization
threshold against the faster AOT control follows from these measurements. The
AOT control exists only for this fixed benchmark query; a production engine
still needs expression coverage, specialization and an execution policy.

The separately measured existing Arrow path is 2.376 / 21.104 / 20.026 us for
16384 rows at 0% / ~50% / 100% selection. These numbers describe whole-engine
paths with different algorithms and must not be read as backend speedups.

Validation: default `cargo test --locked --all-targets` passed 133 tests; with
`--features jit`, 142 tests passed, including nine JIT tests and paired AOT/JIT
checks across chunk boundaries, every tail width, sliced inputs, empty/full/mixed
blocks, bit 63 and checked overflow. Benchmark smoke checks and formatting
passed. Clippy completed with only pre-existing aggregate/scalar warnings.

### Original Arrow versus bitmap AOT and scalar-IR JIT

The following table puts all three end-to-end paths from the same native-target
run side by side. Times are microseconds per batch, including validation,
allocation and Arrow output materialization/destruction, but excluding binding
and JIT compilation. `Arrow` is the original `FilterExec + ProjectionExecutor`.
This is a workload/implementation comparison; only bitmap AOT and bitmap JIT
hold the execution algorithm and wrapper equal.

| Rows | Passing rows | Original Arrow | Bitmap AOT | Bitmap JIT |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 0% | 0.738 | 0.144 | 0.157 |
| 64 | ~50% | 0.807 | 0.217 | 0.217 |
| 64 | 100% | 0.647 | 0.216 | 0.226 |
| 2048 | 0% | 0.908 | 0.341 | 0.696 |
| 2048 | ~50% | 2.902 | 1.283 | 1.611 |
| 2048 | 100% | 2.801 | 1.498 | 2.065 |
| 16384 | 0% | 2.376 | 1.864 | 4.665 |
| 16384 | ~50% | 21.104 | 8.988 | 11.708 |
| 16384 | 100% | 20.026 | 9.861 | 15.030 |

The warm bitmap JIT path is faster than the original Arrow path in eight of
these nine cases, with execution speedups of about 1.30–4.70x. At 16384 rows
and zero selection it is still about 1.96x slower. This is specific to the
fixed query, data distribution and warm-cache setup. It does not establish a
general backend advantage. Bitmap AOT remains the code-generation control.

The approximately 916.7 us compile/drop cost must be added once per fragment:
at 2048 rows/~50% the execution saving is 1.291 us per batch, giving roughly
710 batches to amortize startup; at 16384 rows/~50% it is 9.396 us per batch,
giving roughly 98 batches. These estimates compare against already-bound
Arrow execution and assume repeated use of this exact compiled fragment.

### Diagnosing the 16384-row, zero-selection case

The remaining gap is in **predicate comparison and bitmap packing**, not in
output allocation or projection. At zero selection `execute_batch` skips the
projector and allocates no output values. A prepared predicate-only closure
validates inputs and builds pointer tables once, then reuses the same allocated
mask buffer. Compilation/disassembly collection happens before timing.

On the same M4 Pro/native-CPU setup, 50 samples, 300 ms warmup and 1 s measurement:

| Path | Predicate only, us | Whole batch, us |
| --- | ---: | ---: |
| Bitmap AOT | 1.584 | 1.793 |
| Original bitmap JIT | 4.407 | 4.610 |
| Bitmap AOT, reversed order | 1.594 | 1.841 |
| Original bitmap JIT, reversed order | 4.427 | 4.622 |

The approximately 2.82 us kernel gap accounts for essentially the entire
whole-batch difference in the first run. In the reversed run, original Arrow
measures 1.700 us for expression evaluation **including bitmap materialization**
and 2.288 us for the whole filter/project path. Its predicate measurement is not
allocation-free like the AOT/JIT kernel measurements; even with that allocation,
it is much faster than the scalar JIT predicate.

The assembly establishes the following differences:

- [AOT assembly](diagnostics/bitmap-reject/aot.asm): LLVM uses 31 NEON `cmgt.2d`
  comparisons plus two scalar comparisons per full 64-row chunk. It masks and
  combines the results in vector registers (`and.16b` / `orr.16b`), reducing to a
  scalar bitmap near the end. The main chunk loop is 155 native instructions.
  The exported symbol is the actual function pointer called by the benchmark,
  not a separately compiled look-alike. AOT also uses stack slots, including
  precomputed vector masks; the distinction is not that AOT never spills.
- [Original JIT disassembly](diagnostics/bitmap-reject/jit-before.asm): 64 scalar
  comparisons (`subs`, `cset`, `uxtb`) feed a serial shift/OR packing chain.
  Loads are scheduled before those comparisons, extending their live ranges.
  The full-chunk block has 42 scalar stack stores and 44 scalar stack loads,
  in addition to reading the input; the function reserves 352 bytes of spill
  space beyond saved registers. Many input addresses also use separate
  `movz`/`add` instructions rather than a load's immediate displacement.
  Cranelift's listing has about 540 operations in that block. This count is an
  instruction-list inspection, not a measured CPU cycle count.
- There is no per-row conditional branch in that JIT full-chunk block: `cset`
  computes a Boolean value. These results do not support blaming unpredictable
  filter branches for this all-rejected case. The main difference is the
  emitted comparison/packing sequence and its optimization quality.

Controlled probes help avoid over-attributing the gap to one instruction class:

| Temporary JIT probe | Predicate, us | Whole batch, us | Observation |
| --- | ---: | ---: | --- |
| Original | 4.407 | 4.610 | Scalar full unroll |
| Only add `notrap` | 4.372 | 4.621 | Same disassembly; no benefit |
| Insert block boundaries every 8 rows | 4.407 | 4.583 | Blocks collapse; spills remain |
| Eight-row inner loop | 4.248 | 4.583 | Hot-loop spills disappear, but loop/address/packing overhead remains |
| Immediate load displacements | 4.064 | 4.254 | Fewer address-construction instructions |
| Two-lane SIMD plus immediate displacements | 3.647 | 3.866 | Helps, but per-pair extraction still expensive |

The [simple SIMD probe](diagnostics/bitmap-reject/jit-simd-probe.asm) lowers
`vhigh_bits` to two vector-to-scalar moves, shifts and a scalar combine for every
pair. It still builds the bitmap in a scalar OR chain. Merely changing the
comparison to SIMD therefore does not reproduce LLVM's vector mask/reduction
strategy. Likewise, removing spills alone did not yield an end-to-end speedup:
register pressure is visible, but these measurements do not establish it as the
sole or dominant cause. The next useful lowering experiment is to retain
weighted predicate bits in vector registers, combine them there, and extract the
bitmap once per block, while controlling live ranges and address forms.

A constant-false control (`x < i64::MIN`) additionally shows dead loads remaining
in the JIT: predicate-only ~1.56 us versus AOT ~0.017 us. `notrap` alone does not
remove them in this compiler version. This is a secondary code-quality clue;
it is not the measured query's predicate, which the compiler cannot know is
false without scanning its runtime input.

At this diagnostic stage the emitter was restored to the original baseline. Their
[measurements and confidence intervals](diagnostics/bitmap-reject/results.csv),
[loop8 patch](diagnostics/bitmap-reject/loop8.patch),
[address-form patch](diagnostics/bitmap-reject/displaced.patch), and
[SIMD probe patch](diagnostics/bitmap-reject/simd-probe.patch) are retained as
independent diagnostic experiments against that historical scalar baseline; they
are not patches for the optimized emitter below. The SIMD
probe includes the address-form change and specializes column < constant only.
It passed the nine existing JIT differential tests before measurement, but is
not a general SIMD implementation or the default execution path.

Run the diagnosis on the current emitter and obtain fresh CLIF/disassembly with:

```sh
ROC_BITMAP_DIAGNOSTICS=target/bitmap-diagnosis RUSTFLAGS="-C target-cpu=native" cargo bench --locked --features jit --bench query_compilation -- --noplot
# Reverse the paired backend order:
ROC_BITMAP_DIAGNOSTICS=target/bitmap-diagnosis-reverse ROC_BITMAP_BENCH_REVERSE=1 RUSTFLAGS="-C target-cpu=native" cargo bench --locked --features jit --bench query_compilation -- --noplot
# On macOS: use the exact executable path printed by cargo above.
xcrun llvm-objdump --disassemble --disassemble-symbols=_roc_bitmap_aot_reject <benchmark-executable>
```

`compile_with_disassembly` explicitly opts into machine-instruction formatting;
normal `compile` and startup benchmarks do not. The prepared benchmark closure
borrows code and input for safety and checks mask length before every native
call. After restoring the baseline, all 143 feature-enabled tests and benchmark
smoke checks pass; Clippy reports only the pre-existing warnings. New diagnostic
coverage checks the logical bitmap, zero tail bits, slices, invalid inputs and
rejection of an undersized caller buffer.

### Optimized SIMD lowering (2026-10-08)

The default emitter now uses explicit SIMD for direct Int64 comparisons on
little-endian AArch64: column/constant, constant/column and column/column, with
all six signed comparison operators. Other targets and predicates containing
checked arithmetic retain scalar predicate evaluation. This is a native-target
lowering choice, not a new expression type or a special case for the benchmark
threshold. The input range and actual selection are never assumed by the JIT.

The optimized path preserves the 64-row bitmap algorithm, selected-count
allocation and dense/sparse projection paths of the original AOT control:

- Compare two Int64 rows per vector. Compact the all-zero/all-one comparison
  lanes from 64 to 32 to 16 to 8 bits with pairwise byte shuffles. This preserves
  each row's Boolean result and order. Cranelift lowers these shuffles to NEON
  `UZP1`: 32 vector comparisons and 28 compaction instructions per full chunk.
  Extract four groups of 16 predicate bits and combine them into one `u64`.
  Vector loads remain behind the full-chunk length check; tails remain scalar.
- Unroll the dense projection loop four rows at a time, sharing loop bookkeeping.
  Each row still performs checked arithmetic before its store. The final one to
  three rows use the scalar remainder; sparse bit traversal is unchanged.
- Express checked signed multiplication as low/high product halves and compare
  the high half with the low half's sign extension. This is exactly the signed
  overflow condition and lets Cranelift branch directly on comparison flags.
  Addition/subtraction retain their existing overflow checks.

A weighted vector-OR tree first reduced the zero-selection whole batch from
~4.61 us to 1.926 us. Saturating lane narrowing measured 2.005 us; byte-shuffle
compaction improved it to 1.669 us. Predicate optimization alone left the
16384-row/full-selection case at 12.244 us; the explicit multiply check reduced
it to 11.672 us and four-row dense unrolling to 9.921 us. These are separate
probe runs, not additive cost estimates. Their paired controls and confidence
intervals are retained in [probe results](diagnostics/bitmap-simd/results.csv);
probe console estimates are explicitly labeled as rounded. The [vector-tree
assembly](diagnostics/bitmap-simd/jit-vector-tree.asm) and [final predicate/project
assembly](diagnostics/bitmap-simd/jit-final.asm) record the generated instructions.

Final complete run, same M4 Pro/native CPU settings as above, us per warm batch:

| Rows | Selection | Bitmap AOT | Optimized JIT | Original Arrow | JIT / AOT |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 0% | 0.138 | 0.133 | 0.698 | 0.964 |
| 64 | ~50% | 0.195 | 0.186 | 0.757 | 0.958 |
| 64 | 100% | 0.197 | 0.205 | 0.639 | 1.041 |
| 2048 | 0% | 0.316 | 0.305 | 0.880 | 0.965 |
| 2048 | ~50% | 1.228 | 1.239 | 2.819 | 1.009 |
| 2048 | 100% | 1.436 | 1.423 | 2.736 | 0.991 |
| 16384 | 0% | 1.830 | 1.675 | 2.321 | 0.915 |
| 16384 | ~50% | 9.508 | 9.654 | 21.016 | 1.015 |
| 16384 | 100% | 9.815 | 9.925 | 19.698 | 1.011 |

Across all nine cases JIT/AOT is 0.915–1.041. Reversing their measurement order
reproduces the result, with ratios 0.927–1.043. The reversed 16384-row AOT/JIT
pairs are 1.815/1.682 us (0%), 9.322/9.722 us (~50%), and 9.911/9.944 us (100%).
These point estimates establish similar performance for this workload; they do
not establish equal performance for arbitrary queries or other architectures.
AOT kernels remain unchanged apart from comments. Arrow remains a separate
whole-engine reference: the optimized JIT is 1.39–5.26x faster in these nine warm
cases, excluding compilation.

A fresh prepared-kernel diagnostic gives 1.618 us AOT versus 1.496 us JIT for
the rejection predicate alone; whole batches measure 1.846 versus 1.656 us.
The main table comes from the separate complete run, avoiding mixed-run ratios.
Complete-run estimates and 95% confidence intervals are saved under
`native_simd_aot_first` and `native_simd_jit_first` in
[query_compilation_results.csv](query_compilation_results.csv), alongside the
historical baseline. The benchmark commands above run the optimized emitter.

Compile/drop now costs 543.5 us, versus the historical 916.7 us; compile plus
execute/drop for 2048 rows/~50% costs 552.3 us. Four-row projection unrolling
increases code size and startup cost relative to predicate-only optimization
(~380.6 us), in exchange for closing the full-selection execution gap. Compilation
still must be amortized: the current ~50% cases need approximately 344 batches
at 2048 rows or 48 batches at 16384 rows to recover startup versus already-bound
Arrow execution. These are warm-cache estimates without a compilation cache.

Validation: all 145 feature-enabled tests and benchmark smoke checks pass.
New differential coverage exercises all six comparisons, both constant operand
orders, column/column comparisons, signed integer extremes, slices and chunk/tail
boundaries; one-hot and inverse masks check every bitmap position independently.
Checked multiplication is compared against Rust across signed product boundaries,
and overflow positions include all four unrolled lanes. Clippy reports only
pre-existing unrelated warnings. SIMD performance has been measured on M4 Pro;
the scalar fallback on other architectures has not been benchmarked here.


### Selection-aware vector execution (2026-10-08)

`roc::exec::Batch` wraps an Arrow `RecordBatch`, an optional physical-row
`BooleanBuffer` selection, and a cached logical row count. `None` means all
physical rows are active. `ProcessExecutor::execute`, `finish`, `ProcessResult`,
and the pipeline's pending stack now use `Batch`. Filter composes selections
and shares the original column buffers, including column pruning. Logical
slicing preserves selection and row order. `materialize()` / `into_record_batch()`
are explicit boundaries: they compact selected rows, or reuse an already-dense
batch. Source/sink interfaces still use Arrow; the scheduler wraps source data
and materializes at the sink boundary. The JIT process adapter also materializes
upstream selections, since its current native predicate ABI accepts dense inputs.

Projection has precompiled selection-aware Int64 primitives for checked `+`, `-`,
`*` and six comparisons, plus references and constants. They read active physical
rows directly and write compact outputs. Each arithmetic expression node still
allocates an intermediate array; the generic runtime scalar multiplier is not
specialized for the benchmark query. Nullable values and sliced arrays/bitmaps
are supported. Other expressions explicitly fall back to dense evaluation.
This is a scoped fast path, not complete selection support for all Arrow kernels.
For full selection, projection retains the existing dense Arrow kernels.

The query is unchanged: `SELECT x * 3 + y WHERE x < threshold`. Four backends
are compared within each case, with equality asserted before timing:

- `arrow`: the original eager `filter_record_batch` + dense Arrow expressions.
- `vector_selection`: actual FilterExec + ProjectionExecutor; no filtered x/y
  arrays, but `x * 3` is still a compact intermediate array. No JIT is used.
- `aot`: the original hand-specialized two-kernel bitmap algorithm and common
  allocation wrapper, ahead-of-time compiled by rustc/LLVM.
- `cranelift`: the same two-kernel algorithm, with expression fusion and constants
  embedded in generated code. Compilation is excluded from execution timings.

Measured on Apple M4 Pro, native CPU target, 50 samples per execution case,
300 ms warmup and 1 s measurement; forward order and then reverse order.
Times below are forward-run microseconds/batch; the final two columns show
vector/JIT ratios from both runs. All paths produce the same dense output.
Input creation, plan binding and input Batch wrapping are outside timing;
per-call allocations, outputs and destruction are inside timing.

| Rows | Selected % | Original Arrow | Vector + selection | Fused AOT | JIT | Vector/JIT forward | Vector/JIT reverse |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 0 | 0.698 | 0.434 | 0.135 | 0.132 | 3.28× | 3.05× |
| 64 | 50 | 0.701 | 0.525 | 0.188 | 0.188 | 2.79× | 2.59× |
| 64 | 100 | 0.606 | 0.520 | 0.203 | 0.201 | 2.59× | 2.55× |
| 2048 | 0 | 0.849 | 0.580 | 0.327 | 0.317 | 1.83× | 1.80× |
| 2048 | 50 | 2.804 | 2.352 | 1.291 | 1.253 | 1.88× | 1.83× |
| 2048 | 100 | 2.753 | 2.628 | 1.459 | 1.453 | 1.81× | 1.81× |
| 16384 | 0 | 2.292 | 2.036 | 1.881 | 1.713 | 1.19× | 1.20× |
| 16384 | 50 | 21.344 | 16.562 | 9.549 | 9.664 | 1.71× | 1.70× |
| 16384 | 100 | 19.816 | 19.741 | 9.909 | 9.957 | 1.98× | 2.00× |

At 16384 rows / ~50% selection, vector execution drops from 21.344 to
16.562 µs (22.4% less time). The JIT advantage shrinks from 2.21× against
the original Arrow path to 1.71× against selection-aware vector execution
(1.70× in reversed order). At full selection, eager Arrow already avoids most
filter-copy work; selection alone changes little. At zero selection, neither
path performs projection, so its remaining gap cannot be attributed to
expression fusion.

Remaining vector/JIT differences include expression intermediate allocation,
extra traversal of selected rows, query-specific constants, predicate bitmap
implementation, input validation, dispatch and output allocation strategy.
These timings do not isolate each contribution, nor establish a universal
compilation-versus-vectorization result. The same-algorithm AOT/JIT control
remains close, demonstrating that the fused implementation's speed does not
require runtime compilation. Compile-and-drop measured ~545 µs separately;
short-query end-to-end latency must include that cost.

Raw forward/reverse estimates and confidence intervals:
[`query_compilation_selection_results.csv`](query_compilation_selection_results.csv).
Older results remain in `query_compilation_results.csv` as historical data.

```sh
cargo test --locked --all-targets --features jit
RUSTFLAGS="-C target-cpu=native" cargo bench --locked --features jit --bench query_compilation -- --noplot
ROC_BITMAP_BENCH_REVERSE=1 RUSTFLAGS="-C target-cpu=native" cargo bench --locked --features jit --bench query_compilation -- selection_comparison --noplot
```

Validation includes dense-Arrow differential checks for arithmetic/comparison
operand layouts and nullable slices, bitmap offsets/tails and composition,
zero-column batches, inactive overflow/division-by-zero, buffer sharing, and
selected MoreResult/finish outputs reaching the dense sink in order.
