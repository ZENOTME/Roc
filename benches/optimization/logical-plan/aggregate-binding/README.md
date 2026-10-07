# Aggregate update binding ablation

Binding the update function by itself produced little measurable benefit at normal batch sizes in this experiment. With 8192 nullable rows, fixed MIN/MAX direction and otherwise identical shared loops, changes were between -0.38% and +0.23% across individual aggregate cases; the mixed case was -0.31%. Most individual 65536-row cases were within ±0.5%. A bound function still performs an indirect call and safe enum-state access; the old type/function dispatch occurred once per batch rather than once per row.

MIN/MAX comparator specialization was more useful: with binding fixed, nullable Int64 MIN used 8.26% less time and MAX 5.02% less time at 8192 rows. Utf8 MIN/MAX used 3.80%/2.39% less time. The unchanged shared loop bodies in the controls allow this to be separated from API dispatch and from unrelated compiler inlining differences.

The actual source amendment is a broader refactor than dispatch alone: loops are extracted into functions and accumulator layout changes. At 8192 nullable rows it reduced the seven-aggregate mixed time by 1.69% (individual processes 1.61%–1.87%); 65536 rows reduced mixed time by 1.48% (1.22%–1.62%). AVG improved about 11%, but that is **not** an isolated binding benefit. Nullable COUNT regressed 3.13% at 8192 rows and Float64 SUM regressed 1.68%. COUNT DISTINCT was unstable: its 8192-row process changes ranged from -12.83% to +7.70%, so the median positive result does not establish a reliable win. This does not support a universal performance benefit from binding.

These are warm, single-threaded accumulator microbenchmarks, not full-query, Parquet, pipeline, or DataFusion comparisons. State/output materialization is outside the timer. Earlier approximately 31% SUM results concern different source revisions and are unrelated to this experiment.

## Six versions and what each comparison answers

| Version | Update dispatch | MIN/MAX direction | Kernel implementation/layout |
|---|---|---|---|
| `enum_dynamic` | Enum per batch; SUM type enum per batch | Runtime direction in loop | Shared control module |
| `enum_static` | Enum per batch; SUM type enum per batch | Select constant specialization per batch | Same shared module |
| `bound_dynamic` | Function bound during construction | Runtime direction in loop | Same shared module |
| `bound_static` | Function bound during construction | Constant specialization selected during construction | Same shared module |
| `before` | Real preceding SUM-only binding revision | Runtime direction | Production source snapshot |
| `after` | Real all-aggregate binding revision | Constant specialization | Production source snapshot |

- `enum_static → bound_static`: binding with comparator direction already fixed.
- `enum_dynamic → bound_dynamic`: binding with runtime comparator direction.
- `bound_dynamic → bound_static`: comparator specialization with binding already fixed.
- `enum_dynamic → enum_static`: comparator specialization with enum dispatch.
- `enum_dynamic → bound_static`: both changes together, with control layout held fixed.
- `before → after`: complete actual amendment, including changed compilation/inlining and state layout; cannot attribute the entire delta to binding alone.

All four controls use one state type, one copy of each kernel, and the same runtime direction field/update pointer layout. Shared row kernels are marked `inline(never)` so the direct-dispatch version cannot embed a different optimized loop in its wrapper. Shared typed SUM loops are also kept out of dispatch wrappers. Snapshot versions retain default production inlining. Consequently, control timings identify dispatch/comparator effects under that constraint; snapshot timings measure the complete source change in this harness. Neither is a promise about query-level speed.

The initial attempt copied each factorial variant into a different module. It showed large differences on non-extremum negative controls even when only MIN/MAX direction changed. Module/codegen and/or state allocation effects could not be separated from the intended factor. It was rejected for causal conclusions and is preserved in `diagnostic-separate-modules/`, including source preparation, raw samples and metadata. The shared-kernel experiment above supersedes it. Allocation order rotates across cases and processes in the final experiment as well as measurement order.

## Exact source and environment

- Before: `0a69952f69816a5c4ca6fd96f13c88746af37bfd` (PR #6 after SUM-only binding).
- After: `bb4800ea31fd7feb80dc704b0e42382a6f23a3ca` (PR #6 after all-aggregate binding).
- Runtime SUM reference: `7985dace8669503a7ae0054ab67871e6c86236c8` (typed native states before SUM binding).
- Apple M1 Pro, macOS arm64, 10 physical CPU cores, 32 GiB RAM.
- Rust 1.95.0, Arrow 59.3.0, default Cargo release optimization, no RUSTFLAGS override.
- Three sequential independent processes; 12 rounds/case, balanced six-way cyclic timing order, shuffled case order per process.
- 128/8192/65536 rows; input `id = row % 256`; state capacity is 257 to retain an untouched group for validity checks.
- Non-null and nullable input (`row % 17 == 0` is NULL); covariance's second input additionally uses `row % 23 == 0`.
- Int64 payload `((row * 73 + 17) % 4096) - 2048`; UInt64 and fixed-width Utf8 use the non-negative counterpart. Exact generation and mixed aggregate composition are in `src/main.rs`.
- All variants clone one shared `RandomState` per process during resize so COUNT DISTINCT uses the same hash algorithm and hash seed within a comparison. Different processes use new seeds. This is the only adjustment to the before/after snapshot algorithms; resize is excluded from timing.
- Binding, resize, fixture construction, group-ID generation, output materialization and storage excluded. Casts, row encoding, per-row updates and their allocations included.
- Common per-case iteration count is calibrated toward at least 15 ms per variant sample, capped at 50000 iterations. Small fast cases may run below 15 ms at the cap.

One-batch results are validated against an independent scalar reference, including untouched/all-NULL groups and pairwise-valid covariance. All six versions' outputs are compared exactly after every measurement round. All 78 cases/process completed, producing 16848 retained timing rows. Existing PR #6 correctness validation also passed 111 release tests; no Roc source was changed to perform this benchmark.

Each table reports the median of three process medians for absolute times. Percentage reduction is the median of three **paired process** reductions, `100 * (1 - after / before)`. It need not equal the ratio of the separately reported absolute medians. Ranges show the three process reductions; they are not statistical confidence intervals. See `summary.json` for all process medians and paired-round medians and `summary.csv` for every comparison, row size and null mode.

## Binding only, 8192 nullable rows

| Aggregate | Before µs/batch | After µs/batch | Time reduction | Three process range |
|---|---:|---:|---:|---:|
| avg_i64 | 18.105 | 18.094 | +0.02% | -0.28% … +0.23% |
| count_distinct_i64 | 391.707 | 393.188 | -0.38% | -0.67% … -0.19% |
| count_i64 | 6.431 | 6.391 | +0.18% | +0.12% … +1.89% |
| count_star | 3.859 | 3.837 | +0.13% | +0.13% … +0.58% |
| covar_i64 | 33.902 | 33.900 | -0.00% | -0.29% … +0.00% |
| max_i64 | 44.336 | 44.661 | -0.12% | -0.73% … +0.14% |
| max_utf8 | 107.564 | 107.562 | +0.00% | -0.63% … +0.41% |
| min_i64 | 42.743 | 42.644 | +0.23% | -0.46% … +0.42% |
| min_utf8 | 105.699 | 105.047 | +0.22% | -0.36% … +0.77% |
| mixed_7 | 554.990 | 556.736 | -0.31% | -0.90% … +0.02% |
| sum_f64 | 9.115 | 9.062 | -0.10% | -0.11% … +0.74% |
| sum_i64 | 16.858 | 16.875 | -0.05% | -0.20% … -0.01% |
| sum_u64 | 16.855 | 16.843 | +0.08% | -0.10% … +0.33% |

## Comparator specialization only, 8192 nullable rows

| Aggregate | Before µs/batch | After µs/batch | Time reduction | Three process range |
|---|---:|---:|---:|---:|
| avg_i64 | 18.094 | 18.094 | -0.00% | -0.03% … +0.24% |
| count_distinct_i64 | 393.208 | 393.188 | -0.00% | -0.37% … +1.55% |
| count_i64 | 6.413 | 6.391 | -0.05% | -0.16% … +0.99% |
| count_star | 3.830 | 3.837 | -0.17% | -0.46% … +0.12% |
| covar_i64 | 33.873 | 33.900 | -0.08% | -0.08% … -0.03% |
| max_i64 | 46.996 | 44.661 | +5.02% | +4.97% … +5.18% |
| max_utf8 | 110.196 | 107.562 | +2.39% | +1.64% … +2.48% |
| min_i64 | 46.548 | 42.644 | +8.26% | +7.97% … +8.56% |
| min_utf8 | 109.421 | 105.047 | +3.80% | +2.99% … +4.15% |
| mixed_7 | 558.199 | 556.736 | +0.68% | +0.26% … +1.17% |
| sum_f64 | 9.115 | 9.062 | +0.04% | -0.02% … +0.58% |
| sum_i64 | 16.862 | 16.875 | -0.13% | -0.19% … -0.08% |
| sum_u64 | 16.845 | 16.843 | +0.01% | -0.11% … +1.23% |

## Complete source amendment, 8192 nullable rows

| Aggregate | Before µs/batch | After µs/batch | Time reduction | Three process range |
|---|---:|---:|---:|---:|
| avg_i64 | 20.394 | 18.067 | +11.20% | +11.04% … +11.43% |
| count_distinct_i64 | 371.526 | 350.899 | +5.55% | -12.83% … +7.70% |
| count_i64 | 6.324 | 6.472 | -3.13% | -3.77% … -0.78% |
| count_star | 3.858 | 3.890 | -0.12% | -0.83% … +0.05% |
| covar_i64 | 34.609 | 33.880 | +2.10% | +2.03% … +2.15% |
| max_i64 | 46.759 | 44.888 | +4.13% | +3.70% … +4.41% |
| max_utf8 | 109.309 | 107.442 | +1.67% | +1.35% … +1.89% |
| min_i64 | 46.017 | 42.569 | +7.40% | +6.91% … +7.49% |
| min_utf8 | 108.293 | 105.501 | +3.20% | +2.58% … +3.75% |
| mixed_7 | 515.900 | 507.165 | +1.69% | +1.61% … +1.87% |
| sum_f64 | 9.051 | 9.272 | -1.68% | -2.56% … -1.64% |
| sum_i64 | 16.855 | 16.845 | -0.04% | -0.16% … +0.06% |
| sum_u64 | 16.843 | 16.844 | -0.00% | -0.22% … +0.02% |

## Complete source amendment, 65536 nullable rows

| Aggregate | Before µs/batch | After µs/batch | Time reduction | Three process range |
|---|---:|---:|---:|---:|
| avg_i64 | 156.119 | 138.993 | +10.96% | +10.84% … +10.97% |
| count_distinct_i64 | 2909.643 | 2785.260 | +6.07% | -12.12% … +6.43% |
| count_i64 | 51.261 | 51.734 | -1.39% | -1.75% … -0.42% |
| count_star | 31.615 | 31.139 | +1.57% | +1.43% … +1.61% |
| covar_i64 | 269.559 | 264.034 | +2.06% | +2.05% … +2.17% |
| max_i64 | 365.939 | 352.249 | +4.46% | +3.50% … +4.47% |
| max_utf8 | 863.591 | 846.161 | +1.52% | +1.26% … +2.20% |
| min_i64 | 362.488 | 337.645 | +7.46% | +6.85% … +7.64% |
| min_utf8 | 873.941 | 841.572 | +3.34% | +2.97% … +3.70% |
| mixed_7 | 4060.612 | 4010.984 | +1.48% | +1.22% … +1.62% |
| sum_f64 | 69.926 | 71.045 | -1.62% | -1.63% … -1.60% |
| sum_i64 | 133.188 | 133.165 | -0.02% | -0.11% … +0.11% |
| sum_u64 | 133.162 | 132.838 | +0.03% | -0.00% … +0.24% |

## Complete source amendment, 128 nullable rows

| Aggregate | Before µs/batch | After µs/batch | Time reduction | Three process range |
|---|---:|---:|---:|---:|
| avg_i64 | 0.492 | 0.458 | +6.90% | +6.46% … +7.08% |
| count_distinct_i64 | 5.322 | 5.293 | +0.32% | -0.49% … +7.12% |
| count_i64 | 0.132 | 0.115 | +13.24% | +12.78% … +13.70% |
| count_star | 0.071 | 0.069 | +2.56% | +1.76% … +2.94% |
| covar_i64 | 0.912 | 0.898 | +1.48% | +0.89% … +1.92% |
| max_i64 | 0.907 | 0.855 | +5.82% | +3.96% … +6.12% |
| max_utf8 | 2.013 | 2.031 | +0.24% | -0.89% … +0.31% |
| min_i64 | 0.904 | 0.843 | +6.99% | +6.73% … +7.95% |
| min_utf8 | 2.039 | 2.005 | +2.08% | +1.39% … +2.21% |
| mixed_7 | 9.104 | 8.942 | +2.19% | +1.78% … +2.77% |
| sum_f64 | 0.264 | 0.272 | -2.93% | -5.64% … +0.98% |
| sum_i64 | 0.395 | 0.412 | -0.94% | -4.23% … +1.61% |
| sum_u64 | 0.395 | 0.390 | +0.83% | -3.28% … +3.77% |

## Reproduce and audit

```sh
python prepare.py
cargo build --release --locked
python run.py
python summarize.py
```

With a shared build directory, use `python run.py --binary /absolute/path/to/release/aggregate-binding-ablation`. Exact original source snapshots are included under `source-snapshots/`, with commit, Git blob and SHA-256 identities recorded in metadata. `prepare.py` validates those identities and constructs the shared control module; it does not rely on retaining old force-pushed Git commits. Generated Rust files are ignored, with SHA-256 identities retained in `metadata.json`; reproduction regenerates them. Metadata also records compiler, binary, lockfile and harness identities. Raw samples are `process-0.csv`, `process-1.csv`, and `process-2.csv`; corresponding logs record completed correctness checks. Running the experiment overwrites these outputs and updates local metadata; keep a copy when comparing another host or compiler.

## Subsequent source amendment

PR #6 subsequently removed the unsupported SUM placeholder and moved result-type rejection to accumulator construction. The measurements above remain tied to their recorded before/after snapshots; they were not regenerated for that amendment. Current validation and stack identities are recorded under `early_sum_result_validation` in [../core-source-verification.json](../core-source-verification.json).
