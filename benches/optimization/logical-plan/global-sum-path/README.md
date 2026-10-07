# Global SUM execution-path diagnosis and ablation

October 7, 2026. Core changes follow pending [#18](https://github.com/ZENOTME/Roc/pull/18): [#34](https://github.com/ZENOTME/Roc/pull/34) binds global SUM to native checked loops and retains zero group IDs; [#35](https://github.com/ZENOTME/Roc/pull/35) adds a fused prefix-safety bound for integer SUM. Main is unchanged. The DataFusion integration, complete local source archives and executables remain local.

## Question and storage controls

Why does the ungrouped SUM/COUNT query remain slower after batch COUNT? Run the unchanged SQL with the same optimized DataFusion logical plan, then execute it through native DataFusion or the Roc converter. Parquet plans scan only `value`; neither plan has a filter or an opportunity for predicate/row-group skipping. Both use DataFusion table-provider scans with the same projection. Native aggregate modes are Single at one thread and Partial/Final at four threads. Roc uses worker-local accumulation and the existing merge path.

```sql
SELECT SUM(value) AS total, COUNT(value) AS count FROM p
```

Decode the same single column once, outside timers, and register those exact batches as a resident MemTable `m`, distributed round-robin over the same number of partitions. Repeat the query against `m` to remove Parquet/decompression costs. This is an isolation experiment, not an additive exact accounting of I/O and CPU time. The gap remains almost unchanged at one thread: main is about 14.85 ms behind on Parquet and 14.95 ms behind on resident batches. After batch COUNT, those gaps are about 7.04 and 7.21 ms. Aggregation, rather than a missing storage pushdown, explains the large gap in this query.

## First experiment: isolate COUNT, SUM and zero IDs

Four stages, eight process replicas per stage, ten samples per case/engine. Two randomized cyclic Latin squares balance every stage twice at every position. The three engines (native DF, default Roc, Roc with voluntary yield disabled) rotate within each sample. Values are geometric means of per-process medians. Positive reduction means less Roc elapsed time. All observations are retained.

| Threads | Main Roc ms | Batch COUNT ms | Native global SUM ms | Retained zero IDs ms | DF control at last stage ms | Last stage vs DF |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 52.68 | 44.87 | 38.94 | 38.62 | 37.90 | +1.90% |
| 4 | 14.74 | 12.73 | 11.07 | 10.99 | 11.34 | -3.03% |

| Isolated step | 1t elapsed reduction | 1t paired 95% interval | 4t elapsed reduction | 4t paired 95% interval |
|---|---:|---:|---:|---:|
| Batch COUNT (#18) | 14.83% | 14.57–15.06% | 13.69% | 13.20–14.20% |
| Bound native checked SUM (#34) | 13.22% | 12.76–13.63% | 12.98% | 12.47–13.53% |
| Retain zero IDs (#34) | 0.81% | 0.37–1.33% | 0.73% | 0.11–1.25% |

The old SUM loop reads group IDs, tests per-group validity and writes group state for each valid value. The new global kernel is selected once at construction, keeps a native running value local, and uses Arrow validity slices to visit contiguous valid values. Integer addition stays checked. Float64 preserves the first value (including signed zero) and the original addition order. Shared FILTER-before-argument preparation, casts, grouped updates and merges are retained. Unlike closed #15, this introduces no separate expression-execution route or duplicated FILTER preparation.

The global ID vector remains part of the existing update interface. Its owner has a fixed Global index and only this index writes that vector, so existing entries are already zero. Retain them and initialize only growth; batch shrink/grow and merge sizes are tested. The query gain is small (about 0.8%), but this removes repeated writes with a small change and no new state representation.

## Second experiment: fuse the safety proof and reduction

Ten balanced paired processes per variant, five baseline-first/five candidate-first in randomized order. Identical captured probe source for both builds. Ten timed samples per case/engine after two warmups. This experiment remeasures #34 rather than comparing timestamps across experiments.

| Threads | #34 Roc ms | Fused Roc ms | DF control ms | Fused Roc vs DF | Roc reduction vs #34 | Paired reduction 95% interval |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 39.27 | 37.93 | 38.31 | -0.98% | 3.40% | 2.85–3.99% |
| 4 | 11.10 | 10.81 | 11.43 | -5.49% | 2.60% | 2.26–3.04% |

Negative gap means Roc is faster. Within candidate processes, the 95% interval for Roc vs DF is −1.35% to −0.62% at one thread and −5.83% to −5.10% at four threads. The one-thread DF control changes −0.70% between builds, so the raw 3.40% Roc reduction is not all attributed to the kernel: normalizing to the paired DF control gives a 2.72% improvement (2.26–3.32%). Four-thread normalized improvement is 2.73% (2.35–3.09%). These intervals resample process-level paired log ratios, not individual timings as independent observations.

For Int64, the fused scan computes a conservative magnitude bound and wrapping subtotal together. Let `n` be the valid count, `s` the existing sum, and `B = OR(v XOR (v >> 63)) + 1`. Then `B >= abs(v)` for every valid value. Compute `room = min(MAX - s, s - MIN)` in i128/u128. If `n * B <= room`, every valid prefix fits: its absolute change is at most `n * B`. UInt64 uses `B = OR(v)` and `room = MAX - s`. Bound multiplication fits u128 for the supported native/usize widths.

The scan may include arbitrary NULL payloads. Subtract their wrapping subtotal using Arrow BooleanBuffer inversion and set-index iteration. Modulo-2^64 subtraction cancels ignored payloads even if raw subtotals wrapped; the bound proves the final logical sum and every logical prefix fit. NULL payloads can conservatively reject the fast path, but cannot cause an execution error. If the proof fails, run the native sequential checked loop and retain its successful prefix on overflow. Empty/all-NULL batches preserve state. Float64 and grouped kernels do not use the bound.

The captured **actual core-library** disassembly shows vector `add.2d` and `eor/orr.16b` reductions in the fused Int64 scan. The fallback retains `adds` / `b.vs` checks. No explicit SIMD or unsafe code is added. This is a Roc optimization; native DF currently calls Arrow wrapping SUM. Sources: [DataFusion 55.1.0 SUM](https://github.com/apache/datafusion/blob/55.1.0/datafusion/functions-aggregate/src/sum.rs), [Arrow 59.3.0 aggregate kernels](https://github.com/apache/arrow-rs/blob/59.3.0/arrow-arith/src/aggregate.rs).

## Resident query and remaining limits

| Threads | #34 Roc ms | Fused Roc ms | DF ms | Fused Roc vs DF |
|---:|---:|---:|---:|---:|
| 1 | 2.455 | 1.469 | 1.788 | -17.82% |
| 4 | 0.971 | 0.865 | 0.746 | +15.83% |

The one-thread resident improvement is 40.14%, showing the kernel gain directly. The four-thread resident query remains about 0.118 ms / 15.83% slower. This residual is not localized to a specific scheduling/merge operation; do not label it proven scheduler overhead. Disabling voluntary yields provides no stable improvement in the full-path controls (candidate resident 4t medians: default 0.865 ms, disabled 0.869 ms). There is no justified yield change in these PRs.

These results establish a modest win for the measured warm-cache Parquet SUM/COUNT query, not a universal engine ranking. Int64 input is bounded to ±100000 with NULL every 17th row. Near-limit data may fail the conservative proof and pay the preflight scan before the checked fallback. Float64, UInt64 throughput, different NULL distributions, high-cardinality grouping, joins and spilling are not benchmarked here. A single-thread narrow/non-null workload can have a different relative result. Integer overflow behavior remains intentionally stricter than native DF.

## Kernel controls and rejected implementations

The kernel probe includes the exact accumulator source in its own compilation module. It excludes expression, storage and pipeline costs, and its inlining context differs from the library. Full-path timings execute the actual core library; only the latter establish query-level gains. Arrow wrapping controls are not proposed semantic changes. All inputs in the timing suite are overflow-free.

Candidate-phase one-thread microprobe geometric means of ten process medians:

| Kernel control | ms over 4M decoded values |
|---|---:|
| roc_bound | 1.434 |
| roc_kernel | 1.138 |
| local_checked | 3.556 |
| local_slices | 2.060 |
| arrow_wrapping | 1.520 |
| guarded_arrow | 2.188 |
| guarded_dense | 2.603 |
| wrapping_slices | 1.841 |
| guarded_slices | 2.497 |
| deferred_overflow | 2.142 |
| fused_guard | 1.130 |

The two-pass range-check + Arrow SUM, dense correction + Arrow SUM, guarded wrapping slices, and deferred overflow-flag prototypes do not beat the native checked slice loop; they are not shipped. The useful version fuses bound + subtotal and corrects only ignored payloads. Preliminary JSON results are retained separately; their binaries were not captured and their compile-time source hash was unset, so they are exploratory rather than primary evidence. The final paired binaries capture all controls with source and executable hashes.

## Validation and artifacts

- Core-only #34 head: 130 release tests; #35 head: 132 release tests. All-target compilation passes for both. Source/tests match their local compiled snapshots exactly. Fresh full-workspace tests were not rerun; schema/value checks pass after every timed local integration query.
- New regressions cover sliced validity/all-NULL/empty batches, varying global ID lengths, casts/partial merges, Float64 first signed zero/order/NULL payloads, signed and unsigned cross-batch overflow/successful prefixes, and arbitrary NULL payloads whose raw subtotal wraps. A deterministic test exercises 128 small/extreme cases per integer type with sliced bitmaps and repeated updates.
- [metadata.json](metadata.json) and [paired/metadata.json](paired/metadata.json) record source identities, lockfile/build hashes, hardware, compiler, input-file checksums and process orders. [summary.json](summary.json) records four-stage medians/intervals; [paired/summary.json](paired/summary.json) records final paired medians, intervals and control-normalized effects.
- All process JSONs, build logs, core test/check logs and the two actual core Int64 disassemblies are retained. The first experiment has 3840 full-path timings + 6400 kernel timings; the second has 2400 + 4400. No samples were trimmed.
- Core source archives contain src/tests and production manifests, without the integration. Full local-source archives and binaries stay local. Compiled lockfiles include the private DataFusion harness; their exact digest is recorded separately. Public artifacts are evidence, not a standalone benchmark package; reproduction requires the preserved local harness/archive and original input files.
- build.py / run.py reproduce the first experiment; paired_build.py / paired_run.py reproduce the final paired experiment; normalize.py adds control-normalized statistics; document.py assembles this evidence. They refer to the captured local probe. The main experiment uses batch size 8192, 4M rows in eight Snappy/plain/non-dictionary files, row groups 65536, two warmups and 1/4 Tokio threads. SQL/planning/conversion/Roc graph construction are excluded, full execution and output extraction included. Correctness checks are outside timers.

Core snapshots:

- main: `291317e1c4ee43ca7a613f0a532b1f35fae5aab4`; local snapshot `67fd05a3ecb4c852c96b396869ddfd3d1de5cca8`; [main-core-source.tar.gz](main-core-source.tar.gz).
- count: `0ddd08565871b7e53faa09f738d0cfa25579635c`; local snapshot `590c6875c80689172aa5f72ad1097ed1ea144d0a`; [count-core-source.tar.gz](count-core-source.tar.gz).
- sum: `c0fce1680186df137af1970ed3ee4496d3091112`; local snapshot `e4714899b0bd6385698eef9c1a6303a2ac6efb64`; [sum-core-source.tar.gz](sum-core-source.tar.gz).
- reuse: `b3cdd89314ec5808a6525bd03b16c460133bb2f4`; local snapshot `da4b0e0db2ba1b84008f485e931f7481dde63317`; [reuse-core-source.tar.gz](reuse-core-source.tar.gz).
- fused: `37708712d41b2a51ab30ca951ca98c1e144b3274`; local snapshot `b57681fa2fb5aaca1b31a2cf889b8be459ac8bd7`; [fused-core-source.tar.gz](fused-core-source.tar.gz).
