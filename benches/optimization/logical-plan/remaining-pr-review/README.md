# Review of remaining optimization PRs

Current source: local-only harness `1c2af64854fe30baca4ee149260a8ffc3e3271f3` over core PR #16 `b308030a737e2b705a0bde2ba6b2e1cec0f1f390`, main `47f1511b67561bae0b6c7654e71355f20d1d2e7b`. Four separately compiled binaries retain all remaining optimizations or remove exactly #12, #15 or #16. The without16 variant also disables local converter fusion and column remapping so its output semantics remain equivalent.

Measured on Apple M1 Pro, rustc 1.95.0 (59807616e 2026-04-14), Arrow 59.3.0 and DataFusion 55.1.0. The same 4,000,000 deterministic rows in eight Snappy Parquet files are used by every process; exact file hashes are recorded. SQL planning/conversion are outside timing. Full execution, output collection and Roc cleanup are timed; decoded inputs are not resident-only synthetic arrays.

## Decisions and affected workloads

Positive reduction means the optimization is faster. Values are geometric means of five independent process medians, each with seven timed samples. Intervals use paired-process bootstrap.

| PR | Workload / threads | Without ms | With ms | Reduction | 95% interval | Decision |
|---|---|---:|---:|---:|---:|---|
| #12 | filter_project / 1 | 101.31 | 99.49 | 1.79% | 1.04% to 2.57% | Retain |
| #12 | filter_project / 4 | 27.91 | 27.35 | 1.98% | 1.23% to 2.45% | Retain |
| #15 | global_aggregate / 1 | 54.00 | 54.17 | -0.30% | -0.94% to 0.39% | Close |
| #15 | global_aggregate / 4 | 14.86 | 15.00 | -0.97% | -1.99% to 0.08% | Close |
| #16 | filter_project / 1 | 104.90 | 99.49 | 5.16% | 4.48% to 5.73% | Retain |
| #16 | filter_project / 4 | 28.66 | 27.35 | 4.54% | 3.46% to 5.43% | Retain |

PR #12 deletes 34 net lines of handwritten scalar comparison logic and reuses Arrow. Its measured filter/project savings are modest; simplification and shared kernel semantics justify retaining it. PR #15 adds approximately 100 production lines across three files plus eight new tests, but provides no stable benefit on the measured global SUM/COUNT query; it is deferred and removed from the later stack. This does not rule out benefit for every data type or future implementation. PR #16 eliminates filtering/copying a column used only by the predicate, saving about 5.4 ms at one thread and 1.3 ms at four threads on this workload. The documentation PR #14 remains open because evidence does not add execution complexity.

## All workloads and controls

Unchanged workloads are controls, not optimization targets. A small bootstrap interval does not eliminate binary-layout, hash-randomization, thermal or system confounders. For example, #12 shows about 0.9% slower single-thread global aggregation even though that workload does not execute its comparison change. Treat #12 timing as a small observed difference, not a proven isolated kernel effect. No unrelated control movement is attributed to a specific algorithm.

| Removed PR | Workload / threads | Without ms | With ms | Reduction | 95% interval |
|---|---|---:|---:|---:|---:|
| #12 | scan_only/1t | 106.94 | 106.97 | -0.03% | -0.69% to 0.68% |
| #12 | scan_only/4t | 29.08 | 29.08 | -0.03% | -0.93% to 0.66% |
| #12 | filter_project/1t | 101.31 | 99.49 | 1.79% | 1.04% to 2.57% |
| #12 | filter_project/4t | 27.91 | 27.35 | 1.98% | 1.23% to 2.45% |
| #12 | global_aggregate/1t | 53.71 | 54.17 | -0.86% | -1.66% to -0.05% |
| #12 | global_aggregate/4t | 14.93 | 15.00 | -0.47% | -1.22% to 0.29% |
| #12 | grouped_aggregate/1t | 74.42 | 73.86 | 0.75% | 0.23% to 1.27% |
| #12 | grouped_aggregate/4t | 20.24 | 20.17 | 0.31% | -0.76% to 1.62% |
| #15 | scan_only/1t | 107.23 | 106.97 | 0.25% | -0.04% to 0.51% |
| #15 | scan_only/4t | 29.08 | 29.08 | -0.02% | -1.21% to 0.87% |
| #15 | filter_project/1t | 100.21 | 99.49 | 0.71% | 0.20% to 1.23% |
| #15 | filter_project/4t | 27.47 | 27.35 | 0.42% | -0.23% to 0.85% |
| #15 | global_aggregate/1t | 54.00 | 54.17 | -0.30% | -0.94% to 0.39% |
| #15 | global_aggregate/4t | 14.86 | 15.00 | -0.97% | -1.99% to 0.08% |
| #15 | grouped_aggregate/1t | 74.01 | 73.86 | 0.20% | -1.09% to 1.58% |
| #15 | grouped_aggregate/4t | 20.18 | 20.17 | 0.03% | -0.60% to 0.79% |
| #16 | scan_only/1t | 107.30 | 106.97 | 0.30% | -0.11% to 0.71% |
| #16 | scan_only/4t | 29.15 | 29.08 | 0.25% | -0.91% to 1.04% |
| #16 | filter_project/1t | 104.90 | 99.49 | 5.16% | 4.48% to 5.73% |
| #16 | filter_project/4t | 28.66 | 27.35 | 4.54% | 3.46% to 5.43% |
| #16 | global_aggregate/1t | 54.31 | 54.17 | 0.27% | -0.69% to 1.12% |
| #16 | global_aggregate/4t | 15.02 | 15.00 | 0.11% | -0.38% to 0.69% |
| #16 | grouped_aggregate/1t | 75.15 | 73.86 | 1.72% | -0.04% to 3.96% |
| #16 | grouped_aggregate/4t | 20.41 | 20.17 | 1.15% | -0.00% to 2.43% |

## Retained stack compared with DataFusion

The measured `without15` source is verified byte-for-byte against the retained local harness after removal, for all core source and test files. These rows use the original without15 samples; no samples or source identities are relabeled. The individual #12/#16 effects above were measured in the original all-enabled background, not rerun as a factorial experiment after removing #15.

| Workload / threads | DataFusion ms | Retained Roc ms | DF/Roc |
|---|---:|---:|---:|
| scan_only/1t | 107.15 | 107.23 | 0.999x |
| scan_only/4t | 31.20 | 29.08 | 1.073x |
| filter_project/1t | 100.56 | 100.21 | 1.004x |
| filter_project/4t | 29.51 | 27.47 | 1.074x |
| global_aggregate/1t | 38.57 | 54.00 | 0.714x |
| global_aggregate/4t | 11.36 | 14.86 | 0.765x |
| grouped_aggregate/1t | 70.87 | 74.01 | 0.958x |
| grouped_aggregate/4t | 20.57 | 20.18 | 1.019x |

This is a synthetic warm-cache local Parquet workload, not a general engine ranking. Roc retains checked integer arithmetic and SUM; generated values do not overflow, and both routes produce equal results. The global workload uses Int64 SUM and COUNT; Float64/UInt64 global SUM performance is not measured here.

## Evidence and reproduction

- 20 process runs: four variants times five replicas, seven samples for four workloads at one/four threads; 1120 Roc and 1120 DataFusion timing samples, 2240 total.
- Every process checks full schema and the unordered output row multiset against native DataFusion before timing each case; every timed run checks row count. Two warm-ups per case. Engine order alternates per sample; variant order is randomized per replica.
- Metadata records the original core/local commit identities, exact transformations, source/binary hashes, lockfile identity and machine. Every process verifies compiled source and binary hashes, and all input file hashes must match.
- Core source archives, build logs, exact raw JSON/CSV results and scripts are published. DataFusion integration, full local-source archives and binaries remain local; it is not added to the production workspace by this documentation PR. Core archives are checksum snapshots, not standalone workspaces: their root manifests reference the local-only integration.
- Full local reproduction uses the preserved local harness commit and build.py/run.py in the original workspace; standalone local-source archives remain under target/remaining-pr-review/<variant>/local-source.tar.gz. Data directory generation and recorded input settings appear in earlier benchmark artifacts.
- All timing data were collected before removing PR #15. Updated retained heads and fresh post-removal correctness checks are recorded separately in core-source-verification.json; exact measured identities remain unchanged.

The reproduction script uses the saved prior-ledger.json and prs-before.json source identities rather than the mutable current PR stack. Closing/restacking PR #15 therefore does not change which revisions a repeated historical experiment builds. The local harness commit must be available in the local Git object database; its full source archives remain local.
