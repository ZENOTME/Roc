# Global batch COUNT ablation

October 7, 2026. Compare main `291317e1c4ee43ca7a613f0a532b1f35fae5aab4` with candidate core `0ddd08565871b7e53faa09f738d0cfa25579635c` through the unchanged local-only DataFusion harness. Candidate local source: `590c6875c80689172aa5f72ad1097ed1ea144d0a`; its core source and tests match the candidate core commit exactly. Baseline local source: `67fd05a3ecb4c852c96b396869ddfd3d1de5cca8`; its core source/tests match main. DataFusion integration and executables remain local.

## Isolated change

When constructing an ungrouped aggregate executor, bind ordinary COUNT to a batch update function. It computes `selected_rows - argument.logical_null_count` (or selected_rows for COUNT(*)) and uses checked addition once per batch. Grouped COUNT and COUNT DISTINCT retain the old update functions. Existing FILTER and argument evaluation are shared, including filtering before fallible argument evaluation. No SUM kernel, group-ID generation, merge, scan, projection or pipeline code changes.

The existing group-ID vector still supplies the selected row count and is still built for every batch. This experiment deliberately measures only COUNT reduction; it does not claim to eliminate all ungrouped aggregation overhead. On overflow, COUNT reports the same error and retains the same successful-prefix count as the row loop.

## Global SUM/COUNT query

```sql
SELECT SUM(value) AS total, COUNT(value) AS count FROM t
```

All measurements below are fresh. Primary estimates use the unchanged global SUM/COUNT query alone at one/four threads: ten independent paired processes per variant, ten samples per case/engine, with balanced five main-first/five candidate-first pairs in randomized order. Both focused binaries rebuild the same core snapshots and apply only the identical workload-iterator filter in the local harness. This avoids preceding multi-million-row scan output validation and sorting, while retaining the same SQL, storage, timed execution path, correctness checks and configuration. Values are geometric means of ten process medians. Positive reduction means less Roc elapsed time.

| Threads | Main Roc ms | Batch COUNT Roc ms | Saved ms | Roc reduction | 95% paired interval | Main gap vs native DF | Candidate gap vs native DF |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 54.05 | 46.68 | 7.37 | 13.63% | 12.05% to 15.16% | +39.36% | +19.60% |
| 4 | 15.03 | 12.94 | 2.08 | 13.86% | 12.88% to 14.89% | +30.42% | +13.08% |

Gaps use the native DataFusion timings collected alongside each respective variant, recorded separately below. They are elapsed-time ratios, not throughput gains. COUNT reduction produces a clear improvement in the measured mixed SUM/COUNT query, but the candidate remains slower than native DataFusion. The unchanged SUM and other ungrouped work are candidates for further attribution, not proven explanations of the entire residual gap.

## Complete-query runs and observed interference

Two complete four-query experiments preceded the focused comparison; both are preserved in full. In the first run, the final main process slowed markedly in unchanged queries and native DataFusion controls (four-thread native scan ~61 ms, filter ~117 ms rather than ~31/29 ms). The entire five-pair experiment was repeated, not selectively trimmed. The repeat then showed a single-thread candidate global outlier alongside a native DataFusion outlier (~73/61 ms rather than ~47/39 ms). Its unnormalized single-thread reduction is 7.0%, with a wide interval crossing zero. This uncertainty is retained below; it is not presented as a stable isolated estimate.

Consequently the focused comparison uses only the target query, ten pairs and balanced variant order. No samples in any experiment are removed. Full-query controls remain useful evidence of interference and semantic consistency; their movements are not attributed to COUNT. Raw initial-run data and summary appear under pilot; the complete repeat is under main/count. The focused run is under focused. Intervals bootstrap the corresponding process clusters 10,000 times and do not eliminate all machine or binary-layout confounders.

| Query / threads | Main Roc ms | Candidate Roc ms | Roc reduction | 95% interval | DF with main ms | DF with candidate ms | DF control change |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_only/1t | 108.17 | 116.43 | -7.64% | -25.85% to +1.29% | 108.20 | 118.84 | +9.83% |
| scan_only/4t | 29.65 | 29.19 | +1.57% | -0.65% to +4.39% | 31.64 | 31.53 | -0.36% |
| filter_project/1t | 100.91 | 101.95 | -1.03% | -3.99% to +1.05% | 101.94 | 109.52 | +7.43% |
| filter_project/4t | 27.66 | 27.60 | +0.20% | -0.94% to +1.51% | 29.74 | 29.73 | -0.01% |
| global_aggregate/1t | 55.09 | 51.21 | +7.04% | -11.01% to +15.26% | 39.35 | 43.15 | +9.64% |
| global_aggregate/4t | 14.99 | 12.98 | +13.43% | +12.90% to +13.75% | 11.51 | 11.49 | -0.16% |
| grouped_aggregate/1t | 75.79 | 75.04 | +0.99% | -1.43% to +3.31% | 71.87 | 72.71 | +1.16% |
| grouped_aggregate/4t | 20.72 | 20.37 | +1.72% | -1.53% to +6.63% | 20.65 | 20.92 | +1.26% |

## Method, validation and reproduction

Environment: Apple M1 Pro, rustc 1.95.0 (59807616e 2026-04-14), macOS-26.6.2-arm64-arm-64bit, release builds, DataFusion 55.1.0 / Arrow 59.3.0. Same 4M deterministic rows in eight Snappy Parquet files, 256 Int64 groups, batch size 8192, one/four runtime workers and target scan partitions. Warm OS page cache; Parquet decoding is included.

- Complete four-query pilot: 1600 timings and 80 full correctness comparisons. Complete repeat: 1600 timings and 80 comparisons. Focused ten-pair run: 800 timings and 40 comparisons. Total: 4000 timings and 200 complete schema/unordered-row comparisons, all passing. Every timed execution checks output row count. Input file identities match; exact SQL, plans and session settings are retained.
- Timed: scan/plan state reset, execution initialization, full output collection and Roc task cleanup. Excluded: SQL/logical/physical planning, conversion, Roc graph construction, validation, warmups and output deallocation.
- Core release tests: 125 pass, including three new regression tests for logical NULLs, slices, COUNT(*), DISTINCT and overflow prefix state. Existing tests cover global/grouped merging and FILTER-before-argument behavior.
- Baseline executable is the previously captured clean main build, verified by SHA256 before measurement. Candidate explicitly cleans Roc release artifacts and rebuilds both Roc and harness. Source, executable and lockfile hashes are verified in every process; no compilation overlaps timing.
- Core archives use the recorded core commits and their production manifests, without a DataFusion workspace member. Compiled workspace manifests add the local harness; that exact lockfile digest is recorded separately. Full local-source archives and binaries remain local.
- [focused/summary.json](focused/summary.json) records primary process medians and paired intervals; [summary.json](summary.json) records the complete repeat; [pilot/summary.json](pilot/summary.json) records the first full run. [metadata.json](metadata.json), [input-files.json](input-files.json), build logs and core-release-tests.json record identities and validation. Every process directory preserves full results.json, samples.csv and run.log.
- In the original workspace, build.py requires the preserved main-datafusion-current executable and core/local candidate branches, verifies equal integration/manifests, then captures the candidate executable. run.py remeasures both complete binaries. preserve_pilot.py retains the first entire run. focused_build.py applies the identical single workload filter and cleanly rebuilds both binaries; focused_run.py runs the balanced ten-pair comparison. document.py produces this evidence. Full reproduction requires the preserved local harness; core archives alone do not provide the DataFusion comparison.
- This is a synthetic warm-cache query comparison, not TPC-H or a general engine ranking. Roc retains checked integer SUM and COUNT. Inputs do not overflow; explicit overflow regression tests exercise the failure path. Neither Float64 SUM nor high-cardinality grouping/spilling is measured.
