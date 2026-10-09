# Roc versus DataFusion after global aggregate merges

PRs #18, #34 and #35 were merged to main sequentially. This report measures the resulting core, rather than an unmerged candidate.

Main commit: [`285623a70e2ccb0d88a13847b8137b8033ab1c81`](https://github.com/ZENOTME/Roc/commit/285623a70e2ccb0d88a13847b8137b8033ab1c81).

| PR | Change | Merge commit |
|---|---|---|
| [#18](https://github.com/ZENOTME/Roc/pull/18) | Reduce global COUNT once per batch | `a3e054c0827a26394084cbf459188ff620d752e3` |
| [#34](https://github.com/ZENOTME/Roc/pull/34) | Bind global SUM to checked native loops and reuse group-zero IDs | `7d62bd567dde02f30972d65f8c594c85d388fab3` |
| [#35](https://github.com/ZENOTME/Roc/pull/35) | Vectorize integer SUM with a conservative prefix safety bound | `285623a70e2ccb0d88a13847b8137b8033ab1c81` |

## Source and validation

The final main Git tree is exactly identical to reviewed core `b66a66cce04e6133fe558356b75e46feae83461f`, which passed 133 release tests and all-target compilation. Each restacked head and merged tree was compared with its reviewed version. No implementation change was introduced while restacking. Tests were not rerun solely for the identical merge tree.

The local DataFusion harness was rebuilt in release mode. Before timing, every core source and test file was checked byte for byte against merged main, including file-set equality. Compile-time source hashes and retained executable hashes identify both measured binaries. The local workspace adds the DataFusion converter and benchmark harness; core behavior comes from the verified main source. DataFusion integration remains local and was not included in these merges.

Every global probe timed output and kernel result matched its expected value. The SQL suite passed 40 full schema/unordered-row comparisons before timing and row-count checks in every timed sample. Input file hashes were unchanged after all measurements.

## Focused global SUM/COUNT

Apple M1 Pro, release, Arrow 59.3.0/DataFusion 55.1.0, 4M nullable Int64 rows, eight Snappy Parquet files, batch size 8192. The query is `SELECT SUM(value), COUNT(value)`. Resident input uses the same decoded batches; loading is excluded from the resident timer. Planning, conversion and graph construction excluded. Full execution, decoding where applicable, shutdown and result extraction included.

Ten independent processes, ten timed samples per case/engine and two warmups. DataFusion, default Roc and the retained diagnostic no-yield control rotate order within each sample. Processes run sequentially with no compilation during timing and no samples trimmed. The table uses default Roc only.

Values are geometric means of per-process medians in milliseconds. Positive elapsed change means Roc takes longer than DataFusion. 95% intervals bootstrap paired process-level log ratios (10000 resamples).

| Input | Threads | Roc ms | DataFusion ms | Roc elapsed change | 95% paired interval |
|---|---:|---:|---:|---:|---:|
| Parquet | 1 | 38.1925 | 38.6793 | -1.26% | [-1.67%, -0.89%] |
| Parquet | 4 | 10.8083 | 11.4712 | -5.78% | [-6.19%, -5.35%] |
| Resident | 1 | 1.4133 | 1.7798 | -20.59% | [-21.50%, -19.61%] |
| Resident | 4 | 0.8814 | 0.7501 | +17.50% | [+12.71%, +22.69%] |

Roc takes less time in both Parquet configurations and resident one-thread execution in this experiment. Resident four-thread execution remains 17.50% slower, with an interval above zero. Merging itself is not an additional optimization: differences from earlier absolute timings are measurements from separate runs, not an attributed merge speedup.

## Broader SQL comparison

Five independent processes, ten timed samples per engine/case and two warmups. Both engines start with the same analyzed, optimized DataFusion logical SQL plan; native DataFusion physical planning is compared with the local LogicalPlanConverter producing a Roc pipeline. Session settings are identical. Native scan planning and TableProvider::scan drive their respective Parquet readers. DataFusion-first and Roc-first alternate within the timed samples. Every full result is checked before timing.

Execution, scan-state reset, result collection and task cleanup are timed; planning/conversion/graph construction and output deallocation are excluded. This suite uses a general output collector, while the focused probe includes scalar result extraction, so their close global-aggregate timings are separate measurements.

| Query | Threads | Roc ms | DataFusion ms | Roc elapsed change | 95% paired interval |
|---|---:|---:|---:|---:|---:|
| scan_only | 1 | 106.7555 | 106.4878 | +0.25% | [-0.39%, +0.75%] |
| scan_only | 4 | 29.1059 | 31.0984 | -6.41% | [-7.19%, -5.47%] |
| filter_project | 1 | 99.4695 | 100.1074 | -0.64% | [-0.97%, -0.40%] |
| filter_project | 4 | 27.4144 | 29.4840 | -7.02% | [-7.40%, -6.66%] |
| global_aggregate | 1 | 38.3082 | 38.5084 | -0.52% | [-1.02%, +0.02%] |
| global_aggregate | 4 | 10.7036 | 11.3895 | -6.02% | [-6.31%, -5.79%] |
| grouped_aggregate | 1 | 73.5429 | 70.5992 | +4.17% | [+3.48%, +4.88%] |
| grouped_aggregate | 4 | 20.3371 | 20.6472 | -1.50% | [-2.61%, +0.39%] |

Four-thread scan and filter/projection are 6.41% and 7.02% lower than DataFusion in this suite. Single-thread grouped aggregation remains 4.17% slower, with an interval above zero. Single-thread scan and four-thread grouped aggregation are close and have intervals spanning zero. Single-thread global aggregation is also close in the broader suite. These are whole-query comparisons, not isolated attribution to #18/#34/#35.

The remaining demonstrated gaps are the short resident four-thread global query and single-thread grouped aggregation. This report does not identify their causes or claim Roc is faster for every query. The workload is synthetic with a warm OS cache and 256 groups; this is not TPC-H and does not establish cold-storage, high-cardinality or other-type behavior.

## Retained evidence

`metadata.json`: merged commit/tree identity, reviewed/restacked/merge commits, source/binary/data hashes, process counts and validation. `summary.json`: statistics and every process median. `global/process-*.json`: all 1200 full-path and 2200 kernel timings from the focused probe. `suite/process-*/results.json` and `samples.csv`: all 800 broader SQL timings and plans. `build.py`, `run.py`, `summarize.py` preserve the procedure. Source snapshots and measured binaries remain in the local artifact directory. Runtime checkout information in suite outputs describes the surrounding checkout, as noted by the harness; compile-time source hashes and this report’s verified main identity establish the measured core.
