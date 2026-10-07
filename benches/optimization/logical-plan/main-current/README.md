# Merged main versus DataFusion

Fresh measurements on October 7, 2026, after all retained core optimizations were merged. Roc main: `291317e1c4ee43ca7a613f0a532b1f35fae5aab4`. The local-only harness `67fd05a3ecb4c852c96b396869ddfd3d1de5cca8` has exactly the same `src` and `tests` Git trees as main. Deferred PRs #9, #11 and #15 are absent. Earlier cumulative and ablation samples remain historical; none are reused or relabeled here.

Environment: Apple M1 Pro, rustc 1.95.0 (59807616e 2026-04-14), macOS-26.6.2-arm64-arm-64bit, DataFusion 55.1.0, Arrow 59.3.0; release build. The same 4,000,000 deterministic rows in eight Snappy Parquet files, 256 Int64 groups and NULLs are read by both engines. Batch size is 8192. Each thread configuration sets both runtime worker count and DataFusion target partitions to one or four.

## Results

Times are geometric means of five independent process medians, each based on ten timed samples. Positive elapsed gap means Roc is slower; negative means Roc is faster. Gap = `(Roc / DataFusion - 1) * 100`. Intervals bootstrap the five paired process clusters 10,000 times; they capture observed process variation, not every system or fixed-order confounder.

| Workload | Threads | DataFusion ms | Roc ms | Roc elapsed gap | 95% gap interval |
|---|---:|---:|---:|---:|---:|
| scan_only | 1 | 105.84 | 105.83 | -0.01% | -0.38% to +0.54% |
| scan_only | 4 | 31.14 | 29.13 | -6.45% | -6.90% to -5.88% |
| filter_project | 1 | 99.63 | 99.09 | -0.54% | -0.83% to -0.27% |
| filter_project | 4 | 29.43 | 27.39 | -6.93% | -7.33% to -6.56% |
| global_aggregate | 1 | 38.53 | 53.69 | +39.33% | +39.07% to +39.66% |
| global_aggregate | 4 | 11.36 | 14.87 | +30.95% | +30.26% to +31.56% |
| grouped_aggregate | 1 | 70.41 | 73.34 | +4.16% | +3.48% to +4.97% |
| grouped_aggregate | 4 | 20.59 | 20.29 | -1.49% | -1.97% to -0.91% |

Scan and filter/project are close at one thread and faster on Roc at four threads in this setup. Grouped SUM/COUNT is slightly slower on Roc at one thread and close at four threads. Global SUM/COUNT remains the largest gap at both thread counts. This comparison measures query elapsed time; it does not establish a specific cause of the remaining aggregate gap. Follow-up attribution needs an isolated accumulator benchmark or profile on this exact source.

These are warm-cache synthetic local Parquet queries, not a general engine ranking or TPC-H result. Int64 arithmetic and SUM remain checked in Roc. The generated values do not overflow, and results match DataFusion exactly. Float64 aggregation, high-cardinality/multiple-column grouping, joins, spilling and cold disk I/O are outside this run.

## SQL and execution path

**scan_only**

```sql
SELECT id, group_key, value, selector FROM t
```

**filter_project**

```sql
SELECT id, value + 1 AS adjusted FROM t WHERE selector > 50
```

**global_aggregate**

```sql
SELECT SUM(value) AS total, COUNT(value) AS count FROM t
```

**grouped_aggregate**

```sql
SELECT group_key, SUM(value) AS total, COUNT(value) AS count FROM t GROUP BY group_key
```

Both routes start from the same DataFusion SQL and analyzed, optimized logical plan. DataFusion creates its native physical plan; the local LogicalPlanConverter binds expressions and builds the Roc operator tree. Roc uses the DataFusion table provider Parquet scan with projection and pruning. Recorded plans and session flags are in each results.json. Scan-only reads four columns; filter/project reads three; global SUM/COUNT reads one; grouped SUM/COUNT reads two. Random selectors span each row group, so pruning cannot skip substantial data in this particular filter workload.

The filter workload still evaluates `value + 1` after filtering rows; the local converter activates the merged optional filter output-column projection so the predicate-only selector column is not filtered into the output. The integration and this activation code remain local. The production core supplies the API and kernels.

## Timing and correctness

- Five independently started sequential processes, four workloads, two thread counts, ten paired samples: 400 Roc plus 400 DataFusion timings, 800 total. Two warmups per engine/case. DataFusion runs first on even samples, Roc first on odd samples. Workload/thread order is fixed and recorded.
- Every case in every process compares the full output schema and unordered row multiset against native DataFusion before timing. All 40 comparisons pass. Every one of the 800 timed executions checks the expected output row count.
- Timers include scan/plan state reset, execution state initialization, complete output collection and Roc task cleanup. They exclude SQL/logical planning, physical planning/conversion, Roc graph construction, validation, warmups and output deallocation. Single planning observations in raw JSON are not a planning benchmark.
- The operating-system page cache is warm; Parquet decoding occurs on every execution. Both engines share the same files and session settings. File size and SHA-256 identities match across all processes.
- Roc release artifacts were explicitly cleaned before compilation. Build logs confirm both Roc and its local harness were rebuilt. Every process validates the compile-time source digest, captured executable digest, release flag, DataFusion/Arrow versions and Cargo.lock digest. No compilation or other benchmark overlapped timing.
- Runtime Git metadata in raw reports may describe the surrounding primary checkout because the compiled source is an isolated archive. The compiled source digest, preserved binary and explicit main/harness identities are authoritative; the dirty primary checkout was not used for this build.

## Evidence and reproduction

- [summary.json](summary.json) contains all process medians, paired intervals and exact differences; [metadata.json](metadata.json) records commits, source/binary/archive digests, environment and method; [input-files.json](input-files.json) records the dataset identity.
- process-0 through process-4 preserve full results.json (SQL, plans, settings and raw timings), samples.csv and run.log. Build and clean logs are retained. [core-source.tar.gz](core-source.tar.gz) contains unmodified main core code, tests, benches and main manifests without the integration.
- The DataFusion harness, its full local-source.tar.gz and executable remain local under target/main-datafusion-current. This evidence PR adds no integration or runtime dependency to main. Full engine-comparison reproduction requires that preserved local harness; the core archive alone cannot run DataFusion comparisons.
- In the original workspace, build.py extracts the recorded harness commit, checks core src/tests against recorded main, explicitly rebuilds and captures the binary. run.py uses the recorded eight-file input directory for five processes and computes the summary. document.py creates this evidence. build.py intentionally requires a fresh task directory and the recorded ledger/PR state rather than silently overwriting a previous run.
- To replay the preserved binary in the local workspace: `target/main-datafusion-current/benchmark --logical target/parquet-snappy/data-1791083916888251000 NEW_OUTPUT_DIR main-291317e-current 10`. Check all recorded digests before comparing timings. Input-generation settings and deterministic generator are retained in the local parquet_compare harness and earlier benchmark evidence.
