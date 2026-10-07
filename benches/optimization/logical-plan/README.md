# Roc core optimizations: local DataFusion comparison

**New candidate (October 7, 2026):** [global batch COUNT ablation](global-count-batch/README.md), PR #18, restores batch reduction for ungrouped non-DISTINCT COUNT only. The unchanged global SUM/COUNT query is 13.6% faster at one thread and 13.9% faster at four threads in a balanced ten-pair focused comparison. Remaining native DataFusion gaps are 19.6% and 13.1%. Main and SUM are unchanged; this candidate is not merged. Two noisy complete-query rounds and the focused comparison preserve all 4000 timings and 200 complete result checks, including machine interference and uncertainty.

**Latest measurement (October 7, 2026):** [merged main versus DataFusion](main-current/README.md) freshly measures main `291317e1c4ee43ca7a613f0a532b1f35fae5aab4` with all retained optimizations, five independent processes and 800 timing samples. Scan/filter are near parity at one thread and about 6%-7% faster on Roc at four threads. Grouped SUM/COUNT is 4.2% slower at one thread and 1.5% faster at four. Global SUM/COUNT remains 39.3% and 31.0% slower respectively. Earlier cumulative tables include now-deferred code and do not describe this main revision. All fresh raw results, plans, input hashes and source identities are preserved.

New: [aggregate update binding ablation](aggregate-binding/README.md) separates dispatch binding, MIN/MAX direction specialization, and the complete source amendment. It records neutral results, regressions and process variation; it is an accumulator microbenchmark, not a fresh end-to-end DataFusion comparison.

> Current review baseline: PR #5 was closed and removed from the active code series. PR #6 now targets main. The historical timings below measure their recorded revisions, which include the old global reduction; they are not measurements of the amended series. See the PR #5 closure note at the end for fresh correctness validation.

October 4, 2026. DataFusion 55.1.0 / Arrow 59.3.0; release builds on Apple M1 Pro.

**Measurement version:** the query tables below are historical measurements of earlier scalar-constant revisions, including the subsequent RunArray experiment. They do not measure the current ScalarValue / ColumnValue revision.

**Current evaluation API (October 5, 2026):** expressions return `ColumnValue::Scalar(ScalarValue)` or `ColumnValue::Array(ArrayRef)`. Primitive constants remain inline. Each bound function uses `fn(&[ColumnValue]) -> Result<ColumnValue>` and dispatches representations internally; scalar/scalar results stay scalar. Ordinary array consumers use `into_array(num_rows)`. The previous RunArray constant and decoding paths have been removed. See the [fresh expression comparison](column-values/README.md) for source identities, paired samples and timing scope. The ordinary in-place conjunction algorithm is already merged through PR #17 and retains no scratch bitmap.

The DataFusion adapter is retained only as a local benchmark harness on `codex/local-datafusion-benchmark`; it is not part of the merge series. PR #2 is closed. Core optimization PRs follow merged #3 → #17 → #4 → … → #13 → #15 → #16, starting from `main`. This report is a separate documentation PR.

Both engines start from the same SQL and analyzed, optimized DataFusion logical plan. Native DataFusion physically plans the query; `LogicalPlanConverter` binds expressions, plans table scans and returns a Roc operator tree. Roc owns query kernels, aggregation stages and pipelines.

Input: the same 4,000,000 deterministic rows in eight Snappy Parquet files, 256 Int64 grouping keys, NULL values every 17th row and NULL selectors every 23rd row. SHA-256 identities are recorded in every raw report. Scan projection is active: scan-only reads four columns; filter/project reads three; global SUM/COUNT reads one; grouped SUM/COUNT reads two. Both routes retain the table provider's Parquet predicate pruning. Random selectors span the predicate in each row group, so this is not a selective row-group-skipping benchmark.

Each process uses two warmups and ten paired samples for each workload at one and four runtime threads. Engine order alternates within samples. SQL planning, logical optimization, physical planning/conversion, Roc graph construction, validation and result deallocation are excluded. Scan/plan state reset, execution initialization, output collection and Roc task cleanup are included. OS page cache is warm; no cold-I/O or TPC-H claim is made.

Roc retains checked integer arithmetic and SUM, and `(count=0, sum=0)` empty AVG states. DataFusion uses its defaults, including wrapping integer SUM; generated values do not overflow, so results match exactly for these inputs. No speedup is purchased by removing Roc's overflow checks. Parallel floating-point aggregation and spilling are outside these timing workloads.

## Historical final comparison

The table uses the median of the three independent process medians for the final PR #16 snapshot. DF/Roc > 1 means Roc is faster. The baseline column is the single-process logical-converter checkpoint on main's unoptimized core (#2), not an old manual-plan baseline.

| Workload | Threads | Baseline Roc (ms) | Final DataFusion (ms) | Final Roc (ms) | DF/Roc | Roc time reduction vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| scan_only | 1 | 106.166 | 104.238 | 104.671 | 0.996x | +1.4% |
| filter_project | 1 | 147.450 | 97.822 | 97.301 | 1.005x | +34.0% |
| global_aggregate | 1 | 66.815 | 37.696 | 38.657 | 0.975x | +42.1% |
| grouped_aggregate | 1 | 155.799 | 69.225 | 70.186 | 0.986x | +55.0% |
| scan_only | 4 | 28.822 | 30.570 | 29.110 | 1.050x | -1.0% |
| filter_project | 4 | 40.757 | 28.944 | 27.062 | 1.070x | +33.6% |
| global_aggregate | 4 | 18.419 | 11.106 | 10.761 | 1.032x | +41.6% |
| grouped_aggregate | 4 | 41.115 | 20.182 | 19.270 | 1.047x | +53.1% |

Single-thread scan and filter/project are effectively at parity; global and grouped aggregation remain approximately 2.5% and 1.4% slower, respectively. At four threads, Roc is about 3–7% faster across these four workloads. These are workload-specific observations; small differences should not be treated as a general engine ranking.

## Original optimization sequence through the real converter

One process with ten samples per case at each cumulative checkpoint. Every checkpoint passed release workspace tests and exact schema/unordered-row-multiset validation. Positive step reduction means lower Roc elapsed time than the preceding checkpoint. This is a sequential cumulative experiment, not a statistically isolated attribution: process variance, caching and interactions can affect adjacent deltas. Small and negative deltas are retained; they are not presented as confirmed wins.

| PR | Change | Filter 1t ms | Global 1t ms | Grouped 1t ms | Filter 4t ms | Global 4t ms | Grouped 4t ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| #2 | Converter + main core (baseline) | 147.450 | 66.815 | 155.799 | 40.757 | 18.419 | 41.115 |
| #3 | Predicate bitmap | 108.518 | 68.474 | 150.021 | 30.195 | 18.332 | 40.012 |
| #4 | Scalar constants | 104.772 | 68.511 | 151.848 | 28.751 | 18.282 | 40.486 |
| #5 | Global path / COUNT batch reduction; checked SUM | 102.659 | 69.378 | 151.523 | 28.777 | 19.210 | 41.178 |
| #6 | Typed SUM states | 105.222 | 47.935 | 139.147 | 28.876 | 13.133 | 36.840 |
| #7 | Integer grouping index | 104.548 | 46.712 | 76.018 | 28.840 | 13.085 | 20.870 |
| #8 | Argument preparation / validation | 104.457 | 47.885 | 73.158 | 28.656 | 12.991 | 20.048 |
| #9 | NULL density loops | 105.913 | 46.301 | 71.173 | 28.798 | 12.684 | 19.641 |
| #10 | Whole-batch forwarding | 104.589 | 46.383 | 71.549 | 28.852 | 12.686 | 19.240 |
| #11 | Pending-only cancellation waiters | 104.677 | 45.770 | 70.228 | 28.850 | 12.716 | 19.161 |
| #12 | Arrow scalar comparison | 103.477 | 45.997 | 70.721 | 28.298 | 12.852 | 19.183 |
| #13 | Borrowed direct arguments | 102.703 | 45.926 | 70.407 | 28.180 | 12.687 | 19.211 |

| Added mechanism | Filter 1t ms | Global 1t ms | Grouped 1t ms | Filter 4t ms | Global 4t ms | Grouped 4t ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original eleven optimizations (#13) | 102.102 | 45.483 | 69.920 | 28.335 | 12.626 | 19.126 |
| + Local checked global SUM (#15) | 102.045 | 38.689 | 69.968 | 28.237 | 10.758 | 19.157 |
| + Filter output column pruning (#16) | 97.301 | 38.657 | 70.186 | 27.062 | 10.761 | 19.270 |

These last three rows use three process medians, so they are separate from the single-process cumulative table.

The measured #11 checkpoint also optimizes pending cancellation registration inside the local DataFusion adapter. That adapter change remains local and is excluded from the core PR, so its adjacent timing delta cannot be attributed solely to the Roc scan change. The #16 comparison uses the local converter to activate Roc's new output-column projection API; the core PR supplies that API, while callers must select columns and bind their downstream expressions.

The #5 checkpoint uses static typed checked iteration. Its earlier dynamic-iterator implementation regressed global aggregation; the discarded run is retained in `preliminary/pr-05-dynamic-iterator`. #6 replaces that representation entirely, so correcting #5 changes no measured #13/#15/#16 source content. The original core-only extraction preserved the measured core and regression test sources byte-for-byte. Those source identities and test counts are retained in [core-source-verification.json](core-source-verification.json) as historical evidence. The subsequent RunArray amendment changes core sources at #4 and above; see the amendment record for current identities and fresh validation.

## Repeated isolated checks for the two new mechanisms

The #13, #15 and #16 binaries were compiled once each, then run in three independent processes per binary. Outer checkpoint order is 13→15→16, then 16→15→13, then 13→15→16; internal engine order still alternates. No compilation overlaps a timed run. Every report records the executable hash, compiled source hash/label, clean checkout SHA, input hashes, plans, session settings, raw timings and result validation.

| Mechanism | Threads | Before Roc (ms) | After Roc (ms) | Reduction | Per-process reductions |
| --- | ---: | ---: | ---: | ---: | --- |
| Local checked SUM | 1 | 45.483 | 38.689 | 14.9% | 14.8%, 15.1%, 15.0% |
| Local checked SUM | 4 | 12.626 | 10.758 | 14.8% | 14.2%, 14.8%, 14.6% |
| Predicate-only column pruning | 1 | 102.045 | 97.301 | 4.6% | 4.4%, 4.6%, 4.4% |
| Predicate-only column pruning | 4 | 28.237 | 27.062 | 4.2% | 4.7%, 4.2%, 4.2% |

Local SUM holds the total and validity in local variables while visiting valid values in original order, then writes the state once per batch. It retains successful-prefix state on overflow and preserves floating signed zero/order. Column pruning evaluates the predicate on the original input, selects only columns needed above the filter, then applies row filtering; computed projection expressions still execute after selection. Bound column indices are remapped once to the compact output schema.

## Source identities and reproduction

| Repeated checkpoint | Source commit | Compiled source SHA-256 |
| --- | --- | --- |
| #13 | `b1f6fe72f3ca51657b2fb435547cdaba10f1137b` | `3e80f4e0bced0ea7e015a319915b4f618ed42c7b146c0b2daea9702d62992bde` |
| #15 | `9ecb2b76ae98197b38fe1d645c7b7495ddc73440` | `9fd11d3c52137a4c5af36f035f2a66936715940b9df22dc59e9685a82825b8e9` |
| #16 | `688df1ac12005f2f36af7e739e9611d5d1c2d0f1` | `966508ef13f5da14cd5a38bfef0e1c0d1277bcae5f6c86f647c7a302770b68d9` |

The source hash covers sorted tracked `src`, `integrations`, `Cargo.toml` and `Cargo.lock` paths/content. Cumulative identities, file digests and raw-report digests are in [archived raw results and identities](https://github.com/ZENOTME/Roc/blob/1da4ca4843f387c67d7c25ebbb428f8715247bc3/benches/optimization/logical-plan/index.json). The original cumulative checkpoints precede the added decimal-AVG rejection; the final repeated snapshots include that validation. The measured integer queries do not exercise decimal AVG. Historical manual-plan/wrapping-SUM archives are retained in the pre-reorganization snapshot and remain separate evidence.

```sh
cargo run --release --locked -p roc-datafusion --example parquet_compare -- \
  --logical EXISTING_PARQUET_DIRECTORY OUTPUT_DIRECTORY BUILD_LABEL 10
```

Generate the deterministic Parquet files with the existing `parquet_compare --rows 4000000 --files 8 --compression snappy ...` command, then reuse those exact files across checkpoints. Use `ROC_ABLATION_SOURCE_SHA256` and `ROC_ABLATION_BUILD_LABEL` when compiling to embed identities. Release tests, exact result validation and timed row-count checks must pass before accepting timings. Run this command from the preserved local harness checkout, not the core-only Roc checkout. See [the archived harness documentation](https://github.com/ZENOTME/Roc/blob/1da4ca4843f387c67d7c25ebbb428f8715247bc3/integrations/datafusion/README.md).

Raw results are gzipped JSON (including full plans); sample CSVs remain directly readable. `cumulative/pr-XX` stores original eleven-optimization checkpoints, `repeated/process-N/pr-XX` stores isolated repeated comparisons, and `preliminary` retains the first candidate runs. Preliminary runs are not counted as independent confirmation in the final table.

The raw `cumulative`, `repeated` and `preliminary` directories are retained in the [immutable measurement archive](https://github.com/ZENOTME/Roc/blob/1da4ca4843f387c67d7c25ebbb428f8715247bc3/benches/optimization/logical-plan); they are not duplicated in this documentation PR. The adapter remains available locally for future comparisons. The top-level README is unchanged. PR #7 adds only the core integer-key index dependency `foldhash` 0.2.0; all existing locked package versions are preserved. No DataFusion dependency or workspace member is included.

## Historical RunArray amendment measurements

The earlier PR #4 revision evaluated Const as an Arrow `RunArray<Int64Type>` with one physical value and logical batch length. Binary kernels dispatch on the array representation; numeric array/constant arithmetic extracts the native value once and uses Arrow unary kernels. Integer overflow remains checked. Ordinary projection, Boolean and aggregate consumers materialize via Arrow cast at their type boundaries. Only Const produces encoded outputs; a binary input spanning multiple runs is rejected. Slices use the active physical run index.

The table below compares the original PR #4 with amended PR #4 using the same local logical-plan converter. The original PR #4 already avoided constant broadcasting using bound literal operands. This experiment measures the representation rewrite, not the entire constant-broadcast optimization relative to main. Three independent processes per build, two warmups and ten alternating paired samples per workload/thread count; each cell is the median of process medians. Outer order was old #4 → new #4 → new final, reversed in the second process and repeated in the third. All binaries were compiled before timing. Inputs and timing boundaries match the historical experiment above. Exact schema and full unordered row-multiset equality were checked before timing, and row counts in every sample.

| Workload | Threads | Old #4 Roc ms | New #4 Roc ms | Elapsed reduction | New final DF ms | New final Roc ms | DF/Roc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| scan_only | 1 | 107.315 | 108.030 | -0.7% | 107.689 | 106.429 | 1.012x |
| filter_project | 1 | 106.340 | 106.617 | -0.3% | 99.780 | 100.288 | 0.995x |
| global_aggregate | 1 | 69.481 | 68.739 | +1.1% | 38.691 | 39.861 | 0.971x |
| grouped_aggregate | 1 | 155.337 | 150.670 | +3.0% | 71.421 | 72.223 | 0.989x |
| scan_only | 4 | 29.198 | 28.938 | +0.9% | 30.884 | 29.069 | 1.062x |
| filter_project | 4 | 29.041 | 29.330 | -1.0% | 29.316 | 27.734 | 1.057x |
| global_aggregate | 4 | 18.702 | 18.517 | +1.0% | 11.469 | 11.150 | 1.029x |
| grouped_aggregate | 4 | 41.564 | 40.209 | +3.3% | 20.438 | 19.482 | 1.049x |

Filter/project changes by −0.3% elapsed reduction at one thread and −1.0% at four threads. There is no demonstrated additional speedup from this representation rewrite. Other query timings fluctuate despite not exercising constant arithmetic, so their differences are not attributed to RunArray. The new final stack stays near DataFusion on these inputs: single-thread aggregation is about 1–3% slower, while four-thread queries have a DF/Roc ratio of about 1.03–1.06.

Fresh validation: 87 core tests at amended #4, 111 at the full core stack, and 127 tests including the separately retained local DataFusion harness and its documentation example. Restack-sensitive intermediate checkpoints are recorded in the source verification file. The new core/tests trees exactly match the local snapshots used for new #4 and new final.

[All new raw reports, input/build identities and full plans](runarray-raw.json.gz), [new sample CSV](runarray-samples.csv), [process-median summary](runarray-summary.json). Current core and local snapshot mappings are recorded in [core-source-verification.json](core-source-verification.json). The original core-only mapping in that file remains historical; it is not asserted for the amended code. The integration source is kept local and is not included in any merge PR.

## Previous single evaluation entry amendment

The October 5 revision removes `evaluate_flat`. Every expression combination calls `evaluate`, which returns an `ArrayRef` of logical batch length with either ordinary or run-end-encoded representation. Binary kernels dispatch on storage representation. Negation computes encoded constants once while retaining checked integer errors, and Arrow NULL tests consume logical validity directly. Projection output, Boolean kernels, branch interleave inputs and current accumulators inspect and decode encoded arrays at their own consumption points. No replacement evaluation method or custom array wrapper is introduced.

Fresh release validation passed 88 core tests at #4, 96 at #5, 112 at the final core stack, and 128 including the separately retained local DataFusion harness and its documentation example. The local harness exactly mirrors the new final core/test source trees. This interface amendment has no new performance measurement; all earlier timing tables retain their recorded source identities.

## Previous Arrow accessor amendment

The October 5 accessor revision retains the single `evaluate -> ArrayRef` API and uses Arrow `RunArray::downcast`/`TypedRunArray` with `ArrayAccessor` to read the active constant value. Numeric array/constant arithmetic reads the primitive once before running the ordinary-array kernel. The active physical value supplies validity; a sliced run uses Arrow logical indexing rather than assuming physical value zero. PR #12 continues to delegate comparisons through Arrow scalar kernels using a borrowed one-value slice.

Constant predicates select all or no logical positions without decoding a Boolean input. Filter returns the original batch or an empty slice, applying an optional output projection first in PR #16. NOT reads its constant once and allocates only the result. AND/OR combine constants directly with the existing Boolean bitmap while preserving three-valued logic and evaluation of all arguments. COALESCE skips non-final all-NULL arguments before interleave preparation because they contribute no output positions.

Projection outputs still need ordinary array types to match their RecordBatch schema. CASE/COALESCE still normalize contributing encoded pieces when interleave combines them with ordinary arrays, and current accumulator interfaces require ordinary typed inputs. These conversions remain at the relevant consumption boundaries; no additional evaluation API or custom array wrapper is introduced.

Fresh release validation passed 91 core tests at #4 and 115 at the final core stack, plus 131 in the separately retained local DataFusion workspace including its documentation example. Regression coverage includes sliced active values and NULLs, Boolean Kleene truth tables, operand order, checked overflow, lazy branches, and constant filters with zero/duplicate projected columns and invalid indices. The local harness exactly matches the final core/test source trees. This amendment has not been newly benchmarked; no speedup is inferred from the removal of input decoding.

## Previous shared materialization helper

The current revision adds `utils::arrow::materialized_runarray(ArrayRef) -> Result<ArrayRef>`. Arrow cast expands run-end-encoded storage to its values type, preserving logical lengths, slice offsets and NULLs, including multiple runs. Other arrays are returned unchanged without copying. Arrow decoding failures propagate to the caller. Filter and predicate selection use this helper and their existing Boolean path; the previous constant-filter fast return has been removed. Projection, CASE/COALESCE contributing pieces and current aggregate arguments reuse the same helper. COALESCE uses its existing per-row NULL handling after materialization, with no additional all-NULL argument fast path. Arithmetic and Boolean expression kernels retain direct constant access, and `evaluate` remains the only evaluation entry.

Fresh release validation passed 93 tests at #4, 117 at the final core checkpoint and 133 in the local DataFusion workspace including its documentation example. Helper coverage verifies unchanged ordinary-array identity and decoded numeric/Boolean/string multi-run arrays, NULLs, slices and empty results. The local harness exactly matches final core/test sources. No new benchmark was run for this refactoring.

## Previous scratch-based conjunction revision

[PR #17](https://github.com/ZENOTME/Roc/pull/17) is based directly on main and passed 84 standalone release tests. It contains concrete And/Or evaluations and in-place operations on ordinary Boolean arrays. RunArray access, `boolean_constant`, encoded-input dispatch and tests belong to PR #4, which stacks on #17.

The shared const-generic loop evaluates one argument, merges it immediately, and releases its temporary result before evaluating the next. The first evaluation/update error stops processing in argument order; absorbing Boolean values do not skip later arguments. Empty batches skip children. Concrete evaluations store only their arguments, with no operation mode/function pointer.

All bitmap calculations now use Arrow APIs. Non-nullable updates use `apply_bitwise_binary_op`. The first nullable input constructs validity with `BooleanBuffer::from_bitwise_binary_op`; that allocation is then reused. Both-nullable updates normalize NULL value bits to the operation's neutral value (TRUE for AND, FALSE for OR), using a lazily allocated scratch bitmap. Consequently merged validity is `(Lvalid & Rvalid) | decisive(result_value)`, where FALSE decides AND and TRUE decides OR. Five Arrow in-place operations update scratch, values and validity; no handwritten u64 loop remains. Buffers are reused across arguments and transferred to the final BooleanArray without copying. The extra scratch bitmap costs approximately one bit per logical row and is allocated only when needed.

This preserves Arrow/DataFusion Kleene results while retaining Roc's eager error behavior. DataFusion's general merge calls Arrow `and_kleene`/`or_kleene`, which allocate output buffers; DataFusion additionally uses short-circuiting/preselection. This PR does not adopt that different evaluation behavior. PR #4 reads encoded Boolean constants directly and uses Arrow unary/binary operations, maintaining the same neutral-NULL invariant.

Fresh release validation passed 98 core tests at #4, 122 at the final core stack, and 138 in the local DataFusion workspace including its documentation example. Tests compare each intermediate result with Arrow Kleene across boundaries and slices, verify stable values/validity/scratch addresses, immutable inputs, lazy allocation, final buffer transfer and ordered errors. Core/test trees match the local harness.

### Previous Arrow reuse microbenchmark

The [standalone benchmark](conjunction-arrow/README.md) records three independent processes, nine alternating samples each, four inputs per batch, and exact output equality. The general Arrow sequence is a reference for DataFusion's bitmap merge only; it does not measure the DataFusion executor or short-circuit/preselection paths. Earlier query timings are unchanged.

| Rows | NULLs | Operator | Previous Roc ns/batch | Arrow reuse ns/batch | General Arrow Kleene ns/batch | Change vs previous |
|---:|---|---|---:|---:|---:|---:|
| 8192 | none | AND | 413.4 | 405.4 | 541.2 | -1.9% |
| 8192 | none | OR | 379.9 | 370.0 | 538.0 | -2.6% |
| 8192 | 25% | AND | 1011.4 | 1407.0 | 1615.0 | +39.1% |
| 8192 | 25% | OR | 990.1 | 1388.3 | 1621.9 | +40.2% |
| 65536 | none | AND | 1544.3 | 1548.8 | 1604.9 | +0.3% |
| 65536 | none | OR | 1414.9 | 1407.3 | 1614.3 | -0.5% |
| 65536 | 25% | AND | 6404.7 | 5410.7 | 7854.2 | -15.5% |
| 65536 | 25% | OR | 6439.6 | 5294.6 | 7894.0 | -17.8% |

Positive changes mean slower elapsed time. At 8192 rows, nullable merges regress by about 39–40% against the previous fused loop; at 65536 rows they improve by about 16–18%. All tested cases beat the general Arrow sequence. These findings apply only to the stated microbenchmark, not end-to-end queries. [Source identities and raw measurements](conjunction-arrow/summary.json).

## Direct-validity conjunction amendment

Remove `scratch` and NULL value-bit normalization. Handle the four optional-validity combinations directly; do not scan `null_count` to select an update function. Values are ordinary AND/OR bits; validity uses the original values and checks for a valid decisive FALSE/TRUE. Both-sided validity and values are written together in one safe u64 loop using Arrow slice-aware input iterators. Arrow existing bitmap operations handle the other cases. Values and lazily allocated validity retain their addresses across updates and transfer to the final array without copying. There is no existing Arrow four-input, two-output in-place API; forcing the direct formula into a binary closure was slower and is preserved as a rejected ablation. PR #4 adapts encoded constants without requiring neutral NULL payload bits. Eager streaming and argument error order are preserved.

Fresh release validation passed 85 tests at independent PR #17, 99 at #4, 123 at the final core stack, and 139 in the local DataFusion workspace including its documentation example. Core/test trees match the local harness. New truth-table coverage varies NULL payload bits, explicit all-valid validity, tail lengths and slice offsets; existing tests retain result-buffer identity and immutable input checks.

The [six-way benchmark and ablations](conjunction-direct/README.md) include exact source snapshots, locked dependencies and all 27 samples per case. Final nullable merges reduce elapsed time by 33.7–34.8% at 8192 rows relative to the scratch revision; 65536-row nullable merges are 1.2–1.7% slower. Against the earlier fused loop, they improve by 8.7–10.1% and 14.5–14.9%, respectively. These are bitmap merge measurements for ordinary arrays at independent #17; no expression evaluation, PR #4 representation dispatch or end-to-end query improvements are measured. Historical query tables and earlier scratch measurements retain their original identities.

## Conjunction file-layout and binding simplification

Move the bitmap workspace, update functions, encoded-constant accessor and tests into `src/expr/scalar/conjunction.rs`, deleting its `kernel.rs` submodule. Put recursive argument binding and And/Or evaluation construction directly in `to_evaluation`; remove the forwarding `bind` method. The workspace and bitmap update functions are private to the conjunction module. PR #4 retains the crate-visible Boolean constant accessor required by NOT.

The ordinary and encoded bitmap implementation bodies were checked against their preceding revisions after only visibility and local-reference normalization; no algorithm or expression evaluation behavior changes. Fresh release validation passed 85 tests at independent #17, 99 at #4, 123 at the final core checkpoint and 139 including the separately retained local DataFusion workspace documentation example. Current source identities appear under `conjunction_inline_amendment`; earlier identities and benchmark samples remain historical. No new performance measurement was run for this structural refactoring.

## PR #17 merged; remaining stack rebased on main

PR #17 was squash-merged as `1f86a8d823da506513b36a73a155bb1414e98bdc` (`perf(expr): accumulate conjunctions in place (#17)`). Its complete tree matches the reviewed and tested PR head `af9fdec93de9ee2b8296fd8ff1daf561cbcebc9e`. PR #4 now targets main; conjunction evaluation and ordinary bitmap merging are already in main, while #4 adds RunArray constants and their consumers. Subsequent PRs retain their single-step review scope.

This is a history-only restack. Every remaining PR's core and test trees exactly match its previously validated revision. The preceding release validation (99 at #4, 123 at the final core checkpoint, 139 including the local DataFusion workspace) remains applicable by source identity; no new tests or performance runs were made. New commit identities and tree-equivalence checks appear under `post_conjunction_merge_restack` in the verification file. Historical measurements retain their recorded source identities.

## ScalarValue / ColumnValue amendment

PR #4 now stores constants as inline ScalarValue variants and returns ColumnValue from the single expression evaluation entry. Functions dispatch inside their existing bound eval function; the three-way array/left-constant/right-constant binding table, single-run checks, Boolean accessor and RunArray materialization utility are removed. Scalar arithmetic, comparisons, CAST, NOT, NULL tests and all-scalar conjunctions preserve scalar results. Projection, Filter, contributing CASE/COALESCE branch pieces and aggregate arguments broadcast only when their consumer requires an array. Existing lazy CASE/COALESCE selection and checked arithmetic errors are retained.

Fresh release validation passed 98 tests at PR #4, 122 at the final core checkpoint, and 138 including the local DataFusion workspace and documentation example. The local core/test trees match the final core exactly. The [standalone expression benchmark](column-values/README.md) records all three processes, alternating paired samples, exact source commits and full correctness checks. Its 8192-row `x + 1` and two-comparison AND cases improve by about 11–12%; 65536-row changes are small. Its directly constructed `x + (1 + 2)` benefits from keeping the intermediate scalar, but an SQL optimizer can fold that subexpression already. No fresh end-to-end DataFusion comparison is claimed for this amendment.

## Explicit scalar type coverage amendment

PR #4 now follows the explicit representations in DataFusion 55.1.0's `ScalarValue`: Float16, Date/Time/Timestamp, Duration/Interval, Decimal32/64/128/256, string/binary variants, and typed single-row List/ListView/FixedSizeList/Struct/Map arrays. Union, Dictionary and RunEndEncoded values retain their encoding metadata and a recursively represented scalar. Remove the generic `Arrow(Scalar<ArrayRef>)` fallback. Roc retains `Null(DataType)` for compatibility with its existing typed-null constructor; no DataFusion dependency is added to the core. This expands scalar representation and Arrow conversion coverage, without expanding the existing arithmetic binding signatures.

Scalar extraction honors logical slice positions and validity. Array reconstruction preserves Decimal precision/scale, timestamp timezone and nested/encoded fields. Nested scalar variants reject arrays whose length is not one; invalid encoded metadata and run-end size overflow return errors. Ordinary consumers still call `ColumnValue::into_array` to broadcast.

Release validation passed 106 tests at PR #4, 130 at the final core checkpoint, and 146 in the separately retained local DataFusion workspace including its documentation example. Eight new test groups compare reconstructed values with independent Arrow take/cast results across NULLs, sliced arrays, empty outputs and repeated rows. All twelve core PR checkpoints passed all-target compilation; the early global aggregate checkpoint passed its eleven release regression tests. Final core/test trees exactly match the local harness. Exact current source identities appear under `explicit_scalar_types_amendment` in [core-source-verification.json](core-source-verification.json).

No performance measurements were made for this type-coverage amendment. The expression experiment in [column-values](column-values/README.md) measured the earlier `abb68b16370b000c4938bd5037932e576489e235` implementation, which still had the generic Arrow fallback. All recorded timings retain those original source identities.

## Consolidated nested scalar broadcasting

Move the former `constant.rs::repeat` body into `value.rs::repeat_nested`, alongside its single-row validation, and delete the forwarding call and old helper. Zero-row outputs use an empty slice, one-row outputs retain the input Arc, NULL scalars construct a typed NULL array, and other nested values broadcast through Arrow take with repeated zero indices. Production broadcasting is private to scalar value conversion. Test reference arrays use Arrow take directly.

Fresh release validation passed 106 tests at PR #4 and 130 at the final core checkpoint. The separately retained local DataFusion harness mirrors the final core/test trees exactly; its preceding 146-test workspace result remains recorded under the explicit type-coverage amendment. This consolidation adds no public API or algorithm change and has no new performance measurement. Current source identities appear under `nested_broadcast_consolidation` in [core-source-verification.json](core-source-verification.json).

## Separate scalar and array conjunction updates

Move ColumnValue representation dispatch to the streaming conjunction evaluator. Its scalar arm calls `update_scalar(&ScalarValue)`, and its array arm calls `update_array(&ArrayRef)`. Delete the mixed-input `update(&ColumnValue)` function and its unreachable representation branch. Each private update function keeps its previous value/validity algorithm. Scalar-only expressions retain scalar results; the first array creates the bitmap workspace and seeds it with the preceding scalar result. Argument evaluation order, SQL NULL behavior, length/type errors, immutable inputs and buffer reuse are preserved.

Fresh release validation passed 106 tests at PR #4 and 130 at the final core checkpoint. Existing tests compare scalar/array mixtures and every intermediate bitmap with Arrow Kleene, including NULLs, slices, buffer identity, malformed lengths/types and ordered errors. The local DataFusion harness mirrors the final core/test trees; its preceding 146-test workspace result remains historical. This structural change has no new performance measurement. Current identities appear under `conjunction_update_dispatch` in [core-source-verification.json](core-source-verification.json).

## PR #4 merged; remaining optimization stack rebased

PR #4 was squash-merged as `1c5588a3c456c4c0810daa8c18c0d4c2bea855e2` (`perf(expr): preserve scalar values during evaluation (#4)`). Its complete tree matches reviewed head `26848e709c91f5403b259c3945296a06ca021ee0`, which passed 106 release tests. PR #5 now targets main; subsequent PRs retain their predecessor branches and single-step review scope.

Every restacked core PR's complete repository tree matches its previous revision, including the final core checkpoint that passed 130 release tests. The local DataFusion harness retains identical final core/test sources. This is a history-only restack; tests and performance measurements were not repeated. Current identities and equality checks appear under `post_pr4_merge_restack` in [core-source-verification.json](core-source-verification.json). Existing benchmark samples retain their original source identities.

## PR #5 closed; typed SUM reviewed directly against main

PR #5 had no demonstrated end-to-end win and was closed. PR #6 now targets main and changes native SUM state storage and grouped NULL traversal only; it does not contain the rejected global aggregate route, batch COUNT update, or dedicated AVG loop. PRs #7–#13 retain that removal. PR #8 introduces the internal validated grouped path because its argument-preparation optimization uses it.

PR #15 now explicitly includes the routing required by its local-state global SUM optimization. SUM keeps checked totals and validity in local variables; COUNT, AVG, and other aggregates use ordinary grouped per-row updates, without PR #5's batch COUNT reduction or separate AVG loop. The global-path regression suite is introduced there.

Every remaining core checkpoint passed all-target compilation. Fresh release tests passed 111 at PR #6 and 132 at PR #16. The separately retained local DataFusion harness mirrors the final core/test sources and passed 148 workspace release tests including its documentation example. Exact commit identities and validation appear under `post_pr5_closure` in [core-source-verification.json](core-source-verification.json).

No performance experiment was repeated after this source-changing restack. In particular, the historical approximately 31% global-aggregate reduction attributed to the old PR #6 checkpoint and the approximately 15% PR #15 result are conditional on the old PR #5 route. They do not establish the performance of either amended PR. The original raw samples and their negative PR #5 delta remain unchanged.

## SUM update function bound during construction

PR #6 retains the `SumGroups` enum and pairs it with a `SumUpdateFn` when constructing the accumulator. Binding selects the signed, unsigned or floating implementation by SUM result type. Batch updates invoke the selected function directly; each implementation accesses its matching typed state with a checked `as_*_mut()` accessor and uses the existing typed Arrow array accessor. The state and function are constructed together, preserving their internal type invariant. Variant checks remain outside row loops. Resize and state materialization keep ordinary enum matches; grouped partial merges reuse the bound update function.

Fresh validation passed all-target compilation at every remaining core checkpoint, 111 release tests at PR #6, 132 at the final core, and 148 in the local DataFusion workspace. The final core/test sources match the local harness. Binding changes and current source identities appear under `bound_sum_update_fn` in [core-source-verification.json](core-source-verification.json).

This amendment has no new timing measurement. The previous implementation already used native typed row loops; replacing its per-batch enum dispatch with a bound function does not by itself establish a performance improvement. Historical benchmark samples remain attributed only to their recorded source revisions.

## All aggregate update functions bound during construction

PR #6 now uses one internal `Accumulator` containing an `AccumulatorState` enum and an `UpdateFn`. Construction pairs COUNT, COUNT DISTINCT, AVG and COVAR_POP states with their fixed update functions; SUM selects its signed, unsigned or floating implementation by result type. SUM binding occurs at the same boundary as other aggregates, avoiding a second indirect function call inside SUM. Each function uses private checked state accessors before entering its row loop.

MIN and MAX bind `update_extremum::<true>` and `update_extremum::<false>` respectively. Comparator direction is a compile-time constant and no runtime `minimum` flag is stored. Existing Arrow row encoding, NULL handling, equality behavior and selected-value materialization remain unchanged. Resize, state output and ordinary partial merges retain enum matches; SUM and MIN/MAX partial merges reuse the bound update entry point. COUNT still updates per row; the rejected PR #5 batch COUNT reduction is absent.

All ten core checkpoints passed all-target compilation. Fresh release tests passed 111 at PR #6, 132 at the final core, and 148 in the local DataFusion workspace. Existing aggregate worker-merge, distinct, covariance, extrema, NULL/FILTER, signature-error and overflow cases passed. Final core/test sources match the local harness. Latest source identities and validation appear under `bound_aggregate_update_fns` in [core-source-verification.json](core-source-verification.json).

No performance measurement was repeated for this amendment, and no speedup is inferred from binding changes alone. Earlier SUM-only binding notes and timing samples retain their recorded historical revisions.

## Aggregate binding measured with a factorial ablation

The [six-way experiment](aggregate-binding/README.md) uses native typed SUM states throughout, isolating binding from the earlier native-state optimization. All four controls share one state layout and one copy of every loop. At 8192 nullable rows, binding-only changes across individual aggregates are -0.38% to +0.23%; comparator specialization reduces Int64 MIN/MAX time by 8.26%/5.02%.

The complete real source amendment, which also changes kernel extraction, compiler inlining and state layout, reduces the seven-aggregate mixed case by 1.69% at 8192 rows and 1.48% at 65536. AVG improves about 11%, while nullable COUNT and Float64 SUM regress modestly. COUNT DISTINCT's process changes cross zero widely, so its positive median is not a reliable win. These source-level deltas must not all be attributed to binding itself.

Three independent processes, 78 cases each and 12 six-way rounds per case produced 16848 raw timing rows with correctness validation. The rejected separate-module control design is preserved separately for audit. Source, compiler, binary, lockfile and data-generation identities are retained, along with original source snapshots so old force-pushed commits need not remain locally available. Roc code was not changed for this experiment and no full-query/DataFusion speedup is inferred.

## SUM result validation during binding

The latest PR #6 amendment removes `SumGroups::Unsupported`. `SumGroups::bind` now returns `Result` and rejects result types other than `Int64`, `UInt64` and `Float64` with `InvalidPlan` while constructing the accumulator, before any input is processed. Invalid types cannot create placeholder state, including for empty or all-NULL inputs. `SumGroups::state` returns `ArrayRef` directly. The existing operator test now checks worker-state construction; added tests check eight unsupported result types and supported Int32/UInt32/Float32 input widening.

Fresh validation passed 113 release tests at PR #6, 134 at the final core, and 150 in the local-only DataFusion workspace. All ten core checkpoints passed all-target compilation. Final core and test trees match the local harness. Current source identities appear under `early_sum_result_validation` in [core-source-verification.json](core-source-verification.json); older entries retain their historical revisions.

No performance rerun is claimed for this amendment. The aggregate-binding benchmark and earlier timing samples retain their measured source snapshots. The later Float64 SUM function-boundary experiment is not part of the core change.

## PR #6 merged

PR #6 was squash merged into main. Its merge tree exactly matches the reviewed revision with 113 passing release tests and all-target compilation. Remaining core PRs and the local-only DataFusion harness were reparented without source-tree changes; they retain the previous all-target checks, 134 final-core release tests and 150 local-workspace release tests. No tests or performance measurements were repeated for the ancestry-only update. Current identities appear under `after_pr6_merge` in [core-source-verification.json](core-source-verification.json). Recorded benchmark snapshots remain unchanged.

## PR #7 merged

PR #7 was squash merged into main. Its merge tree exactly matches the reviewed revision with 117 passing release tests and all-target compilation. Remaining core PRs and the local-only DataFusion harness were reparented without source-tree changes; they retain the previous all-target checks, 134 final-core release tests and 150 local-workspace release tests. No tests or performance measurements were repeated for the ancestry-only update. Current identities appear under `after_pr7_merge` in [core-source-verification.json](core-source-verification.json). Recorded benchmark snapshots remain unchanged.

## Aggregate Reference specialization removed

PR #8 now evaluates every argument through the existing `ScalarExpressionEvaluation` path. The aggregate-specific `AggregateArgument` enum and direct Reference column-access branch were removed. It retains same-type cast borrowing, the internal validated group-ID path, and constructing a FILTER evaluation context only when a filter exists. PR #13 no longer borrows single-reference arguments; its remaining shortcut skips scalar argument setup for argument-free aggregates such as COUNT(*).

Fresh release tests passed: 120 at PR #8, 134 at final core PR #16, and 150 in the separately retained local DataFusion workspace. Every remaining core checkpoint passed all-target compilation. Updated source identities appear under `remove_aggregate_reference_specialization` in [core-source-verification.json](core-source-verification.json).

No performance measurements were rerun. Historical #8 and #13 samples include the removed Reference paths and do not measure these amended revisions. Existing benchmark snapshots and raw samples retain their original identities.

## Single-argument aggregate API

PR #8 removes COVAR_POP and replaces the aggregate argument vector with `Option<ScalarExprRef>`. COUNT(*) uses None; COUNT(expr), SUM, AVG, MIN and MAX use Some(expr). The evaluator and bound update function pass one optional array without allocating an argument vector. Non-COUNT aggregates and COUNT DISTINCT reject a missing argument during executor construction. AVG still exports two partial-state arrays, count and sum; partial states are separate from expression arguments.

The optional-argument evaluator naturally skips COUNT(*) setup, superseding PR #13. Its reference bounds regression coverage moves into PR #8, and PR #15 now follows PR #12. DataFusion conversion remains local and rejects COVAR_POP as unsupported.

Fresh release validation passed 120 tests at PR #8, 134 at final core PR #16, and 150 in the local DataFusion workspace. All active core checkpoints passed all-target compilation. Current source identities appear under `single_argument_aggregates` in [core-source-verification.json](core-source-verification.json).

No new performance measurements were collected for this API simplification. Historical samples, including COVAR_POP measurements, retain their original source identities and must not be attributed to the amended revisions.

## Fresh current PR #8 aggregate measurements

[Current PR #8 report](pr8-current/README.md) compares the actual amended PR against main and preserves 5,760 samples plus source archives. At 2048 rows per batch and three columns, aggregate-execution time falls about 8.7% for COUNT(*), 6.8% for SUM, 12.6% for SUM/COUNT/AVG, and 3.2% for filtered SUM. Repeated group-ID validation explains the largest demonstrated contribution. Eager-context elimination is near neutral at this batch size but helps small wide batches. The cast control has unexplained non-target timing changes and does not establish a cast-cost benefit. These tests exclude storage, pipeline scheduling and DataFusion; they do not replace historical full-query comparisons.

## PR #8 merged

PR #8 was squash merged into main. Its tree exactly matches the reviewed head validated by 120 release tests and all-target compilation, and the current PR #8 benchmark candidate. PR #9 now starts from main. Remaining core PRs and the local-only DataFusion harness were reparented with identical source trees, retaining their earlier compilation checks, 134 final-core and 150 local-workspace release tests. No checks or timings were repeated for the ancestry-only update. Updated commit identities appear under `after_pr8_merge` in core-source-verification.json; benchmark snapshots and samples keep their measured identities.

## PR #9 deferred and removed from the remaining stack

PR #9 was closed at the user's request. Remove its NULL-density dispatch and integer SUM first-value specialization from every later core PR and the local-only DataFusion harness. PR #10 now follows main. Grouped SUM and COUNT use exactly the main implementation: direct non-null iteration, otherwise Arrow valid_indices; SUM preserves the first value for all native types. PR #15 retains only global SUM local-state accumulation, with the same ordinary nullable iteration and first-value semantics. Its dense bitmap loops and integer first-value shortcut were removed too. The PR #9-specific regression file was removed from later PRs.

Fresh release tests passed 121 at PR #10, 132 at final core PR #16, and 148 in the local-only DataFusion workspace. All remaining core heads passed all-target compilation. Current identities appear under drop_pr9 in core-source-verification.json.

No new performance result is claimed for the amended later PRs. Their historical cumulative/global-SUM samples include removed PR #9 code and do not measure these heads. Main is unchanged, and the fresh PR #8 report's exact candidate tree remains unchanged; its source archives and 5,760 samples retain their original identities.

## PR #10 merged

PR #10 was squash merged into main. Its tree exactly matches the reviewed revision validated by 121 release tests and all-target compilation. PR #11 now starts from main. Remaining core PRs and the local-only DataFusion harness have identical source trees and incremental diffs after reparenting, preserving earlier compilation checks, 132 final-core and 148 local-workspace release tests. No checks or timings were repeated for ancestry-only changes. Updated identities appear under `after_pr10_merge` in core-source-verification.json; historical benchmark snapshots and timing samples remain unchanged. No isolated PR #10 performance improvement is claimed.

## Current PR #11 scan-path ablation

[Exact current-revision comparison and all 360 raw samples](pr11-current/README.md) measure ScanExec next_batch in isolation. Already-ready scan_channel reads decrease from 108.0 to 85.0 ns per batch (21.3%, five-process paired interval 15.6% to 26.7%). At 2048 rows per batch this is about 0.011 ms of scan-entry overhead per million rows, not a measured SQL-query speedup. The additional Pending case is artificial and includes no real I/O latency. No end-to-end or DataFusion performance improvement is claimed. Measured source snapshots, lockfiles and binary hashes are preserved.

## PR #11 deferred

PR #11 is closed. Its isolated ready-channel scan path saved about 23 ns per batch, but did not demonstrate a material SQL-query improvement sufficient to justify the extra manual polling logic. Restore main's simpler select_biased implementation in all later core and local-only harness heads. PR #12 now starts from main, with all remaining incremental diffs preserved exactly. All remaining core heads pass all-target compilation; final core passes 130 release tests and the local-only DataFusion workspace passes 146. The two PR #11-specific unit tests were removed with its implementation. Keep the 360 timing samples and exact measured source snapshots as historical evidence, without relabeling them. Updated identities are under `drop_pr11` in core-source-verification.json. No timings were collected for the post-removal stack.

## Review of remaining optimization PRs

[Current-source leave-one-out results and all raw samples](remaining-pr-review/README.md) cover 4M Snappy Parquet rows, five paired process replicas and 2240 total Roc/DataFusion timing samples. PR #15 is deferred: global Int64 SUM/COUNT shows no stable improvement (54.00 to 54.17 ms at one thread; 14.86 to 15.00 ms at four threads) while adding an extra global update route and duplicated preparation. Retain #12 for removing 34 net handwritten comparison lines and reusing Arrow, with modest observed filter/project savings of 1.8%-2.0%; small control movements prevent strong causal timing claims. Retain #16 for reducing actual column-filter work, with 4.5%-5.2% lower elapsed time in this setup. The original all-enabled ablation background included #15; percentages are not additive. The measured without15 source matches the retained core source and tests exactly, and its samples provide the retained-stack DataFusion comparison. Full DataFusion harness archives and binaries remain local.

After removal, PR #16 directly bases on #12 and keeps its identical incremental diff. Fresh final-core all-target compilation and 122 release tests pass; local-only DataFusion workspace passes 138 release tests. Eight PR #15-specific tests were removed with its implementation. Exact measured identities, raw samples and historical evidence remain unchanged; updated retained identities appear under `remaining_pr_review` in core-source-verification.json.

## PR #12 merged

PR #12 was squash merged into main. Its tree exactly matches the reviewed revision validated by 121 release tests and all-target compilation. PR #16 now starts from main. Remaining core PRs and the local-only DataFusion harness have identical source trees and incremental diffs after reparenting, preserving earlier compilation checks, 122 final-core and 138 local-workspace release tests. No checks or timings were repeated for ancestry-only changes. Updated identities appear under `after_pr12_merge` in core-source-verification.json; historical benchmark snapshots and timing samples remain unchanged. The current-source review keeps its original measured identities and raw samples; merged source is unchanged from the reviewed implementation.

## PR #16 merged

PR #16 was squash merged into main. Its tree exactly matches the reviewed revision validated by 122 release tests and all-target compilation. Only documentation PR #14 remains, now based directly on main. The local-only DataFusion harness has an identical source tree after reparenting, preserving earlier compilation checks, 122 final-core and 138 local-workspace release tests. No checks or timings were repeated for ancestry-only changes. Updated identities appear under `after_pr16_merge` in core-source-verification.json; historical benchmark snapshots and timing samples remain unchanged. The current-source review keeps its original measured identities and raw samples; merged source is unchanged from the reviewed implementation.
