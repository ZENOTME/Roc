# NumberBatchAdd initialized with a running total

Change the helper to the user-requested API: initialize it with a number, then add one or more Arrow arrays. The helper stores only its current total and does not retain array references.

```rust
let mut sum = NumberBatchAdd::new(5i64);
sum.add(&Int64Array::from(vec![Some(10), None, Some(-3)]))?;
assert_eq!(sum.total(), 12);
```

`add` computes the wrapping subtotal and conservative magnitude bound in one scan. If the bound proves every valid prefix fits, it removes arbitrary NULL payloads and adds the subtotal. Otherwise it performs native checked additions in input order. Overflow returns an error while retaining the successful prefix; the aggregate commits that prefix as before. Empty/all-NULL batches leave the total unchanged. Aggregate output validity is handled by the caller, so all-NULL SUM remains NULL.

The helper uses compilation-time specialization for Int64/UInt64. No heap allocation or dynamic dispatch is introduced. Float64 and grouped aggregation retain their existing paths.

## Provenance and validation

- Baseline core `ff096d95f29a55da4fd7a38956d8fd8ed110ca4a`; full local harness `f11eb6186f2cfc118b520cbc9bc1bb11acb03815`.
- Current core `b66a66cce04e6133fe558356b75e46feae83461f`; full local harness `f9cbb8d723a7b8b12d54a52dc556d5ae211cdb82`.
- The baseline binary is reused byte for byte from the previous helper experiment. Its source and binary hashes were verified; the candidate core and probe were rebuilt before timing. Both source trees were checked against their pinned core commits.
- 133 core release tests pass, including a new initial-value/multiple-batch/overflow-prefix helper regression. All-target compilation passes without new warnings.
- Existing signed/unsigned, slice, NULL-payload, empty/all-NULL, cast, merge and extreme-value regressions pass.
- Actual core disassembly retains seven vector-add instructions in both builds. This confirms vectorization is retained, not identical complete code generation.

## Paired measurements

Apple M1 Pro, release, Arrow 59.3.0/DataFusion 55.1.0, 4M nullable Int64 rows, batch size 8192, `SELECT SUM(value), COUNT(value)`. Values are in [-100000, 100000]. Parquet uses the same Snappy files and projection; resident input uses the same decoded batches. Planning/conversion/graph construction excluded; execution, decoding where applicable, shutdown and result extraction included.

Ten balanced process pairs, five in each order, with ten timed samples per case/engine and two warmups. Engine order rotates within each sample. No compilation during timing or observations trimmed. Every timed result is verified outside the timer. 2400 full-path measurements and 4400 kernel measurements are retained.

Positive elapsed change means slower. Values are geometric means of process medians; 95% intervals bootstrap paired process log ratios (10000 resamples).

| Input | Threads | Previous API ms | Initial-value API ms | Elapsed change | 95% paired interval |
|---|---:|---:|---:|---:|---:|
| Parquet | 1 | 38.6547 | 38.3955 | -0.67% | [-1.45%, +0.17%] |
| Parquet | 4 | 10.9182 | 10.9059 | -0.11% | [-0.87%, +0.83%] |
| Resident | 1 | 1.4744 | 1.4896 | +1.03% | [-3.32%, +5.87%] |
| Resident | 4 | 0.8946 | 0.9540 | +6.64% | [-2.25%, +15.78%] |

All full-path intervals include zero: no clear elapsed-time change is demonstrated. The resident four-thread point estimate is slower with wide uncertainty; this result does not prove equivalence or justify a speedup claim.

DataFusion-normalized elapsed changes:
- p/1t: -0.04% [-0.71%, +0.73%].
- p/4t: +0.35% [-0.25%, +0.95%].
- m/1t: +3.14% [+0.31%, +6.72%].
- m/4t: +5.35% [-1.75%, +12.94%].

The resident one-thread control-normalized comparison is +3.14% with an interval above zero, while the direct full-path comparison is +1.03% with an interval spanning zero. This is a possible small regression signal, not proof that the helper causes it. The kernel comparisons also span zero. No zero-overhead or general equivalence claim is made.

The accumulator microbenchmark compiles the exact module separately inside the example. Both configurations run sequential kernel work; probe thread counts do not describe parallelism within that kernel.

| Probe threads | Previous API ms | Initial-value API ms | Elapsed change | 95% paired interval |
|---:|---:|---:|---:|---:|
| 1 | 1.1292 | 1.1211 | -0.72% | [-3.92%, +2.73%] |
| 4 | 1.1282 | 1.1419 | +1.22% | [-1.76%, +4.15%] |

This experiment validates an API refactor, not another optimization. It does not establish performance for UInt64, grouped SUM, near-limit fallback data, or cold storage. The original #35 optimization ablation retains its original source hashes and is a separate measurement.

## Retained evidence

`metadata.json` records source/binary/data hashes and process order. `summary.json` and `kernels.json` contain aggregate statistics and process medians. The two variant directories contain all raw samples, build logs and actual core disassembly. Core test/check logs are included. `build.py` and `run.py` preserve the measurement procedure; full source snapshots, binaries and the exact refactor patch remain in the local artifact directory. DataFusion integration remains local.
