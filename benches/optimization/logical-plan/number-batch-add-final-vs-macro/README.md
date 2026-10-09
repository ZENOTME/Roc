# Current NumberBatchAdd compared directly with original PR #35

This addresses whether the final `new(initial)` / `add(array)` API retains the original safety-bound optimization. No source change was made for this experiment. Two existing verified binaries were compared in the same balanced run; figures from earlier runs are not used to calculate these paired changes.

Original core: `37708712d41b2a51ab30ca951ca98c1e144b3274`. Current core: `b66a66cce04e6133fe558356b75e46feae83461f`.

Both timed sources match their pinned core commits and both harnesses have the same probe hash. Binaries are reused byte for byte from the earlier isolated builds; their source, lockfile, binary and compiled-source hashes are checked. The current unchanged core passed 133 release tests and all-target compilation in the preceding API refactor. These tests were not rerun for this timing-only experiment. Actual core disassembly retains vector additions in both binaries.

Ten balanced process pairs, five in each order, ten timed samples per case/engine and two warmups. Engine order rotates within each sample. No compilation during timing and no observations trimmed. Every output is checked outside the timer. 2400 full-path measurements and 4400 kernel measurements are retained.

Apple M1 Pro, release, Arrow 59.3.0/DataFusion 55.1.0, 4M nullable Int64 rows, batch size 8192, values in [-100000, 100000], `SELECT SUM(value), COUNT(value)`. Parquet uses the same Snappy files and projection. Resident input uses the same decoded batches. Full execution including decoding where applicable, shutdown and output extraction is timed; planning/conversion/graph construction excluded.

Values are geometric means of ten process medians. Positive change means slower. Intervals bootstrap paired process-level log ratios, 10000 resamples. The DataFusion column is the control measured alongside the current binary.

| Input | Threads | Original #35 ms | Current #35 ms | Native DataFusion ms | Current / original elapsed change | 95% paired interval |
|---|---:|---:|---:|---:|---:|---:|
| Parquet | 1 | 38.4676 | 38.4663 | 39.0354 | -0.00% | [-0.79%, +0.69%] |
| Parquet | 4 | 10.9243 | 10.8743 | 11.5168 | -0.46% | [-1.33%, +0.45%] |
| Resident | 1 | 1.5498 | 1.4651 | 1.8088 | -5.47% | [-9.60%, -0.96%] |
| Resident | 4 | 0.9356 | 0.9656 | 0.7612 | +3.21% | [-5.34%, +12.60%] |

Parquet differences are small with intervals including zero. The current resident one-thread result is 5.47% lower than the original #35 in this round; this is an observed difference, not a general claim that the API change improves performance. The resident four-thread point estimate is 3.21% slower, with an interval of [-5.34%, +12.60%]; a small regression cannot be excluded. Current resident four-thread execution remains about 27% slower than DataFusion, a gap already present before this refactor.

These results support retention of the major original optimization; they do not prove that every scenario is regression-free. The earlier adjacent-refactor comparison showed a possible small resident one-thread control-normalized regression against the intermediate helper. That separate comparison is retained with its original source hashes. The direct original-to-current comparison here does not show loss of the original one-thread benefit.

DataFusion-normalized elapsed changes (current / original, dividing by the corresponding control change):
- p/1t: -0.44% [-0.85%, -0.02%].
- p/4t: -0.37% [-0.80%, +0.15%].
- m/1t: -4.07% [-6.49%, -1.28%].
- m/4t: +1.88% [-6.35%, +10.47%].

The original #34/#35/wide-integer table is a different experiment. Its absolute timings should not be subtracted from this table to attribute a refactor cost. This experiment does not remeasure #34 or the 128-bit variant.

`metadata.json` records provenance, data hashes, configuration and process order. `summary.json` contains all process medians and confidence intervals. The variant directories retain every raw sample, the original build logs and actual core disassembly. Full local sources and binaries remain in the local artifact directory; DataFusion integration remains local.
