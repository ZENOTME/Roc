# Source parallelism evidence

Cumulative implementation measurements for
[`design/source-parallelism.md`](../../design/source-parallelism.md), based on
`cc7d80382f80e7e25fbcb1b095bc04b7c46b0e50` (parallel sink finalization).

`results.csv` retains the fastest validated run out of three for each configuration,
including translate/build/execute and final sink time from that same run. The
`*-{zero,compile}-{1,4}-{queries,groups,limit}.txt` files are the original reports.
The six TPC-DS queries are q23a, q23b, q47, q51, q57 and q67. `groups` returns
240,000 groups from store_sales; `limit` orders store_sales by net paid and ticket
number and returns 100 rows. SQL is in `reproduce.py`.

Workers and runtime threads are both 1 or both 4. Builds use the locked default
release profile. Timings do not overlap this task's compilation, tests, profiling,
or other query runs. The machine remains a normal desktop environment; these
measurements are not cross-machine guarantees. Each timed query also compares
its output to DataFusion. Sink finish time is **not comparable as a measure of
remaining computation** after work moves into sources; use total/execute times.

Stages:

- `baseline`: the committed parallel sink finish implementation.
- `instances`: one async task and LocalSource per instance, retaining eager finalizers.
- `aggregate`: additionally publish aggregate partitions and merge on read.
- `window`: additionally compute single-specification window buckets on read.
- `sort`: additionally sample key ranges and merge only requested sort ranges.
- `final`: additionally pull DataFusion partitions directly per consumer, removing
  the previous per-consumer decoder tasks and queues. Those tasks could claim a
  shared partition for a private queue, leaving other instance consumers idle.

`checkpoints/*.patch.gz` are cumulative patches against the baseline and were
checked with `git apply --check`. Apply exactly one to a fresh baseline copy.
The common new partitioned.rs file includes later test-only additions; production
code matches the intermediate aggregate/window/sort binaries. Metadata records
binary and lockfile hashes. Intermediate binaries are preserved locally under
`/tmp/roc-source-parallelism` for this run.

```sh
CARGO_TARGET_DIR=/path/to/target LLVM_SYS_191_PREFIX=/path/to/llvm19 \
  cargo build --locked --release -p roc-datafusion --example tpcds
python3 benches/source_parallelism/reproduce.py \
  /path/to/target/release/examples/tpcds /path/to/sf1 final
python3 benches/source_parallelism/summarize.py

# Full correctness, one repetition per statement, run sequentially.
/path/to/tpcds /path/to/sf1 --mode zero --workers 4 --threads 4
/path/to/tpcds /path/to/sf1 --mode compile --workers 4 --threads 4
```

Full SF1 outputs are `validation-zero.txt` and `validation-compile.txt`. These
single-run validation timings are not substituted for the repeated matrix.

Two additional four-worker rounds alternate baseline/final order. Run:

```sh
python3 benches/source_parallelism/recheck.py /path/to/baseline /path/to/final /path/to/sf1
python3 benches/source_parallelism/summarize.py
```

The design's recheck table uses the median of the two per-round best-of-three
values. Across 256 matrix/recheck configurations, all outputs match DataFusion.
The final workspace test run passes 198 tests, and both full SF1 validation runs
pass 103/103. Rechecks retain a roughly 20% high-cardinality GROUP BY regression
(about 4–5 ms) and a 6–7% q67 regression, alongside a 24–29% q51 improvement.
These limitations are part of the result, not excluded observations.
