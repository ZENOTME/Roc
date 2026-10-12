# DataFusion Parquet storage

`roc-datafusion` connects a DataFusion physical scan to Roc's `ScanStorage`
interface. DataFusion handles file access, Parquet decoding, and any projection
or predicate pushdown already present in that plan. Roc runs the downstream
pipeline over the resulting Arrow batches. The core `roc` package does not
depend on DataFusion.

```rust
use std::sync::Arc;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use roc::operator::ScanOperator;
use roc_datafusion::{DataFusionScan, DataFusionStorage};

async fn parquet_scan(path: &str) -> datafusion::error::Result<ScanOperator<DataFusionScan>> {
    let ctx = SessionContext::new();
    let frame = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
    // Apply filters/projections before planning when pushdown is wanted.
    let plan = frame.create_physical_plan().await?;
    let source = DataFusionScan::new(plan, ctx.task_ctx());
    Ok(ScanOperator::new(source, Arc::new(DataFusionStorage)))
}
```

Each scan execution resets DataFusion's execution state and creates fresh
partition streams. This matters because Parquet scans can keep a shared queue of
unopened files: reusing an exhausted queue would silently skip data on the next
query. Consumers claim partitions exactly once and pull batches directly;
there is no additional producer task or batch queue. This retains demand-driven
backpressure. Use enough
Roc workers to consume the available scan partitions concurrently.

The consumers share an unordered work pool. The adapter does not promise a stable
partition-to-worker mapping or global output ordering. It therefore supplies a
Roc storage input; it is not a replacement for DataFusion's `ExecutionPlan`
partitioning contract. Finishing or dropping a scan releases its streams and wakes
pending consumers. Caller shutdown cancels pending reads, and scan errors propagate
through the consumer and finalization paths.

Adapt plans whose output partitions can be consumed independently, such as
Parquet scans. Coalesce cross-partition exchange plans inside DataFusion first:
draining one exchange partition at a time can otherwise wait for consumers that
have not started. Dynamic-filter and recursive-query plans are unsupported,
because DataFusion's state-reset helper cannot make them independently reusable.

## Validation

```sh
cargo test --release -p roc-datafusion
```

## Performance comparison

For the optimized implementation and saved per-step experiments, see
[execution optimization and ablations](#execution-optimization-and-ablations).
The initial measurements below are historical.

The `parquet_compare` example compares native DataFusion operators and native Roc
operators above the same DataFusion Parquet scan configuration, with fresh scan
state for every execution. This isolates the downstream
execution paths and adapter cost: both routes use DataFusion's reader. It does not
compare two independent Parquet implementations, and it does not measure the full
SQL planning/optimization workflow.

Data generation, planning, result validation and warmups are outside the measured
interval. Both routes materialize their output batches. The harness checks the
complete output schema and row multiset, including duplicate rows and NULLs,
before timing. Samples alternate engine order and are recorded individually.
Repeated reads use the operating system's warm filesystem cache; no cold-storage
claim is made.

Run from the repository root (the generated data stays under the ignored
`target` directory):

```sh
cargo run --release --locked -p roc-datafusion --example parquet_compare -- \
  --rows 4000000 --files 8 --samples 10 --warmups 2 \
  --partitions 1,4 --compression snappy --output-dir target/parquet-comparison
```

`results.json` records environment details, locked dependency versions, actual
scan partition counts, output row counts, physical plans, correctness checks, and raw timings.
`samples.csv` contains each timed pair and which engine ran first. A fresh data
subdirectory is created on each run; the harness never removes an existing data
directory. Use a different `--output-dir` to preserve multiple result reports.

The generated data has four Int64 columns, NULLs in `value` and `selector`, eight
files by default, 65,536-row groups, and 256 group keys. Both engines read all
four columns in every workload. Filter/projection and aggregate nodes are built
above the shared scan, so metadata-only COUNT and differing scan pushdown cannot
change the amount of input read. The primary comparison uses Snappy compression;
`--compression uncompressed` provides a decoding-cost sensitivity check.

DataFusion uses a single aggregate stage for one scan partition. With multiple
partitions, global aggregation uses partials followed by coalescing and a final
aggregate; grouped aggregation uses partials, hash repartitioning, and partitioned
final aggregation. Roc uses its existing worker-local aggregation and final merge.
The benchmark recreates execution state each iteration, including DataFusion's
Parquet file queues and stateful repartition nodes. Resetting state is included
in the measured interval for both engines. These are finite, in-memory aggregation workloads;
spilling and memory-limit behavior are outside this comparison.

The original `filter_project` timings below have two execution differences:
DataFusion's manually constructed `BinaryExpr` defaults to wrapping addition,
whereas Roc checks integer addition for overflow; DataFusion's `FilterExec` also
coalesces small output batches, whereas Roc forwards them individually. The
generated values cannot overflow, so exact results match, but the work performed
is not identical. Treat those timings as observations of the original paths,
not an isolated comparison of execution models. The aggregate workloads contain
no scalar addition or filter and are unaffected by these two differences.

## Initial measurements (before execution optimization)

Measured on 2026-10-04 with an Apple M1 Pro (10 logical CPUs), macOS arm64,
Rust 1.95.0, DataFusion 55.1.0, and Arrow/Parquet 59.3.0. The release build
completed before measurement. Each case uses 4,000,000 rows in eight files,
two warmup pairs and ten alternating measured pairs. Runtime worker count,
Roc worker count and DataFusion scan partition count are all either one or four.
Both compressed and uncompressed runs passed complete schema and row-multiset
comparison for every workload/thread count; every timed execution also checked
its output row count.

Times below are medians in milliseconds, written as **DataFusion / Roc**.

| Workload | Snappy, 1 thread | Snappy, 4 threads | Uncompressed, 1 thread | Uncompressed, 4 threads |
| --- | ---: | ---: | ---: | ---: |
| Scan all four columns | 105.98 / 106.54 | 30.83 / 30.67 | 40.45 / 40.88 | 12.74 / 12.91 |
| Filter and project | 121.49 / 165.36 | 35.69 / 47.76 | 52.24 / 96.42 | 17.13 / 28.71 |
| Global SUM + COUNT | 105.24 / 138.67 | 30.71 / 40.04 | 36.53 / 70.21 | 12.22 / 20.97 |
| Grouped SUM + COUNT, 256 keys | 120.09 / 201.11 | 35.38 / 57.88 | 52.14 / 134.98 | 16.08 / 36.97 |

Snappy files total 49.22 MiB; uncompressed files total 120.60 MiB. Pure scan
times are close in both runs, so these measurements do not establish a scan
speed advantage. With four threads and Snappy, Roc takes 34% more time for
filter/project, 30% more for global aggregation and 64% more for grouped
aggregation. With uncompressed input those gaps are 68%, 72% and 130%.
This initial synthetic workload favored DataFusion's compute operators;
the larger relative gaps with uncompressed input motivate profiling Roc's
compute paths. These timings alone do not attribute costs to specific functions.

Useful profiling candidates in Roc are global aggregation's per-row group-ID
handling, row encoding and hashing of the single integer grouping key, and the
intermediate arrays built by filtering and scalar constant evaluation. The later
execution optimization and ablation section below follows those findings; these
initial measurements are retained as historical evidence.

Saved evidence:

- [Snappy report and physical plans](../../benches/parquet_snappy_results.json)
  and [individual samples](../../benches/parquet_snappy_samples.csv).
- [Uncompressed report and physical plans](../../benches/parquet_uncompressed_results.json)
  and [individual samples](../../benches/parquet_uncompressed_samples.csv).
- [Build command and source checksums](../../benches/parquet_build_metadata.json).

For these runs the benchmark binary was built with
`cargo build --release --workspace --all-targets -j 6` and then run directly
from `target/release/examples/parquet_compare` using the arguments above,
with distinct output directories for each compression setting.

## Initial execution diagnostics

Run the separate diagnostic mode against an existing generated data directory:

```sh
cargo run --release --locked -p roc-datafusion --example parquet_compare -- \
  --diagnose target/parquet-comparison/data-<timestamp> target/parquet-diagnostics
```

This first audits the actual work: independently planned Parquet sources for
each engine, observed input/output batch histograms, scanned-byte counters and
pruning counters. Source metrics are read into ordinary numbers; reusing live
metric handles would otherwise mix successive executions. This audit is untimed.
The report also saves DataFusion's default SQL physical plans and session
settings, separately from the manually constructed benchmark plans. A predicate
in a scan plan is not proof of decoder-level row filtering: DataFusion 55.1
defaults to row-group pruning enabled and row filter pushdown disabled.

The next experiment decodes Parquet once and feeds the same resident Arrow
batches to every variant. It compares native DataFusion, Roc with its usual
yield interval, and Roc with that interval increased to `usize::MAX`. The
filter/project case additionally compares checked DataFusion addition, then
checked addition with small-batch coalescing bypassed. Setting the filter target
to one makes each nonempty filtered batch pass through; it does not split the
output into single-row batches.

Each case validates complete output schemas and row multisets before two
warmup rounds and ten measured rounds. Variant order rotates between rounds.
Plan construction and result deallocation are outside timing. Resident timings
include both operators and their drivers, so they do not by themselves isolate
kernel costs or establish that one execution model is faster. Increasing Roc's
yield interval changes just one scheduling factor; other explicit yields remain.
The diagnostic variant is not a production recommendation to disable yielding.

The 4,000,000-row Snappy dataset was audited with one and four threads. In every
workload both engines produced 496 scan batches, reported 51,539,593 scanned
bytes, matched 64 row groups and pruned zero row groups. All four columns were
scanned. With four threads, filter/project produced 231 output batches in
DataFusion and 496 in Roc; checked DataFusion with coalescing bypassed also
produced 496. The output values, schemas and row multisets matched in every case.

Default SQL planning is a separate case: DataFusion projects three scan columns
for filter/project, one for global aggregation, and two for grouped aggregation.
It also projects away the predicate column inside FilterExec and attaches a
pruning predicate to the Parquet scan. These SQL plans were inspected but not
timed or executed in the audit. Those optimizations were absent from both routes
in the original controlled benchmark.

Resident-input medians on the same M1 Pro, four threads, milliseconds:

| Workload | DataFusion original | DataFusion checked add | DataFusion checked, no filter coalescing | Roc yield 16 | Roc yield max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Scan/collect resident batches | 1.013 | — | — | 0.394 | 0.380 |
| Filter/project | 6.375 | 6.627 | 6.204 | 17.282 | 17.373 |
| Global SUM + COUNT | 0.830 | — | — | 9.779 | 9.886 |
| Grouped SUM + COUNT | 4.904 | — | — | 26.404 | 26.283 |

Aligning addition checks and filtered batch shapes leaves a substantial gap.
Increasing Roc's yield interval does not materially close it in this run.
The bare resident scan/collect is faster through Roc with four threads, but that
case also compares its worker-local collection against DataFusion's partition
coalescing; it does not establish a general push-versus-pull advantage. These
results favor investigating aggregate reductions, primitive group-key indexing,
constant expansion and filter-mask conversions before changing the scheduler.
They do not yet isolate the contribution of each of those implementation details.

The [diagnostic report](../../benches/datafusion_execution_diagnostics.json)
contains scan counters, batch histograms, SQL plans, one-thread results and all
timed variants. [Individual samples](../../benches/datafusion_execution_samples.csv)
and [build metadata/source checksums](../../benches/datafusion_execution_build.json)
are also saved.

## Execution optimization and ablations

### Final measured result

On the same M1 Pro / Rust 1.95.0 / DataFusion 55.1.0 / Arrow 59.3.0 setup,
the retained `borrowed_arguments` build passed **120 release workspace tests,
including one documentation test**. The final Snappy comparison uses three
independent processes, each with two warmups and ten samples. Values below are
medians of process medians, milliseconds, written as **DataFusion / Roc**.
Filter/project uses checked addition and matching output batch shapes.

| Workload | Resident, 1 thread | Resident, 4 threads | Snappy, 1 thread | Snappy, 4 threads |
| --- | ---: | ---: | ---: | ---: |
| Scan/collect | 0.099 / 0.189 | 1.015 / 0.166 | 104.254 / 104.374 | 30.884 / 30.756 |
| Filter/project | 17.239 / 16.900 | 5.911 / 5.163 | 119.130 / 118.447 | 35.379 / 35.269 |
| Global SUM + COUNT | 1.870 / 1.884 | 0.751 / 0.811 | 103.900 / 103.937 | 30.699 / 30.770 |
| Grouped SUM + COUNT | 16.254 / 13.073 | 4.807 / 4.142 | 119.066 / 115.887 | 34.967 / 33.974 |

Against the original controlled resident baseline, four-thread Roc time fell
**70.4% for filter/project, 91.7% for global aggregation, and 84.6% for grouped
aggregation**. In the final comparison, resident filtering and grouping take
12.7% and 13.8% less time than their DataFusion controls. Snappy end-to-end scan,
filter and global aggregation are effectively at parity in these samples;
grouping takes about 2.8% less time. The four-thread resident global aggregate
still takes 0.060 ms (8.0%) more than DataFusion. Single-thread resident scan
also retains a small absolute adapter/driver gap. These remaining differences
are not hidden by reporting only the Parquet-dominated total.

A separate one-process, ten-sample uncompressed sensitivity run gave four-thread
DataFusion / Roc medians of 13.155 / 13.011 ms for scan, 16.761 / 16.585 ms for
filter/project, 12.272 / 12.318 ms for global aggregation, and 16.376 / 15.499 ms
for grouped aggregation. This is additional evidence, not three-process data.
The final diagnostic audit again found equal scan rows, batches, bytes and
pruning. Raising Roc's yield interval from 16 to the maximum changed that
separate diagnostic's four-thread global median from 0.873 to 0.859 ms, against
DataFusion's 0.774 ms. This small effect does not justify disabling cooperative
yielding, and does not isolate the other adapter/allocation/driver costs.

The workload remains four million generated Int64 rows, uniform 256-key grouping,
1/4 input partitions and warm cache. These results do not cover high-cardinality
or string grouping, joins/sorts, spill, memory limits, cold storage, or the full
SQL optimizer. Input pruning/projection is still deliberately equal in both
routes. Neither this benchmark nor the resident scan comparison proves a general
push-versus-pull execution-model advantage.

[Final repeated report](../../benches/optimization/results/final-borrow/summary/summary.md)
contains process ranges and both filter controls. See also the
[uncompressed run](../../benches/optimization/results/final-uncompressed/uncompressed-final/ablation.json)
and [final driver diagnostic](../../benches/optimization/results/final-diagnostics/diagnostics.json).

### Where the changes save work

The optimization sequence follows the diagnostic result: first remove avoidable
operator work, then examine the driver. The controlled benchmark continues to
use the same DataFusion reader, scan columns, input batches and requested runtime/input parallelism.
This does not establish that push execution is intrinsically faster: DataFusion
also evaluates expressions and aggregates over Arrow batches.

For `SELECT group_key, SUM(value), COUNT(value) ... GROUP BY group_key`, each
worker first assigns group IDs, then updates its local sums/counts, and finally
merges those states. The new integer index hashes the primitive key directly,
keeping a separate NULL group. This avoids encoding and allocating general row
keys for a single integer column. Strings and composite keys retain the general
row representation because their equality and ordering need that encoding.
Typed aggregate states then downcast once per batch and update native values,
avoiding a type/number-enum match for every row. Dense and sparse validity paths
both skip NULL payloads before arithmetic.

An ungrouped integer SUM needs only one state. It now uses Arrow's batch reduction
rather than allocating group IDs and dispatching one update per row; COUNT uses
the logical NULL count. Floating SUM/AVG retain input-order accumulation so this
optimization does not silently change signed zero or reassociate arithmetic.
Integer SUM keeps wrapping overflow, COUNT keeps checked overflow, and empty AVG
partial states retain the DataFusion-compatible NULL representation.

For `selector > 50` and `value + 1`, the predicate's existing BooleanArray now
feeds Arrow filtering directly. The old path converted it into row indices and
then a new boolean mask. Bound constants remain scalar operands, avoiding
repeated full-column expansion. Scalar comparisons use Arrow's bitmap kernel;
checked arithmetic, NULL handling and floating total-order comparisons remain
part of the contract. Aggregate argument preparation also avoids cloning every
input column and recasting arrays already in the required type.

At the driver boundary, a batch that already fits is moved instead of sliced;
this avoids rebuilding array wrappers and recounting validity bits. Ready scans
check the cancellation flag but only register a cancellation waiter when the
read actually blocks. The pending path registers and rechecks the latch, which
prevents a shutdown between those steps from being lost. The cooperative yield
interval remains 16. A trial limited aggregate output workers by the number of
remaining morsels, but repeated measurements showed no stable benefit. That
trial and its new source interface were removed. The last retained change borrows
empty/single-reference aggregate argument slices directly from the input batch;
SUM(value) and COUNT(value) no longer allocate an argument vector or clone/drop
an array reference on every batch.

### Cumulative implementation sequence

Below are four-thread **resident-input Roc medians in milliseconds**, ten samples
in one process per stage. Each row adds the named change. These are marginal
measurements in this order, not independent contributions that can be added
arbitrarily. Small changes need the repeated leave-one-out experiment below.

| Stage | Scan | Filter/project | Global SUM + COUNT | Grouped SUM + COUNT |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 0.403 | 17.441 | 9.834 | 26.812 |
| Direct filter mask | 0.405 | 7.259 | 9.696 | 26.572 |
| Bound scalar constants | 0.641 | 6.387 | 9.895 | 26.364 |
| Global batch reduction | 0.454 | 6.200 | 1.422 | 26.024 |
| Typed aggregate state | 0.407 | 6.465 | 1.503 | 22.421 |
| Primitive integer group index | 0.443 | 6.040 | 1.503 | 5.427 |
| Argument preparation / validated IDs / no redundant cast | 0.409 | 6.399 | 1.002 | 4.648 |
| Dense/sparse aggregate loops | 0.423 | 6.199 | 1.393 | 4.201 |
| Move whole batches | 0.388 | 5.941 | 1.341 | 4.556 |
| Register cancellation waiters only on Pending | 0.188 | 6.003 | 0.934 | 7.842 |
| Arrow scalar comparison | 0.225 | 5.585 | 0.879 | 4.101 |
| Bound aggregate output worker count | 0.216 | 5.522 | 0.869 | 4.297 |

The cancellation stage's 7.842 ms grouped result is deliberately retained.
Restarting the **same binary** gave 4.062 ms with a DataFusion control interleaved in that process
of 5.134 ms. A five-process hash-only probe did not reproduce that large change:
process medians were 8.636–8.841 ms with random seeds and 8.529–8.596 ms with
DataFusion's fixed seed. This does not identify the source of the outlier;
production hashing was not changed on that evidence. Subsequent comparisons use
three independent processes per configuration instead of relying on one run.

Floating SUM/AVG semantics were corrected after this cumulative sequence; the
final repeated experiment includes that correction and its regression tests.
The timed SUM workloads use Int64, so no floating speedup is claimed.

### Repeated removal experiments

The saved leave-one-out experiment used the `float_semantics` checkpoint: all
optimizations through the output-worker trial, plus the floating semantic
correction. Each configuration ran in **three independent processes**, with ten
samples per process. The table reports the median of the three process medians.
Positive penalties mean removing the mechanism made Roc slower. These conditional
penalties do not sum to the cumulative speedup.

Four threads, resident input, milliseconds:

| Removed mechanism | Workload | Complete checkpoint | Without mechanism | Removal penalty |
| --- | --- | ---: | ---: | ---: |
| Direct filter mask | Filter/project | 5.568 | 15.334 | +9.766 (+175.4%) |
| Scalar constant path, including Arrow scalar comparison | Filter/project | 5.568 | 7.013 | +1.446 (+26.0%) |
| Global batch reduction | Global SUM + COUNT | 0.854 | 4.633 | +3.779 (+442.3%) |
| Native integer key/index/hash path | Grouped SUM + COUNT | 4.201 | 29.255 | +25.054 (+596.4%) |
| Ready-scan cancellation fast path | Global SUM + COUNT | 0.854 | 1.394 | +0.540 (+63.2%) |
| Output worker limit trial | Global SUM + COUNT | 0.854 | 0.850 | −0.004 (−0.5%) |

The integer-index control changes both key representation and hash-map strategy;
it does not separate row-encoding cost from hasher cost. The scalar control
removes the dependent Arrow scalar-comparison specialization with constant
binding. Cancellation's global-aggregate process medians ranged from 0.857 to
1.415 ms when disabled, versus 0.850–0.864 ms when enabled; its resident scan
penalty was present in all three processes. The
[full removal report](../../benches/optimization/results/leave-one-out/summary/summary.md)
includes every workload, process range, DataFusion control and raw sample range.

Removing the output-worker trial also changed the grouped median from 4.201 to
3.996 ms. These measurements did not justify retaining the extra interface.
The final argument-borrowing change is a separate three-process comparison
against `without_source_parallelism`, which has the same surrounding production
code without borrowed arguments. Thus the saved removal experiment is explicitly
a historical checkpoint, not a claim that all mechanisms were re-ablated after
the last small change.

### Final argument-borrowing increment

After removing the output-worker trial, borrowing direct aggregate arguments
changed the four-thread resident global median from 0.850 to 0.811 ms (4.6%
less), while its DataFusion control changed from 0.755 to 0.751 ms (0.5% less).
The single-thread global median changed from 1.975 to 1.884 ms (4.6% less), with
a 2.7% improvement in the DataFusion control. This is a small incremental gain,
not the explanation for the order-of-magnitude global-aggregate speedup.

Grouped aggregation did not improve in this increment: the four-thread Roc
median changed from 3.996 to 4.142 ms (+3.6%), while DataFusion changed from
4.977 to 4.807 ms. Roc's process ranges overlap (3.844–4.135 versus
4.095–4.299 ms), but these samples do not establish a grouped benefit. The
argument borrowing avoids allocations on both paths; its measured global gain
is retained without claiming that every workload became faster. Off-path timing
changes, such as filtering in this experiment, are not attributed to argument
borrowing. The [incremental report](../../benches/optimization/results/borrowed-arguments-comparison/summary.md)
preserves every result and control.

### Reproduction and evidence

Generate the dataset with the earlier command, then supply its actual `data-*`
directory. The runner works in isolated source copies and never reverses patches
in the working checkout:

```sh
python3 benches/optimization/run.py target/parquet-comparison/data-<timestamp> \
  --output target/roc-cumulative --samples 10
python3 benches/optimization/run.py target/parquet-comparison/data-<timestamp> \
  --output target/roc-removals --leave-one-out --replicas 3 --samples 10
python3 benches/optimization/summarize.py target/roc-removals \
  --leave-one-out --output-dir target/roc-removals-summary
python3 benches/optimization/archive.py --builds target/roc-removals/builds.json \
  --series reproduced-removals
```

Every configuration receives a fresh build of the two local packages and a full
release workspace test run. The runner rejects reused library artifacts or
artifacts pointing at the wrong source copy, then checks the executable hash and
embedded source/build identity. This guard was added after Cargo reused a local
package from another source directory in an early attempt; those invalid timings
were discarded and all affected stages rerun. The dependency lockfile, features,
release flags and benchmark harness are held constant across comparisons.
The current commands rebuild the latest implementation and its controls; the
[historical runner](../../benches/optimization/results/leave-one-out/runner.py),
per-stage patches and source manifests identify the earlier removal checkpoint.
The cumulative command includes the later float correction, rejected trial
removal and argument-borrowing stages in addition to the twelve-row table above.
All builds finish before timed processes start. Configuration order rotates
between process rounds; engine order rotates within each case.

Each process uses two warmups and ten measured runs per variant. Full schemas
and row multisets are compared before timing, with row counts checked after
every timed run. Filter controls use checked addition; the filter-batch1 control
also matches Roc's output batch histogram. Resident batches are sorted by their
unique ID and partitioned identically, so a different decode completion order
cannot change worker inputs. Reported process ranges are observations, not
confidence intervals; within-process samples are not independent processes.

[The cumulative report](../../benches/optimization/results/cumulative-summary/summary.md)
contains the per-stage deltas and both DataFusion controls. [The archive index](../../benches/optimization/results/index.json) links build
identities and validation status. Each run preserves the original sample CSV,
a readable report, the full original report as lossless gzip, source manifest,
and test log. The [reversible patch-chain audit](../../benches/optimization/results/source-chain-audit.json)
verified that undoing every stage returns the exact baseline files and applying
them again returns the measured source. The
[hash probe](../../benches/optimization/results/hash-probe/analysis.json)
and [invalid-build explanation](../../benches/optimization/results/invalidated-build-attempt.txt)
are retained alongside successful experiments.
