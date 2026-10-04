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
