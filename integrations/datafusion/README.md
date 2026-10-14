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

async fn parquet_scan(
    path: &str,
    columns: Vec<roc::column::ColumnId>,
) -> datafusion::error::Result<ScanOperator<DataFusionScan>> {
    let ctx = SessionContext::new();
    let frame = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
    // Apply filters/projections before planning when pushdown is wanted.
    let plan = frame.create_physical_plan().await?;
    let source = DataFusionScan::new(plan, ctx.task_ctx());
    // `columns` are the Roc columns bound to the scan's output fields.
    Ok(ScanOperator::new(source, Arc::new(DataFusionStorage), columns))
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
