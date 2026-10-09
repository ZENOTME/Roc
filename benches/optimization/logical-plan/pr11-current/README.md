# PR #11 current scan-path ablation

**Historical result:** PR #11 is closed and its optimization has been removed from subsequent source heads. The isolated ready-channel savings of about 23 ns per batch do not demonstrate a material SQL-query benefit, so the simpler existing scan implementation is retained. The following measured revisions, source archives and all timing samples are unchanged.

Compare main `47f1511b67561bae0b6c7654e71355f20d1d2e7b` with exact PR #11 `6ee7251da77e7238e44034807447ae510420b510`. Only the PR #11 scan polling change and its tests differ. Both use Arrow 59.3.0 and asyncband 0.7.3.

Measured on Apple M1 Pro, rustc 1.95.0 (59807616e 2026-04-14). Production source snapshots, identical probe source, locks, logs, binary/source hashes and all 360 timing samples are included. Binaries stay local.

## Results

Time is nanoseconds per batch; lower is better. Five independent process medians are aggregated geometrically. Reduction intervals use paired process bootstrap.

| Case | main ns/batch | PR #11 ns/batch | Saved ns/batch | Reduction | 95% interval |
|---|---:|---:|---:|---:|---:|
| ready | 119.1 | 88.3 | 30.8 | 25.9% | 16.5% to 34.2% |
| ready_channel | 108.0 | 85.0 | 23.0 | 21.3% | 15.6% to 26.7% |
| ready_sum | 276.1 | 253.4 | 22.7 | 8.2% | 6.1% to 11.3% |
| pending_once | 132.0 | 114.6 | 17.4 | 13.2% | 4.4% to 23.7% |

## Scope and interpretation

- `ready`: a consumer immediately returns a shared 2048-row Int64 batch. Measures the actual public ScanExec next_batch path, including its boxed futures, consumer call, batch clone/drop and identical correctness checks.
- `ready_channel`: 100000 batches are queued in the actual scan_channel before timing; receiver drains already-ready items. Queue creation, producers and prefill are excluded.
- `ready_sum`: the ready path followed by Arrow SUM over 2048 Int64 values. This is a synthetic scan-plus-kernel loop, not Roc aggregation or a SQL query.
- `pending_once`: a synthetic consumer returns Pending once and calls wake_by_ref, then returns Ready. This checks wake/re-poll overhead, with no disk, network or producer latency. Its percentage must not be interpreted as a real I/O speedup.

Every timed batch is checked for 2048 rows and original column ArrayRef identity. ready_sum also verifies the exact total 2096128. Setup and finalization are outside timing. Each of five process pairs uses a randomized variant order, with reversed case order in alternating pairs; each case has 3000 warm-up reads and nine samples of 100000 reads. The two binaries run sequentially. Source rebuilds and binary hashes are recorded.

The already-ready channel case saves about 23 ns per batch. At 2048 rows per batch, one million rows requires about 489 reads, implying roughly 0.011 ms of saved scan-entry overhead under this synthetic setup. This is an illustration, not a measured SQL-query result. Storage, decode, filtering, grouping, pipeline scheduling, contention and real asynchronous waits are excluded. A 21% decrease in this small scan-entry component does not imply a 21% query improvement. The cancellation regression tests remain in the measured PR source; this timing probe does not replace them.

The whole patch also removes fuse/select_biased machinery; these measurements compare complete revisions and do not isolate how much comes specifically from avoiding cancellation waiter registration versus other code generation changes.

## Reproduction

The source archives are standalone copies with an empty workspace table added identically for isolation. Extract either archive and run:

```sh
cargo build --locked --release --example ready_poll_probe
target/release/examples/ready_poll_probe
target/release/examples/ready_poll_probe reverse
```

build.py and run.py record the original local paths and paired run procedure; metadata.json records exact identities. Raw CSVs contain all nine samples for each case in each process. summary.json retains each process median and bootstrap intervals.
