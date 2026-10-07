# Conjunction Arrow reuse microbenchmark

This standalone benchmark compares the previous Roc merge, the amended implementation composed from Arrow APIs, and repeated Arrow `and_kleene`/`or_kleene` calls used by DataFusion's general merge path. The last case is not a full DataFusion execution and excludes short-circuiting and row preselection. Both source snapshots are included for reproduction.

Four Boolean inputs use deterministic mixed values, 25% NULLs in nullable cases, and slice offsets 0/7/14/21. Output equality is checked before timing. Each of three independently launched processes records nine samples per case, reversing kernel order between samples. Inputs are warm and preparation is excluded; each timed iteration creates and finishes one accumulator. 8192-row samples use 4000 iterations; 65536-row samples use 500. Medians pool all 27 samples. Values are nanoseconds per four-input batch merge, not query latency.

`summary.json` records source identities, environment, medians and relative elapsed-time changes. `canonical-samples-*.csv` retain all samples. Positive relative change against the previous implementation means a regression. The 8192-row nullable regression is retained explicitly; this is a reuse refactoring with a measured tradeoff, not a universal speedup.

Reproduce from the repository root:

```sh
cargo run --release --locked --manifest-path benches/optimization/logical-plan/conjunction-arrow/Cargo.toml
```

No DataFusion integration dependency or workspace member is introduced by this benchmark.
