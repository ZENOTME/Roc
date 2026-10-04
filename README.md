# Roc

Roc is a composable execution engine for relational workloads, provided as a
Rust library. It executes physical operator trees over Apache Arrow data and
provides extensible components for building data processing systems.

Host systems supply query planning, catalog and metadata lookup, and expression
type resolution. Roc provides APIs for constructing physical plans and their
physical expressions, along with an execution engine that splits those plans
into pipelines and executes them. Hosts integrate their storage, data exchange,
and task scheduling through Roc's interfaces.

## Components

- **Operators**: scans, filtering, projection, aggregation, and data exchange,
  with support for custom operators.
- **Expressions**: vectorized scalar expression evaluation and aggregate
  functions, including arithmetic, comparisons, casts, Boolean operations,
  conditional expressions, and common aggregates.
- **Pipelines**: operator-tree conversion into pipelines, dependency scheduling,
  and parallel execution with cancellation support.
- **Integration**: interfaces for storage adapters, exchange services, result
  sinks, and task executors.

## Getting Started

Requires Rust 1.95 or later.

```sh
git clone https://github.com/ZENOTME/Roc.git
cd Roc
cargo build
cargo test --all-targets
```

Generate the API documentation with:

```sh
cargo doc --no-deps
```

## DataFusion Parquet integration

The optional [`roc-datafusion`](integrations/datafusion/README.md) workspace crate
adapts DataFusion's planned Parquet scans to Roc's `ScanStorage` interface. It
reuses DataFusion's I/O and decoding while Roc executes downstream operators.
The integration includes storage correctness tests and a reproducible comparison
of both engines over the same Parquet scan plan.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
