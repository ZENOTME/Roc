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

## DataFusion integration

The optional [`roc-datafusion`](integrations/datafusion/README.md) workspace crate
converts DataFusion's analyzed, optimized logical plans and bound expressions
into Roc operator trees. Users retain DataFusion's SQL/DataFrame APIs and table
providers, then explicitly execute the returned plan through Roc. DataFusion
handles scan I/O and pruning; Roc owns operators, aggregation phases and pipelines.
The integration includes correctness tests and a reproducible SQL comparison.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
