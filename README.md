# Roc

Roc is a high-performance execution runtime for relational workloads, provided
as a Rust library.

Roc executes a fully specified physical IR over Apache Arrow data. It assumes
that query analysis and optimization have already been performed by the host
system. The host lowers its optimized plan into Roc's IR, specifying the
physical operators, expressions, types, column indices, and other information
required for execution.

Roc does not provide query analysis or optimization. It focuses solely on
executing physical plans efficiently.

## Features

- **Executable physical IR** — a low-level representation of physical
  operators and expressions designed for direct execution.
- **Vectorized execution** — operators and expressions process data in Apache
  Arrow batches.
- **Ahead-of-time specialization** — operators and expressions are bound to
  type-specific implementations before execution, reducing dynamic dispatch
  and branching in hot paths.
- **Parallel execution** — operators use worker-local execution state together
  with shared global state to support parallel pipelines.
- **Pipeline scheduling** — operator trees are decomposed into pipelines and
  scheduled according to their dependencies, with cooperative execution and
  cancellation.
- **Embeddable and extensible** — hosts provide storage, exchange, result
  sinks, and task execution through Roc's interfaces, and can implement custom
  physical operators.

## Architecture

```text
            Host System
                 │
      Analysis / Optimization
                 │
                 ▼
      Optimized Physical Plan
                 │
              lowering
                 ▼
   ┌───────────────────────────┐
   │          Roc IR           │
   │                           │
   │  Physical Operators       │
   │  Physical Expressions     │
   │  Physical Information     │
   └─────────────┬─────────────┘
                 │
                 ▼
          Specialization
                 │
                 ▼
        Pipeline Formation
                 │
                 ▼
          Task Scheduling
                 │
                 ▼
       Vectorized Execution
```

The boundary is simple:

**The host decides what to execute. Roc executes it efficiently.**

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

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
