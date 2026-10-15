# Roc

Roc is a high-performance execution runtime for relational workloads, provided
as a Rust library.

Roc executes a fully specified physical IR over Apache Arrow data. It assumes
that query analysis and optimization have already been performed by the host
system. The host lowers its optimized plan into Roc's IR, specifying the
physical operators, expressions, types, column identifiers, and other
information required for execution.

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

Requires Rust 1.95 or later and LLVM 19 (set `LLVM_SYS_191_PREFIX`, for
example to `$(brew --prefix llvm@19)`).

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

## Program execution

The public physical operators and typed expression constructors remain the plan
interface. Columns are identified by `ColumnId`s allocated in a `ColumnContext`;
every expression names the column it produces. Each pipeline collects a
`KernelGraph` through `PipelineBuilder.kernels` (a `KernelBuilder`): operators
first build their children, then add their kernels; expressions lower themselves;
aggregation registers its grouping and accumulator states. The graph holds typed
variables, row domains, kernels and states. Scalar kernels are Constant, Call,
Select, RaiseIf, GroupLookup and AggregateUpdate; a Filter kernel opens a child
domain holding the rows whose predicate is TRUE. Ancestor variables stay visible
in descendant domains.

Each pipeline becomes one `Program`, in one of two execution modes chosen when
the pipeline graph is built. Both run the plan over windows of 1024 rows: values
keep their Arrow width (inputs are read in place, root outputs written in place),
Booleans — values, validity and error flags — are bitmaps, constants are
broadcast, and a Filter domain is a row mask of the window.

- `ExecutionMode::ZeroCompile` generates no code. Every kernel calls a function
  of the kernel library that the build script compiles into the binary, in
  explicit SIMD for each CPU class (AVX2 or SSE2 on x86_64, NEON on aarch64)
  and selected at run time.
- `ExecutionMode::Compile` (the default) additionally fuses each run of
  consecutive kernels into one vectorized function compiled with LLVM when the
  program is prepared; the values between them stay in registers. A window
  where a fused function may raise reruns the kernels it replaced, which find
  the failing row.

Both modes generate code with one emitter (`src/kernel/codegen`), which defines
every kernel's semantics once. They execute the same plan and return equal
results and equal errors: when several rows fail, the lowest failing row wins.
The test suite runs every expression, aggregate and pipeline test in both modes
and compares them.

```rust,ignore
use roc::{kernel::ExecutionMode,
          pipeline::{build_pipeline_graph, build_pipeline_graph_with_mode}};
// tree is the fully typed physical operator tree supplied by the caller.
let graph = build_pipeline_graph(&tree)?; // ExecutionMode::Compile
let graph = build_pipeline_graph_with_mode(&tree, ExecutionMode::ZeroCompile)?;
// PipelineGraphExecutor::new(graph) ...
```

The [annotated Rust walkthrough](design/execution_architecture.rs) is executable:

```sh
cargo run --example execution_architecture
cargo test --all-targets
```

`PipelineGraph::explain()` prints each pipeline's kernel graph and
`Program::generated_ir()` the LLVM IR of a compiled program's fused functions.
String parsing and formatting and hash-table or aggregate state operations are
runtime functions with bound signatures. Unsupported signatures fail when the
program is built; there is no interpreter fallback. Execution polls cancellation
and never publishes the output of a chunk that failed.

Roc depends on `arrow-array`, `arrow-buffer`, and `arrow-schema` for interchange;
the umbrella `arrow` crate is only a test oracle. Output columns that pass an
input through keep its Arrow array when all rows survive.

One async `Pipeline::execute` coordinator owns the source cursor and a bounded
pool of `(LocalState, LocalSink)` pairs. Program calls and local sink work run as
tasks of the caller's executor; a single worker runs inline, and ordered
pipelines use one instance. After each program run, `LocalSink::consume` receives
the optional output and returns `End` (continue), `Flush` (hand off and reuse),
or `Stop` (stop dispatching new input). `LocalSink::finish` prepares each handoff;
`SinkExec::consume` takes the prepared data on the coordinator and restores the
instance after Flush or Stop. Once input and in-flight jobs end, every instance
prepares End in parallel, all End handoffs complete, and `SinkExec::finish`
merges and publishes the result. Only this final finish receives `Cpu`, which
submits work to the same executor: aggregates and windows finish hash partitions
in parallel, tables assemble contiguous bucket ranges, and sorts gather columns
in parallel. Local aggregate End preparation also releases its kernel state on
the worker; Flush keeps the slots available for reuse. Exchange sinks send
buffered chunks asynchronously on handoff. See
[`design/sink-parallelism.md`](design/sink-parallelism.md) for the protocol.

Run the fixed-condition CPU benchmark (preparation and execution reported separately):

```sh
cargo run --release --features jit --example vector_program_bench
```

The benchmark separates compilation from execution and checks results before timing.
It measures compute loops, not storage, scheduling, or a DuckDB comparison.
JIT speedups depend on the workload; allocation, output materialization,
random memory accesses, and CPU scheduling still have costs. Global integer
SUM/COUNT reductions lower to SIMD with sparse/dense null handling. Ordinary
SUM/COUNT state updates are emitted directly; word-key grouping uses a bounded,
collision-checked lookup cache with a hash-table slow path.

## DataFusion Parquet integration

The optional [`roc-datafusion`](integrations/datafusion/README.md) workspace crate
adapts DataFusion's planned Parquet scans to Roc's `ScanStorage` interface. It
reuses DataFusion's I/O and decoding while Roc executes downstream operators.
The integration includes storage correctness tests and a reproducible comparison
of both engines over the same Parquet scan plan.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
