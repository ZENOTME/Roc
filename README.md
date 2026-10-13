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
- **Expressions**: typed descriptions lowered to kernel graphs and aggregate
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
the pipeline graph is built:

- `ExecutionMode::Compile` (the default) fuses the whole graph into one generated
  LLVM row loop: Filter opens a nested `if`, CASE/COALESCE emit balanced
  branches, Count/Sum updates are inlined and global integer reductions lower to
  SIMD. Compilation happens during graph construction.
- `ExecutionMode::ZeroCompile(library)` generates no code for the pipeline. Each
  kernel binds a precompiled function and runs over windows of 1024 rows in plan
  order; a Filter domain is a selection vector of root rows. With
  `KernelLibrary::Generated`, a Call runs LLVM code generated once per
  signature with the instruction selection of fused programs and cached for the
  life of the process; `prewarm_kernels()` generates every signature up front, and
  a miss generates its signature when the program is prepared. With
  `KernelLibrary::Native`, Calls run Rust implementations with the same results.

Both modes execute the same plan and return equal results and equal errors: when
several rows fail, the lowest failing row wins, as in the fused row loop. The
test suite runs every expression, aggregate and pipeline test in all modes and
compares them.

```rust,ignore
use roc::{kernel::{ExecutionMode, KernelLibrary, prewarm_kernels},
          pipeline::{build_pipeline_graph, build_pipeline_graph_with_mode}};
// tree is the fully typed physical operator tree supplied by the caller.
let graph = build_pipeline_graph(&tree)?; // ExecutionMode::Compile
prewarm_kernels()?;
let graph = build_pipeline_graph_with_mode(
    &tree,
    ExecutionMode::ZeroCompile(KernelLibrary::Generated),
)?;
// PipelineGraphExecutor::new(graph) ...
```

The [annotated Rust walkthrough](design/execution_architecture.rs) is executable:

```sh
cargo run --example execution_architecture
cargo test --all-targets
```

`PipelineGraph::explain()` prints each pipeline's kernel graph and
`Program::generated_ir()` the optimized LLVM IR of a compiled program. String parsing
and formatting and hash-table or aggregate state operations use scalar runtime
functions with bound signatures. Unsupported signatures fail when the program
is built; there is no interpreter fallback. Execution polls cancellation and
never publishes the output of a chunk that failed.

Roc depends on `arrow-array`, `arrow-buffer`, and `arrow-schema` for interchange;
the umbrella `arrow` crate is only a test oracle. Output columns that pass an
input through keep its Arrow array when all rows survive.

One async `Pipeline::execute` coordinator owns the source cursor and a bounded
pool of `LocalState`s, one per in-flight execution instance. Program calls run on
Rayon (inline with one worker) and the coordinator passes each output chunk to
`SinkExec::consume`. Once all work succeeds, it hands every `LocalState` to
`SinkExec::combine`, which takes the states the sink registered while building
the pipeline and merges them into state it owns exclusively, then calls
`SinkExec::finish`. Aggregation shares only its finalized result with the
pipeline reading it; exchange sinks flush asynchronously.

Run the fixed-condition CPU benchmark (preparation and execution reported separately):

```sh
cargo run --release --features jit --example vector_program_bench
```

This experimental branch uses Inkwell 0.7.1 with LLVM 19.1 for all generated
code, including cached kernels and specialized reductions. Install the LLVM 19
development tools and shared library, then set `LLVM_SYS_191_PREFIX` to the
installation prefix if `llvm-config` is not on PATH. For example:

```sh
LLVM_SYS_191_PREFIX=/path/to/llvm19 cargo test --locked -p roc
```

Generated modules are verified, optimized with LLVM's `default<O3>` pipeline
for the host CPU/features, verified again, and finalized with MCJIT at aggressive
optimization. No fast-math or unchecked-overflow flags are enabled. The generated
kernel library pays this cost once at prewarm/cache-miss time; fused programs pay
it during each graph construction. Native Rust kernels are unchanged.

The benchmark separates compilation from execution and checks results before timing.
It measures compute loops, not storage, scheduling, or a DuckDB comparison.
JIT speedups depend on the workload; allocation, output materialization,
random memory accesses, and CPU scheduling still have costs. Global integer
SUM/COUNT reductions lower to SIMD with sparse/dense null handling. Ordinary
SUM/COUNT state updates are emitted directly; word-key grouping uses a bounded,
collision-checked lookup cache with a hash-table slow path.

For a whole-engine comparison against a pinned main commit, use
`python3 benches/main_compare/run.py --baseline <commit>`. The shared harness
validates every output row, builds isolated release binaries, and separates
preparation/JIT time from execution. It covers resident scan, filter/project,
arithmetic, division, global aggregation, and grouped aggregation with 1/4 workers,
and measures the current engine in every execution mode.

## DataFusion Parquet integration

The optional [`roc-datafusion`](integrations/datafusion/README.md) workspace crate
adapts DataFusion's planned Parquet scans to Roc's `ScanStorage` interface. It
reuses DataFusion's I/O and decoding while Roc executes downstream operators.
The integration includes storage correctness tests and a reproducible comparison
of both engines over the same Parquet scan plan.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
