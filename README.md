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
- **Expressions**: typed descriptions lowered to variable/suboperator programs and aggregate
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
interface. Physical operators first build their children, then append directly to
`PipelineBuilder.suboperators`, without retaining an operation queue or a sink
lowering hook. Each operator and expression owns its `build_suboperator` logic:
aggregation registers grouping state, each aggregate expression registers its
accumulator, and scalar expressions build their own inputs and compute nodes.
`SuboperatorScopeBuilder` provides individual suboperator construction, state
registration, column bindings, and validation; it does not traverse physical
operators or expression trees. Each pipeline lowers into a `SuboperatorScope`:
ordinary `Suboperator`s form a data DAG connected by dense `VariableId`s, while
ordered `ScopeBoundary`s seal each DAG segment and open its nested successor.
`ScopeSuboperator::Filter` opens an `if` body without changing column bindings or
creating a new source batch. Ancestor variables remain visible to descendants.
The compiler completes each local DAG before opening the next scope and closes
scopes in reverse order. Separate local/scope emission contexts keep ordinary
suboperators from leaving a control scope open.
Scope suboperators can also define fresh body variables. `filter_with_outputs`
returns selected-value bindings that become visible only after body entry;
ordinary `filter` needs no such bindings. Both kinds of producer participate in
variable lookup and visibility checks. `DependencyToken` expresses ordering
without allocating a value buffer: `dependency_of(value)` names its producer,
state updates return a token, and `with_dependencies` attaches prerequisites to
newly constructed nodes. A scope token means body entry, not loop completion.
Token producers execute once in their owning scope; local conditional programs
capture completed definitions instead of repeating their side effects.

`ProgramContext` owns a generated Cranelift entry point. Each in-flight batch
owns a reusable `ProgramExecutionContext` with ordinary state slots. The runtime
retains no expression graph, vector instruction dispatcher, or native fallback.
`Program` entries exposed for inspection describe the compile-time schedule only.

All query computation uses generated row code. A pipeline compiles to one row
loop; Filter opens a nested `if`, and CASE/COALESCE emit balanced local branches
with short-circuit semantics. Intermediate numeric values use their actual SSA
types. Arithmetic, comparisons, integer division/remainder, and numeric casts
emit typed instructions. Type and cast-mode dispatch happens during compilation,
not inside the generated loop. String parsing/formatting and mutable hash-table
or aggregate state operations use scalar runtime functions with bound signatures.
These helpers never receive a batch, operation tag, or expression tree.

Roc depends on `arrow-array`, `arrow-buffer`, and `arrow-schema` for interchange. Arrow compute and
row kernels are absent from the library dependency graph; the umbrella `arrow`
crate is used only as an independent test oracle. Generated numeric loads read Arrow
buffers and null bitmaps directly. Generated 64-bit outputs become Arrow buffers
without a scalar conversion pass; other outputs retain scalar ABI conversion.
Identity outputs
retain their Arrow owners when all rows survive. Variable-width state values own
their bytes beyond the current batch. Supported generated values include standard
integers, Float32/64, Boolean, strings/binary, dates/timestamps, and NULL. Unsupported
signatures fail explicitly; there is no interpreter fallback.

Cranelift is always enabled. `ExecutionMode::Compiled` is the sole mode; the `jit`
Cargo feature remains an empty compatibility feature. Sources remain asynchronous.
Parallel execution uses Rayon with exclusive state ownership; single-worker
execution runs inline and yields between bounded work intervals. Generated
output ports collect rows for the batch sink interface; publication and async I/O
happen after successful computation. Batch sinks terminate row computation; a
continuation requiring the published batch belongs to another pipeline.
Aggregation exports/merges local partial
states during the pipeline lifecycle. Exchange sinks flush asynchronously.

```rust,ignore
use roc::{pipeline::build_pipeline_graph, suboperator::ExecutionMode};
// tree is the fully typed physical operator tree supplied by the caller.
let graph = build_pipeline_graph(tree, ExecutionMode::Compiled)?;
// PipelineGraphExecutor::new(graph) ...
```

The [annotated Rust walkthrough](design/execution_architecture.rs) is executable:

```sh
cargo run --example execution_architecture --features jit
cargo test --workspace --all-features --all-targets
```

Compilation happens during graph construction. `PipelineGraph::explain()` prints
the generated schedule; `ProgramContext::generated_ir()` exposes the typed
Cranelift IR. `SuboperatorScope::compile(mode)` validates data/control/effect
visibility, generates code, and discards the construction graph. State bindings
and source/output layouts are fixed before execution. Generated loops poll
cancellation at bounded row intervals and propagate helper/arithmetic failures without
publishing partial output batches.

One async `Pipeline::execute` coordinator owns a single source cursor and a bounded
pool of exclusive `ProgramExecutionContext`s. Parallel program calls run on
Rayon and return local state for asynchronous flushing and reuse. Immediately
available source batches share a bounded CPU task to amortize dispatch; pending
source reads remain alive, preserving asynchronous source/sink feedback. Once all work
succeeds, the coordinator combines locals and asynchronously finalizes the sink.
`Pipeline.sink` is a `Box<dyn SinkExec>`. Physical operators register ordinary
states while building suboperators and give their IDs to the concrete sink.
`combine(&mut self, local: ProgramExecutionContext, guard)` uses these IDs to take
local states and merge them into state owned exclusively by the sink. Aggregation
shares only its finalized result through an `Arc<OnceLock<_>>` with its source;
there is no generic global-state reference or lock around mutable merge state.
The `SourceExec` itself owns its cursor and runtime resources; `start` opens I/O
only when the pipeline runs. Aggregation has no extra sink-update call or wrapper state.
Exchange queues batches into an ordinary local state and drains it asynchronously.
Custom result sinks can register a `Vec<RecordBatch>` with a `BatchUpdate`
output port, then consume that vector in `combine`. There is no separate local sink
executor interface. Intermediate expressions remain scalar SSA values rather
than Arrow arrays. There is no execution-time compilation API.

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

For a whole-engine comparison against a pinned main commit, use
`python3 benches/main_compare/run.py --baseline <commit>`. The shared harness
validates every output row, builds isolated release binaries, and separates
preparation/JIT time from execution. It covers resident scan, filter/project,
arithmetic, division, global aggregation, and grouped aggregation with 1/4 workers.

## DataFusion Parquet integration

The optional [`roc-datafusion`](integrations/datafusion/README.md) workspace crate
adapts DataFusion's planned Parquet scans to Roc's `ScanStorage` interface. It
reuses DataFusion's I/O and decoding while Roc executes downstream operators.
The integration includes storage correctness tests and a reproducible comparison
of both engines over the same Parquet scan plan.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
