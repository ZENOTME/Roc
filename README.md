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
`SuboperatorBuilder` provides individual suboperator construction, state
registration, column bindings, and validation; it does not traverse physical
operators or expression trees. Each pipeline lowers into one context of `Suboperator` enums connected by dense
`VariableId`s. `ProgramContext` binds native kernels; each in-flight batch owns a reusable
`ProgramExecutionContext` with vector buffer slots and ordinary state slots.
State factories are generic: the runtime does not have a special aggregate-state
variant. The independent scalar `Evaluation` API has been removed.

Sources remain asynchronous. Rayon runs synchronous processing and sink local
updates with exclusive ownership of each instance. The pipeline collects local
states for combination before asynchronous finalization. Exchange sinks flush
bounded batches asynchronously after CPU completion.

Native execution is the default and never invokes a runtime compiler. Optional
Cranelift compilation fuses scalar regions across consecutive physical projections.
`Staged` execution limits each compiled loop to four scalar operations. Selection,
stateful kernels, and source/sink I/O are explicit boundaries; compilation
continues on both sides of native boundaries. Group lookup and each accumulator
update are distinct suboperators using ordinary state slots.

Compiled scalar operations cover signed/unsigned integers of all standard widths,
Float32/64, and Boolean, including NULL propagation and checked integer arithmetic.
Integer division/remainder, general casts, complex Arrow types, and aggregation
kernels retain native vector calls. All existing operators and expression types
remain executable in every mode; this is not a claim that every kernel is JIT
compiled. Arrow supplies owned storage layouts and complex-type primitives.

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

Compilation happens during graph construction; `PipelineGraph::explain()` prints
its executable program/state schedule. `SuboperatorContext::compile(mode)` consumes
the build IR and returns a `ProgramContext` containing only programs and state
builders. Selection, projection materialization, and sink local updates are
ordinary programs; the runtime retains no physical process callbacks or outer
region interpreter. Each program owns its bound kernel parameters or JIT code.

One async `Pipeline::execute` coordinator owns a single source cursor and a bounded
pool of exclusive `ProgramExecutionContext`s. Synchronous program calls run on
Rayon and return local state for asynchronous flushing and reuse. Once all work
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
Custom result sinks can register a `Vec<RecordBatch>` with a native `BatchUpdate`
program, then consume that vector in `combine`. There is no separate local sink
executor interface. Dead intermediate arrays are
released at their planned last use. There is no execution-time compilation API.

Run the fixed-condition CPU benchmark (preparation and execution reported separately):

```sh
cargo run --release --features jit --example vector_program_bench
```

The benchmark checks output equality before timing and rotates mode order.
It measures compute loops, not storage, scheduling, or a DuckDB comparison.
JIT speedups depend on the workload; nullable frame conversion, allocation,
random memory accesses, and CPU scheduling still have costs.

## DataFusion Parquet integration

The optional [`roc-datafusion`](integrations/datafusion/README.md) workspace crate
adapts DataFusion's planned Parquet scans to Roc's `ScanStorage` interface. It
reuses DataFusion's I/O and decoding while Roc executes downstream operators.
The integration includes storage correctness tests and a reproducible comparison
of both engines over the same Parquet scan plan.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
