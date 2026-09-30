# Roc

Roc is an extensible, composable execution engine for relational workloads.
It provides relational operators, pipeline execution, and pipeline scheduling.

Hosts own metadata, catalog lookup, logical expressions, and physical planning.
They construct operator trees with explicit column positions and casts. Roc does
not parse SQL, resolve names, insert casts, extract aggregates from projections,
or rewrite subqueries. For `SUM(x) * 3`, supply an Aggregate operator followed by
a Project that multiplies the aggregate's output column by three.

## Expressions and execution

`expr/scalar/` contains `ScalarExpression` and one file per node: reference,
constant, function, cast, conjunction, not, case, and coalesce. Nodes are static
descriptions with private fields and public constructors/accessors. They carry no
inferred result metadata or mutable evaluation state. Constants carry their own
value type and casts carry their target type as part of the operation itself.
`expr/agg/` separately describes bound aggregate calls. There is no umbrella
expression type combining scalar and aggregate expressions.

`expr::scalar::executor::ScalarExpressionExecutor` binds one expression against a
host-supplied Arrow schema; `ExpressionExecutor` binds a list of expressions.
`ScalarExpressionExecutor` is a dispatch enum. Each expression file implements
its concrete executor: Reference, Constant, Cast, Conjunction, Not, Case, and
Coalesce. Function expressions choose UnaryFunction or BinaryFunction executors.
Concrete executors can be created and used independently through `try_new()` or
the expression's `create_executor()`, and converted to the dispatch enum with
`Into`. They own their kernels, child executors, and worker-local scratch space.
Binding returns output metadata for the outer `ExpressionExecutor` collection;
the dispatch enum stores no `ExpressionResult` or scalar flag.
Function evaluation uses `UnaryEvalFn = fn(&ArrayRef) -> Result<ArrayRef>` or
`BinaryEvalFn = fn(&ArrayRef, &ArrayRef) -> Result<ArrayRef>`.
Initialization chooses a function pointer for the operation, primitive numeric
type, and broadcast direction. Evaluation traverses the bound executor tree
without interpreting the expression description. Function executors pass child
results directly to the kernel, without an argument vector or intermediate buffer.
Expression Arcs can be shared, while each worker has its own executor.

Evaluation accepts `ExpressionInput::new(&columns, num_rows)`, without a
RecordBatch. Optional `with_selection(&rows)` addresses the original columns;
array outputs follow selection order and preserve duplicates. The caller supplies
columns matching the bound layout, consistent row counts, and valid selection
indices. Evaluation performs no input or output validation; typed kernels
downcast their arguments directly. `evaluate()`
returns `ArrayRef`: scalar nodes hold one value, array nodes hold the selected row
count. The executor's `is_scalar()` determines broadcast semantics; an array of
length one remains an array. CASE and COALESCE conservatively always return
arrays. `evaluate_array()` on a single executor or `evaluate_arrays()` on a list
materializes scalar results when an operator needs full columns. Empty inputs
always return empty arrays and skip kernels, including constant expressions that
would otherwise fail. Zero-column inputs retain their explicit row counts.

For example, a host can execute a binary function directly:

```rust
let expression = FunctionExpression::new(
    FunctionKind::Add,
    vec![
        ReferenceExpression::new(0).into_ref(),
        ConstantExpression::int64(Some(3)).into_ref(),
    ],
);
let mut executor = BinaryFunctionExpressionExecutor::try_new(&expression, schema)?;
let output = executor.evaluate(&ExpressionInput::new(&columns, num_rows))?;
```

Scalar built-ins include arithmetic, comparisons, null tests, null-safe
comparisons, strict/try casts, Boolean operations, CASE, and COALESCE. Numeric
arithmetic initially supports Arrow integer types up to 64 bits and Float32/64,
with matching argument types and checked integer overflow. Other type signatures
are rejected during executor initialization. Comparison semantics follow the
corresponding Arrow kernels, including floating-point ordering. Explicit casts
use Arrow's conversion behavior; TRY_CAST returns NULL for supported value
conversion failures, but unsupported type conversions remain errors.

Boolean value evaluation uses three-valued logic. Predicate selection accepts
only TRUE; AND/OR predicates evaluate children in supplied order on remaining
candidate rows. Value evaluation of AND/OR evaluates all children. CASE and
COALESCE evaluate only the required rows, then restore the original order.
`select()` returns original row indices in selection order. Selected references
use Arrow take on the referenced columns and may allocate; executor state does
not imply reusable output buffers for every Arrow kernel. Casts and comparisons
for non-numeric types currently retain Arrow's internal type dispatch. The
current built-ins are deterministic and need no function-local mutable state;
volatile functions and user-defined function state are not exposed yet.

## Operator parameters

- `FilterOperator::new(predicate)` accepts a `ScalarExprRef`. Its worker
  initializes the expression executor against the first batch schema, requires a
  Boolean result, and preserves the input schema.
- `ProjectOperator::new(projection)` accepts a `Projection`: a host-supplied input
  schema and named scalar expressions. Its worker owns `ProjectionExecutor`,
  including the derived output schema. `Projection::output_schema()` resolves
  metadata through executor initialization without storing it in expressions.
- `AggregateOperator::try_new(groups, aggregates)` accepts a grouping `Projection`
  and `Arc<AggregateExpression>` values, all bound to the same input layout.
  An optional `with_alias(...)` names each output field; otherwise its aggregate
  function name is used.
  Workers evaluate group keys and aggregate arguments with scalar executors and
  maintain separate aggregate states. Output consists of group fields followed by
  aggregate fields; input schema metadata is retained. `output_schema()` returns
  a fallible, executor-derived schema.

Built-in aggregates are COUNT(*), COUNT(expr), COUNT(DISTINCT expr), SUM, AVG,
MIN, MAX, and COVAR_POP. FILTER runs before argument evaluation. DISTINCT is
currently supported only for COUNT(expr). Aggregate ORDER BY is not exposed.
Signed integer SUM returns Int64, unsigned SUM returns UInt64, and floating SUM
returns Float64. AVG and COVAR_POP return Float64; MIN/MAX preserve the input type.
Numeric SUM/AVG/COVAR_POP accept the same integer/float types listed above.
MIN/MAX and DISTINCT use Arrow row encoding for supported input types. Integer
sum/count overflow is an error. Floating accumulation uses ordinary floating-point
arithmetic; merge order can affect rounding.

Each aggregate exports typed partial-state arrays for worker merging. DISTINCT
exports the set of non-null keys, preserving cross-worker deduplication, and
COVAR_POP exports count, means, and co-moment. Empty global aggregation returns
one row (COUNT is zero; other aggregates are NULL); empty grouped aggregation
returns no rows. Grouping without aggregate functions is supported.

For example, a host can construct a projection over an existing child node:

```rust
use arrow::datatypes::SchemaRef;
use roc::{
    expr::scalar::{ReferenceExpression, ConstantExpression,
                  FunctionExpression, FunctionKind},
    operator::{OperatorTreeNode, ProjectOperator, Projection, ProjectionExpression},
};

fn project_plus_one(child: OperatorTreeNode, input_schema: SchemaRef) -> OperatorTreeNode {
    // The host has established that input column 0 is Int64.
    let expression = FunctionExpression::new(
        FunctionKind::Add,
        vec![
            ReferenceExpression::new(0).into_ref(),
            ConstantExpression::int64(Some(1)).into_ref(),
        ],
    ).into_ref();
    let projection = Projection::new(input_schema, vec![
        ProjectionExpression::new(expression, "incremented"),
    ]);
    OperatorTreeNode::new(ProjectOperator::new(projection), vec![child])
}
```

Roc uses Arrow 59.x directly for arrays and compute kernels. Hosts must use
compatible Arrow versions. Expression and operator descriptions are not currently
serde-serializable; transport formats and their decoding belong to the host.
See `tests/physical_expressions.rs` for a complete operator-tree example and
`tests/scalar_expressions.rs` for scalar semantics.

`ScanStorage` defines its task descriptor through `StorageTaskDesc`. `ScanOperator`
forwards that opaque descriptor to storage and relays returned batches unchanged.
Read columns, pushdown predicates, and storage planning belong to the host.
Use separate Filter and Project operators for Roc-side processing.
The optional `scan_channel` helper uses asyncband's bounded MPMC queue. Cloned
receivers compete for batches, and sends wait when the queue is full. Drop all
senders to let receivers drain and finish; drop all receivers to reject sends.
Channel endpoints do not expose an explicit `close()` method.
`ExchangeSourceOperator` holds an exchange ID and service; the sink retains a
schema because endpoints must also be initialized for empty input.

`PipelineGraphExecutor` creates a `(Shutdown, ShutdownGuard)` pair.
Call `executor.shutdown()` before starting execution to obtain the caller's
`Shutdown` handle. Only the caller requests shutdown: `request_shutdown()` signals
it, and awaiting `Shutdown` both requests shutdown and waits for every guard to
be released. Roc directly re-exports `asyncband::shutdown::Shutdown`.
Internally, guards use asyncband's `ShutdownGuard` directly. Standalone pipelines
can create a pair with `asyncband::shutdown::new()`.

Pipelines, workers, and host services receive only `ShutdownGuard`. Its
`is_shutdown_requested()` checks the shared atomic state, and
`shutdown_requested().await` waits for the request. Guard clones keep shutdown
completion pending until their work ends. There is no cancellation tree or
internal shutdown request, including on errors, normal completion, or drop.
No runtime or background forwarding task is required.

Execution errors return promptly so the caller can decide to shut down remaining
work. Dropping the execution future also leaves that decision to the caller;
spawned work may still be running. For example:

```rust
let shutdown = executor.shutdown();
let result = executor.execute().await;
if result.is_err() {
    shutdown.await; // Request shutdown and wait for remaining guards to drop.
}
```

Execution roles initialize shared state with `init_global_context(&shutdown_guard)`.
`ScanRequest::new(source, shutdown_guard)` exposes `source()`, `shutdown_guard()`,
and `into_parts()`. Scan and Exchange forward the same execution guard to host
services. Background work must retain a guard clone until its cleanup finishes.
On normal completion (including early stop), service `finish()` stops and joins
its own work without requiring a shutdown request. On failure or drop, handles
release their resources; any remaining background work observes caller shutdown.

`PipelineExecutionConfig::batch_rows` controls pipeline processing batches; it is
not passed into operators. Aggregation uses internal 2048-row morsels for partial
merges and shared output consumption, with cancellation checks between morsels.

`ProcessExecutor::execute(&input)` returns `ProcessResult`:
`NeedMoreInput(batch)` completes the current input, `MoreResult(batch)` requests
another call with the same input, and `Finished(batch)` delivers a final output
and stops accepting input in this worker.
Outputs are processed downstream before a continuation is resumed, including when
multiple processors expand their input. Empty outputs skip downstream processing;
an empty `MoreResult` still resumes the current processor. Processors may keep
buffered data across `NeedMoreInput` calls.

When source input ends, the executor calls `ProcessExecutor::finish()` from
upstream to downstream. Each call returns `Some(batch)` until the processor returns
`None`. Every output and its downstream continuations drain before the next finish
call, and downstream processors finish only after all upstream outputs have arrived.
Empty finish outputs are valid, including on an empty source. `Finished(batch)`
discards upstream continuations and finishes only downstream processors after
delivering its output. A finished sink stops all further processing and finishing.
Sink combine and pipeline finalization still run after a successful early stop;
errors and cancellation skip successful completion. Other workers continue; global
early-stop semantics require coordination through operator shared state.
`yield_batches` bounds execute/finish/sink calls between cooperative yields,
including empty outputs, so cancellation remains observable while draining results.

Process execute and finish calls are synchronous and do not take a cancellation
handle. The pipeline checks cancellation between calls; an individual synchronous
call runs to completion before the next check.
