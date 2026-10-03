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
descriptions with private fields and public constructors/accessors. Every node
carries the result type its host resolved: `data_type` and `nullable` are
constructor arguments. Constants carry the type of their literal and a cast's
target type is its result type. The library performs no type inference, name
resolution, coercion, or plan validation: the host has already derived types in
its own logical bind step. Descriptions hold no mutable evaluation state.
`expr/agg/` separately describes bound aggregate calls with the same convention.
There is no umbrella expression type combining scalar and aggregate expressions.

`ScalarExpressionExecutor` provides shared Arrow columns and an explicit row
count. Create it with `ScalarExpressionExecutor::new(columns, num_rows)`, or
replace its input with `set_input(columns, num_rows)`. It clones only ArrayRef
handles and needs no RecordBatch schema. The row count is separate because
zero-column inputs can still contain rows, such as constant-only projections.
Callers provide columns with lengths matching the row count; physical operators
obtain both from their Arrow RecordBatch.

`expression.to_evaluation()` builds an immutable `ScalarExpressionEvaluation`.
The enum dispatches to concrete evaluations containing bound operations, child
expressions, and the static type needed for empty results. They retain no output
arrays. Every output has one entry per input row, in the same order.
`evaluate` is public; each concrete evaluation has a private `eval`:

```rust
pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef>;
fn eval(&self, executor: &ScalarExpressionExecutor,
    input: &[&ArrayRef]) -> Result<ArrayRef>;
```

`evaluate` recursively obtains child arrays and calls `eval` with references to
those arrays. Unary nodes consume one result and binary nodes consume two.
Reference and Constant have no child inputs. CASE and COALESCE additionally pass
their local row mapping to `eval` to restore the original order.
Reference clones the shared input column handle; computed arrays are returned
by value without copying their buffers. Callers own outputs independently of
later evaluations or the evaluation's lifetime. There is no `result()` accessor.
Every call recomputes the tree. Once a parent consumes its child arrays, those
intermediates are released unless another owner retains them. CASE and COALESCE
keep temporary branch arrays and row mappings within that call. Their scratch
vectors are allocated per call; the existing Arrow kernels allocate output
buffers rather than reusing previously returned arrays.

Filters retain one predicate evaluation. Projection expressions and aggregate
arguments use `Vec<ScalarExpressionEvaluation>`, sharing one input executor per
batch. Result types come from the static descriptions. Descriptions and immutable
evaluations can be shared across workers. Kernel binding uses the host-declared
operand types without an input schema or type inference.
Empty inputs return empty arrays and skip unused children and function kernels.
Constants expand to the current logical row count. Evaluating against an unbound
executor returns an execution error. Arrow validates the batch's internal shape;
expression layout and operand types remain the host's responsibility.

For example:

```rust
let expression = FunctionExpression::binary(
    FunctionKind::Add,
    ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true)).into_ref(),
    ConstantExpression::int64(Some(3)).into_ref(),
    DataType::Int64,
    true,
);
let executor = ScalarExpressionExecutor::new(batch.columns(), batch.num_rows());
let evaluation = expression.to_evaluation()?;
let output = evaluation.evaluate(&executor)?;
```

Scalar built-ins include arithmetic, comparisons, null tests, null-safe
comparisons, strict/try casts, Boolean operations, CASE, and COALESCE. Numeric
arithmetic initially supports Arrow integer types up to 64 bits and Float32/64,
with matching argument types and checked integer overflow. Which signatures are
admissible is the host's decision; a declared type or operation pair without a
kernel fails evaluation construction, and everything else is host error. Comparison
semantics follow the corresponding Arrow kernels, including floating-point
ordering. Explicit casts
use Arrow's conversion behavior; TRY_CAST returns NULL for supported value
conversion failures, while unsupported type conversions surface as execution
errors, because a cast description already names its target type.

Boolean evaluation uses three-valued logic. AND/OR evaluate all children and
produce a Boolean array aligned with the input batch. Evaluations have no
`select` method, and the executor has no selection state or row-selection API.
Filters and aggregate filters use the separate `expr::predicate::select_true`
to convert that result to row positions, accepting only valid TRUE.
CASE and COALESCE evaluate only required branches: they gather branch columns
with Arrow `take`, pass the branch row count explicitly, and
restore the original row order with Arrow `interleave`. Their private `eval`
merges the prepared branch arrays. These conditional expressions protect unused
operations such as division by zero; AND/OR do not provide that protection.
Aggregate filters use Arrow `filter_record_batch` and keep group IDs in matching
order, including zero-column batches. Gather and kernel results may allocate;
executor state does not imply reusable output buffers for every Arrow kernel.
Casts and comparisons for non-numeric types currently retain Arrow's internal
type dispatch. The
current built-ins are deterministic and need no function-local mutable state;
volatile functions and user-defined function state are not exposed yet.

## Operator parameters

- `FilterOperator::new(predicate)` accepts a `ScalarExprRef`. Its worker builds the
  predicate evaluation once in `new_executor()` from the description, without
  consulting any batch schema, and preserves the input schema. The host must
  declare a Boolean predicate.
- `ProjectOperator::new(projection)` accepts a `Projection`: named scalar
  expressions whose descriptions already carry their result type. Its worker owns
  `ProjectionExecutor`, including the derived output schema.
  `Projection::output_schema()` assembles fields from the descriptions, with no
  executor and no fallible step. `Projection::from_indices(schema, indices)` is a
  convenience that reads column types, nullability, names, and metadata from the
  schema; `Projection::new(expressions)` leaves metadata empty unless
  `with_metadata(...)` supplies it.
- `AggregateOperator::try_new(groups, aggregates)` accepts a grouping `Projection`
  and `Arc<AggregateExpression>` values whose descriptions carry their own result
  metadata. An optional `with_alias(...)` names each output field; otherwise its
  aggregate function name is used.
  Workers evaluate group keys and aggregate arguments with scalar executors and
  maintain separate aggregate states. Output consists of group fields followed by
  aggregate fields; grouping metadata is retained. `output_schema()` reads the
  descriptions and is infallible.

Built-in aggregates are COUNT(*), COUNT(expr), COUNT(DISTINCT expr), SUM, AVG,
MIN, MAX, and COVAR_POP. FILTER runs before argument evaluation. DISTINCT is
currently supported only for COUNT(expr); other DISTINCT aggregates are rejected
when the executor is built rather than silently evaluated without it. Aggregate
ORDER BY is not exposed. Signed integer SUM returns Int64, unsigned SUM returns
UInt64, and floating SUM returns Float64. AVG and COVAR_POP return Float64;
MIN/MAX preserve the input type. Those result types are declared by the host, which
is also responsible for supplying numeric SUM/AVG/COVAR_POP arguments and the
argument counts the accumulators consume; nothing is checked up front. A missing
argument surfaces as an execution error when data flows through the accumulator,
and a non-numeric SUM argument follows Arrow's safe-cast rules, so unparseable
values become NULL. MIN/MAX and COUNT(DISTINCT) need an argument type to build
their row encoder, so those two reject a description without arguments.
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
use arrow::datatypes::{DataType, SchemaRef};
use roc::{
    expr::scalar::{ReferenceExpression, ConstantExpression,
                  FunctionExpression, FunctionKind},
    operator::{OperatorTreeNode, ProjectOperator, Projection, ProjectionExpression},
};

fn project_plus_one(child: OperatorTreeNode, input_schema: SchemaRef) -> OperatorTreeNode {
    // The host resolves column 0 from its own schema, as its logical bind step did.
    let field = input_schema.field(0);
    let expression = FunctionExpression::binary(
        FunctionKind::Add,
        ReferenceExpression::new(0, field.data_type().clone(), field.is_nullable()).into_ref(),
        ConstantExpression::int64(Some(1)).into_ref(),
        DataType::Int64,
        true,
    ).into_ref();
    let projection = Projection::new(vec![
        ProjectionExpression::new(expression, "incremented"),
    ]).with_metadata(input_schema.metadata().clone());
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
`ExchangeConsumer::next()` returns `Result<Option<RecordBatch>>`: `Ok(None)` ends
the input normally, and read errors propagate to the execution caller.

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
