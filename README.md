# Roc

Roc is an extensible, composable execution engine for relational workloads.

It provides relational operators, pipeline execution of operator trees, and pipeline scheduling.

Roc accepts fully bound physical plans. Hosts construct operators with DataFusion
`PhysicalExprRef` (`Arc<dyn PhysicalExpr>`) values and compose them into an
`OperatorTree`. Before constructing the tree, the host fixes input/output schemas,
column indices, and any casts, and ensures each child's output matches its parent's
input. Roc evaluates physical expressions directly through DataFusion. All operator fields
are private; construct operators with `new` or `try_new`. Pipeline
construction does not resolve column names, insert casts, or rebind expressions.

- `FilterOperator` holds only a bound `predicate`. It evaluates the incoming batch
  directly and preserves its schema.
- `ProjectOperator::new(projector)` takes a DataFusion `Projector` prepared by the host with
  `ProjectionExprs::make_projector(child_output_schema)`. It already holds the
  expressions and output schema; Roc calls `project_batch` directly. The operator
  has no separate expression list or input/output schema fields.
- `AggregateOperator::try_new(groups, aggregates)` takes a DataFusion `Projector`
  for group keys and `Vec<Arc<AggregateFunctionExpr>>`. Both are bound to the same
  child output. The operator stores only these two inputs. `output_schema()`
  derives group fields followed by aggregate result fields on demand. Workers keep
  each aggregate's state arrays separately, without a flattened partial schema or
  column-offset table. DataFusion
  handles accumulation, state merging, and final evaluation, using batch-oriented
  `GroupsAccumulator` implementations or its `GroupsAccumulatorAdapter` fallback.
  An empty group projector denotes global aggregation, which uses ordinary
  accumulators through the adapter, including their empty-input behavior.
  Roc manages group IDs and pipeline scheduling. Numeric semantics follow the
  supplied DataFusion aggregates; the host supplies any required argument casts.
  Aggregates with an effective `ORDER BY` are currently rejected because Roc does
  not establish ordering across workers.
- `ScanStorage` defines its task descriptor through the `StorageTaskDesc` associated
  type. `ScanOperator` holds only that opaque descriptor and storage service. It
  forwards the task to storage and relays returned batches unchanged, managing
  cancellation and scan finalization. Read columns, storage predicates, and any
  pushdown belong to the host's task descriptor and storage implementation.
  Use separate `FilterOperator` and `ProjectOperator` nodes for Roc-side processing.
- `ExchangeSourceOperator` holds only the exchange ID and service. The sink retains
  a schema because the service needs it to create endpoints, including for empty
  input.

For example, a host can construct a projection over an existing child node:

```rust
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion_common::ScalarValue;
use datafusion_expr_common::operator::Operator as ExprOp;
use datafusion_physical_expr::{
    expressions::{BinaryExpr, Column, Literal},
    projection::{ProjectionExpr, ProjectionExprs},
};
use roc::{PhysicalExprRef, Result, operator::{OperatorTreeNode, ProjectOperator}};

fn project_plus_one(child: OperatorTreeNode, input_schema: SchemaRef) -> Result<OperatorTreeNode> {
    // The host has established that input column 0 is a nullable Int64.
    let expression: PhysicalExprRef = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("value", 0)),
        ExprOp::Plus,
        Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
    ));
    let projector = ProjectionExprs::from(vec![
        ProjectionExpr::new(expression, "incremented"),
    ]).make_projector(&input_schema)?;
    Ok(OperatorTreeNode::new(ProjectOperator::new(projector), vec![child]))
}
```

There is no Roc expression layer: `PhysicalExpr` and `PhysicalExprRef` are re-exported
from DataFusion at the crate root. Operators check their required tree shape;
DataFusion and Arrow report evaluation errors. Aggregate executors create their
accumulators from the supplied expressions. Execution does not infer or repair the plan.
Physical expressions are runtime objects; operator descriptors containing them are
not serde-serializable. Hosts that transport plans must decode and bind their own
plan representation before constructing the runtime tree.

Roc uses DataFusion 55.1.0 and Arrow 59.x. Hosts supplying expressions must use
compatible dependency versions. See `tests/physical_expressions.rs` for an executable
operator-tree example including scan column reordering and aggregation.

Cancellation uses Roc's `Cancel`, backed by `asyncband::Shutdown` and
`ShutdownWatch`. `cancel()` requests cancellation, `cancelled().await` observes it,
and `is_cancelled()` checks it synchronously. Clones share cancellation; `child()`
also observes ancestor cancellation without cancelling its parent or siblings.
Waiting and dropping handles do not request cancellation. No runtime or background
task is needed; the pipeline scheduler separately waits for workers to finish.
`ScanRequest::new(source, cancel)` has private fields exposed through `source()`,
`cancel()`, and the consuming `into_parts()` method.

Execution roles initialize shared state with `init_global_context(&Cancel)`.
`PipelineExecutionConfig::batch_rows` controls pipeline processing batches; it is
not passed into operators. Aggregation uses internal 2048-row morsels for partial
merges and shared output consumption, with cancellation checks between morsels.

`ProcessExecutor::execute(cancel, &input)` returns `ProcessResult`:
`NeedMoreInput(batch)` completes the current input, `MoreResult(batch)` requests
another call with the same input, and `Finished(batch)` delivers a final output
and stops accepting input in this worker.
Outputs are processed downstream before a continuation is resumed, including when
multiple processors expand their input. Empty outputs skip downstream processing;
an empty `MoreResult` still resumes the current processor. Processors may keep
buffered data across `NeedMoreInput` calls.

When source input ends, the executor calls `ProcessExecutor::finish(cancel)` from
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
