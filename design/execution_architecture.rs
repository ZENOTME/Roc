//! Roc execution model: a top-down review entry point and a runnable example.
//!
//! cargo run --example execution_architecture --features jit
//!
//! Take Scan(x) -> Filter(x > 0) -> Project(x * 2 + 1) -> SUM(...) as the example.
//!
//! 1. Logical plan (src/column.rs, src/operator, src/expr).
//!    The host allocates every column in a `ColumnContext` with its type and
//!    nullability. Expressions name their output column; a Reference names the
//!    column it reads. Operators expose `output_columns()`, so the tree is
//!    fully typed before any execution object exists and can be deserialized.
//!
//! 2. Pipelines (src/pipeline). `build_pipeline_graph(&tree)` lets each
//!    operator split blocking dependencies and build its children first.
//!    A pipeline is a source, one fused `Program` and a sink. The leaf binds
//!    the source columns (`set_source`), every operator adds kernels to the
//!    pipeline's `KernelBuilder`, and the sink names its output columns, or
//!    none when it only combines states (`set_sink`).
//!
//! 3. Kernels (src/kernel). `KernelBuilder` turns expressions into variables,
//!    scalar kernels and row domains:
//!    - a column is bound to variables once per pipeline, so repeated
//!      subexpressions are shared;
//!    - Filter opens a child row domain; later kernels run only on kept rows;
//!    - fallible kernels return pending errors with an error flag; the
//!      consumer raises them, so CASE/COALESCE/AND/OR guard errors of
//!      unselected rows instead of evaluating lazily;
//!    - GroupLookup and AggregateUpdate use per-instance states declared with
//!      a `StateBuilder`; aggregate FILTER runs updates in a child domain.
//!
//! 4. Code generation (src/kernel/jit). The graph is compiled into one row
//!    loop. Pure kernels are placed at the lowest common domain of their
//!    uses; effectful kernels stay in their domain, which is opened lazily as
//!    a branch on its predicate. Booleans are bytes in registers and bitmaps
//!    in Arrow buffers. Errors return a status naming the failing site.
//!    Output columns that forward an unfiltered input keep its Arrow array.
//!    Global COUNT/SUM over one Int64 column use a dedicated reduction loop.
//!
//! 5. Execution (src/pipeline/execution.rs). One async coordinator per
//!    pipeline reads the source and runs the program on CPU workers, each
//!    with an exclusive `LocalState`. Non-empty output chunks go to
//!    `SinkExec::consume` on the coordinator, which may finish early. After
//!    the source ends, every local state is passed to `SinkExec::combine`,
//!    then the source and sink finish. Aggregates combine group indexes and
//!    accumulators; the next pipeline reads the finalized result.

use arrow::{
    array::{ArrayRef, AsArray, Int64Array},
    datatypes::{DataType, Int64Type},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnType},
    expr::scalar::{ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression},
    kernel::{KernelBuilder, Program},
    operator::FilterOperator,
};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The host describes typed columns and expressions; no variables yet.
    let mut columns = ColumnContext::new();
    let int = ColumnType::new(DataType::Int64, false);
    let x = columns.allocate_named(ColumnType::new(DataType::Int64, true), "x");
    let zero = columns.allocate(int.clone());
    let two = columns.allocate(int.clone());
    let one = columns.allocate(int);
    let positive = columns.allocate(ColumnType::new(DataType::Boolean, true));
    let doubled = columns.allocate(ColumnType::new(DataType::Int64, true));
    let y = columns.allocate_named(ColumnType::new(DataType::Int64, true), "y");
    let reference = || ReferenceExpression::new(x).into_ref();
    let predicate = FunctionExpression::binary(
        FunctionKind::GreaterThan,
        reference(),
        ConstantExpression::int64(Some(0), zero).into_ref(),
        positive,
    )
    .into_ref();
    let multiply = FunctionExpression::binary(
        FunctionKind::Multiply,
        reference(),
        ConstantExpression::int64(Some(2), two).into_ref(),
        doubled,
    )
    .into_ref();
    let output = FunctionExpression::binary(
        FunctionKind::Add,
        multiply,
        ConstantExpression::int64(Some(1), one).into_ref(),
        y,
    )
    .into_ref();

    // What a pipeline does: bind the source, add operator kernels, compile.
    let mut builder = KernelBuilder::new(&columns);
    builder.bind_inputs(&[x])?;
    FilterOperator::new(predicate, vec![x]).build_kernels(&mut builder)?;
    let errors = output.build_kernels(&mut builder)?;
    builder.raise(errors)?;
    let program = Program::compile(builder.finish(Some(&[y]))?)?;
    println!("{}", program.explain());

    // Each parallel instance owns a LocalState; the program is shared.
    let mut local = program.new_local()?;
    let (_shutdown, guard) = asyncband::shutdown::new();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-3), Some(3)]));
    let batch = RecordBatch::try_new(columns.schema(&[x])?, vec![input])?;
    let chunk = program
        .run(&batch, &mut local, &guard)?
        .expect("program has an output");
    assert_eq!(
        chunk
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), Some(7)],
    );
    println!("y = [3, 7]");
    Ok(())
}
