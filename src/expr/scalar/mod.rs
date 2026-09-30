//! Scalar expressions contain no inferred result metadata or execution state.
use crate::error::{Error, Result};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt64Array, new_empty_array, new_null_array},
    compute::{kernels::interleave::interleave, take},
    datatypes::{DataType, SchemaRef},
};
use executor::{ExpressionInput, ExpressionResult, ScalarExpressionExecutor};
use std::sync::Arc;

mod case;
mod cast;
mod coalesce;
mod conjunction;
mod constant;
pub mod executor;
mod function;
mod kernels;
mod not;
mod reference;

pub use case::{CaseExpression, CaseExpressionExecutor};
pub use cast::{CastExpression, CastExpressionExecutor, CastMode};
pub use coalesce::{CoalesceExpression, CoalesceExpressionExecutor};
pub use conjunction::{Conjunction, ConjunctionExpression, ConjunctionExpressionExecutor};
pub use constant::{ConstantExpression, ConstantExpressionExecutor};
pub use function::{
    BinaryFunctionExpressionExecutor, FunctionExpression, FunctionKind,
    UnaryFunctionExpressionExecutor,
};
pub use not::{NotExpression, NotExpressionExecutor};
pub use reference::{ReferenceExpression, ReferenceExpressionExecutor};

pub type ScalarExprRef = Arc<ScalarExpression>;

#[derive(Clone, Debug)]
pub enum ScalarExpression {
    Reference(ReferenceExpression),
    Constant(ConstantExpression),
    Function(FunctionExpression),
    Cast(CastExpression),
    Conjunction(ConjunctionExpression),
    Not(NotExpression),
    Case(CaseExpression),
    Coalesce(CoalesceExpression),
}

impl ScalarExpression {
    pub fn into_ref(self) -> ScalarExprRef {
        Arc::new(self)
    }
    pub fn create_executor(&self, input_schema: SchemaRef) -> Result<ScalarExpressionExecutor> {
        ScalarExpressionExecutor::bind(self, &input_schema).map(|(executor, _)| executor)
    }
}

macro_rules! scalar_node {
    ($($variant:ident($ty:ty)),* $(,)?) => {$(
        impl From<$ty> for ScalarExpression {
            fn from(value: $ty) -> Self { Self::$variant(value) }
        }
        impl $ty {
            pub fn into_ref(self) -> ScalarExprRef {
                Arc::new(ScalarExpression::$variant(self))
            }
        }
    )*};
}
scalar_node! {
    Reference(ReferenceExpression), Constant(ConstantExpression),
    Function(FunctionExpression), Cast(CastExpression),
    Conjunction(ConjunctionExpression), Not(NotExpression),
    Case(CaseExpression), Coalesce(CoalesceExpression),
}

// Binding metadata is used during initialization and retained only for roots in
// ExpressionExecutor. Concrete executors keep their own execution state.
trait BindScalarExpression: Sized {
    type Expression;
    fn bind(expression: &Self::Expression, schema: &SchemaRef) -> Result<(Self, ExpressionResult)>;
}
trait SelectExpression {
    fn select_positions(&mut self, input: &ExpressionInput<'_>) -> Result<Vec<usize>>;
}

macro_rules! scalar_executor {
    ($($variant:ident($expression:ty, $executor:ty)),* $(,)?) => {$(
        impl From<$executor> for ScalarExpressionExecutor {
            fn from(executor: $executor) -> Self { Self::$variant(executor) }
        }
    )*};
}
scalar_executor! {
    Reference(ReferenceExpression, ReferenceExpressionExecutor),
    Constant(ConstantExpression, ConstantExpressionExecutor),
    UnaryFunction(FunctionExpression, UnaryFunctionExpressionExecutor),
    BinaryFunction(FunctionExpression, BinaryFunctionExpressionExecutor),
    Cast(CastExpression, CastExpressionExecutor),
    Conjunction(ConjunctionExpression, ConjunctionExpressionExecutor),
    Not(NotExpression, NotExpressionExecutor),
    Case(CaseExpression, CaseExpressionExecutor),
    Coalesce(CoalesceExpression, CoalesceExpressionExecutor),
}

fn row_index(input: &ExpressionInput<'_>, position: usize) -> usize {
    input.selection().map_or(position, |rows| rows[position])
}
fn selected_input<'a>(input: &ExpressionInput<'a>, rows: &'a [usize]) -> ExpressionInput<'a> {
    ExpressionInput::new(input.columns(), input.num_rows()).with_selection(rows)
}

#[derive(Debug, Default)]
struct SelectionBuffers {
    remaining: Vec<usize>,
    next: Vec<usize>,
    rows: Vec<usize>,
}
impl SelectionBuffers {
    fn reset(&mut self, len: usize) {
        self.remaining.clear();
        self.remaining.extend(0..len);
        self.next.clear();
        self.rows.clear();
    }
    fn map_rows(&mut self, input: &ExpressionInput<'_>) {
        self.rows.clear();
        self.rows
            .extend(self.remaining.iter().map(|&i| row_index(input, i)));
    }
}
#[derive(Debug, Default)]
struct BranchBuffers {
    selection: SelectionBuffers,
    matched: Vec<usize>,
    pieces: Vec<ArrayRef>,
    mapping: Vec<(usize, usize)>,
}
impl BranchBuffers {
    fn reset(&mut self, len: usize) {
        self.selection.reset(len);
        self.matched.clear();
        self.pieces.clear();
        self.mapping.clear();
        self.mapping.resize(len, (0, 0));
    }
    fn finish(&self) -> Result<ArrayRef> {
        let refs = self.pieces.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        Ok(interleave(&refs, &self.mapping)?)
    }
}

fn materialize(value: ArrayRef, scalar: bool, rows: usize) -> Result<ArrayRef> {
    if rows == 0 {
        return Ok(new_empty_array(value.data_type()));
    }
    if !scalar || rows == 1 {
        return Ok(value);
    }
    if value.logical_null_count() != 0 {
        return Ok(new_null_array(value.data_type(), rows));
    }
    Ok(take(
        value.as_ref(),
        &UInt64Array::from(vec![0; rows]),
        None,
    )?)
}
pub fn boolean(array: &dyn Array) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| Error::Execution("expected Boolean expression".into()))
}
pub fn require_boolean(result: &ExpressionResult) -> Result<()> {
    if result.data_type != DataType::Boolean {
        return Err(Error::InvalidPlan("expected Boolean expression".into()));
    }
    Ok(())
}
fn require_same_type(a: &ExpressionResult, b: &ExpressionResult) -> Result<()> {
    if a.data_type != b.data_type {
        return Err(Error::InvalidPlan(
            "branch types must match; supply explicit casts".into(),
        ));
    }
    Ok(())
}
