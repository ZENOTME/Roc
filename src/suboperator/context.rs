//! Register suboperators and dense variable/state IDs, with scoped column bindings.
use super::{CastMode, Conjunction, FunctionKind, ProgramContext, native};
use crate::{
    error::{Error, Result},
    expr::ExpressionResultType,
};
use arrow::{array::ArrayRef, datatypes::DataType};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct VariableId {
    pub(crate) scope: u64,
    pub(crate) index: usize,
}
impl VariableId {
    pub fn index(self) -> usize {
        self.index
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StateId {
    pub(crate) scope: u64,
    pub(crate) index: usize,
}
impl StateId {
    pub fn index(self) -> usize {
        self.index
    }
}
/// Identifies a batch row domain; selection or a host boundary starts a new one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SourceId(pub usize);
#[derive(Clone, Debug)]
pub struct Variable {
    pub ty: ExpressionResultType,
}

/// A factory for ordinary, exclusively owned program state. The program runtime
/// knows neither the concrete type nor its update/combine semantics.
#[derive(Clone)]
pub struct StateBuilder {
    name: &'static str,
    factory: Option<Arc<dyn Fn() -> Result<State> + Send + Sync>>,
    pub(crate) id: Option<StateId>,
    type_id: std::any::TypeId,
}
impl std::fmt::Debug for StateBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateBuilder")
            .field("type", &self.name)
            .finish_non_exhaustive()
    }
}
impl StateBuilder {
    pub fn new<T: Send + 'static>(factory: impl Fn() -> Result<T> + Send + Sync + 'static) -> Self {
        Self {
            name: std::any::type_name::<T>(),
            factory: Some(Arc::new(move || factory().map(State::new))),
            id: None,
            type_id: std::any::TypeId::of::<T>(),
        }
    }
    /// External state is supplied by the caller before program execution.
    /// It still occupies an ordinary typed slot and is owned by one instance.
    pub fn external<T: Send + 'static>() -> Self {
        Self {
            name: std::any::type_name::<T>(),
            type_id: std::any::TypeId::of::<T>(),
            factory: None,
            id: None,
        }
    }
    pub(crate) fn accepts(&self, state: &State) -> bool {
        self.type_id == state.value.as_ref().type_id()
    }
    pub(crate) fn build(&self) -> Result<State> {
        self.factory.as_ref().ok_or_else(|| {
            Error::Execution(format!("external state {} was not bound", self.name))
        })?()
    }
}
/// Type erasure happens at the batch boundary, never for each row. Kernels
/// borrow their concrete local state and update it without locks.
pub struct State {
    value: Box<dyn std::any::Any + Send>,
}
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("State(..)")
    }
}
impl State {
    pub fn new<T: Send + 'static>(value: T) -> Self {
        Self {
            value: Box::new(value),
        }
    }
    pub fn get<T: 'static>(&self) -> Result<&T> {
        self.value
            .downcast_ref()
            .ok_or_else(|| Error::Execution("state binding type mismatch".into()))
    }
    pub fn get_mut<T: 'static>(&mut self) -> Result<&mut T> {
        self.value
            .downcast_mut()
            .ok_or_else(|| Error::Execution("state binding type mismatch".into()))
    }
    pub fn into_inner<T: 'static>(self) -> Result<T> {
        self.value
            .downcast::<T>()
            .map(|v| *v)
            .map_err(|_| Error::Execution("state binding type mismatch".into()))
    }
}

#[derive(Clone, Debug)]
pub enum Suboperator {
    /// Publish projection columns and/or select rows; starts a new row domain.
    Materialize {
        inputs: Vec<VariableId>,
        schema: Option<arrow::datatypes::SchemaRef>,
        predicate: bool,
    },
    /// A batch entry point updating an ordinary local state slot.
    BatchUpdate {
        state: StateId,
        call: super::BatchUpdate,
    },
    /// Random-access boundary: scalar group keys become stable group IDs.
    GroupLookup {
        state: StateId,
        inputs: Vec<VariableId>,
        output: VariableId,
    },
    /// Update one ordinary accumulator state using group IDs and scalar values.
    AggregateUpdate {
        states: [StateId; 2],
        inputs: Vec<VariableId>,
    },
    SourceColumn {
        source: SourceId,
        column: usize,
        output: VariableId,
    },
    Constant {
        value: ArrayRef,
        state: StateId,
        output: VariableId,
    },
    Call {
        function: FunctionKind,
        inputs: Vec<VariableId>,
        output: VariableId,
    },
    Cast {
        input: VariableId,
        mode: CastMode,
        output: VariableId,
    },
    Not {
        input: VariableId,
        output: VariableId,
    },
    Boolean {
        and: bool,
        inputs: Vec<VariableId>,
        output: VariableId,
    },
    Case {
        branches: Vec<(VariableId, VariableId)>,
        otherwise: VariableId,
        inputs: Vec<VariableId>,
        output: VariableId,
    },
    Coalesce {
        inputs: Vec<VariableId>,
        output: VariableId,
    },
}
impl Suboperator {
    pub fn inputs(&self) -> &[VariableId] {
        match self {
            Self::SourceColumn { .. } | Self::Constant { .. } | Self::BatchUpdate { .. } => &[],
            Self::Materialize { inputs, .. } => inputs,
            Self::GroupLookup { inputs, .. }
            | Self::AggregateUpdate { inputs, .. }
            | Self::Call { inputs, .. }
            | Self::Boolean { inputs, .. }
            | Self::Case { inputs, .. }
            | Self::Coalesce { inputs, .. } => inputs,
            Self::Cast { input, .. } | Self::Not { input, .. } => std::slice::from_ref(input),
        }
    }
    pub fn outputs(&self) -> &[VariableId] {
        if matches!(
            self,
            Self::AggregateUpdate { .. } | Self::Materialize { .. } | Self::BatchUpdate { .. }
        ) {
            return &[];
        }
        let (Self::GroupLookup { output, .. }
        | Self::SourceColumn { output, .. }
        | Self::Constant { output, .. }
        | Self::Call { output, .. }
        | Self::Cast { output, .. }
        | Self::Not { output, .. }
        | Self::Boolean { output, .. }
        | Self::Case { output, .. }
        | Self::Coalesce { output, .. }) = self
        else {
            unreachable!()
        };
        std::slice::from_ref(output)
    }
    pub fn states(&self) -> &[StateId] {
        match self {
            Self::Constant { state, .. }
            | Self::GroupLookup { state, .. }
            | Self::BatchUpdate { state, .. } => std::slice::from_ref(state),
            Self::AggregateUpdate { states, .. } => states,
            _ => &[],
        }
    }
    pub(crate) fn eager_inputs(&self) -> &[VariableId] {
        match self {
            Self::Case { .. } | Self::Coalesce { .. } => &[],
            _ => self.inputs(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SuboperatorContext {
    pub(crate) scope: u64,
    pub(crate) variables: Vec<Variable>,
    pub(crate) suboperators: Vec<Suboperator>,
    pub(crate) state_builders: Vec<StateBuilder>,
    pub(crate) outputs: Vec<VariableId>,
    pub(crate) roots: Vec<VariableId>,
}
impl SuboperatorContext {
    pub fn variables(&self) -> &[Variable] {
        &self.variables
    }
    pub fn suboperators(&self) -> &[Suboperator] {
        &self.suboperators
    }
    pub fn state_builders(&self) -> &[StateBuilder] {
        &self.state_builders
    }
    pub fn outputs(&self) -> &[VariableId] {
        &self.outputs
    }
    pub fn compile(self, mode: super::ExecutionMode) -> Result<ProgramContext> {
        super::ProgramBuilder::new(Arc::new(self), mode)?.build()
    }
}

#[derive(Debug)]
pub struct SuboperatorBuilder {
    scope: u64,
    variables: Vec<Variable>,
    suboperators: Vec<Suboperator>,
    states: Vec<StateBuilder>,
    // Hold the Arc as well as its address: allocator address reuse cannot turn a
    // new expression into an accidental common subexpression.
    bindings: HashMap<usize, (Arc<dyn std::any::Any + Send + Sync>, VariableId)>,
    columns: Option<Vec<VariableId>>,
    source: SourceId,
    schema: Option<arrow::datatypes::SchemaRef>,
    roots: Vec<VariableId>,
}
impl Default for SuboperatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl SuboperatorBuilder {
    pub fn new() -> Self {
        Self {
            scope: NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed),
            variables: vec![],
            suboperators: vec![],
            states: vec![],
            bindings: HashMap::new(),
            columns: None,
            source: SourceId(0),
            schema: None,
            roots: vec![],
        }
    }
    fn push(
        &mut self,
        ty: ExpressionResultType,
        op: impl FnOnce(VariableId) -> Suboperator,
    ) -> VariableId {
        let id = VariableId {
            scope: self.scope,
            index: self.variables.len(),
        };
        self.variables.push(Variable { ty });
        self.suboperators.push(op(id));
        id
    }
    pub fn state(&mut self, mut factory: StateBuilder) -> StateId {
        let id = StateId {
            scope: self.scope,
            index: self.states.len(),
        };
        factory.id = Some(id);
        self.states.push(factory);
        id
    }
    pub fn group_lookup(&mut self, state: StateId, inputs: Vec<VariableId>) -> Result<VariableId> {
        self.check_state(state)?;
        for &id in &inputs {
            self.result_type(id)?;
        }
        Ok(self.push(
            ExpressionResultType::new(DataType::UInt64, false),
            |output| Suboperator::GroupLookup {
                state,
                inputs,
                output,
            },
        ))
    }
    pub fn aggregate_update(
        &mut self,
        states: [StateId; 2],
        inputs: Vec<VariableId>,
    ) -> Result<()> {
        for &state in &states {
            self.check_state(state)?;
        }
        for &id in &inputs {
            self.result_type(id)?;
        }
        self.suboperators
            .push(Suboperator::AggregateUpdate { states, inputs });
        Ok(())
    }
    fn check_state(&self, state: StateId) -> Result<()> {
        if state.scope != self.scope || state.index >= self.states.len() {
            return Err(Error::InvalidPlan(
                "state belongs to another context".into(),
            ));
        }
        Ok(())
    }
    /// A projection changes names/positions, not the scalar dependency graph.
    fn bind_columns(&mut self, columns: Option<Vec<VariableId>>) {
        if columns.is_none() {
            self.source.0 += 1;
        }
        self.columns = columns;
        self.bindings.clear();
    }
    pub(crate) fn require(&mut self, id: VariableId) {
        self.roots.push(id);
    }
    /// Bind positions and names to existing variables without materializing arrays.
    pub fn set_columns(
        &mut self,
        columns: Vec<VariableId>,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<()> {
        if columns.len() != schema.fields().len() {
            return Err(Error::InvalidPlan(
                "column binding count differs from schema".into(),
            ));
        }
        for (&column, field) in columns.iter().zip(schema.fields()) {
            if self.result_type(column)?.data_type() != field.data_type() {
                return Err(Error::InvalidPlan(
                    "column binding type differs from schema".into(),
                ));
            }
        }
        for &column in &columns {
            self.require(column);
        }
        self.bind_columns(Some(columns));
        self.schema = Some(schema);
        Ok(())
    }
    /// A state-producing sink consumed the current view; there is no batch to publish.
    pub fn clear_columns(&mut self) {
        self.bind_columns(None);
        self.schema = None;
    }
    /// Resolve a column position against the current view, or the source batch.
    pub fn column(&mut self, index: usize, ty: ExpressionResultType) -> Result<VariableId> {
        if let Some(columns) = &self.columns {
            columns
                .get(index)
                .copied()
                .ok_or_else(|| Error::InvalidPlan(format!("column index {index} out of bounds")))
        } else {
            Ok(self.input(index, ty))
        }
    }
    /// Reuse a node's variable only within the current column/row domain.
    pub fn cached<T: Send + Sync + 'static>(&self, node: &Arc<T>) -> Option<VariableId> {
        self.bindings
            .get(&(Arc::as_ptr(node) as usize))
            .map(|(_, id)| *id)
    }
    pub fn cache<T: Send + Sync + 'static>(&mut self, node: Arc<T>, id: VariableId) -> Result<()> {
        self.result_type(id)?;
        self.bindings
            .insert(Arc::as_ptr(&node) as usize, (node, id));
        Ok(())
    }
    pub fn materialize(&mut self, predicate: Option<VariableId>) -> Result<()> {
        if let Some(predicate) = predicate {
            self.require_bool(predicate)?;
        }
        if self.columns.is_none() && predicate.is_none() {
            return Ok(());
        }
        let mut inputs = self.columns.take().unwrap_or_default();
        inputs.extend(predicate);
        self.suboperators.push(Suboperator::Materialize {
            inputs,
            schema: self.schema.take(),
            predicate: predicate.is_some(),
        });
        self.bind_columns(None);
        Ok(())
    }
    /// Materialize the current batch view, then update a declared ordinary state.
    pub fn batch_update(&mut self, state: StateId, call: super::BatchUpdate) -> Result<()> {
        self.check_state(state)?;
        self.materialize(None)?;
        self.suboperators
            .push(Suboperator::BatchUpdate { state, call });
        Ok(())
    }
    pub fn result_type(&self, id: VariableId) -> Result<&ExpressionResultType> {
        if id.scope != self.scope {
            return Err(Error::InvalidPlan(
                "variable belongs to another context".into(),
            ));
        }
        self.variables
            .get(id.index)
            .map(|v| &v.ty)
            .ok_or_else(|| Error::InvalidPlan("undefined variable".into()))
    }
    pub fn input(&mut self, column: usize, ty: ExpressionResultType) -> VariableId {
        let source = self.source;
        self.push(ty, |output| Suboperator::SourceColumn {
            source,
            column,
            output,
        })
    }
    pub fn constant(&mut self, value: ArrayRef) -> Result<VariableId> {
        if value.len() != 1 {
            return Err(Error::InvalidPlan("constant requires one value".into()));
        }
        let ty = ExpressionResultType::new(
            value.data_type().clone(),
            value.logical_nulls().is_some_and(|n| n.null_count() != 0),
        );
        let literal = value.clone();
        let state = self.state(StateBuilder::new(move || Ok(literal.clone())));
        Ok(self.push(ty, |output| Suboperator::Constant {
            value,
            state,
            output,
        }))
    }
    pub fn unary(&mut self, function: FunctionKind, input: VariableId) -> Result<VariableId> {
        let ty = self.result_type(input)?.clone();
        native::bind_unary(function, ty.data_type())?;
        let ty = if matches!(function, FunctionKind::IsNull | FunctionKind::IsNotNull) {
            ExpressionResultType::new(DataType::Boolean, false)
        } else {
            ty
        };
        Ok(self.push(ty, |output| Suboperator::Call {
            function,
            inputs: vec![input],
            output,
        }))
    }
    pub fn binary(
        &mut self,
        function: FunctionKind,
        left: VariableId,
        right: VariableId,
    ) -> Result<VariableId> {
        let l = self.result_type(left)?;
        let r = self.result_type(right)?;
        if l.data_type() != r.data_type() {
            return Err(Error::InvalidPlan(
                "operand types differ; insert an explicit cast".into(),
            ));
        }
        native::bind_binary(function, l.data_type())?;
        use FunctionKind::*;
        let t = if matches!(function, Add | Subtract | Multiply | Divide | Remainder) {
            l.data_type().clone()
        } else {
            DataType::Boolean
        };
        let nullable = !matches!(function, IsDistinctFrom | IsNotDistinctFrom)
            && (l.is_nullable() || r.is_nullable());
        Ok(self.push(ExpressionResultType::new(t, nullable), |output| {
            Suboperator::Call {
                function,
                inputs: vec![left, right],
                output,
            }
        }))
    }
    pub fn cast(
        &mut self,
        input: VariableId,
        target: DataType,
        mode: CastMode,
    ) -> Result<VariableId> {
        let source = self.result_type(input)?;
        if !arrow::compute::can_cast_types(source.data_type(), &target) {
            return Err(Error::InvalidPlan(format!("unsupported cast to {target}")));
        }
        let nullable = source.is_nullable() || matches!(mode, CastMode::Try);
        Ok(
            self.push(ExpressionResultType::new(target, nullable), |output| {
                Suboperator::Cast {
                    input,
                    mode,
                    output,
                }
            }),
        )
    }
    fn require_bool(&self, input: VariableId) -> Result<()> {
        if self.result_type(input)?.data_type() != &DataType::Boolean {
            return Err(Error::InvalidPlan("expected Boolean variable".into()));
        }
        Ok(())
    }
    pub fn not(&mut self, input: VariableId) -> Result<VariableId> {
        self.require_bool(input)?;
        let ty = self.result_type(input)?.clone();
        Ok(self.push(ty, |output| Suboperator::Not { input, output }))
    }
    pub fn boolean(
        &mut self,
        conjunction: Conjunction,
        inputs: Vec<VariableId>,
    ) -> Result<VariableId> {
        for &i in &inputs {
            self.require_bool(i)?;
        }
        let nullable = inputs
            .iter()
            .any(|i| self.variables[i.index].ty.is_nullable());
        Ok(self.push(
            ExpressionResultType::new(DataType::Boolean, nullable),
            |output| Suboperator::Boolean {
                and: conjunction == Conjunction::And,
                inputs,
                output,
            },
        ))
    }
    pub fn case(
        &mut self,
        branches: Vec<(VariableId, VariableId)>,
        otherwise: VariableId,
    ) -> Result<VariableId> {
        let mut ty = self.result_type(otherwise)?.clone();
        let mut inputs = vec![];
        for &(condition, value) in &branches {
            self.require_bool(condition)?;
            let v = self.result_type(value)?;
            if v.data_type() != ty.data_type() {
                return Err(Error::InvalidPlan("CASE branch types differ".into()));
            }
            ty = ExpressionResultType::new(
                ty.data_type().clone(),
                ty.is_nullable() || v.is_nullable(),
            );
            inputs.extend([condition, value]);
        }
        inputs.push(otherwise);
        Ok(self.push(ty, |output| Suboperator::Case {
            branches,
            otherwise,
            inputs,
            output,
        }))
    }
    pub fn coalesce(&mut self, inputs: Vec<VariableId>) -> Result<VariableId> {
        let first = *inputs
            .first()
            .ok_or_else(|| Error::InvalidPlan("coalesce requires an input".into()))?;
        let mut ty = self.result_type(first)?.clone();
        for &i in &inputs {
            let t = self.result_type(i)?;
            if ty.data_type() != t.data_type() {
                return Err(Error::InvalidPlan("COALESCE types differ".into()));
            }
            ty = ExpressionResultType::new(
                ty.data_type().clone(),
                ty.is_nullable() && t.is_nullable(),
            );
        }
        Ok(self.push(ty, |output| Suboperator::Coalesce { inputs, output }))
    }
    pub fn finish(mut self, outputs: Vec<VariableId>) -> Result<SuboperatorContext> {
        for &id in &outputs {
            self.result_type(id)?;
        }
        self.materialize(None)?;
        Ok(SuboperatorContext {
            scope: self.scope,
            variables: self.variables,
            suboperators: self.suboperators,
            state_builders: self.states,
            outputs,
            roots: self.roots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn ordinary_state_factory_is_lazy_retryable_and_local_to_each_instance() {
        let calls = Arc::new(AtomicUsize::new(0));
        let literal: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let mut builder = SuboperatorBuilder::new();
        let counter = calls.clone();
        let value = literal.clone();
        let state = builder.state(StateBuilder::new(move || {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Execution("factory failed".into()));
            }
            Ok(value.clone())
        }));
        let output = builder.push(
            ExpressionResultType::new(DataType::Int64, false),
            |output| Suboperator::Constant {
                value: literal,
                state,
                output,
            },
        );
        let program = builder
            .finish(vec![output])
            .unwrap()
            .compile(super::super::ExecutionMode::Native)
            .unwrap();
        let mut first = program.new_execution_context();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(program.execute(&[], 2, &mut first).is_err());
        assert_eq!(program.execute(&[], 2, &mut first).unwrap()[0].len(), 2);
        program.execute(&[], 3, &mut first).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let mut second = program.new_execution_context();
        program.execute(&[], 1, &mut second).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let mut other = SuboperatorBuilder::new();
        let foreign = other.state(StateBuilder::new(|| Ok(0usize)));
        assert_eq!(foreign.index(), state.index());
        assert!(first.take_state(foreign).is_none());
        let owned = first
            .take_state(state)
            .unwrap()
            .into_inner::<ArrayRef>()
            .unwrap();
        assert_eq!(owned.len(), 1);
        assert!(first.take_state(state).is_none());
        // Moving out one instance's state cannot consume another instance's slot.
        assert!(second.take_state(state).is_some());
        program.execute(&[], 2, &mut first).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }
}
