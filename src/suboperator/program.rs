//! Compiled query entry points and exclusively owned execution state.
use super::{context::Producer, jit::CompiledProgram, runtime::Runtime, scalar, *};
use crate::{
    arrow::{array::ArrayRef, datatypes::*, record_batch::*},
    error::{Error, Result},
    expr::scalar::ScalarExprRef,
};
use std::sync::Arc;
/// Compile-time schedule metadata, never an executable instruction/dispatcher.
#[derive(Debug)]
pub struct Program {
    kind: &'static str,
    inputs: Vec<VariableId>,
    outputs: Vec<VariableId>,
    states: Vec<StateId>,
}
impl Program {
    pub(super) fn new(op: &Suboperator) -> Self {
        let kind = match op {
            Suboperator::SourceColumn { .. } => "source_column",
            Suboperator::Constant { .. } => "constant",
            Suboperator::Call { .. } => "scalar",
            Suboperator::Cast { .. } => "cast",
            Suboperator::Not { .. } => "not",
            Suboperator::Boolean { .. } => "boolean",
            Suboperator::Case { .. } => "case",
            Suboperator::Coalesce { .. } => "coalesce",
            Suboperator::GroupLookup { .. } => "group_lookup",
            Suboperator::AggregateUpdate { .. } => "aggregate_update",
            Suboperator::Materialize { .. } => "projection",
            Suboperator::BatchUpdate { .. } => "output_batch",
        };
        Self {
            kind,
            inputs: op.inputs().to_vec(),
            outputs: op.outputs().to_vec(),
            states: op.states().to_vec(),
        }
    }
    pub fn kind(&self) -> &'static str {
        self.kind
    }
    pub fn inputs(&self) -> &[VariableId] {
        &self.inputs
    }
    pub fn outputs(&self) -> &[VariableId] {
        &self.outputs
    }
    pub fn states(&self) -> &[StateId] {
        &self.states
    }
}
pub struct ProgramBuilder {
    context: Arc<SuboperatorScope>,
    producers: Vec<Option<Producer>>,
}
impl ProgramBuilder {
    pub fn new(context: Arc<SuboperatorScope>, _mode: ExecutionMode) -> Result<Self> {
        if let Some(sink) = context
            .suboperators
            .iter()
            .position(|op| matches!(op, Suboperator::BatchUpdate { .. }))
            && (context.suboperators[sink + 1..]
                .iter()
                .any(|op| !matches!(op, Suboperator::BatchUpdate { .. }))
                || context.boundaries.iter().any(|b| b.end > sink))
        {
            return Err(Error::InvalidPlan(
                "batch sinks terminate row computation; continue in another pipeline".into(),
            ));
        }
        let mut end = 0;
        for boundary in &context.boundaries {
            if boundary.end < end || boundary.end > context.suboperators.len() {
                return Err(Error::InvalidPlan(
                    "scope boundaries must be ordered".into(),
                ));
            }
            end = boundary.end;
        }
        let mut producers = vec![None; context.variables.len()];
        let definitions = context
            .suboperators
            .iter()
            .enumerate()
            .map(|(i, op)| (Producer::Node(i), op.outputs()))
            .chain(
                context
                    .boundaries
                    .iter()
                    .enumerate()
                    .map(|(i, b)| (Producer::Scope(i), b.operator.outputs())),
            );
        for (producer, outputs) in definitions {
            for output in outputs {
                if output.scope != context.scope
                    || output.index >= producers.len()
                    || producers[output.index].replace(producer).is_some()
                {
                    return Err(Error::InvalidPlan(
                        "variable must have exactly one producer in its scope".into(),
                    ));
                }
            }
        }
        let check_input = |input: VariableId, consumer: Producer| -> Result<()> {
            if input.scope != context.scope || input.index >= producers.len() {
                return Err(Error::InvalidPlan("foreign variable".into()));
            }
            let producer = producers[input.index]
                .ok_or_else(|| Error::InvalidPlan("missing variable producer".into()))?;
            if context.rank(producer) >= context.rank(consumer) {
                return Err(Error::InvalidPlan(
                    "input must be visible before its consumer".into(),
                ));
            }
            Ok(())
        };
        for (i, boundary) in context.boundaries.iter().enumerate() {
            for &input in boundary.operator.inputs() {
                check_input(input, Producer::Scope(i))?;
            }
            match &boundary.operator {
                ScopeSuboperator::Filter {
                    predicate,
                    body_inputs,
                    body_outputs,
                } => {
                    if context.variables[predicate.index].ty.data_type() != &DataType::Boolean
                        || body_inputs.len() != body_outputs.len()
                        || body_inputs.iter().zip(body_outputs).any(|(input, output)| {
                            context.variables[input.index].ty != context.variables[output.index].ty
                        })
                    {
                        return Err(Error::InvalidPlan(
                            "invalid filter predicate or body output type".into(),
                        ));
                    }
                }
            }
        }
        for (i, op) in context.suboperators.iter().enumerate() {
            for &input in op.inputs() {
                check_input(input, Producer::Node(i))?;
            }
        }
        for (&consumer, dependencies) in &context.dependencies {
            let rank = context
                .rank(consumer)
                .ok_or_else(|| Error::InvalidPlan("undefined dependency consumer".into()))?;
            for dependency in dependencies {
                if dependency.scope != context.scope
                    || context
                        .rank(dependency.producer)
                        .is_none_or(|before| before >= rank)
                {
                    return Err(Error::InvalidPlan(
                        "dependency token must precede its consumer in a visible scope".into(),
                    ));
                }
            }
        }
        for id in context.outputs.iter().chain(&context.roots) {
            if id.scope != context.scope
                || id.index >= producers.len()
                || producers[id.index].is_none()
            {
                return Err(Error::InvalidPlan("undefined output or root".into()));
            }
        }
        Ok(Self { context, producers })
    }

    pub fn build(self) -> Result<ProgramContext> {
        self.build_projection(false)
    }
    fn build_projection(self, empty: bool) -> Result<ProgramContext> {
        let code = CompiledProgram::compile(&self.context, self.producers, empty)?;
        Ok(ProgramContext {
            state_builders: self.context.state_builders.clone(),
            scope_id: self.context.scope,
            filter_scopes: self.context.boundaries.len(),
            state_bindings: self
                .context
                .suboperators
                .iter()
                .filter_map(|op| match op {
                    Suboperator::GroupLookup { state, .. } => Some(StateBinding {
                        id: *state,
                        bind: bind_typed::<aggregation::GroupState>,
                    }),
                    Suboperator::AggregateUpdate { states, .. } => Some(StateBinding {
                        id: states[1],
                        bind: bind_typed::<aggregation::AccumulatorState>,
                    }),
                    _ => None,
                })
                .collect(),
            code,
        })
    }
}
#[derive(Debug)]
pub struct ProgramContext {
    scope_id: u64,
    filter_scopes: usize,
    state_bindings: Vec<StateBinding>,
    state_builders: Vec<StateBuilder>,
    code: CompiledProgram,
}
#[derive(Debug, Default)]
pub struct ProgramExecutionContext {
    scope: Option<u64>,
    states: Vec<Option<State>>,
    pub(crate) stopped: bool,
    result_schema: Option<SchemaRef>,
    result_rows: usize,
}
impl ProgramContext {
    pub fn from_expressions(expressions: &[ScalarExprRef], mode: ExecutionMode) -> Result<Self> {
        let mut b = SuboperatorScopeBuilder::new();
        let out = expressions
            .iter()
            .map(|e| e.build_suboperator(&mut b))
            .collect::<Result<Vec<_>>>()?;
        ProgramBuilder::new(Arc::new(b.finish(out)?), mode)?
            .build_projection(expressions.is_empty())
    }
    pub fn programs(&self) -> &[Program] {
        &self.code.schedule
    }
    pub fn new_execution_context(&self) -> ProgramExecutionContext {
        ProgramExecutionContext::default()
    }
    pub fn state_builders(&self) -> &[StateBuilder] {
        &self.state_builders
    }
    fn scope(&self) -> Option<u64> {
        Some(self.scope_id)
    }
    pub fn is_compiled(&self) -> bool {
        true
    }
    pub fn compiled_program_count(&self) -> usize {
        1
    }
    pub fn generated_ir(&self) -> &str {
        &self.code.ir
    }
    pub fn explain(&self) -> String {
        use std::fmt::Write;
        let mut out = format!("cranelift_loop\n  filter scopes: {}\n", self.filter_scopes);
        for (i, p) in self.programs().iter().enumerate() {
            writeln!(
                out,
                "  node {i}: {} inputs={:?} outputs={:?}",
                p.kind,
                p.inputs.iter().map(|v| v.index).collect::<Vec<_>>(),
                p.outputs.iter().map(|v| v.index).collect::<Vec<_>>()
            )
            .unwrap();
        }
        out
    }
    pub fn initialize(&self, local: &mut ProgramExecutionContext) -> Result<()> {
        if local.scope != self.scope() {
            local.scope = self.scope();
            local.states.clear();
            local.stopped = false;
        }
        local.states.resize_with(self.state_builders.len(), || None);
        // Only states referenced by emitted code are required. Dead expressions
        // must not instantiate external states or run user factories.
        for p in self.programs() {
            for id in &p.states {
                if local.states[id.index].is_none() {
                    local.states[id.index] = Some(self.state_builders[id.index].build()?);
                }
            }
        }
        Ok(())
    }
    pub fn execute_batch(
        &self,
        batch: &RecordBatch,
        local: &mut ProgramExecutionContext,
    ) -> Result<Vec<ArrayRef>> {
        self.execute_frame(
            batch.columns(),
            batch.num_rows(),
            Some(batch.schema()),
            local,
            None,
            true,
        )
    }
    pub fn execute(
        &self,
        columns: &[ArrayRef],
        rows: usize,
        local: &mut ProgramExecutionContext,
    ) -> Result<Vec<ArrayRef>> {
        self.execute_frame(columns, rows, None, local, None, true)
    }
    pub fn execute_record_batch(
        &self,
        batch: &RecordBatch,
        local: &mut ProgramExecutionContext,
    ) -> Result<RecordBatch> {
        let columns = self.execute_batch(batch, local)?;
        Ok(RecordBatch::try_new_with_options(
            local
                .result_schema
                .clone()
                .unwrap_or_else(|| batch.schema()),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(local.result_rows)),
        )?)
    }
    pub(crate) fn run(
        &self,
        batch: &RecordBatch,
        local: &mut ProgramExecutionContext,
        guard: &asyncband::shutdown::ShutdownGuard,
    ) -> Result<()> {
        if let Some(entry) = self.code.reduction {
            // A reduction has no row output or batch callbacks. Bind its small
            // Arrow/state ABI on the stack rather than building output frames.
            if self.code.inputs.len() <= 16 && self.state_builders.len() <= 64 {
                if guard.is_shutdown_requested() {
                    return Err(Error::Cancelled);
                }
                self.initialize(local)?;
                let mut inputs = [super::column::Column::default(); 16];
                for (slot, input) in self.code.inputs.iter().enumerate() {
                    let column = batch
                        .columns()
                        .get(input.column.unwrap())
                        .ok_or_else(|| Error::Execution("source column out of bounds".into()))?;
                    if column.data_type() != &input.ty {
                        return Err(Error::Execution(
                            "source column type differs from variable type".into(),
                        ));
                    }
                    if !input.nullable && column.null_count() != 0 {
                        return Err(Error::Execution(
                            "NULL in a non-nullable source column".into(),
                        ));
                    }
                    inputs[slot] = super::column::input(column, &mut None)?;
                }
                let mut states = [std::ptr::null_mut(); 64];
                for binding in &self.state_bindings {
                    states[binding.id.index] =
                        (binding.bind)(local.states[binding.id.index].as_mut().unwrap())?;
                }
                let mut runtime = Runtime {
                    guard,
                    ..Runtime::default()
                };
                // The reduction only touches inputs and states, never ports/counts.
                let status = unsafe {
                    entry(
                        &mut runtime,
                        inputs.as_ptr(),
                        std::ptr::null(),
                        std::ptr::null_mut(),
                        states.as_ptr(),
                        batch.num_rows(),
                        0,
                    )
                };
                return if status == 0 {
                    Ok(())
                } else {
                    Err(runtime.error.unwrap_or_else(|| {
                        crate::arrow::error::ArrowError::ArithmeticOverflow(
                            "generated aggregate arithmetic".into(),
                        )
                        .into()
                    }))
                };
            }
        }
        if self.code.passthrough {
            if guard.is_shutdown_requested() {
                return Err(Error::Cancelled);
            }
            for port in &self.code.ports {
                if let Some((state, call)) = port.sink
                    && batch.num_rows() != 0
                {
                    local.stopped = call(local.state_mut(state)?, batch)? == ProgramControl::Stop;
                    if local.stopped {
                        break;
                    }
                }
            }
        } else {
            self.execute_frame(
                batch.columns(),
                batch.num_rows(),
                Some(batch.schema()),
                local,
                Some(guard),
                false,
            )?;
        }
        Ok(())
    }
    fn execute_frame(
        &self,
        columns: &[ArrayRef],
        rows: usize,
        schema: Option<SchemaRef>,
        local: &mut ProgramExecutionContext,
        guard: Option<&asyncband::shutdown::ShutdownGuard>,
        collect: bool,
    ) -> Result<Vec<ArrayRef>> {
        if guard.is_some_and(|g| g.is_shutdown_requested()) {
            return Err(Error::Cancelled);
        }
        if columns.iter().any(|a| a.len() != rows) {
            return Err(Error::Execution(
                "input column lengths differ from row count".into(),
            ));
        }
        self.initialize(local)?;
        let mut runtime = Runtime {
            guard: guard.map_or(std::ptr::null(), |g| g as *const _),
            ..Runtime::default()
        };
        let mut inputs = vec![];
        let mut input_ptrs = vec![];
        for input in &self.code.inputs {
            let decoded = (|| {
                let a = if let Some(a) = &input.literal {
                    a
                } else {
                    let i = input.column.unwrap();
                    columns.get(i).ok_or_else(|| {
                        Error::Execution(format!("column index {i} out of bounds"))
                    })?
                };
                if a.data_type() != &input.ty {
                    return Err(Error::Execution(
                        "source column type differs from variable type".into(),
                    ));
                }
                if !input.nullable && super::column::direct(&input.ty) && a.null_count() != 0 {
                    return Err(Error::Execution(
                        "NULL in a non-nullable source column".into(),
                    ));
                }
                let mut cells = None;
                let descriptor = super::column::input(a, &mut cells)?;
                Ok((cells, descriptor))
            })();
            match decoded {
                Ok((cells, descriptor)) => {
                    inputs.push(cells);
                    input_ptrs.push(descriptor);
                    runtime.input_errors.push(None);
                }
                Err(e) if input.lazy => {
                    inputs.push(None);
                    input_ptrs.push(super::column::Column::default());
                    runtime.input_errors.push(Some(e));
                }
                Err(e) => return Err(e),
            }
        }
        let mut output = self
            .code
            .ports
            .iter()
            .flat_map(|p| {
                p.types.iter().map(|t| {
                    super::column::Output::new(
                        t,
                        if p.sink.is_some() || collect { rows } else { 0 },
                    )
                })
            })
            .collect::<Vec<_>>();
        let output_ptrs = output
            .iter_mut()
            .zip(self.code.ports.iter().flat_map(|p| &p.types))
            .map(|(c, t)| c.descriptor(t))
            .collect::<Vec<_>>();
        let mut counts = vec![0usize; self.code.ports.len()];
        let mut states = vec![std::ptr::null_mut(); local.states.len()];
        for binding in &self.state_bindings {
            if let Some(state) = local.states[binding.id.index].as_mut() {
                states[binding.id.index] = (binding.bind)(state)?;
            }
        }
        // SAFETY: compiled signatures and buffer contracts are private. Inputs,
        // states and output capacity remain live/exclusive for this invocation;
        // one input row writes at most one row to each output port.
        let status = unsafe {
            (if collect {
                self.code.entry
            } else {
                self.code.reduction.unwrap_or(self.code.entry)
            })(
                &mut runtime,
                input_ptrs.as_ptr(),
                output_ptrs.as_ptr(),
                counts.as_mut_ptr(),
                states.as_ptr(),
                rows,
                usize::from(collect),
            )
        };
        if status != 0 {
            return Err(runtime.error.unwrap_or_else(|| match status {
                2 => crate::arrow::error::ArrowError::ArithmeticOverflow(
                    "generated integer arithmetic".into(),
                )
                .into(),
                3 => crate::arrow::error::ArrowError::DivideByZero.into(),
                4 => Error::Execution("invalid scalar cast".into()),
                _ => Error::Execution("generated runtime call failed".into()),
            }));
        }
        let mut output = output.into_iter();
        let mut result = vec![];
        for (port, &count) in self.code.ports.iter().zip(&counts) {
            if guard.is_some_and(|g| g.is_shutdown_requested()) {
                return Err(Error::Cancelled);
            }
            let cells = output.by_ref().take(port.types.len()).collect::<Vec<_>>();
            let wanted = port.sink.is_some() || collect;
            let arrays = if !wanted {
                vec![]
            } else if port.passthrough {
                columns
                    .iter()
                    .map(|a| {
                        if count == rows {
                            return Ok(a.clone());
                        }
                        let values = scalar::decode(a)?;
                        let selected = (0..count)
                            .map(|row| values[cells[0].row_id(row)])
                            .collect::<Vec<_>>();
                        scalar::encode(a.data_type(), &selected)
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                port.types
                    .iter()
                    .zip(cells)
                    .zip(&port.aliases)
                    .map(|((t, c), alias)| {
                        if count == rows && alias.is_some() {
                            Ok(columns[alias.unwrap()].clone())
                        } else {
                            c.finish(t, count)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            let schema = port.schema.clone().or_else(|| schema.clone());
            if let Some((state, call)) = port.sink {
                if count != 0 {
                    let batch = RecordBatch::try_new_with_options(
                        schema.ok_or_else(|| {
                            Error::Execution("sink requires a batch schema".into())
                        })?,
                        arrays,
                        &RecordBatchOptions::new().with_row_count(Some(count)),
                    )?;
                    local.stopped = call(local.state_mut(state)?, &batch)? == ProgramControl::Stop;
                    if local.stopped {
                        break;
                    }
                }
            } else {
                local.result_rows = count;
                local.result_schema = schema;
                result = arrays;
            }
        }
        Ok(result)
    }
}
impl ProgramExecutionContext {
    pub fn take_state(&mut self, id: StateId) -> Option<State> {
        if self.scope != Some(id.scope) {
            return None;
        }
        self.states.get_mut(id.index()).and_then(Option::take)
    }
}

impl ProgramExecutionContext {
    pub fn bind_state(
        &mut self,
        context: &ProgramContext,
        id: StateId,
        state: State,
    ) -> Result<()> {
        if Some(id.scope) != context.scope() || id.index >= context.state_builders.len() {
            return Err(Error::InvalidPlan("foreign state binding".into()));
        }
        if !context.state_builders[id.index].accepts(&state) {
            return Err(Error::Execution("state binding type mismatch".into()));
        }
        if self.scope != context.scope() {
            self.scope = context.scope();
            self.states.clear();
        }
        self.states
            .resize_with(context.state_builders.len().max(self.states.len()), || None);
        self.states[id.index] = Some(state);
        Ok(())
    }
    pub fn state_mut(&mut self, id: StateId) -> Result<&mut State> {
        if self.scope != Some(id.scope) {
            return Err(Error::Execution("foreign local state".into()));
        }
        self.states
            .get_mut(id.index)
            .and_then(Option::as_mut)
            .ok_or_else(|| Error::Execution("state is not initialized".into()))
    }
}

#[cfg(test)]
#[path = "program_tests.rs"]
mod tests;

#[derive(Debug)]
struct StateBinding {
    id: StateId,
    bind: fn(&mut State) -> Result<*mut ()>,
}
fn bind_typed<T: 'static>(state: &mut State) -> Result<*mut ()> {
    Ok(state.get_mut::<T>()? as *mut T as *mut ())
}
