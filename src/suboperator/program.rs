//! Prepared vector programs and their exclusive execution instances.
use super::{
    ColumnBuffer, ExecutionMode, State, StateBuilder, StateId, Suboperator, SuboperatorBuilder,
    SuboperatorContext, VariableId, native,
};
use crate::{
    error::{Error, Result},
    expr::scalar::{CastMode, ScalarExprRef},
};
use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, UInt64Array, new_empty_array},
    compute::{
        CastOptions, and_kleene, cast_with_options, kernels::interleave::interleave, not,
        or_kleene, take,
    },
    datatypes::DataType,
};
use std::sync::Arc;

/// A prebound batch entry point. The frame is built once per call, never per row.
type StatefulCall =
    fn(&mut [Option<State>], &[StateId], &[ArrayRef], usize) -> Result<Option<ArrayRef>>;
enum CallOutput {
    None,
    Domain {
        columns: Vec<ArrayRef>,
        rows: usize,
        schema: Option<arrow::datatypes::SchemaRef>,
    },
    One(ArrayRef),
    #[cfg(feature = "jit")]
    Many(Vec<ArrayRef>),
}
type NativeCall = fn(&Program, &mut CallFrame<'_>) -> Result<CallOutput>;

#[derive(Debug)]
pub struct Program {
    inputs: Vec<VariableId>,
    outputs: Vec<VariableId>,
    states: Vec<StateId>,
    call: NativeCall,
    operation: BoundOperation,
    output_type: Option<DataType>,
    releases: Vec<VariableId>,
}
impl Program {
    pub fn kind(&self) -> &'static str {
        match &self.operation {
            #[cfg(feature = "jit")]
            BoundOperation::Fused(_) => "cranelift_loop",
            BoundOperation::Source(_) => "source_column",
            BoundOperation::Materialize { .. } => "materialize",
            BoundOperation::Return => "return",
            BoundOperation::BatchUpdate(_) => "batch_update",
            BoundOperation::Constant => "constant",
            BoundOperation::Stateful { name, .. } => name,
            BoundOperation::Unary(_) => "native_unary",
            BoundOperation::Binary { .. } => "native_binary",
            BoundOperation::Cast(_) => "native_cast",
            BoundOperation::Not => "not",
            BoundOperation::Boolean(_) => "boolean",
            BoundOperation::Case { .. } => "case_selection",
            BoundOperation::Coalesce(_) => "coalesce_selection",
        }
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
#[derive(Clone, Copy, Debug)]
enum Operand {
    Input(usize),
    State(StateId),
}
#[derive(Debug)]
enum BoundOperation {
    Materialize {
        schema: Option<arrow::datatypes::SchemaRef>,
        predicate: bool,
    },
    Return,
    BatchUpdate(super::BatchUpdate),
    #[cfg(feature = "jit")]
    Fused(Box<super::jit::CompiledProgram>),
    Source(usize),
    Constant,
    Stateful {
        name: &'static str,
        call: StatefulCall,
    },
    Unary(native::UnaryEvalFn),
    Binary {
        call: native::BinaryEvalFn,
        operands: [Operand; 2],
        scalar_output: bool,
    },
    Cast(CastMode),
    Not,
    Boolean(bool),
    Case {
        branches: Vec<(ProgramContext, ProgramContext)>,
        otherwise: Box<ProgramContext>,
    },
    Coalesce(Vec<ProgramContext>),
}

/// Programs are immutable and shareable. Variables/states live only in the
/// execution context. Conditional regions have independent execution instances.
#[derive(Debug)]
pub struct ProgramContext {
    programs: Vec<Program>,
    state_builders: Vec<StateBuilder>,
}

/// Construction-only dependency lookup. Variable itself has no ProducedBy field.
pub struct ProgramBuilder {
    context: Arc<SuboperatorContext>,
    producers: Vec<usize>,
    mode: ExecutionMode,
}
impl ProgramBuilder {
    pub fn new(context: Arc<SuboperatorContext>, mode: ExecutionMode) -> Result<Self> {
        #[cfg(not(feature = "jit"))]
        if mode != ExecutionMode::Native {
            return Err(Error::InvalidPlan(
                "compiled execution requires the jit feature".into(),
            ));
        }
        let mut producers = vec![usize::MAX; context.variables.len()];
        for (index, op) in context.suboperators.iter().enumerate() {
            for output in op.outputs() {
                if output.scope != context.scope
                    || output.index >= producers.len()
                    || producers[output.index] != usize::MAX
                {
                    return Err(Error::InvalidPlan(
                        "variable must have exactly one producer in its context".into(),
                    ));
                }
                producers[output.index] = index;
            }
        }
        Ok(Self {
            context,
            producers,
            mode,
        })
    }
    /// Consume the build IR. Selection boundaries produce ordinary calls in one
    /// flat schedule; neither IR nor execution-mode switches survive compilation.
    pub fn build(self) -> Result<ProgramContext> {
        let mut programs = vec![];
        let mut start = 0;
        for (index, op) in self.context.suboperators.iter().enumerate() {
            let operation = match op {
                Suboperator::Materialize {
                    schema, predicate, ..
                } => BoundOperation::Materialize {
                    schema: schema.clone(),
                    predicate: *predicate,
                },
                Suboperator::BatchUpdate { call, .. } => BoundOperation::BatchUpdate(*call),
                _ => continue,
            };
            let part = self.segment(start, index, op.inputs().to_vec())?;
            programs.extend(part.programs);
            programs.push(Program {
                inputs: op.inputs().to_vec(),
                outputs: vec![],
                states: op.states().to_vec(),
                call: call_native,
                operation,
                output_type: None,
                releases: vec![],
            });
            start = index + 1;
        }
        programs.extend(
            self.segment(
                start,
                self.context.suboperators.len(),
                self.context.outputs.clone(),
            )?
            .programs,
        );
        if !self.context.outputs.is_empty() {
            programs.push(Program {
                inputs: self.context.outputs.clone(),
                outputs: vec![],
                states: vec![],
                call: call_native,
                operation: BoundOperation::Return,
                output_type: None,
                releases: vec![],
            });
        }
        assign_releases(&mut programs, &[], self.context.variables.len());
        Ok(ProgramContext {
            programs,
            state_builders: self.context.state_builders.clone(),
        })
    }
    fn segment(
        &self,
        start: usize,
        end: usize,
        outputs: Vec<VariableId>,
    ) -> Result<ProgramContext> {
        let ops = self.context.suboperators[start..end].to_vec();
        let roots = self
            .context
            .roots
            .iter()
            .filter(|id| ops.iter().any(|op| op.outputs().contains(id)))
            .copied()
            .collect();
        let context = Arc::new(SuboperatorContext {
            scope: self.context.scope,
            variables: self.context.variables.clone(),
            state_builders: self.context.state_builders.clone(),
            suboperators: ops,
            outputs: outputs.clone(),
            roots,
        });
        Self::new(context, self.mode)?.build_region(outputs, true, false)
    }
    fn build_region(
        &self,
        outputs: Vec<VariableId>,
        include_effects: bool,
        capture: bool,
    ) -> Result<ProgramContext> {
        let mut programs = vec![];
        let mut visited = vec![false; self.producers.len()];
        if include_effects {
            for &root in &self.context.roots {
                self.append(root, &mut visited, &mut programs)?;
            }
        }
        for &output in &outputs {
            self.append(output, &mut visited, &mut programs)?;
        }
        for op in self.context.suboperators.iter().filter(|_| include_effects) {
            if let Suboperator::AggregateUpdate { states, inputs } = op {
                for &input in inputs {
                    self.append(input, &mut visited, &mut programs)?;
                }
                programs.push(Program {
                    inputs: inputs.clone(),
                    outputs: vec![],
                    states: states.to_vec(),
                    call: call_native,
                    operation: BoundOperation::Stateful {
                        name: "aggregate_update",
                        call: crate::suboperator::aggregation::update,
                    },
                    output_type: None,
                    releases: vec![],
                });
            }
        }
        // Column imports only bind owners. Hoist them within this row domain so
        // reading another source column does not split an otherwise fused loop.
        // Conditional branches have their own schedule, preserving lazy reads.
        let (mut sources, body): (Vec<_>, Vec<_>) = programs
            .into_iter()
            .partition(|p| matches!(p.operation, BoundOperation::Source(_)));
        sources.extend(body);
        let mut programs = sources;
        #[cfg(feature = "jit")]
        if self.mode != ExecutionMode::Native {
            fuse(&self.context, &mut programs, &outputs, self.mode)?;
        }
        if capture {
            programs.push(Program {
                inputs: outputs.clone(),
                outputs: vec![],
                states: vec![],
                call: call_native,
                operation: BoundOperation::Return,
                output_type: None,
                releases: vec![],
            });
        }
        assign_releases(
            &mut programs,
            if capture { &[] } else { &outputs },
            self.context.variables.len(),
        );
        Ok(ProgramContext {
            programs,
            state_builders: self.context.state_builders.clone(),
        })
    }

    fn append(
        &self,
        output: VariableId,
        visited: &mut [bool],
        programs: &mut Vec<Program>,
    ) -> Result<()> {
        if output.scope != self.context.scope || output.index >= visited.len() {
            return Err(Error::InvalidPlan("foreign variable".into()));
        }
        if visited[output.index] {
            return Ok(());
        }
        let op = self
            .context
            .suboperators
            .get(self.producers[output.index])
            .ok_or_else(|| Error::InvalidPlan("missing variable producer".into()))?;
        // All IDs originate from a builder that only accepts preceding variables.
        let mut bound_inputs = op.eager_inputs().to_vec();
        let mut states = op.states().to_vec();
        let operation = match op {
            Suboperator::AggregateUpdate { .. }
            | Suboperator::Materialize { .. }
            | Suboperator::BatchUpdate { .. } => unreachable!(),
            Suboperator::GroupLookup { .. } => BoundOperation::Stateful {
                name: "group_lookup",
                call: crate::suboperator::aggregation::lookup,
            },
            Suboperator::SourceColumn { column, .. } => BoundOperation::Source(*column),
            Suboperator::Constant { .. } => BoundOperation::Constant,
            Suboperator::Call {
                function, inputs, ..
            } => {
                let ty = self.context.variables[inputs[0].index].ty.data_type();
                match inputs.len() {
                    1 => BoundOperation::Unary(native::bind_unary(*function, ty)?),
                    2 => {
                        bound_inputs.clear();
                        let operands = inputs
                            .iter()
                            .map(|v| {
                                if let Suboperator::Constant { state, .. } =
                                    &self.context.suboperators[self.producers[v.index]]
                                {
                                    if !states.contains(state) {
                                        states.push(*state);
                                    }
                                    Operand::State(*state)
                                } else {
                                    let i = bound_inputs.len();
                                    bound_inputs.push(*v);
                                    Operand::Input(i)
                                }
                            })
                            .collect::<Vec<_>>();
                        let left = matches!(operands[0], Operand::State(_));
                        let right = matches!(operands[1], Operand::State(_));
                        let call = if left != right {
                            native::bind_binary_scalar(*function, ty, left)?
                        } else {
                            native::bind_binary(*function, ty)?
                        };
                        BoundOperation::Binary {
                            call,
                            operands: [operands[0], operands[1]],
                            scalar_output: left && right,
                        }
                    }
                    _ => return Err(Error::InvalidPlan("invalid call arity".into())),
                }
            }
            Suboperator::Cast { mode, .. } => BoundOperation::Cast(*mode),
            Suboperator::Not { .. } => BoundOperation::Not,
            Suboperator::Boolean { and, .. } => BoundOperation::Boolean(*and),
            Suboperator::Case {
                branches,
                otherwise,
                ..
            } => BoundOperation::Case {
                branches: branches
                    .iter()
                    .map(|&(c, v)| {
                        Ok((
                            self.build_region(vec![c], false, true)?,
                            self.build_region(vec![v], false, true)?,
                        ))
                    })
                    .collect::<Result<_>>()?,
                otherwise: Box::new(self.build_region(vec![*otherwise], false, true)?),
            },
            Suboperator::Coalesce { inputs, .. } => BoundOperation::Coalesce(
                inputs
                    .iter()
                    .map(|&v| self.build_region(vec![v], false, true))
                    .collect::<Result<_>>()?,
            ),
        };
        for &input in &bound_inputs {
            self.append(input, visited, programs)?;
        }
        programs.push(Program {
            inputs: bound_inputs,
            outputs: op.outputs().to_vec(),
            states,
            call: call_native,
            operation,
            output_type: Some(self.context.variables[output.index].ty.data_type().clone()),
            releases: vec![],
        });
        visited[output.index] = true;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ProgramExecutionContext {
    scope: Option<u64>,
    states: Vec<Option<State>>,
    buffers: Vec<Option<ColumnBuffer>>,
    // Reuse descriptor storage; no HashMap lookups on the execution path.
    frame_inputs: Vec<ArrayRef>,
    // Lazily instantiated scratch for each conditional program's row regions.
    regions: Vec<Option<Box<ProgramExecutionContext>>>,
    pub(crate) stopped: bool,
    result_schema: Option<arrow::datatypes::SchemaRef>,
    result_rows: usize,
}
struct CallFrame<'a> {
    inputs: &'a [ArrayRef],
    source: &'a [ArrayRef],
    rows: usize,
    states: &'a mut [Option<State>],
    builders: &'a [StateBuilder],
    schema: Option<arrow::datatypes::SchemaRef>,
    guard: Option<&'a asyncband::shutdown::ShutdownGuard>,
    stopped: &'a mut bool,
    region: &'a mut Option<Box<ProgramExecutionContext>>,
}
impl CallFrame<'_> {
    fn state(&mut self, id: StateId) -> Result<&mut State> {
        if self.states[id.index].is_none() {
            self.states[id.index] = Some(self.builders[id.index].build()?);
        }
        Ok(self.states[id.index].as_mut().unwrap())
    }
    fn region(&mut self, program: &ProgramContext, rows: &[usize]) -> Result<ArrayRef> {
        let input = if rows.len() == self.rows {
            self.source.to_vec()
        } else {
            let indices = UInt64Array::from_iter_values(rows.iter().map(|&i| i as u64));
            self.source
                .iter()
                .map(|a| Ok(take(a.as_ref(), &indices, None)?))
                .collect::<Result<Vec<_>>>()?
        };
        let local = self.region.get_or_insert_with(Default::default);
        Ok(program
            .execute_frame(&input, rows.len(), None, local, self.guard)?
            .remove(0))
    }
}
impl ProgramContext {
    pub fn from_expressions(expressions: &[ScalarExprRef], mode: ExecutionMode) -> Result<Self> {
        let mut b = SuboperatorBuilder::new();
        let outputs = expressions
            .iter()
            .map(|e| e.build_suboperator(&mut b))
            .collect::<Result<_>>()?;
        let mut program = b.finish(outputs)?.compile(mode)?;
        // An empty expression list exports zero columns, whereas a pipeline
        // with no projection deliberately passes its input batch through.
        if expressions.is_empty() {
            program.programs.push(Program {
                inputs: vec![],
                outputs: vec![],
                states: vec![],
                call: call_native,
                operation: BoundOperation::Return,
                output_type: None,
                releases: vec![],
            });
        }
        Ok(program)
    }
    pub fn programs(&self) -> &[Program] {
        &self.programs
    }
    pub fn new_execution_context(&self) -> ProgramExecutionContext {
        ProgramExecutionContext::default()
    }
    pub fn state_builders(&self) -> &[StateBuilder] {
        &self.state_builders
    }
    fn scope(&self) -> Option<u64> {
        self.state_builders
            .first()
            .and_then(|b| b.id)
            .map(|id| id.scope)
            .or_else(|| {
                self.programs
                    .iter()
                    .flat_map(|p| p.inputs.iter().chain(&p.outputs))
                    .next()
                    .map(|v| v.scope)
            })
    }
    pub fn is_compiled(&self) -> bool {
        self.compiled_program_count() != 0
    }
    /// Includes independently scheduled CASE / COALESCE branches.
    pub fn compiled_program_count(&self) -> usize {
        self.programs
            .iter()
            .map(|p| match &p.operation {
                #[cfg(feature = "jit")]
                BoundOperation::Fused(_) => 1,
                BoundOperation::Case {
                    branches,
                    otherwise,
                } => {
                    branches
                        .iter()
                        .map(|(c, v)| c.compiled_program_count() + v.compiled_program_count())
                        .sum::<usize>()
                        + otherwise.compiled_program_count()
                }
                BoundOperation::Coalesce(args) => {
                    args.iter().map(Self::compiled_program_count).sum()
                }
                _ => 0,
            })
            .sum()
    }
    /// Stable, address-free schedule for plan inspection and benchmarks.
    pub fn explain(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        for (index, p) in self.programs.iter().enumerate() {
            let inputs = p.inputs.iter().map(|v| v.index()).collect::<Vec<_>>();
            let outputs = p.outputs.iter().map(|v| v.index()).collect::<Vec<_>>();
            let states = p.states.iter().map(|s| s.index()).collect::<Vec<_>>();
            writeln!(
                out,
                "  program {index}: {} inputs={inputs:?} outputs={outputs:?} states={states:?}",
                p.kind()
            )
            .unwrap();
            match &p.operation {
                BoundOperation::Case {
                    branches,
                    otherwise,
                } => {
                    for (i, (c, v)) in branches.iter().enumerate() {
                        writeln!(
                            out,
                            "    case {i} condition:\n{}    value:\n{}",
                            c.explain(),
                            v.explain()
                        )
                        .unwrap();
                    }
                    writeln!(out, "    otherwise:\n{}", otherwise.explain()).unwrap();
                }
                BoundOperation::Coalesce(args) => {
                    for (i, a) in args.iter().enumerate() {
                        writeln!(out, "    coalesce {i}:\n{}", a.explain()).unwrap();
                    }
                }
                _ => {}
            }
        }
        out
    }
    pub fn initialize(&self, local: &mut ProgramExecutionContext) -> Result<()> {
        if local.scope != self.scope() {
            local.scope = self.scope();
            local.states.clear();
            local.regions.clear();
        }
        local
            .states
            .resize_with(self.state_builders.len().max(local.states.len()), || None);
        for program in &self.programs {
            for &id in &program.states {
                if local.states[id.index].is_none() {
                    local.states[id.index] = Some(self.state_builders[id.index].build()?);
                }
            }
        }
        Ok(())
    }
    pub fn execute_batch(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        local: &mut ProgramExecutionContext,
    ) -> Result<Vec<ArrayRef>> {
        self.execute_frame(
            batch.columns(),
            batch.num_rows(),
            Some(batch.schema()),
            local,
            None,
        )
    }
    pub fn execute(
        &self,
        columns: &[ArrayRef],
        rows: usize,
        local: &mut ProgramExecutionContext,
    ) -> Result<Vec<ArrayRef>> {
        self.execute_frame(columns, rows, None, local, None)
    }
    pub fn execute_record_batch(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        local: &mut ProgramExecutionContext,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let columns = self.execute_batch(batch, local)?;
        Ok(arrow::record_batch::RecordBatch::try_new_with_options(
            local
                .result_schema
                .clone()
                .unwrap_or_else(|| batch.schema()),
            columns,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(local.result_rows)),
        )?)
    }
    pub(crate) fn run(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        local: &mut ProgramExecutionContext,
        guard: &asyncband::shutdown::ShutdownGuard,
    ) -> Result<()> {
        self.execute_frame(
            batch.columns(),
            batch.num_rows(),
            Some(batch.schema()),
            local,
            Some(guard),
        )?;
        Ok(())
    }
    fn execute_frame(
        &self,
        columns: &[ArrayRef],
        mut rows: usize,
        mut schema: Option<arrow::datatypes::SchemaRef>,
        local: &mut ProgramExecutionContext,
        guard: Option<&asyncband::shutdown::ShutdownGuard>,
    ) -> Result<Vec<ArrayRef>> {
        if columns.iter().any(|a| a.len() != rows) {
            return Err(Error::Execution(
                "input column lengths differ from row count".into(),
            ));
        }
        self.initialize(local)?;
        let mut columns = columns.to_vec();
        let slots = self
            .programs
            .iter()
            .flat_map(|p| p.inputs.iter().chain(&p.outputs))
            .map(|v| v.index + 1)
            .max()
            .unwrap_or(0);
        local.buffers.clear();
        local.buffers.resize_with(slots, || None);
        local.regions.resize_with(self.programs.len(), || None);
        for (index, program) in self.programs.iter().enumerate() {
            if guard.is_some_and(|g| g.is_shutdown_requested()) {
                return Err(Error::Cancelled);
            }
            local.frame_inputs.clear();
            for input in &program.inputs {
                local.frame_inputs.push(
                    local.buffers[input.index]
                        .as_ref()
                        .ok_or_else(|| Error::Execution("unbound input variable".into()))?
                        .as_arrow()
                        .clone(),
                );
            }
            let mut frame = CallFrame {
                inputs: &local.frame_inputs,
                source: &columns,
                rows,
                schema: schema.clone(),
                states: &mut local.states,
                builders: &self.state_builders,
                region: &mut local.regions[index],
                guard,
                stopped: &mut local.stopped,
            };
            let result = (program.call)(program, &mut frame)?;
            let mut bind = |id: VariableId, output: ArrayRef| -> Result<()> {
                if output.len() != rows
                    || program
                        .output_type
                        .as_ref()
                        .is_some_and(|ty| ty != output.data_type())
                {
                    return Err(Error::Execution(
                        "program output violates its buffer contract".into(),
                    ));
                }
                local.buffers[id.index] = Some(ColumnBuffer::from_arrow(output));
                Ok(())
            };
            match result {
                CallOutput::None if program.outputs.is_empty() => {}
                CallOutput::Domain {
                    columns: next,
                    rows: count,
                    schema: next_schema,
                } => {
                    columns = next;
                    rows = count;
                    if next_schema.is_some() {
                        schema = next_schema;
                    }
                }
                CallOutput::One(output) if program.outputs.len() == 1 => {
                    bind(program.outputs[0], output)?
                }
                #[cfg(feature = "jit")]
                CallOutput::Many(outputs) if outputs.len() == program.outputs.len() => {
                    for (&id, output) in program.outputs.iter().zip(outputs) {
                        bind(id, output)?;
                    }
                }
                _ => return Err(Error::Execution("program output arity mismatch".into())),
            }
            local.frame_inputs.clear();
            for id in &program.releases {
                local.buffers[id.index] = None;
            }
            if local.stopped {
                break;
            }
        }
        local.result_schema = schema;
        local.result_rows = rows;
        Ok(columns)
    }
}

fn call_native(p: &Program, f: &mut CallFrame<'_>) -> Result<CallOutput> {
    match &p.operation {
        BoundOperation::Return => {
            return Ok(CallOutput::Domain {
                columns: f.inputs.to_vec(),
                rows: f.rows,
                schema: None,
            });
        }
        BoundOperation::Materialize { schema, predicate } => {
            let count = f.inputs.len() - usize::from(*predicate);
            let mut columns = if schema.is_some() {
                f.inputs[..count].to_vec()
            } else {
                f.source.to_vec()
            };
            let mut rows = f.rows;
            if *predicate {
                let mask = f.inputs[count].as_boolean();
                rows = (0..mask.len())
                    .filter(|&i| mask.is_valid(i) && mask.value(i))
                    .count();
                columns = columns
                    .iter()
                    .map(|a| arrow::compute::filter(a.as_ref(), mask).map_err(Into::into))
                    .collect::<Result<_>>()?;
            }
            return Ok(CallOutput::Domain {
                columns,
                rows,
                schema: schema.clone(),
            });
        }
        BoundOperation::BatchUpdate(call) => {
            if f.rows != 0 {
                let batch = arrow::record_batch::RecordBatch::try_new_with_options(
                    f.schema
                        .clone()
                        .ok_or_else(|| Error::Execution("sink requires a batch schema".into()))?,
                    f.source.to_vec(),
                    &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(f.rows)),
                )?;
                *f.stopped = call(f.state(p.states[0])?, &batch)? == super::ProgramControl::Stop;
            }
            return Ok(CallOutput::None);
        }
        _ => {}
    }

    #[cfg(feature = "jit")]
    if let BoundOperation::Fused(code) = &p.operation {
        return Ok(CallOutput::Many(code.execute(f.inputs, f.rows)?));
    }
    if let BoundOperation::Stateful { call, .. } = p.operation {
        for &id in &p.states {
            f.state(id)?;
        }
        return Ok(match call(f.states, &p.states, f.inputs, f.rows)? {
            Some(a) => CallOutput::One(a),
            None => CallOutput::None,
        });
    }
    let output_type = p.output_type.as_ref().unwrap();
    // Input references still validate bindings on empty batches. Other operations
    // create typed empty outputs without calling fallible arithmetic or branches.
    if let BoundOperation::Source(index) = p.operation {
        let a = f
            .source
            .get(index)
            .ok_or_else(|| Error::Execution(format!("column index {index} out of bounds")))?;
        if a.data_type() != output_type {
            return Err(Error::Execution(
                "source column type differs from variable type".into(),
            ));
        }
        return Ok(CallOutput::One(a.clone()));
    }
    if f.rows == 0 {
        return Ok(CallOutput::One(new_empty_array(output_type)));
    }
    Ok(CallOutput::One(match &p.operation {
        #[cfg(feature = "jit")]
        BoundOperation::Fused(_) => unreachable!(),
        BoundOperation::Source(_)
        | BoundOperation::Stateful { .. }
        | BoundOperation::Materialize { .. }
        | BoundOperation::Return
        | BoundOperation::BatchUpdate(_) => unreachable!(),
        BoundOperation::Constant => {
            let rows = f.rows;
            let a = f.state(p.states[0])?.get::<ArrayRef>()?;
            repeat(a, rows)?
        }
        BoundOperation::Unary(call) => call(&f.inputs[0])?,
        BoundOperation::Binary {
            call,
            operands,
            scalar_output,
        } => {
            let mut argument = |operand| -> Result<ArrayRef> {
                Ok(match operand {
                    Operand::Input(i) => f.inputs[i].clone(),
                    Operand::State(id) => {
                        let a = f.state(id)?.get::<ArrayRef>()?;
                        a.clone()
                    }
                })
            };
            let left = argument(operands[0])?;
            let right = argument(operands[1])?;
            let value = call(&left, &right)?;
            if *scalar_output {
                repeat(&value, f.rows)?
            } else {
                value
            }
        }
        BoundOperation::Cast(mode) => cast_with_options(
            f.inputs[0].as_ref(),
            output_type,
            &CastOptions {
                safe: matches!(mode, CastMode::Try),
                ..Default::default()
            },
        )?,
        BoundOperation::Not => Arc::new(not(f.inputs[0].as_boolean())?),
        BoundOperation::Boolean(and) => {
            let mut out = BooleanArray::from(vec![*and; f.rows]);
            for a in f.inputs {
                out = if *and {
                    and_kleene(&out, a.as_boolean())?
                } else {
                    or_kleene(&out, a.as_boolean())?
                };
            }
            Arc::new(out)
        }
        BoundOperation::Case {
            branches,
            otherwise,
        } => {
            let mut remaining = (0..f.rows).collect::<Vec<_>>();
            let mut mapping = vec![(0, 0); f.rows];
            let mut pieces = vec![];
            for (cond, value) in branches {
                if remaining.is_empty() {
                    break;
                }
                let c = f.region(cond, &remaining)?;
                let c = c.as_boolean();
                let mut selected = vec![];
                let mut next = vec![];
                for (i, row) in remaining.into_iter().enumerate() {
                    if c.is_valid(i) && c.value(i) {
                        selected.push(row);
                    } else {
                        next.push(row);
                    }
                }
                remaining = next;
                if !selected.is_empty() {
                    let a = f.region(value, &selected)?;
                    for (i, &r) in selected.iter().enumerate() {
                        mapping[r] = (pieces.len(), i);
                    }
                    pieces.push(a);
                }
            }
            if !remaining.is_empty() {
                let a = f.region(otherwise, &remaining)?;
                for (i, &r) in remaining.iter().enumerate() {
                    mapping[r] = (pieces.len(), i);
                }
                pieces.push(a);
            }
            interleave(
                &pieces.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
                &mapping,
            )?
        }
        BoundOperation::Coalesce(args) => {
            let mut remaining = (0..f.rows).collect::<Vec<_>>();
            let mut mapping = vec![(0, 0); f.rows];
            let mut pieces = vec![];
            for (index, arg) in args.iter().enumerate() {
                if remaining.is_empty() {
                    break;
                }
                let a = f.region(arg, &remaining)?;
                let nulls = a.logical_nulls();
                let mut next = vec![];
                for (i, row) in remaining.into_iter().enumerate() {
                    if index + 1 < args.len() && nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                        next.push(row);
                    } else {
                        mapping[row] = (pieces.len(), i);
                    }
                }
                pieces.push(a);
                remaining = next;
            }
            interleave(
                &pieces.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
                &mapping,
            )?
        }
    }))
}

fn repeat(value: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    Ok(if rows == 0 {
        value.slice(0, 0)
    } else if rows == 1 {
        value.clone()
    } else if value.logical_null_count() != 0 {
        arrow::array::new_null_array(value.data_type(), rows)
    } else {
        take(value.as_ref(), &UInt64Array::from(vec![0; rows]), None)?
    })
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
            self.regions.clear();
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

#[cfg(feature = "jit")]
/// Partition the scheduled native programs, not the original expression
/// trees. A native stateful/selection kernel is a boundary, never a reason
/// to abandon compilation of the rest of the pipeline.
fn fuse(
    context: &SuboperatorContext,
    programs: &mut Vec<Program>,
    final_outputs: &[VariableId],
    mode: ExecutionMode,
) -> Result<()> {
    let mut producers = vec![None; context.variables.len()];
    for (i, op) in context.suboperators.iter().enumerate() {
        for o in op.outputs() {
            producers[o.index] = Some(i);
        }
    }
    let supported = |p: &Program| -> bool {
        if matches!(
            p.operation,
            BoundOperation::Source(_) | BoundOperation::Stateful { .. }
        ) {
            return false;
        }
        let Some(output) = p.outputs.first() else {
            return false;
        };
        let Some(index) = producers[output.index] else {
            return false;
        };
        super::jit::supports(context, &context.suboperators[index])
    };
    let limit = if mode == ExecutionMode::Staged {
        4
    } else {
        usize::MAX
    };
    let mut ranges = vec![];
    let mut start = 0;
    while start < programs.len() {
        if !supported(&programs[start]) {
            start += 1;
            continue;
        }
        let mut end = start + 1;
        while end < programs.len() && end - start < limit && supported(&programs[end]) {
            end += 1;
        }
        ranges.push((start, end));
        start = end;
    }
    // Replace from the end so earlier schedule positions remain stable.
    for (start, end) in ranges.into_iter().rev() {
        let group = &programs[start..end];
        let defined = group
            .iter()
            .flat_map(|p| p.outputs.iter().copied())
            .collect::<Vec<_>>();
        let mut external = vec![];
        let mut literals = vec![];
        let mut body = vec![];
        for p in group {
            let op = &context.suboperators[producers[p.outputs[0].index].unwrap()];
            for &input in op.inputs() {
                if defined.contains(&input) {
                    continue;
                }
                let producer = &context.suboperators[producers[input.index].unwrap()];
                if matches!(producer, Suboperator::Constant { .. }) {
                    if !literals
                        .iter()
                        .any(|op: &Suboperator| op.outputs().contains(&input))
                    {
                        literals.push(producer.clone());
                    }
                } else if !external.contains(&input) {
                    external.push(input);
                }
            }
            body.push(op.clone());
        }
        let outputs = defined
            .iter()
            .filter(|id| {
                final_outputs.contains(id) || programs[end..].iter().any(|p| p.inputs.contains(id))
            })
            .copied()
            .collect::<Vec<_>>();
        let mut suboperators = external
            .iter()
            .enumerate()
            .map(|(column, &output)| Suboperator::SourceColumn {
                source: super::SourceId(0),
                column,
                output,
            })
            .collect::<Vec<_>>();
        suboperators.extend(literals);
        suboperators.extend(body);
        let context = SuboperatorContext {
            scope: context.scope,
            variables: context.variables.clone(),
            state_builders: context.state_builders.clone(),
            suboperators,
            outputs: outputs.clone(),
            roots: defined,
        };
        if let Some(code) = super::jit::CompiledProgram::compile(&context, &outputs)? {
            programs.splice(
                start..end,
                [Program {
                    inputs: external,
                    outputs,
                    states: vec![],
                    call: call_native,
                    operation: BoundOperation::Fused(Box::new(code)),
                    output_type: None,
                    releases: vec![],
                }],
            );
        }
    }
    Ok(())
}

/// Last use is planned once, not counted at runtime. Returned outputs retain
/// their owners; dead intermediate arrays can be reclaimed within the batch.
fn release_schedule(
    programs: &[Program],
    outputs: &[VariableId],
    variables: usize,
) -> Vec<Vec<VariableId>> {
    let mut last = vec![None; variables];
    for (index, p) in programs.iter().enumerate() {
        for &id in p.outputs.iter().chain(&p.inputs) {
            last[id.index] = Some((index, id));
        }
    }
    for id in outputs {
        last[id.index] = None;
    }
    let mut releases = vec![vec![]; programs.len()];
    for (index, id) in last.into_iter().flatten() {
        releases[index].push(id);
    }
    releases
}

fn assign_releases(programs: &mut [Program], outputs: &[VariableId], variables: usize) {
    let releases = release_schedule(programs, outputs, variables);
    for (program, releases) in programs.iter_mut().zip(releases) {
        program.releases = releases;
    }
}

#[cfg(test)]
#[path = "program_tests.rs"]
mod tests;
