//! Aggregation's synchronous work is emitted into the process DAG. Each update
//! owns its accumulator. Only finalization hands partial states to the sink.
use super::*;
use crate::{
    expr::agg::{AggregateFunction, accumulator::Accumulator},
    program::{ProcessProgram, Program, ProgramBuilder, ProgramContext, Value, ValueId},
};
use arrow::array::AsArray;

#[derive(Default)]
struct Completion {
    groups: Option<RecordBatch>,
    states: Vec<Option<Vec<ArrayRef>>>,
}
struct GroupProgram {
    inputs: [ValueId; 2],
    output: [ValueId; 1],
    index: GroupIndex,
    ids: Arc<Vec<usize>>,
    schema: SchemaRef,
    completion: Arc<Mutex<Completion>>,
}
impl Program for GroupProgram {
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &self.output
    }
    fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
        let rows = c.rows(self.inputs[0])?;
        let keys = c.batch(self.inputs[1])?;
        let ids = Arc::make_mut(&mut self.ids);
        self.index.intern(keys.physical().columns(), rows, ids)?;
        c.set(
            self.output[0],
            Value::Groups {
                ids: self.ids.clone(),
                count: self.index.len(),
            },
        );
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let groups = RecordBatch::try_new_with_options(
            self.schema.clone(),
            self.index.columns()?,
            &RecordBatchOptions::new().with_row_count(Some(self.index.len())),
        )?;
        self.completion.lock().unwrap().groups = Some(groups);
        Ok(())
    }
}
struct UpdateProgram {
    inputs: Vec<ValueId>,
    accumulator: Accumulator,
    index: usize,
    completion: Arc<Mutex<Completion>>,
}
impl Program for UpdateProgram {
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &[]
    }
    fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
        let (ids, count) = match c.get(self.inputs[1])? {
            Value::Groups { ids, count } => (ids, *count),
            _ => return Err(Error::Execution("expected group IDs".into())),
        };
        self.accumulator.resize(count);
        if ids.is_empty() {
            return Ok(());
        }
        let value = self
            .inputs
            .get(2)
            .map(|&v| c.column(v, self.inputs[0])?.into_array(ids.len()))
            .transpose()?;
        self.accumulator.update(value.as_ref(), ids)
    }
    fn finish(&mut self) -> Result<()> {
        self.completion.lock().unwrap().states[self.index] = Some(self.accumulator.state()?);
        Ok(())
    }
}
/// A FILTER region isolates argument evaluation and group IDs in its selected
/// row domain. Empty regions have no effects and do not stop sibling updates.
struct FilteredUpdate {
    inputs: [ValueId; 3],
    region: ProcessProgram,
    domain: ValueId,
    groups: ValueId,
}
impl Program for FilteredUpdate {
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &[]
    }
    fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
        let batch = c.batch(self.inputs[0])?;
        let v = c
            .column(self.inputs[2], self.inputs[0])?
            .into_array(batch.num_rows())?;
        let mask = v
            .as_boolean_opt()
            .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
        let selected = crate::expr::predicate::select_true(v.clone())?;
        if selected.is_empty() {
            return Ok(());
        }
        let (ids, count) = match c.get(self.inputs[1])? {
            Value::Groups { ids, count } => (ids, *count),
            _ => return Err(Error::Execution("expected group IDs".into())),
        };
        let ids = Arc::new(selected.iter().map(|&i| ids[i]).collect());
        let batch = batch.filter(mask)?;
        self.region.context.set(self.domain, Value::Batch(batch));
        self.region
            .context
            .set(self.groups, Value::Groups { ids, count });
        let result = self.region.call();
        self.region.context.clear();
        result
    }
    fn finish(&mut self) -> Result<()> {
        self.region.finish()
    }
}
struct CompletionSink {
    global: Arc<AggregateSinkGlobalContext>,
    completion: Arc<Mutex<Completion>>,
}
impl SinkExecutor for CompletionSink {
    fn sink<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        _: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async {
            Err(Error::Execution(
                "aggregate update must run in its Program".into(),
            ))
        })
    }
    fn combine(self: Box<Self>, _: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut done = self.completion.lock().unwrap();
            let groups = done
                .groups
                .take()
                .ok_or_else(|| Error::Execution("aggregate programs were not finished".into()))?;
            let states = done
                .states
                .iter_mut()
                .map(|s| {
                    s.take()
                        .ok_or_else(|| Error::Execution("aggregate update was not finished".into()))
                })
                .collect::<Result<_>>()?;
            self.global
                .partials
                .lock()
                .unwrap()
                .push(PartialAggregate { groups, states });
            Ok(())
        })
    }
}
/// Standalone sink API adapter, using exactly the same emitted update programs.
struct StandaloneSink {
    program: ProcessProgram,
    sink: Box<dyn SinkExecutor>,
}
impl SinkExecutor for StandaloneSink {
    fn sink<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            self.program.execute(&input.clone().into())?;
            Ok(SinkResult::NeedMoreInput)
        })
    }
    fn combine(mut self: Box<Self>, guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.program.finish()?;
            self.sink.combine(guard).await
        })
    }
}
impl AggregateSinkExec {
    pub(super) fn emit_updates(
        &self,
        global: GlobalExecContextRef,
        b: &mut ProgramBuilder,
        input: ValueId,
    ) -> Result<Box<dyn SinkExecutor>> {
        let global = global
            .downcast::<AggregateSinkGlobalContext>()
            .map_err(|_| {
                Error::Execution("aggregate sink received an invalid global context".into())
            })?;
        let completion = Arc::new(Mutex::new(Completion {
            groups: None,
            states: vec![None; self.operator.aggregates().len()],
        }));
        let keys = b.emit_project(input, self.operator.groups())?;
        let groups = b.value();
        let schema = self.operator.groups().output_schema();
        b.emit(GroupProgram {
            inputs: [input, keys],
            output: [groups],
            index: GroupIndex::new(&schema)?,
            ids: Arc::new(Vec::new()),
            schema: schema.clone(),
            completion: completion.clone(),
        });
        for (index, expr) in self.operator.aggregates().iter().enumerate() {
            if expr.is_distinct() && expr.function() != AggregateFunction::Count {
                return Err(Error::InvalidPlan(
                    "initial aggregate implementation supports DISTINCT only for COUNT(expr)"
                        .into(),
                ));
            }
            let mut accumulator = Accumulator::new(
                expr.function(),
                expr.is_distinct(),
                expr.argument().map(|e| e.result_type().data_type()),
                expr.result_type().data_type(),
            )?;
            if schema.fields().is_empty() {
                accumulator.resize(1);
                accumulator.bind_global_count();
                accumulator.bind_global_sum();
            }
            if let Some(filter) = expr.filter() {
                let filter = filter.emit(b, input)?;
                let mut region = ProgramBuilder::default();
                let domain = region.value();
                let ids = region.value();
                let mut inputs = vec![domain, ids];
                if let Some(arg) = expr.argument() {
                    inputs.push(arg.emit(&mut region, domain)?);
                }
                region.emit(UpdateProgram {
                    inputs,
                    accumulator,
                    index,
                    completion: completion.clone(),
                });
                b.emit(FilteredUpdate {
                    inputs: [input, groups, filter],
                    region: region.build()?,
                    domain,
                    groups: ids,
                });
            } else {
                let mut inputs = vec![input, groups];
                if let Some(arg) = expr.argument() {
                    inputs.push(arg.emit(b, input)?);
                }
                b.emit(UpdateProgram {
                    inputs,
                    accumulator,
                    index,
                    completion: completion.clone(),
                });
            }
        }
        Ok(Box::new(CompletionSink { global, completion }))
    }
    pub(super) fn standalone(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        let mut b = ProgramBuilder::default();
        let input = b.value();
        let sink = self.emit_updates(global, &mut b, input)?;
        Ok(Box::new(StandaloneSink {
            program: b.build_batch(input, None)?,
            sink,
        }))
    }
}
