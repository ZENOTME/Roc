use super::*;
use crate::{expr::scalar::ScalarExprRef, operator::Projection};
use arrow::{
    array::AsArray,
    record_batch::{RecordBatch, RecordBatchOptions},
};

#[derive(Debug)]
struct FilterProgram {
    inputs: [ValueId; 2],
    output: [ValueId; 1],
    indices: Option<Vec<usize>>,
}
impl Program for FilterProgram {
    fn lowering(&self) -> Option<Lowering<'_>> {
        Some(Lowering::Filter {
            input: self.inputs[0],
            output: self.output[0],
            predicate: self.inputs[1],
            indices: self.indices.as_deref(),
        })
    }
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &self.output
    }
    fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
        let b = c.batch(self.inputs[0])?;
        let v = c
            .column(self.inputs[1], self.inputs[0])?
            .into_array(b.num_rows())?;
        let mask = v
            .as_boolean_opt()
            .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
        let b = b.filter(mask)?;
        let b = if let Some(indices) = &self.indices {
            b.project(indices)?
        } else {
            b
        };
        if b.num_rows() == 0 {
            c.halted = Some(b.clone());
        }
        c.set(self.output[0], Value::Batch(b));
        Ok(())
    }
}
#[derive(Debug)]
struct ProjectProgram {
    inputs: Vec<ValueId>,
    output: [ValueId; 1],
    schema: arrow::datatypes::SchemaRef,
    last_use: Vec<bool>,
}
impl Program for ProjectProgram {
    fn empty_batch(&self)->Option<Batch>{Some(RecordBatch::new_empty(self.schema.clone()).into())}
    fn bind_lifetimes(&mut self, last: &[bool]) {
        self.last_use = last.to_vec();
    }
    fn lowering(&self) -> Option<Lowering<'_>> {
        Some(Lowering::Project {
            input: self.inputs[0],
            output: self.output[0],
            values: &self.inputs[1..],
            schema: &self.schema,
        })
    }
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &self.output
    }
    fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
        let domain = self.inputs[0];
        let rows = c.rows(domain)?;
        let columns = self.inputs[1..]
            .iter()
            .enumerate()
            .map(|(i, &v)| c.operand(v, domain, self.last_use[i + 1])?.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let b = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        c.set(self.output[0], Value::Batch(b.into()));
        Ok(())
    }
}
impl ProgramBuilder {
    pub fn emit_filter(
        &mut self,
        input: ValueId,
        predicate: &ScalarExprRef,
        indices: Option<Vec<usize>>,
    ) -> Result<ValueId> {
        let predicate = predicate.emit(self, input)?;
        let output = self.value();
        self.emit(FilterProgram {
            inputs: [input, predicate],
            output: [output],
            indices,
        });
        Ok(output)
    }
    pub fn emit_project(&mut self, input: ValueId, projection: &Projection) -> Result<ValueId> {
        let mut inputs = vec![input];
        for e in projection.expressions() {
            inputs.push(e.expression().emit(self, input)?);
        }
        let output = self.value();
        self.emit(ProjectProgram {
            inputs,
            output: [output],
            schema: projection.output_schema(),
            last_use: vec![],
        });
        Ok(output)
    }
    pub fn build_batch(mut self, input: ValueId, output: Option<ValueId>) -> Result<ProcessProgram> {
        if input.0>=self.slots{return Err(Error::InvalidPlan("invalid program input".into()));}
        if let Some(output)=output{self.retain(output);}
        let mut p = self.build()?;
        p.input = Some(input);
        p.output = output;
        Ok(p)
    }
}
impl ProcessProgram {
    /// Runs one batch through a precomputed schedule. Data outputs and control
    /// are independent; an aggregate update can consume input with no output.
    pub fn execute(&mut self, input: &Batch) -> Result<crate::exec::ProcessResult> {
        let id = self
            .input
            .ok_or_else(|| Error::Execution("program has no batch input".into()))?;
        self.context.set(id, Value::Batch(input.clone()));
        let result = self.call().and_then(|_| {
            if let Some(control) = self.context.control.take() {
                return Ok(control);
            }
            if let Some(empty) = self.context.halted.take() {
                return Ok(match self.output {None=>crate::exec::ProcessResult::Consumed,Some(_)=>crate::exec::ProcessResult::NeedMoreInput(self.programs.last().and_then(|p|p.empty_batch()).unwrap_or(empty))});
            }
            match self.output {
                Some(v) => match self.context.take(v)? {
                    Value::Batch(batch) => Ok(crate::exec::ProcessResult::NeedMoreInput(batch)),
                    _ => Err(Error::Execution("expected batch result".into())),
                },
                None => Ok(crate::exec::ProcessResult::Consumed),
            }
        });
        self.context.clear();
        result
    }
    pub fn finish_batch(&mut self) -> Result<Option<Batch>> {
        for p in &mut self.programs {
            if let Some(b) = p.finish_batch(&mut self.context)? {
                return Ok(Some(b));
            }
        }
        Ok(None)
    }
}

impl ProcessProgram {
    pub fn run_batch(&mut self, input: &Batch) -> Result<RecordBatch> {
        match self.execute(input)? {
            crate::exec::ProcessResult::NeedMoreInput(b) => b.into_record_batch(),
            _ => Err(Error::Execution(
                "program did not return a completed batch".into(),
            )),
        }
    }
}
