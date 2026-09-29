use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::expr::scalar::executor::ExpressionExecutor;
use crate::{error::Result, operator::Projection};
use arrow::{
    datatypes::{Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ProjectExec {
    projection: Projection,
}
impl ProjectExec {
    pub fn new(projection: Projection) -> Self {
        Self { projection }
    }
}
impl ProcessExec for ProjectExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        Ok(Box::new(ProjectionExecutor::try_new(
            self.projection.clone(),
        )?))
    }
}

#[derive(Debug)]
pub struct ProjectionExecutor {
    expressions: ExpressionExecutor,
    output_schema: SchemaRef,
}
impl ProjectionExecutor {
    pub fn try_new(projection: Projection) -> Result<Self> {
        let expressions = ExpressionExecutor::try_new(
            projection
                .expressions()
                .iter()
                .map(|e| e.expression().clone())
                .collect(),
            projection.input_schema().clone(),
        )?;
        let fields = expressions
            .results()
            .zip(projection.expressions())
            .map(|(result, e)| Field::new(e.name(), result.data_type.clone(), result.nullable))
            .collect::<Vec<_>>();
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            projection.input_schema().metadata().clone(),
        ));
        Ok(Self {
            expressions,
            output_schema,
        })
    }
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
    pub fn project_batch(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .expressions
            .evaluate(input)?
            .into_iter()
            .map(|v| v.into_array(input.num_rows()))
            .collect::<Result<_>>()?;
        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
        )?)
    }
}
impl ProcessExecutor for ProjectionExecutor {
    fn execute(&mut self, input: &RecordBatch) -> Result<ProcessResult> {
        Ok(ProcessResult::NeedMoreInput(self.project_batch(input)?))
    }
    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
