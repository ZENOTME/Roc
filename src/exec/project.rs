use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::expr::scalar::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::{error::Result, operator::Projection};
use arrow::{
    datatypes::SchemaRef,
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
    expressions: Vec<ScalarExpressionEvaluation>,
    output_schema: SchemaRef,
}
impl ProjectionExecutor {
    pub fn try_new(projection: Projection) -> Result<Self> {
        let expressions = projection
            .expressions()
            .iter()
            .map(|e| e.expression().to_evaluation())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            expressions,
            output_schema: projection.output_schema(),
        })
    }
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
    pub fn project_batch(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let columns = self
            .expressions
            .iter()
            .map(|e| e.evaluate(&executor))
            .collect::<Result<Vec<_>>>()?;
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
