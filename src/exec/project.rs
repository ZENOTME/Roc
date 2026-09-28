use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor};
use crate::{Error, Result, expr, operator::ProjectOperator};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::sync::Arc;

pub struct ProjectExec {
    operator: ProjectOperator,
    schema: SchemaRef,
}

impl ProjectExec {
    pub fn new(operator: ProjectOperator, schema: SchemaRef) -> Self {
        Self { operator, schema }
    }
}

impl ProcessExec for ProjectExec {
    fn init_global_context(
        &self,
        _batch_rows: usize,
        _cancel: &crate::CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        global.downcast::<()>().map_err(|_| {
            Error::Execution("project exec received an invalid global context".into())
        })?;
        Ok(Box::new(ProjectExecutor {
            operator: self.operator.clone(),
            schema: self.schema.clone(),
        }))
    }
}

struct ProjectExecutor {
    operator: ProjectOperator,
    schema: SchemaRef,
}

impl ProcessExecutor for ProjectExecutor {
    fn execute(
        &mut self,
        _cancel: &crate::CancellationToken,
        input: RecordBatch,
    ) -> Result<RecordBatch> {
        expr::project(input, &self.operator.expressions, self.schema.clone())
    }
}
