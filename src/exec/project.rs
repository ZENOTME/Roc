use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::{Cancel, Result};
use arrow::record_batch::RecordBatch;
use datafusion_physical_expr::projection::Projector;
use std::sync::Arc;

#[derive(Clone)]
pub struct ProjectExec {
    projector: Projector,
}

impl ProjectExec {
    pub fn new(projector: Projector) -> Self {
        Self { projector }
    }
}

impl ProcessExec for ProjectExec {
    fn init_global_context(&self, _cancel: &Cancel) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }

    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        Ok(Box::new(self.clone()))
    }
}

impl ProcessExecutor for ProjectExec {
    fn execute(&mut self, _cancel: &Cancel, input: &RecordBatch) -> Result<ProcessResult> {
        Ok(ProcessResult::NeedMoreInput(
            self.projector.project_batch(input)?,
        ))
    }

    fn finish(&mut self, _cancel: &Cancel) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
