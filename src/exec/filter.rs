use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor};
use crate::{Error, Result, expr, operator::FilterOperator};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub struct FilterExec {
    operator: FilterOperator,
}

impl FilterExec {
    pub fn new(operator: FilterOperator) -> Self {
        Self { operator }
    }
}

impl ProcessExec for FilterExec {
    fn init_global_context(
        &self,
        _batch_rows: usize,
        _cancel: &crate::CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        global.downcast::<()>().map_err(|_| {
            Error::Execution("filter exec received an invalid global context".into())
        })?;
        Ok(Box::new(FilterExecutor {
            operator: self.operator.clone(),
        }))
    }
}

struct FilterExecutor {
    operator: FilterOperator,
}

impl ProcessExecutor for FilterExecutor {
    fn execute(
        &mut self,
        _cancel: &crate::CancellationToken,
        input: RecordBatch,
    ) -> Result<RecordBatch> {
        expr::filter(input, &self.operator.predicate)
    }
}
