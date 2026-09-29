use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::expr::scalar::executor::ExpressionExecutor;
use crate::{
    error::{Error, Result},
    expr::scalar::BoundScalarExprRef,
};
use arrow::{compute::filter_record_batch, datatypes::DataType, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FilterExec {
    predicate: BoundScalarExprRef,
}
impl FilterExec {
    pub fn new(predicate: BoundScalarExprRef) -> Self {
        Self { predicate }
    }
}
impl ProcessExec for FilterExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        Ok(Box::new(FilterExecutor {
            predicate: self.predicate.clone(),
            expressions: None,
        }))
    }
}

/// Initialized against the first input schema, then retained across batches.
struct FilterExecutor {
    predicate: BoundScalarExprRef,
    expressions: Option<ExpressionExecutor>,
}
impl ProcessExecutor for FilterExecutor {
    fn execute(&mut self, input: &RecordBatch) -> Result<ProcessResult> {
        if self.expressions.is_none() {
            let executor =
                ExpressionExecutor::try_new(vec![self.predicate.clone()], input.schema())?;
            if executor.results().next().unwrap().data_type != DataType::Boolean {
                return Err(Error::Execution("predicate must be Boolean".into()));
            }
            self.expressions = Some(executor);
        }
        let mask = self.expressions.as_mut().unwrap().select(input)?;
        let output = if mask.true_count() == mask.len() {
            input.clone()
        } else if mask.true_count() == 0 {
            input.slice(0, 0)
        } else {
            filter_record_batch(input, &mask)?
        };
        Ok(ProcessResult::NeedMoreInput(output))
    }
    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
