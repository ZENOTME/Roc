use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::expr::scalar::executor::{ExpressionExecutor, ExpressionInput};
use crate::{
    error::{Error, Result},
    expr::scalar::ScalarExprRef,
};
use arrow::{
    array::BooleanArray, compute::filter_record_batch, datatypes::DataType,
    record_batch::RecordBatch,
};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FilterExec {
    predicate: ScalarExprRef,
}
impl FilterExec {
    pub fn new(predicate: ScalarExprRef) -> Self {
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
    predicate: ScalarExprRef,
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
        let selected = self
            .expressions
            .as_mut()
            .unwrap()
            .select(&ExpressionInput::new(input.columns(), input.num_rows()))?;
        let output = if selected.len() == input.num_rows() {
            input.clone()
        } else if selected.is_empty() {
            input.slice(0, 0)
        } else {
            let mut mask = vec![false; input.num_rows()];
            for row in selected {
                mask[row] = true;
            }
            filter_record_batch(input, &BooleanArray::from(mask))?
        };
        Ok(ProcessResult::NeedMoreInput(output))
    }
    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
