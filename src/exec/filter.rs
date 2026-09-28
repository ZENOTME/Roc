use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::{Cancel, Error, PhysicalExprRef, Result};
use arrow::{array::BooleanArray, compute::filter_record_batch, record_batch::RecordBatch};
use datafusion_common::ScalarValue;
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::sync::Arc;

#[derive(Clone)]
pub struct FilterExec {
    predicate: PhysicalExprRef,
}

impl FilterExec {
    pub fn new(predicate: PhysicalExprRef) -> Self {
        Self { predicate }
    }
}

impl ProcessExec for FilterExec {
    fn init_global_context(&self, _cancel: &Cancel) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }

    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        Ok(Box::new(self.clone()))
    }
}

impl ProcessExecutor for FilterExec {
    fn execute(&mut self, _cancel: &Cancel, input: &RecordBatch) -> Result<ProcessResult> {
        let output = match self.predicate.evaluate(input)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => input.clone(),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => input.slice(0, 0),
            ColumnarValue::Array(array) => {
                let mask = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::Execution("predicate must be Boolean".into()))?;
                filter_record_batch(input, mask)?
            }
            _ => return Err(Error::Execution("predicate must be Boolean".into())),
        };
        Ok(ProcessResult::NeedMoreInput(output))
    }

    fn finish(&mut self, _cancel: &Cancel) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
