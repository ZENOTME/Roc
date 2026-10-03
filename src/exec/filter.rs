// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::expr::predicate::select_true;
use crate::expr::scalar::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::{error::Result, expr::scalar::ScalarExprRef};
use arrow::{array::BooleanArray, compute::filter_record_batch, record_batch::RecordBatch};
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
            predicate: self.predicate.to_evaluation()?,
        }))
    }
}

/// Built once from the description, then retained across batches.
struct FilterExecutor {
    predicate: ScalarExpressionEvaluation,
}
impl ProcessExecutor for FilterExecutor {
    fn execute(&mut self, input: &RecordBatch) -> Result<ProcessResult> {
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let selected = select_true(self.predicate.evaluate(&executor)?)?;
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
