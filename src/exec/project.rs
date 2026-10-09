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
use crate::expr::scalar::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::{
    error::{Error, ErrorContext, Result, ResultExt},
    operator::Projection,
};
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
            .enumerate()
            .map(|(index, e)| {
                e.expression()
                    .to_evaluation()
                    .with_context(|| ErrorContext::new("projection.bind").field("column", index))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            expressions,
            output_schema: projection.output_schema(),
        })
    }
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
    pub fn project_batch(&self, input: &RecordBatch) -> Result<RecordBatch> {
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let columns = self
            .expressions
            .iter()
            .enumerate()
            .map(|(index, e)| {
                let value = e.evaluate(&executor).with_context(|| {
                    ErrorContext::new("projection.evaluate").field("column", index)
                })?;
                // RecordBatch checks each column's concrete Arrow type against its schema.
                let value = value.into_array(input.num_rows()).with_context(|| {
                    ErrorContext::new("projection.materialize").field("column", index)
                })?;
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
        )
        .map_err(|source| {
            Error::invalid_input("projection output does not match its schema".into())
                .with_source(source)
        })
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
