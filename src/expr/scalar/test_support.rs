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

//! Evaluates root expressions through a one-pipeline program.
use super::ScalarExprRef;
use crate::arrow::{
    array::ArrayRef,
    datatypes::DataType,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::column::{ColumnContext, ColumnId, ColumnType};
use crate::error::Result;
use crate::kernel::{ExecutionMode, KernelBuilder, MODES, Program, agree};

#[derive(Clone)]
pub(crate) struct TestPlan {
    pub columns: ColumnContext,
    pub inputs: Vec<ColumnId>,
}

impl TestPlan {
    pub fn new(inputs: &[(DataType, bool)]) -> Self {
        let mut columns = ColumnContext::new();
        let inputs = inputs
            .iter()
            .map(|(t, nullable)| columns.allocate(ColumnType::new(t.clone(), *nullable)))
            .collect();
        Self { columns, inputs }
    }

    pub fn input(&self, i: usize) -> ColumnId {
        self.inputs[i]
    }

    pub fn column(&mut self, data_type: DataType, nullable: bool) -> ColumnId {
        self.columns.allocate(ColumnType::new(data_type, nullable))
    }

    pub fn program_with(&self, exprs: &[ScalarExprRef], mode: ExecutionMode) -> Result<Program> {
        let mut builder = KernelBuilder::new(&self.columns);
        builder.bind_inputs(&self.inputs)?;
        for expr in exprs {
            let errors = expr.build_kernels(&mut builder)?;
            builder.raise(errors)?;
        }
        let outputs = exprs.iter().map(|e| e.output()).collect::<Vec<_>>();
        Program::new(builder.finish(Some(&outputs))?, mode)
    }

    pub fn evaluate_all(
        &self,
        exprs: &[ScalarExprRef],
        input: &[ArrayRef],
        rows: usize,
    ) -> Result<Vec<ArrayRef>> {
        let batch = RecordBatch::try_new_with_options(
            self.columns.schema(&self.inputs)?,
            input.to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let (_shutdown, guard) = asyncband::shutdown::new();
        let run = |mode| {
            let program = self.program_with(exprs, mode)?;
            let mut local = program.new_local()?;
            let output = program.run(&batch, &mut local, &guard)?.unwrap();
            Ok(output.columns().to_vec())
        };
        agree(MODES.map(|m| (m, run(m))).into(), |a, b| a == b)
    }

    pub fn evaluate(
        &self,
        expr: &ScalarExprRef,
        input: &[ArrayRef],
        rows: usize,
    ) -> Result<ArrayRef> {
        Ok(self
            .evaluate_all(std::slice::from_ref(expr), input, rows)?
            .remove(0))
    }
}
