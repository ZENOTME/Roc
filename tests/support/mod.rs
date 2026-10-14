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

//! Builds expressions over one process-wide column context and evaluates them
//! as standalone programs. Each input column remembers the chunk position it
//! reads, so tests can describe expressions without threading a plan around.
#![allow(dead_code)]

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use roc::column::{ColumnContext, ColumnId, ColumnType};
use roc::error::{Error, Result};
use roc::expr::scalar::{ConstantExpression, ScalarExprRef, ScalarExpression};
use roc::kernel::{ExecutionMode, KernelBuilder, Program};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

#[derive(Default)]
struct Registry {
    columns: ColumnContext,
    positions: HashMap<ColumnId, usize>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);

/// A column computed by an expression.
pub fn output_column(data_type: DataType, nullable: bool) -> ColumnId {
    let mut registry = REGISTRY.lock().unwrap();
    registry
        .columns
        .allocate(ColumnType::new(data_type, nullable))
}

/// A column read from position `position` of the input chunk.
pub fn input_column(position: usize, data_type: DataType, nullable: bool) -> ColumnId {
    let mut registry = REGISTRY.lock().unwrap();
    let id = registry
        .columns
        .allocate(ColumnType::new(data_type, nullable));
    registry.positions.insert(id, position);
    id
}

/// The declared type of a column.
pub fn column_type(id: ColumnId) -> ColumnType {
    REGISTRY
        .lock()
        .unwrap()
        .columns
        .column_type(id)
        .unwrap()
        .clone()
}

/// A one-value constant whose column takes the value's type.
pub fn constant(value: ArrayRef) -> Result<ConstantExpression> {
    let output = output_column(value.data_type().clone(), value.null_count() != 0);
    ConstantExpression::try_new(value, output)
}

#[derive(Default)]
pub struct TestInput {
    columns: Option<Vec<ArrayRef>>,
    num_rows: usize,
}

impl TestInput {
    pub fn new(columns: &[ArrayRef], num_rows: usize) -> Self {
        Self {
            columns: Some(columns.to_vec()),
            num_rows,
        }
    }
    pub fn set_input(&mut self, columns: &[ArrayRef], num_rows: usize) {
        self.columns = Some(columns.to_vec());
        self.num_rows = num_rows;
    }
}

/// Every execution mode; each expression runs in all of them.
pub const MODES: [ExecutionMode; 2] = [ExecutionMode::Compile, ExecutionMode::ZeroCompile];

/// A program per execution mode computing expressions over the input columns
/// they reference. Evaluation requires every mode to return equal arrays or
/// equal errors.
pub struct TestProgram {
    programs: Vec<Program>,
    /// Chunk position and declared type of each program input.
    inputs: Vec<(usize, DataType)>,
}

impl TestProgram {
    pub fn try_new(expression: ScalarExprRef) -> Result<Self> {
        Self::try_new_all(&[expression])
    }

    pub fn try_new_all(expressions: &[ScalarExprRef]) -> Result<Self> {
        let (columns, positions) = {
            let registry = REGISTRY.lock().unwrap();
            (registry.columns.clone(), registry.positions.clone())
        };
        let mut referenced = vec![];
        let mut pending = expressions.iter().collect::<Vec<_>>();
        while let Some(e) = pending.pop() {
            if let ScalarExpression::Reference(r) = e.as_ref()
                && let Some(&position) = positions.get(&r.column())
            {
                referenced.push((position, r.column()));
            }
            pending.extend(e.children());
        }
        referenced.sort();
        referenced.dedup();
        let programs = MODES
            .iter()
            .map(|&mode| Self::build(&columns, &referenced, expressions, mode))
            .collect::<Result<Vec<_>>>()?;
        let mut inputs: Vec<(usize, ColumnId)> = vec![];
        for &(position, id) in &referenced {
            if inputs.last().is_none_or(|&(p, _)| p != position) {
                inputs.push((position, id));
            }
        }
        let inputs = inputs
            .into_iter()
            .map(|(p, id)| Ok((p, columns.column_type(id)?.data_type.clone())))
            .collect::<Result<_>>()?;
        Ok(Self { programs, inputs })
    }

    fn build(
        columns: &ColumnContext,
        referenced: &[(usize, ColumnId)],
        expressions: &[ScalarExprRef],
        mode: ExecutionMode,
    ) -> Result<Program> {
        let mut builder = KernelBuilder::new(columns);
        let mut inputs: Vec<(usize, ColumnId)> = vec![];
        for &(position, id) in referenced {
            if inputs.last().is_none_or(|&(p, _)| p != position) {
                inputs.push((position, id));
            }
        }
        builder.bind_inputs(&inputs.iter().map(|&(_, id)| id).collect::<Vec<_>>())?;
        for &(position, id) in referenced {
            let &(_, first) = inputs.iter().find(|&&(p, _)| p == position).unwrap();
            if first != id {
                let binding = builder.column_binding(first)?;
                builder.bind_column(id, binding, vec![])?;
            }
        }
        for expression in expressions {
            let errors = expression.build_kernels(&mut builder)?;
            builder.raise(errors)?;
        }
        let outputs = expressions.iter().map(|e| e.output()).collect::<Vec<_>>();
        Program::new(builder.finish(Some(&outputs))?, mode)
    }

    /// The compiled program.
    pub fn program(&self) -> &Program {
        &self.programs[0]
    }

    pub fn evaluate_all(&self, input: &TestInput) -> Result<Vec<ArrayRef>> {
        let columns = input
            .columns
            .as_deref()
            .ok_or_else(|| Error::invalid_input("input columns are not set".into()))?;
        // Empty inputs may omit columns; nothing reads them.
        let arrays = self
            .inputs
            .iter()
            .map(|(p, t)| match columns.get(*p) {
                Some(array) => Ok(array.clone()),
                None if input.num_rows == 0 => Ok(arrow::array::new_empty_array(t)),
                None => Err(Error::invalid_input(format!(
                    "column index {p} out of bounds"
                ))),
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(input.num_rows)),
        )
        .map_err(|source| Error::invalid_input("invalid test input".into()).with_source(source))?;
        let (_shutdown, guard) = asyncband::shutdown::new();
        let mut results = self.programs.iter().map(|program| {
            let mut local = program.new_local()?;
            let output = program
                .run(&batch, &mut local, &guard)?
                .ok_or_else(|| Error::internal("program has no output".into()))?;
            Ok::<_, Error>(output.columns().to_vec())
        });
        let first = results.next().unwrap();
        for (mode, other) in MODES.iter().skip(1).zip(results) {
            match (&first, &other) {
                (Ok(a), Ok(b)) => assert!(a == b, "{mode:?} differs: {a:?} vs {b:?}"),
                (Err(a), Err(b)) => {
                    assert_eq!(format!("{a:?}"), format!("{b:?}"), "{mode:?} error differs")
                }
                (a, b) => panic!("{mode:?} disagrees with Compile: {a:?} vs {b:?}"),
            }
        }
        first
    }

    pub fn evaluate(&self, input: &TestInput) -> Result<ArrayRef> {
        Ok(self.evaluate_all(input)?.remove(0))
    }
}

pub trait PrepareTest {
    fn prepare_test(&self) -> Result<TestProgram>;
}

impl<T: Clone + Into<ScalarExpression>> PrepareTest for T {
    fn prepare_test(&self) -> Result<TestProgram> {
        TestProgram::try_new(Arc::new(self.clone().into()))
    }
}

/// Rows where a Boolean result is TRUE.
pub fn select_true(array: ArrayRef) -> Result<Vec<usize>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| Error::invalid_input("predicate is not Boolean".into()))?;
    Ok((0..array.len())
        .filter(|&i| arrow::array::Array::is_valid(array, i) && array.value(i))
        .collect())
}
