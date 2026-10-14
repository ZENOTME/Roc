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

use super::accumulator::Accumulator;
use crate::arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray},
    compute::filter_record_batch,
    datatypes::DataType,
    record_batch::RecordBatch,
};
use crate::column::{ColumnId, ColumnType};
use crate::expr::scalar::{CastMode, test_support::TestPlan};
use crate::kernel::{KernelBuilder, LocalState, Program};
use crate::{
    error::{Error, Result},
    expr::agg::AggregateExpression,
};
use asyncband::shutdown::{Shutdown, ShutdownGuard};
use std::sync::Arc;

/// Drives one accumulator with caller-provided group IDs. FILTER and the
/// coerced arguments are evaluated by programs over the plan's input columns;
/// arguments only see rows kept by FILTER.
pub struct AccumulatorTestHarness {
    expression: Arc<AggregateExpression>,
    filter: Option<(Program, LocalState)>,
    arguments: (Program, LocalState),
    accumulator: Accumulator,
    _shutdown: Shutdown,
    guard: ShutdownGuard,
}

impl AccumulatorTestHarness {
    pub fn try_new(plan: &TestPlan, expression: Arc<AggregateExpression>) -> Result<Self> {
        let mut columns = plan.columns.clone();
        let accumulator = expression.accumulator(&columns)?;
        let coerced = expression
            .input_types(&columns)?
            .into_iter()
            .map(|t| columns.allocate(ColumnType::new(t, true)))
            .collect::<Vec<ColumnId>>();
        let filter = expression
            .filter()
            .map(|filter| {
                let mut builder = KernelBuilder::new(&columns);
                builder.bind_inputs(&plan.inputs)?;
                let errors = filter.build_kernels(&mut builder)?;
                builder.raise(errors)?;
                let program = Program::compile(builder.finish(Some(&[filter.output()]))?)?;
                let local = program.new_local()?;
                Ok::<_, Error>((program, local))
            })
            .transpose()?;
        let mut builder = KernelBuilder::new(&columns);
        builder.bind_inputs(&plan.inputs)?;
        for (argument, &column) in expression.arguments().iter().zip(&coerced) {
            let errors = argument.build_kernels(&mut builder)?;
            builder.raise(errors)?;
            let binding = builder.column_binding(argument.output())?;
            let from = columns.column_type(argument.output())?.data_type.clone();
            let to = columns.column_type(column)?.data_type.clone();
            let (binding, errors) = builder.cast_binding(&binding, &from, &to, CastMode::Try)?;
            builder.bind_column(column, binding, errors)?;
        }
        let program = Program::compile(builder.finish(Some(&coerced))?)?;
        let local = program.new_local()?;
        let (shutdown, guard) = asyncband::shutdown::new();
        Ok(Self {
            expression,
            filter,
            arguments: (program, local),
            accumulator,
            _shutdown: shutdown,
            guard,
        })
    }
    pub fn state_types(&self) -> Vec<DataType> {
        self.accumulator.state_types()
    }
    pub fn resize(&mut self, count: usize) {
        self.accumulator.resize(count);
    }
    pub fn update(&mut self, input: &RecordBatch, ids: &[usize], groups: usize) -> Result<()> {
        if ids.len() != input.num_rows() || ids.iter().any(|&id| id >= groups) {
            return Err(Error::invalid_input(
                "aggregate group IDs do not match input".into(),
            ));
        }
        self.resize(groups);
        let (input, selected) = self.select(input)?;
        let ids = match selected {
            Some(rows) => rows.into_iter().map(|i| ids[i]).collect(),
            None => ids.to_vec(),
        };
        if ids.is_empty() {
            return Ok(());
        }
        let values = self.evaluate_arguments(&input)?;
        self.accumulator.update(&values, &ids)
    }
    /// Updates an ungrouped aggregate, retaining one state even for empty input.
    pub fn update_single(&mut self, input: &RecordBatch) -> Result<()> {
        self.resize(1);
        let (input, _) = self.select(input)?;
        if input.num_rows() == 0 {
            return Ok(());
        }
        let values = self.evaluate_arguments(&input)?;
        self.accumulator.update_single(&values, input.num_rows())
    }
    /// Applies FILTER, returning the kept rows and their original positions.
    fn select(&mut self, input: &RecordBatch) -> Result<(RecordBatch, Option<Vec<usize>>)> {
        let Some((program, local)) = &mut self.filter else {
            return Ok((input.clone(), None));
        };
        let output = program.run(input, local, &self.guard)?.unwrap();
        let mask = output.column(0).as_boolean();
        let rows = (0..mask.len())
            .filter(|&i| mask.is_valid(i) && mask.value(i))
            .collect::<Vec<_>>();
        let mut keep = vec![false; input.num_rows()];
        for &row in &rows {
            keep[row] = true;
        }
        let input = filter_record_batch(input, &BooleanArray::from(keep))
            .map_err(|source| Error::internal("FILTER mask failed".into()).with_source(source))?;
        Ok((input, Some(rows)))
    }
    fn evaluate_arguments(&mut self, input: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let (program, local) = &mut self.arguments;
        Ok(program
            .run(input, local, &self.guard)?
            .unwrap()
            .columns()
            .to_vec())
    }

    pub fn merge(&mut self, state: &[ArrayRef], ids: &[usize], groups: usize) -> Result<()> {
        self.resize(groups);
        let types = self.state_types();
        if ids.iter().any(|&id| id >= groups)
            || state.len() != types.len()
            || state
                .iter()
                .zip(types)
                .any(|(a, t)| a.len() != ids.len() || a.data_type() != &t)
        {
            return Err(Error::invalid_input(format!(
                "invalid partial state for {:?}",
                self.expression.function()
            )));
        }
        self.accumulator.merge(state, ids)
    }
    pub fn state(&self) -> Result<Vec<ArrayRef>> {
        self.accumulator.state()
    }
    pub fn evaluate(&self) -> Result<ArrayRef> {
        self.accumulator.evaluate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Float64Array, Int64Array};
    use crate::expr::agg::AggregateFunction;
    use crate::expr::scalar::{
        ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
    };

    fn input(plan: &TestPlan, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(plan.columns.schema(&plan.inputs).unwrap(), columns).unwrap()
    }

    #[test]
    fn filtered_arguments_and_constants_follow_original_group_ids() {
        let mut plan = TestPlan::new(&[(DataType::Int64, true), (DataType::Boolean, false)]);
        let input = input(
            &plan,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(40),
                    Some(10),
                    Some(40),
                    None,
                    Some(20),
                ])),
                Arc::new(BooleanArray::from(vec![true, true, true, true, false])),
            ],
        );
        let ids = [1, 0, 1, 0, 1];
        let three = plan.column(DataType::Int64, false);
        for (argument, expected) in [
            (
                ReferenceExpression::new(plan.input(0)).into_ref(),
                vec![Some(10), Some(80)],
            ),
            (
                ConstantExpression::int64(Some(3), three).into_ref(),
                vec![Some(6), Some(6)],
            ),
        ] {
            let output = plan.column(DataType::Int64, true);
            let expression = Arc::new(
                AggregateExpression::new(AggregateFunction::Sum, vec![argument], output)
                    .with_filter(ReferenceExpression::new(plan.input(1)).into_ref()),
            );
            let mut executor = AccumulatorTestHarness::try_new(&plan, expression).unwrap();
            executor.update(&input, &ids, 2).unwrap();
            let result = executor.evaluate().unwrap();
            assert_eq!(
                result
                    .as_primitive::<crate::arrow::datatypes::Int64Type>()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let output = plan.column(DataType::Int64, false);
        let expression = Arc::new(AggregateExpression::new(
            AggregateFunction::Count,
            vec![],
            output,
        ));
        let mut executor = AccumulatorTestHarness::try_new(&plan, expression).unwrap();
        executor.update(&input, &ids, 2).unwrap();
        assert_eq!(
            executor
                .evaluate()
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(3)]
        );
        assert!(
            executor
                .merge(&executor.state().unwrap(), &[0, 2], 2)
                .is_err()
        );
    }

    #[test]
    fn rejected_rows_skip_fallible_arguments() {
        let mut plan = TestPlan::new(&[(DataType::Int64, false)]);
        let input = input(&plan, vec![Arc::new(Int64Array::from(vec![0, 0]))]);
        let eight = plan.column(DataType::Int64, false);
        let quotient = plan.column(DataType::Int64, false);
        let never = plan.column(DataType::Boolean, false);
        let output = plan.column(DataType::Int64, true);
        let expression = AggregateExpression::new(
            AggregateFunction::Sum,
            vec![
                FunctionExpression::binary(
                    FunctionKind::Divide,
                    ConstantExpression::int64(Some(8), eight).into_ref(),
                    ReferenceExpression::new(plan.input(0)).into_ref(),
                    quotient,
                )
                .into_ref(),
            ],
            output,
        );
        let mut unfiltered =
            AccumulatorTestHarness::try_new(&plan, Arc::new(expression.clone())).unwrap();
        assert!(unfiltered.update_single(&input).is_err());
        let expression =
            expression.with_filter(ConstantExpression::boolean(Some(false), never).into_ref());
        let mut filtered = AccumulatorTestHarness::try_new(&plan, Arc::new(expression)).unwrap();
        filtered.update(&input, &[0, 0], 1).unwrap();
        filtered.update_single(&input).unwrap();
        assert!(filtered.evaluate().unwrap().is_null(0));
    }

    #[test]
    fn mixed_bound_and_computed_arguments_use_filtered_input() {
        let mut plan = TestPlan::new(&[(DataType::Int64, false), (DataType::Boolean, false)]);
        let input = input(
            &plan,
            vec![
                Arc::new(Int64Array::from(vec![0, 2, 4])),
                Arc::new(BooleanArray::from(vec![false, true, true])),
            ],
        );
        let reference = ReferenceExpression::new(plan.input(0)).into_ref();
        let eight = plan.column(DataType::Int64, false);
        let quotient = plan.column(DataType::Int64, false);
        let output = plan.column(DataType::Float64, true);
        let divided = FunctionExpression::binary(
            FunctionKind::Divide,
            ConstantExpression::int64(Some(8), eight).into_ref(),
            reference.clone(),
            quotient,
        )
        .into_ref();
        let expression = Arc::new(
            AggregateExpression::new(
                AggregateFunction::CovarPop,
                vec![reference, divided],
                output,
            )
            .with_filter(ReferenceExpression::new(plan.input(1)).into_ref()),
        );
        let mut grouped = AccumulatorTestHarness::try_new(&plan, expression.clone()).unwrap();
        let mut single = AccumulatorTestHarness::try_new(&plan, expression).unwrap();
        grouped.update(&input, &[0, 0, 0], 1).unwrap();
        single.update_single(&input).unwrap();
        for executor in [grouped, single] {
            assert_eq!(
                executor
                    .evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                -1.0
            );
        }
    }
}
