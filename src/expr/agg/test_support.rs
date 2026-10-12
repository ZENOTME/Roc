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
use crate::expr::scalar::ScalarExpression;
use crate::suboperator::test_support::select_true;
use crate::suboperator::{ProgramContext, ProgramExecutionContext};
use crate::{
    error::{Error, Result},
    expr::agg::{AggregateExpression, AggregateFunction},
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray},
    compute::filter_record_batch,
    datatypes::DataType,
    record_batch::RecordBatch,
};
use std::{borrow::Cow, sync::Arc};

enum AggregateArgument {
    Reference(usize),
    Program(Box<(ProgramContext, ProgramExecutionContext)>),
}

/// Test fixture for direct accumulator update/merge parity. Production aggregation
/// lowers through suboperator::aggregation; this helper is never in the library.
pub struct AccumulatorTestHarness {
    expression: Arc<AggregateExpression>,
    arguments: Vec<AggregateArgument>,
    filter: Option<(ProgramContext, ProgramExecutionContext)>,
    accumulator: Accumulator,
}

impl AccumulatorTestHarness {
    pub fn try_new(expression: Arc<AggregateExpression>) -> Result<Self> {
        let arguments = expression
            .arguments()
            .iter()
            .map(|argument| match argument.as_ref() {
                ScalarExpression::Reference(reference) => {
                    Ok(AggregateArgument::Reference(reference.index()))
                }
                _ => Ok(AggregateArgument::Program(Box::new((
                    ProgramContext::from_expressions(
                        std::slice::from_ref(argument),
                        crate::suboperator::ExecutionMode::Native,
                    )?,
                    ProgramExecutionContext::default(),
                )))),
            })
            .collect::<Result<Vec<_>>>()?;
        let types = expression
            .arguments()
            .iter()
            .map(|e| e.result_type().data_type.clone())
            .collect::<Vec<_>>();
        // No accumulator implements it, so accepting it would silently drop DISTINCT.
        if expression.is_distinct() && expression.function() != AggregateFunction::Count {
            return Err(Error::InvalidPlan(
                "initial aggregate implementation supports DISTINCT only for COUNT(expr)".into(),
            ));
        }
        let filter = expression
            .filter()
            .map(|e| {
                Ok::<_, Error>((
                    ProgramContext::from_expressions(
                        std::slice::from_ref(e),
                        crate::suboperator::ExecutionMode::Native,
                    )?,
                    ProgramExecutionContext::default(),
                ))
            })
            .transpose()?;
        let accumulator = Accumulator::new(
            expression.function(),
            expression.is_distinct(),
            &types,
            expression.result_type().data_type(),
        )?;
        Ok(Self {
            expression,
            arguments,
            filter,
            accumulator,
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
            return Err(Error::Execution(
                "aggregate group IDs do not match input".into(),
            ));
        }
        self.update_validated(input, ids, groups)
    }
    /// Internal path for IDs produced and validated by the grouping operator.
    pub(crate) fn update_validated(
        &mut self,
        input: &RecordBatch,
        ids: &[usize],
        groups: usize,
    ) -> Result<()> {
        debug_assert_eq!(ids.len(), input.num_rows());
        debug_assert!(ids.iter().all(|&id| id < groups));
        self.resize(groups);
        let selected = self
            .filter
            .as_mut()
            .map(|(program, local)| {
                select_true(
                    program
                        .execute(input.columns(), input.num_rows(), local)?
                        .remove(0),
                )
            })
            .transpose()?;
        let selected_ids = selected
            .as_ref()
            .map(|rows| rows.iter().map(|&i| ids[i]).collect::<Vec<_>>());
        let ids = selected_ids.as_deref().unwrap_or(ids);
        if ids.is_empty() {
            return Ok(());
        }
        let selected_input = selected
            .as_ref()
            .map(|rows| {
                let mut mask = vec![false; input.num_rows()];
                for &row in rows {
                    mask[row] = true;
                }
                filter_record_batch(input, &BooleanArray::from(mask))
            })
            .transpose()?;
        let input = selected_input.as_ref().unwrap_or(input);
        let values = self.evaluate_arguments(input)?;
        self.accumulator.update(&values, ids)
    }
    /// Update an ungrouped aggregate, retaining one state even for empty input.
    pub fn update_single(&mut self, input: &RecordBatch) -> Result<()> {
        self.resize(1);
        let selected = self
            .filter
            .as_mut()
            .map(|(program, local)| {
                select_true(
                    program
                        .execute(input.columns(), input.num_rows(), local)?
                        .remove(0),
                )
            })
            .transpose()?;
        let rows = selected.as_ref().map_or(input.num_rows(), Vec::len);
        if rows == 0 {
            return Ok(());
        }
        let selected_input = selected
            .as_ref()
            .map(|rows| {
                let mut mask = vec![false; input.num_rows()];
                for &row in rows {
                    mask[row] = true;
                }
                filter_record_batch(input, &BooleanArray::from(mask))
            })
            .transpose()?;
        // FILTER must run before argument evaluation, including fallible arguments.
        let input = selected_input.as_ref().unwrap_or(input);
        let values = self.evaluate_arguments(input)?;
        self.accumulator.update_single(&values, rows)
    }
    /// Borrow empty and single-reference arguments from the selected input.
    /// Build the shared scalar executor lazily for arguments that evaluate a tree.
    fn evaluate_arguments<'a>(&mut self, input: &'a RecordBatch) -> Result<Cow<'a, [ArrayRef]>> {
        match self.arguments.as_slice() {
            [] => return Ok(Cow::Borrowed(&[])),
            [AggregateArgument::Reference(index)] => {
                return input
                    .columns()
                    .get(*index..=*index)
                    .map(Cow::Borrowed)
                    .ok_or_else(|| {
                        Error::Execution(format!("column index {index} out of bounds"))
                    });
            }
            _ => {}
        }
        self.arguments
            .iter_mut()
            .map(|argument| match argument {
                AggregateArgument::Reference(index) => {
                    input.columns().get(*index).cloned().ok_or_else(|| {
                        Error::Execution(format!("column index {index} out of bounds"))
                    })
                }
                AggregateArgument::Program(bound) => {
                    let (program, local) = bound.as_mut();
                    Ok(program
                        .execute(input.columns(), input.num_rows(), local)?
                        .remove(0))
                }
            })
            .collect::<Result<Vec<_>>>()
            .map(Cow::Owned)
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
            return Err(Error::Execution(format!(
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
    use crate::expr::ExpressionResultType;
    use crate::expr::scalar::{ConstantExpression, ReferenceExpression};
    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
    };

    #[test]
    fn filtered_arguments_and_constants_follow_original_group_ids() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(40),
                Some(10),
                Some(40),
                None,
                Some(20),
            ])),
            Arc::new(BooleanArray::from(vec![true, true, true, true, false])),
        ];
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, true),
                Field::new("keep", DataType::Boolean, false),
            ])),
            columns,
        )
        .unwrap();
        let ids = [1, 0, 1, 0, 1];
        for (arguments, expected) in [
            (
                vec![
                    ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true))
                        .into_ref(),
                ],
                vec![Some(10), Some(80)],
            ),
            (
                vec![ConstantExpression::int64(Some(3)).into_ref()],
                vec![Some(6), Some(6)],
            ),
        ] {
            let expression = Arc::new(
                AggregateExpression::new(AggregateFunction::Sum, arguments, DataType::Int64, true)
                    .with_filter(
                        ReferenceExpression::new(
                            1,
                            ExpressionResultType::new(DataType::Boolean, false),
                        )
                        .into_ref(),
                    ),
            );
            let mut executor = AccumulatorTestHarness::try_new(expression).unwrap();
            executor.update(&input, &ids, 2).unwrap();
            let result = executor.evaluate().unwrap();
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let expression = Arc::new(AggregateExpression::new(
            AggregateFunction::Count,
            vec![],
            DataType::Int64,
            false,
        ));
        let mut executor = AccumulatorTestHarness::try_new(expression).unwrap();
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
    fn bound_references_check_indices_after_filtering() {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        // The inclusive single-column range must also reject usize::MAX
        // without overflowing its end or panicking.
        for index in [1, usize::MAX] {
            let expression = AggregateExpression::new(
                AggregateFunction::Sum,
                vec![
                    ReferenceExpression::new(
                        index,
                        ExpressionResultType::new(DataType::Int64, false),
                    )
                    .into_ref(),
                ],
                DataType::Int64,
                true,
            );
            let mut grouped =
                AccumulatorTestHarness::try_new(Arc::new(expression.clone())).unwrap();
            let mut single = AccumulatorTestHarness::try_new(Arc::new(expression.clone())).unwrap();
            for result in [
                grouped.update(&input, &[0, 0], 1),
                single.update_single(&input),
            ] {
                assert!(
                    matches!(result, Err(Error::Execution(message)) if message == format!("column index {index} out of bounds"))
                );
            }
            // When FILTER rejects every row, argument evaluation is skipped entirely,
            // including its bounds checks, as on the generic expression path.
            let expression =
                expression.with_filter(ConstantExpression::boolean(Some(false)).into_ref());
            let mut filtered = AccumulatorTestHarness::try_new(Arc::new(expression)).unwrap();
            filtered.update(&input, &[0, 0], 1).unwrap();
            filtered.update_single(&input).unwrap();
            assert!(filtered.evaluate().unwrap().is_null(0));
        }
    }

    #[test]
    fn mixed_bound_and_computed_arguments_use_filtered_input() {
        use crate::expr::scalar::{FunctionExpression, FunctionKind};
        use arrow::array::Float64Array;

        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new("keep", DataType::Boolean, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![0, 2, 4])),
                Arc::new(BooleanArray::from(vec![false, true, true])),
            ],
        )
        .unwrap();
        let reference =
            ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, false))
                .into_ref();
        let divided = FunctionExpression::binary(
            FunctionKind::Divide,
            ConstantExpression::int64(Some(8)).into_ref(),
            reference.clone(),
            DataType::Int64,
            false,
        )
        .into_ref();
        let expression = Arc::new(
            AggregateExpression::new(
                AggregateFunction::CovarPop,
                vec![reference, divided],
                DataType::Float64,
                true,
            )
            .with_filter(
                ReferenceExpression::new(1, ExpressionResultType::new(DataType::Boolean, false))
                    .into_ref(),
            ),
        );
        let mut grouped = AccumulatorTestHarness::try_new(expression.clone()).unwrap();
        let mut single = AccumulatorTestHarness::try_new(expression).unwrap();
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
