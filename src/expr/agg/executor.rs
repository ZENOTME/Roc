use super::accumulator::Accumulator;
use crate::expr::ExpressionResultType;
use crate::expr::predicate::select_true;
use crate::expr::scalar::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
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
use std::sync::Arc;

/// Worker-local argument evaluators, result type, and aggregate state.
pub struct AggregateExpressionExecutor {
    expression: Arc<AggregateExpression>,
    arguments: Vec<ScalarExpressionEvaluation>,
    filter: Option<ScalarExpressionEvaluation>,
    result_type: ExpressionResultType,
    accumulator: Accumulator,
}

impl AggregateExpressionExecutor {
    pub fn try_new(expression: Arc<AggregateExpression>) -> Result<Self> {
        let arguments = expression
            .arguments()
            .iter()
            .map(|e| e.to_evaluation())
            .collect::<Result<Vec<_>>>()?;
        let types = expression
            .arguments()
            .iter()
            .map(|e| e.result_type().data_type.clone())
            .collect::<Vec<_>>();
        let result_type = expression.result_type().clone();
        // No accumulator implements it, so accepting it would silently drop DISTINCT.
        if expression.is_distinct() && expression.function() != AggregateFunction::Count {
            return Err(Error::InvalidPlan(
                "initial aggregate implementation supports DISTINCT only for COUNT(expr)".into(),
            ));
        }
        let filter = expression.filter().map(|e| e.to_evaluation()).transpose()?;
        let accumulator = Accumulator::new(
            expression.function(),
            expression.is_distinct(),
            &types,
            &result_type.data_type,
        )?;
        Ok(Self {
            expression,
            arguments,
            filter,
            result_type,
            accumulator,
        })
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }
    pub fn state_types(&self) -> Vec<DataType> {
        self.accumulator.state_types()
    }
    pub fn resize(&mut self, count: usize) {
        self.accumulator.resize(count);
    }
    pub fn update(&mut self, input: &RecordBatch, ids: &[usize], groups: usize) -> Result<()> {
        self.resize(groups);
        if ids.len() != input.num_rows() || ids.iter().any(|&id| id >= groups) {
            return Err(Error::Execution(
                "aggregate group IDs do not match input".into(),
            ));
        }
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let selected = self
            .filter
            .as_ref()
            .map(|filter| select_true(filter.evaluate(&executor)?))
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
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let values = self
            .arguments
            .iter()
            .map(|e| e.evaluate(&executor))
            .collect::<Result<Vec<_>>>()?;
        self.accumulator.update(&values, ids)
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
            let mut executor = AggregateExpressionExecutor::try_new(expression).unwrap();
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
        let mut executor = AggregateExpressionExecutor::try_new(expression).unwrap();
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
}
