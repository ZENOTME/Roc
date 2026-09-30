use super::accumulator::Accumulator;
use crate::expr::scalar::executor::{
    ExpressionExecutor, ExpressionInput, ExpressionResult, is_number, require_boolean,
};
use crate::{
    error::{Error, Result},
    expr::agg::{AggregateExpression, AggregateFunction},
};
use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, SchemaRef},
};
use std::sync::Arc;

/// Worker-local argument evaluators, result metadata, and aggregate state.
pub struct AggregateExpressionExecutor {
    expression: Arc<AggregateExpression>,
    arguments: ExpressionExecutor,
    filter: Option<ExpressionExecutor>,
    result: ExpressionResult,
    accumulator: Accumulator,
}

impl AggregateExpressionExecutor {
    pub fn try_new(expression: Arc<AggregateExpression>, input_schema: SchemaRef) -> Result<Self> {
        use AggregateFunction::*;
        let arguments =
            ExpressionExecutor::try_new(expression.arguments().to_vec(), input_schema.clone())?;
        let types = arguments
            .results()
            .map(|r| r.data_type.clone())
            .collect::<Vec<_>>();
        let valid_arity = match expression.function() {
            Count => types.len() <= 1,
            CovarPop => types.len() == 2,
            _ => types.len() == 1,
        };
        if !valid_arity {
            return Err(Error::InvalidPlan(format!(
                "invalid argument count for {:?}",
                expression.function()
            )));
        }
        if expression.is_distinct() && (expression.function() != Count || types.len() != 1) {
            return Err(Error::InvalidPlan(
                "initial aggregate implementation supports DISTINCT only for COUNT(expr)".into(),
            ));
        }
        if matches!(expression.function(), Sum | Avg | CovarPop)
            && types.iter().any(|t| !is_number(t))
        {
            return Err(Error::InvalidPlan(
                "numeric aggregate requires integer or floating-point arguments".into(),
            ));
        }
        let data_type = match expression.function() {
            Count => DataType::Int64,
            Avg | CovarPop => DataType::Float64,
            Sum => match types[0] {
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    DataType::UInt64
                }
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                _ => DataType::Int64,
            },
            Min | Max => types[0].clone(),
        };
        let result = ExpressionResult {
            data_type,
            nullable: expression.function() != Count,
        };
        let filter = expression
            .filter()
            .map(|e| -> Result<_> {
                let executor = ExpressionExecutor::try_new(vec![e.clone()], input_schema.clone())?;
                require_boolean(executor.results().next().unwrap())?;
                Ok(executor)
            })
            .transpose()?;
        let accumulator = Accumulator::new(
            expression.function(),
            expression.is_distinct(),
            &types,
            &result.data_type,
        )?;
        Ok(Self {
            expression,
            arguments,
            filter,
            result,
            accumulator,
        })
    }
    pub fn result(&self) -> &ExpressionResult {
        &self.result
    }
    pub fn state_types(&self) -> Vec<DataType> {
        self.accumulator.state_types()
    }
    pub fn resize(&mut self, count: usize) {
        self.accumulator.resize(count);
    }
    pub fn update(
        &mut self,
        input: &ExpressionInput<'_>,
        ids: &[usize],
        groups: usize,
    ) -> Result<()> {
        self.resize(groups);
        if ids.len() != input.num_rows() || ids.iter().any(|&id| id >= groups) {
            return Err(Error::Execution(
                "aggregate group IDs do not match input".into(),
            ));
        }
        let selected = if let Some(filter) = &mut self.filter {
            Some(filter.select(input)?)
        } else {
            input.selection().map(<[usize]>::to_vec)
        };
        let selected_ids = selected.as_ref().map_or_else(
            || ids.to_vec(),
            |rows| rows.iter().map(|&i| ids[i]).collect::<Vec<_>>(),
        );
        if selected_ids.is_empty() {
            return Ok(());
        }
        let selected_input = selected.as_ref().map(|rows| {
            ExpressionInput::new(input.columns(), input.num_rows()).with_selection(rows)
        });
        let values = self
            .arguments
            .evaluate_arrays(selected_input.as_ref().unwrap_or(input))?;
        self.accumulator.update(&values, &selected_ids)
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
        array::{BooleanArray, Int64Array},
        datatypes::{Field, Schema},
    };

    #[test]
    fn filtered_arguments_and_constants_follow_original_group_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int64, true),
            Field::new("paid", DataType::Boolean, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(40)])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
        ];
        let rows = [3, 0, 3, 2, 1];
        let input = ExpressionInput::new(&columns, 4).with_selection(&rows);
        let ids = [0, 1, 0, 1];
        for (arguments, expected) in [
            (
                vec![ReferenceExpression::new(0).into_ref()],
                vec![Some(10), Some(80)],
            ),
            (
                vec![ConstantExpression::int64(Some(3)).into_ref()],
                vec![Some(6), Some(6)],
            ),
        ] {
            let expression = Arc::new(
                AggregateExpression::new(AggregateFunction::Sum, arguments)
                    .with_filter(ReferenceExpression::new(1).into_ref()),
            );
            let mut executor =
                AggregateExpressionExecutor::try_new(expression, schema.clone()).unwrap();
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
        let expression = Arc::new(AggregateExpression::new(AggregateFunction::Count, vec![]));
        let mut executor = AggregateExpressionExecutor::try_new(expression, schema).unwrap();
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
