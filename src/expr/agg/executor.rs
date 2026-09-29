use super::accumulator::Accumulator;
use crate::expr::scalar::executor::{
    ExpressionExecutor, ExpressionResult, is_number, require_boolean, select_batch,
};
use crate::{
    error::{Error, Result},
    expr::agg::{AggregateFunction, BoundAggregateExpression},
};
use arrow::{
    array::{Array, ArrayRef},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Worker-local argument evaluators, result metadata, and aggregate state.
pub struct AggregateExpressionExecutor {
    expression: Arc<BoundAggregateExpression>,
    arguments: ExpressionExecutor,
    filter: Option<ExpressionExecutor>,
    result: ExpressionResult,
    accumulator: Accumulator,
}
impl AggregateExpressionExecutor {
    pub fn try_new(
        expression: Arc<BoundAggregateExpression>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
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
    pub fn update(&mut self, input: &RecordBatch, ids: &[usize], groups: usize) -> Result<()> {
        self.resize(groups);
        if ids.len() != input.num_rows() {
            return Err(Error::Execution(
                "aggregate group IDs do not match input".into(),
            ));
        }
        let (batch, selected_ids) = if let Some(filter) = &mut self.filter {
            let mask = filter.select(input)?;
            let selected: Vec<_> = (0..mask.len()).filter(|i| mask.value(*i)).collect();
            let selected_ids = selected.iter().map(|i| ids[*i]).collect::<Vec<_>>();
            (select_batch(input, &selected)?, selected_ids)
        } else {
            (input.clone(), ids.to_vec())
        };
        if selected_ids.is_empty() {
            return Ok(());
        }
        let values = self
            .arguments
            .evaluate(&batch)?
            .into_iter()
            .map(|v| v.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        self.accumulator.update(&values, &selected_ids)
    }
    pub fn merge(&mut self, state: &[ArrayRef], ids: &[usize], groups: usize) -> Result<()> {
        self.resize(groups);
        let types = self.state_types();
        if state.len() != types.len()
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
