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
use crate::{
    error::{Error, Result},
    expr::agg::{AggregateExpression, AggregateFunction},
};
use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
};
use std::sync::Arc;

/// Typed worker-local aggregate state. Input expressions are separate Programs.
pub struct AggregateAccumulator {
    expression: Arc<AggregateExpression>,
    accumulator: Accumulator,
}

impl AggregateAccumulator {
    pub fn try_new(expression: Arc<AggregateExpression>) -> Result<Self> {
        let input_type = expression
            .argument()
            .map(|argument| argument.result_type().data_type());
        // No accumulator implements it, so accepting it would silently drop DISTINCT.
        if expression.is_distinct() && expression.function() != AggregateFunction::Count {
            return Err(Error::InvalidPlan(
                "initial aggregate implementation supports DISTINCT only for COUNT(expr)".into(),
            ));
        }
        let accumulator = Accumulator::new(
            expression.function(),
            expression.is_distinct(),
            input_type,
            expression.result_type().data_type(),
        )?;
        Ok(Self {
            expression,
            accumulator,
        })
    }
    pub fn state_types(&self) -> Vec<DataType> {
        self.accumulator.state_types()
    }
    pub fn resize(&mut self, count: usize) {
        self.accumulator.resize(count);
    }
    pub fn bind_global_count(&mut self) {
        self.accumulator.bind_global_count();
    }
    pub fn bind_global_sum(&mut self) {
        self.accumulator.bind_global_sum();
    }
    pub fn update(&mut self,value:Option<&ArrayRef>,ids:&[usize],groups:usize)->Result<()>{
        if ids.iter().any(|&id|id>=groups)||value.is_some_and(|v|v.len()!=ids.len()){
            return Err(Error::Execution("aggregate group IDs do not match input".into()));
        }
        self.resize(groups);self.accumulator.update(value,ids)
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

