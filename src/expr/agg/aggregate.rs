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
use crate::arrow::datatypes::DataType;
use crate::expr::scalar::ScalarExprRef;
use crate::{
    error::{Error, Result},
    expr::{ExpressionResultType, scalar::ConstantExpression},
    suboperator::{
        StateBuilder, StateId, SuboperatorScopeBuilder, VariableId, aggregation::AccumulatorState,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CovarPop,
}

impl AggregateFunction {
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::CovarPop => "covar_pop",
        }
    }
}

/// An aggregate call such as COUNT, SUM, or MIN, with optional DISTINCT and
/// FILTER. COUNT without arguments is COUNT(*).
#[derive(Clone, Debug)]
pub struct AggregateExpression {
    alias: Option<String>,
    function: AggregateFunction,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
    distinct: bool,
    filter: Option<ScalarExprRef>,
}
impl AggregateExpression {
    /// Build argument/mask computation and a single accumulator update, connected
    /// to the grouping operator through its state handle and group-ID variable.
    pub(crate) fn build_suboperator(
        &self,
        builder: &mut SuboperatorScopeBuilder,
        grouping: StateId,
        group_ids: VariableId,
        global: bool,
    ) -> Result<StateId> {
        if self.distinct && self.function != AggregateFunction::Count {
            return Err(Error::InvalidPlan(
                "DISTINCT is supported only for COUNT".into(),
            ));
        }
        let mut arguments = self.arguments.clone();
        // COUNT(*) FILTER becomes COUNT(nullable_one), preserving groups even
        // when every row in a group is excluded by this aggregate's mask.
        if self.filter.is_some() && arguments.is_empty() {
            arguments.push(ConstantExpression::int64(Some(1)).into_ref());
        }
        // Numeric coercions are explicit scalar nodes, generated alongside the
        // aggregate update. Runtime state never interprets source Arrow types.
        let target = match self.function {
            AggregateFunction::Sum
                if matches!(
                    self.result_type.data_type(),
                    DataType::Int64 | DataType::UInt64 | DataType::Float64
                ) =>
            {
                Some(self.result_type.data_type().clone())
            }
            AggregateFunction::Avg | AggregateFunction::CovarPop => Some(DataType::Float64),
            _ => None,
        };
        if let Some(target) = target {
            arguments = arguments
                .into_iter()
                .map(|a| {
                    if a.result_type().data_type() == &target {
                        a
                    } else {
                        crate::expr::scalar::CastExpression::new(
                            a,
                            target.clone(),
                            crate::expr::scalar::CastMode::Try,
                            true,
                        )
                        .into_ref()
                    }
                })
                .collect();
        }
        let types = arguments
            .iter()
            .map(|e| e.result_type().data_type().clone())
            .collect::<Vec<_>>();
        let function = self.function;
        let distinct = self.distinct;
        let ty = self.result_type.data_type().clone();
        // Validate at build time, including when the eventual input is empty.
        Accumulator::new(function, distinct, &types, &ty)?;
        let accumulator = builder.state(StateBuilder::new(move || {
            let mut accumulator = Accumulator::new(function, distinct, &types, &ty)?;
            if global {
                accumulator.resize(1);
            }
            Ok(AccumulatorState { accumulator })
        }));
        let mask = self
            .filter
            .as_ref()
            .map(|e| e.build_suboperator(builder))
            .transpose()?;
        let mut inputs = vec![group_ids];
        for argument in arguments {
            let value = argument.build_suboperator(builder)?;
            let value = if let Some(mask) = mask {
                let null = builder.constant(crate::arrow::array::new_null_array(
                    argument.result_type().data_type(),
                    1,
                ))?;
                builder.case(vec![(mask, value)], null)?
            } else {
                value
            };
            inputs.push(value);
        }
        use crate::suboperator::AggregateKernel;
        let kernel = match self.function {
            AggregateFunction::Count if self.distinct => AggregateKernel::CountDistinct,
            AggregateFunction::Count if inputs.len() == 1 => AggregateKernel::CountRows,
            AggregateFunction::Count => AggregateKernel::CountValues,
            AggregateFunction::Sum => match self.result_type.data_type() {
                DataType::Int64 => AggregateKernel::SumI64,
                DataType::UInt64 => AggregateKernel::SumU64,
                DataType::Float64 => AggregateKernel::SumF64,
                _ => return Err(Error::InvalidPlan("unsupported SUM output".into())),
            },
            AggregateFunction::Avg => AggregateKernel::Avg,
            AggregateFunction::CovarPop => AggregateKernel::CovarPop,
            AggregateFunction::Min => AggregateKernel::Min,
            AggregateFunction::Max => AggregateKernel::Max,
        };
        builder.aggregate_update([grouping, accumulator], inputs, kernel)?;
        Ok(accumulator)
    }

    pub fn new(
        function: AggregateFunction,
        arguments: Vec<ScalarExprRef>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            alias: None,
            function,
            arguments,
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
            distinct: false,
            filter: None,
        }
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
    /// Output field name; aliases do not affect aggregate evaluation.
    pub fn output_name(&self) -> &str {
        self.alias().unwrap_or_else(|| self.function.name())
    }
    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }
    pub fn with_filter(mut self, filter: ScalarExprRef) -> Self {
        self.filter = Some(filter);
        self
    }
    pub fn function(&self) -> AggregateFunction {
        self.function
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
    pub fn filter(&self) -> Option<&ScalarExprRef> {
        self.filter.as_ref()
    }
}
