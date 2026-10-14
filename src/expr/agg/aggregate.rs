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
use crate::column::{ColumnContext, ColumnId};
use crate::expr::scalar::{CastMode, ScalarExprRef};
use crate::kernel::{AggregateUpdateKind, KernelBuilder, StateBuilder, StateId, VariableId};
use crate::{
    error::{Error, Result},
    kernel::aggregation::AccumulatorState,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CovarPop,
    /// The sample standard deviation of Float64 values; NULL below two.
    StddevSamp,
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
            Self::StddevSamp => "stddev_samp",
        }
    }
}

/// An aggregate call such as COUNT, SUM, or MIN, with optional DISTINCT and
/// FILTER. COUNT without arguments is COUNT(*). The result type is the type of
/// the output column.
#[derive(Clone, Debug)]
pub struct AggregateExpression {
    function: AggregateFunction,
    arguments: Vec<ScalarExprRef>,
    output: ColumnId,
    distinct: bool,
    filter: Option<ScalarExprRef>,
}

impl AggregateExpression {
    pub fn new(
        function: AggregateFunction,
        arguments: Vec<ScalarExprRef>,
        output: ColumnId,
    ) -> Self {
        Self {
            function,
            arguments,
            output,
            distinct: false,
            filter: None,
        }
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
    pub fn output(&self) -> ColumnId {
        self.output
    }
    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
    pub fn filter(&self) -> Option<&ScalarExprRef> {
        self.filter.as_ref()
    }

    /// The type every argument is converted to before it reaches the
    /// accumulator, if any. Runtime state never interprets source types.
    fn coercion(&self, columns: &ColumnContext) -> Result<Option<DataType>> {
        let output = &columns.column_type(self.output)?.data_type;
        Ok(match self.function {
            AggregateFunction::Sum
                if matches!(
                    output,
                    DataType::Int64
                        | DataType::UInt64
                        | DataType::Float64
                        | DataType::Decimal128(..)
                ) =>
            {
                Some(output.clone())
            }
            // A decimal average keeps its input's scale.
            AggregateFunction::Avg if matches!(output, DataType::Decimal128(..)) => None,
            AggregateFunction::Avg
            | AggregateFunction::CovarPop
            | AggregateFunction::StddevSamp => Some(DataType::Float64),
            _ => None,
        })
    }

    /// Accumulator input types after coercion.
    pub(crate) fn input_types(&self, columns: &ColumnContext) -> Result<Vec<DataType>> {
        let target = self.coercion(columns)?;
        self.arguments
            .iter()
            .map(|a| match &target {
                Some(t) => Ok(t.clone()),
                None => Ok(columns.column_type(a.output())?.data_type.clone()),
            })
            .collect()
    }

    /// A fresh accumulator for this aggregate.
    pub(crate) fn accumulator(&self, columns: &ColumnContext) -> Result<Accumulator> {
        if self.distinct && self.function != AggregateFunction::Count {
            return Err(Error::unsupported(
                "DISTINCT is supported only for COUNT".into(),
            ));
        }
        Accumulator::new(
            self.function,
            self.distinct,
            &self.input_types(columns)?,
            &columns.column_type(self.output)?.data_type,
        )
    }

    /// Builds the arguments, the FILTER predicate and one accumulator update
    /// keyed by `group`, which belongs to the current domain. Returns the
    /// accumulator state.
    pub(crate) fn build_kernels(
        &self,
        builder: &mut KernelBuilder<'_>,
        group: VariableId,
        global: bool,
    ) -> Result<StateId> {
        let columns = builder.columns();
        // Validate at build time, including when the eventual input is empty.
        self.accumulator(columns)?;
        let (function, distinct) = (self.function, self.distinct);
        let types = self.input_types(columns)?;
        let ty = columns.column_type(self.output)?.data_type.clone();
        let target = self.coercion(columns)?;
        let mut arguments = Vec::with_capacity(self.arguments.len());
        let mut errors = vec![];
        for argument in &self.arguments {
            errors.extend(argument.build_kernels(builder)?);
            let binding = builder.column_binding(argument.output())?;
            let from = columns.column_type(argument.output())?.data_type.clone();
            let binding = match &target {
                Some(to) => {
                    let (binding, cast_errors) =
                        builder.cast_binding(&binding, &from, to, CastMode::Try)?;
                    errors.extend(cast_errors);
                    binding
                }
                None => binding,
            };
            arguments.push(binding);
        }
        let parent = builder.current_domain();
        if let Some(filter) = &self.filter {
            let ty = &columns.column_type(filter.output())?.data_type;
            if ty != &DataType::Boolean {
                return Err(Error::invalid_plan(format!(
                    "aggregate FILTER must be Boolean, got {ty}"
                )));
            }
            let filter_errors = filter.build_kernels(builder)?;
            builder.raise(filter_errors)?;
            let binding = builder.column_binding(filter.output())?;
            let predicate = builder.predicate(&binding)?;
            // Rows excluded by FILTER neither update nor raise argument
            // errors; the group itself was already found in the parent domain.
            let domain = builder.filter(predicate)?;
            builder.set_current_domain(domain)?;
        }
        let state = builder.state(StateBuilder::new(move || {
            let mut accumulator = Accumulator::new(function, distinct, &types, &ty)?;
            if global {
                accumulator.resize(1);
            }
            Ok(AccumulatorState { accumulator })
        }));
        let kind = match self.function {
            AggregateFunction::Count if self.distinct => AggregateUpdateKind::CountDistinct,
            AggregateFunction::Count if arguments.is_empty() => AggregateUpdateKind::CountRows,
            AggregateFunction::Count => AggregateUpdateKind::CountValues,
            AggregateFunction::Sum => match &columns.column_type(self.output)?.data_type {
                DataType::Int64 => AggregateUpdateKind::SumI64,
                DataType::UInt64 => AggregateUpdateKind::SumU64,
                DataType::Float64 => AggregateUpdateKind::SumF64,
                DataType::Decimal128(..) => AggregateUpdateKind::SumI128,
                _ => return Err(Error::internal("unsupported SUM output".into())),
            },
            AggregateFunction::Avg
                if matches!(
                    columns.column_type(self.output)?.data_type,
                    DataType::Decimal128(..)
                ) =>
            {
                AggregateUpdateKind::AvgI128
            }
            AggregateFunction::Avg => AggregateUpdateKind::Avg,
            AggregateFunction::StddevSamp => AggregateUpdateKind::Variance,
            AggregateFunction::CovarPop => AggregateUpdateKind::CovarPop,
            AggregateFunction::Min => AggregateUpdateKind::Min,
            AggregateFunction::Max => AggregateUpdateKind::Max,
        };
        builder.raise(errors)?;
        builder.aggregate_update(state, kind, group, arguments)?;
        builder.set_current_domain(parent)?;
        Ok(state)
    }
}
