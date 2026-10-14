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

use super::Operator;
use crate::column::ColumnId;
use crate::exec::aggregate::group_index::GroupIndex;
use crate::expr::agg::AggregateExpression;
use crate::expr::scalar::ScalarExprRef;
use crate::kernel::{KernelBuilder, StateBuilder, StateId, aggregation::GroupState};
use crate::{
    error::{Error, ErrorContext, Result, ResultExt},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use std::sync::Arc;

/// Groups rows by the group expressions and computes aggregates per group.
/// Outputs the group columns followed by the aggregate columns.
#[derive(Clone, Debug)]
pub struct AggregateOperator {
    groups: Vec<ScalarExprRef>,
    aggregates: Vec<Arc<AggregateExpression>>,
}

impl AggregateOperator {
    pub fn try_new(
        groups: Vec<ScalarExprRef>,
        aggregates: Vec<Arc<AggregateExpression>>,
    ) -> Result<Self> {
        if aggregates.is_empty() && groups.is_empty() {
            return Err(Error::invalid_plan(
                "aggregate needs a group or function".into(),
            ));
        }
        Ok(Self { groups, aggregates })
    }

    pub fn groups(&self) -> &[ScalarExprRef] {
        &self.groups
    }

    pub fn aggregates(&self) -> &[Arc<AggregateExpression>] {
        &self.aggregates
    }

    pub fn group_columns(&self) -> Vec<ColumnId> {
        self.groups.iter().map(|g| g.output()).collect()
    }

    /// Builds the group lookup and one update per aggregate into the current
    /// domain. Returns the grouping state followed by the accumulator states,
    /// in the order the sink merges them.
    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<Vec<StateId>> {
        let schema = builder.columns().schema(&self.group_columns())?;
        let grouping = builder.state(StateBuilder::new(move || {
            Ok(GroupState {
                index: GroupIndex::new(&schema)?,
                schema: schema.clone(),
            })
        }));
        let mut keys = Vec::with_capacity(self.groups.len());
        for (index, group) in self.groups.iter().enumerate() {
            let key = group
                .build_kernels(builder)
                .and_then(|errors| builder.raise(errors))
                .and_then(|()| builder.column_binding(group.output()))
                .with_context(|| ErrorContext::new("aggregate.bind_group").field("group", index))?;
            keys.push(key);
        }
        let ids = builder.group_lookup(grouping, keys)?;
        let mut states = vec![grouping];
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            let state = aggregate
                .build_kernels(builder, ids, self.groups.is_empty())
                .with_context(|| ErrorContext::new("aggregate.bind").field("aggregate", index))?;
            states.push(state);
        }
        Ok(states)
    }
}

impl Operator for AggregateOperator {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        let mut columns = self.group_columns();
        columns.extend(self.aggregates.iter().map(|a| a.output()));
        columns
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::invalid_plan(format!(
                "aggregate operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        let input = graph.new_dependency(current)?;
        build_pipeline_on_node(child, input, graph)?;
        let state = self.new_execute_state(graph.columns())?;
        let locals = self.build_kernels(&mut graph.pipeline_mut(input)?.kernels)?;
        let (source, sink) = self.build_exec(state, locals);
        graph.pipeline_mut(input)?.set_sink(Box::new(sink), None)?;
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(source), &self.output_columns())
    }
}
