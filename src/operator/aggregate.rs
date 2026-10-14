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
///
/// With grouping sets, rows are grouped once per set of group expressions:
/// the group columns outside a set are NULL in its rows, and a grouping ID
/// column after the group columns has bit `n - 1 - i` set when expression
/// `i` of `n` is outside the set.
#[derive(Clone, Debug)]
pub struct AggregateOperator {
    groups: Vec<ScalarExprRef>,
    aggregates: Vec<Arc<AggregateExpression>>,
    grouping: Option<(Vec<Vec<usize>>, ColumnId)>,
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
        Ok(Self {
            groups,
            aggregates,
            grouping: None,
        })
    }

    /// Groups by each set of group expression indexes; `id` receives the
    /// grouping ID, of an unsigned integer type.
    pub fn with_grouping_sets(mut self, sets: Vec<Vec<usize>>, id: ColumnId) -> Result<Self> {
        if sets.is_empty() || sets.iter().flatten().any(|&g| g >= self.groups.len()) {
            return Err(Error::invalid_plan(
                "grouping sets list group expressions".into(),
            ));
        }
        if self.groups.len() > 64 {
            return Err(Error::unsupported("at most 64 grouping expressions".into()));
        }
        self.grouping = Some((sets, id));
        Ok(self)
    }

    /// The sets of group expression indexes rows are grouped by.
    pub fn sets(&self) -> Vec<Vec<usize>> {
        match &self.grouping {
            Some((sets, _)) => sets.clone(),
            None => vec![(0..self.groups.len()).collect()],
        }
    }

    pub fn grouping_id(&self) -> Option<ColumnId> {
        self.grouping.as_ref().map(|g| g.1)
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
        Ok(self.build_set_kernels(builder)?.remove(0))
    }

    /// The states of `build_kernels` for each grouping set.
    pub fn build_set_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<Vec<Vec<StateId>>> {
        let mut keys = Vec::with_capacity(self.groups.len());
        for (index, group) in self.groups.iter().enumerate() {
            let key = group
                .build_kernels(builder)
                .and_then(|errors| builder.raise(errors))
                .and_then(|()| builder.column_binding(group.output()))
                .with_context(|| ErrorContext::new("aggregate.bind_group").field("group", index))?;
            keys.push(key);
        }
        let columns = self.group_columns();
        let mut sets = vec![];
        for set in self.sets() {
            let set_columns = set.iter().map(|&g| columns[g]).collect::<Vec<_>>();
            let schema = builder.columns().schema(&set_columns)?;
            let grouping = builder.state(StateBuilder::new(move || {
                Ok(GroupState {
                    index: GroupIndex::new(&schema)?,
                    schema: schema.clone(),
                })
            }));
            let set_keys = set.iter().map(|&g| keys[g]).collect();
            let ids = builder.group_lookup(grouping, set_keys)?;
            let mut states = vec![grouping];
            for (index, aggregate) in self.aggregates.iter().enumerate() {
                let state = aggregate
                    .build_kernels(builder, ids, set.is_empty())
                    .with_context(|| {
                        ErrorContext::new("aggregate.bind").field("aggregate", index)
                    })?;
                states.push(state);
            }
            sets.push(states);
        }
        Ok(sets)
    }
}

impl Operator for AggregateOperator {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        let mut columns = self.group_columns();
        columns.extend(self.grouping_id());
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
        let locals = self.build_set_kernels(&mut graph.pipeline_mut(input)?.kernels)?;
        let (source, sink) = self.build_exec(state, locals);
        graph.pipeline_mut(input)?.set_sink(Box::new(sink), None)?;
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(source), &self.output_columns())
    }
}
