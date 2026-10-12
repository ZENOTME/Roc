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
use super::Projection;
use crate::arrow::datatypes::{Field, Schema, SchemaRef};
use crate::exec::aggregate::group_index::GroupIndex;
use crate::expr::agg::AggregateExpression;
use crate::suboperator::{StateBuilder, StateId, SuboperatorScopeBuilder, aggregation::GroupState};
use crate::{
    error::{Error, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use std::sync::Arc;

/// Fully specified grouping and aggregate descriptions, without execution state.
#[derive(Clone, Debug)]
pub struct AggregateOperator {
    groups: Projection,
    aggregates: Vec<Arc<AggregateExpression>>,
}
impl AggregateOperator {
    /// This operator chooses its grouping state and each aggregate chooses its
    /// accumulator state. The returned ordinary IDs are also used by sink.combine.
    pub fn build_suboperator(&self, builder: &mut SuboperatorScopeBuilder) -> Result<Vec<StateId>> {
        let schema = self.groups.output_schema();
        let group_schema = schema.clone();
        let grouping = builder.state(StateBuilder::new(move || {
            Ok(GroupState {
                index: GroupIndex::new(&group_schema)?,
                schema: group_schema.clone(),
            })
        }));
        let keys = self
            .groups
            .expressions()
            .iter()
            .map(|e| e.expression().build_suboperator(builder))
            .collect::<Result<Vec<_>>>()?;
        let ids = builder.group_lookup(grouping, keys)?;
        // Grouping also produces output when no accumulator is present (DISTINCT).
        builder.require(ids);
        let mut states = vec![grouping];
        for expression in &self.aggregates {
            states.push(expression.build_suboperator(
                builder,
                grouping,
                ids,
                schema.fields().is_empty(),
            )?);
        }
        // The values are consumed into states; do not materialize an output batch.
        builder.clear_columns();
        Ok(states)
    }

    pub fn try_new(groups: Projection, aggregates: Vec<Arc<AggregateExpression>>) -> Result<Self> {
        if aggregates.is_empty() && groups.expressions().is_empty() {
            return Err(Error::InvalidPlan(
                "aggregate needs a group or function".into(),
            ));
        }
        Ok(Self { groups, aggregates })
    }
    pub fn groups(&self) -> &Projection {
        &self.groups
    }
    pub fn aggregates(&self) -> &[Arc<AggregateExpression>] {
        &self.aggregates
    }
    /// Result types come from the descriptions; no executor is built.
    pub fn output_schema(&self) -> SchemaRef {
        let groups = self.groups.output_schema();
        let mut fields = groups.fields().to_vec();
        for aggregate in &self.aggregates {
            let result_type = aggregate.result_type();
            fields.push(Arc::new(Field::new(
                aggregate.output_name(),
                result_type.data_type().clone(),
                result_type.is_nullable(),
            )));
        }
        Arc::new(Schema::new_with_metadata(fields, groups.metadata().clone()))
    }
}

impl Operator for AggregateOperator {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::InvalidPlan(format!(
                "aggregate operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        let input = graph.new_dependency(current)?;
        build_pipeline_on_node(child, input, graph)?;
        let state = self.new_execute_state()?;
        let locals = self.build_suboperator(&mut graph.pipeline_mut(input)?.suboperators)?;
        let (source, sink) = self.build_exec(state, locals);
        graph.pipeline_mut(input)?.set_sink(Box::new(sink))?;
        graph.pipeline_mut(current)?.set_source(Box::new(source))
    }
}
