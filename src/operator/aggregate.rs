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
use crate::expr::agg::AggregateExpression;
use crate::{
    error::{Error, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use std::sync::Arc;

/// Fully specified grouping and aggregate descriptions, without execution state.
#[derive(Clone, Debug)]
pub struct AggregateOperator {
    groups: Projection,
    aggregates: Vec<Arc<AggregateExpression>>,
}
impl AggregateOperator {
    pub fn try_new(groups: Projection, aggregates: Vec<Arc<AggregateExpression>>) -> Result<Self> {
        if aggregates.is_empty() && groups.expressions().is_empty() {
            return Err(Error::invalid_plan(
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
            return Err(Error::invalid_plan(format!(
                "aggregate operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        let (sink, source) = self.clone().into_execs();
        graph.pipeline_mut(current)?.set_source(Box::new(source))?;
        let input = graph.new_dependency(current)?;
        graph.pipeline_mut(input)?.set_sink(Box::new(sink))?;
        build_pipeline_on_node(child, input, graph)
    }
}
