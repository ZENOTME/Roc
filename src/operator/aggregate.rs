use super::Operator;
use crate::{
    Error, Result,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_physical_expr::{aggregate::AggregateFunctionExpr, projection::Projector};
use std::sync::Arc;

/// Prepared DataFusion aggregates and grouping expressions.
#[derive(Clone, Debug)]
pub struct AggregateOperator {
    groups: Projector,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
}

impl AggregateOperator {
    /// Both the group projector and aggregate expressions must already be bound
    /// to the same input. Fields and partial states come directly from DataFusion.
    /// An empty group projector denotes global aggregation. Aggregates with an
    /// effective ORDER BY are unsupported because workers do not preserve order.
    pub fn try_new(groups: Projector, aggregates: Vec<Arc<AggregateFunctionExpr>>) -> Result<Self> {
        if aggregates.is_empty() && groups.output_schema().fields().is_empty() {
            return Err(Error::Plan("aggregate needs a group or function".into()));
        }
        for aggregate in &aggregates {
            if !aggregate.order_bys().is_empty() {
                return Err(Error::Plan(
                    "ordered aggregates require ordering support in the aggregate executor".into(),
                ));
            }
        }
        Ok(Self { groups, aggregates })
    }

    pub(crate) fn groups(&self) -> &Projector {
        &self.groups
    }

    pub(crate) fn aggregates(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggregates
    }

    pub fn output_schema(&self) -> SchemaRef {
        let mut fields = self.groups.output_schema().fields().to_vec();
        fields.extend(self.aggregates.iter().map(|aggregate| aggregate.field()));
        Arc::new(Schema::new_with_metadata(
            fields,
            self.groups.output_schema().metadata().clone(),
        ))
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
            return Err(Error::Plan("aggregate operator requires one child".into()));
        };
        let (sink, source) = self.clone().into_execs();
        graph.pipeline_mut(current)?.set_source(Box::new(source))?;
        let input = graph.new_dependency(current)?;
        graph.pipeline_mut(input)?.set_sink(Box::new(sink))?;
        build_pipeline_on_node(child, input, graph)
    }
}
