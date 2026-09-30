use super::Operator;
use super::Projection;
use crate::{
    error::{Error, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use crate::{
    exec::ProjectionExecutor,
    expr::agg::{AggregateExpression, executor::AggregateExpressionExecutor},
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
            return Err(Error::InvalidPlan(
                "aggregate needs a group or function".into(),
            ));
        }
        let operator = Self { groups, aggregates };
        operator.output_schema()?;
        Ok(operator)
    }
    pub fn groups(&self) -> &Projection {
        &self.groups
    }
    pub fn aggregates(&self) -> &[Arc<AggregateExpression>] {
        &self.aggregates
    }
    /// Obtain result metadata from executor initialization, including on empty inputs.
    pub fn output_schema(&self) -> Result<SchemaRef> {
        let groups = ProjectionExecutor::try_new(self.groups.clone())?;
        let mut fields = groups.output_schema().fields().to_vec();
        for aggregate in &self.aggregates {
            let executor = AggregateExpressionExecutor::try_new(
                aggregate.clone(),
                self.groups.input_schema().clone(),
            )?;
            let result = executor.result();
            fields.push(Arc::new(Field::new(
                aggregate.output_name(),
                result.data_type.clone(),
                result.nullable,
            )));
        }
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            groups.output_schema().metadata().clone(),
        )))
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
        let (sink, source) = self.clone().into_execs();
        graph.pipeline_mut(current)?.set_source(Box::new(source))?;
        let input = graph.new_dependency(current)?;
        graph.pipeline_mut(input)?.set_sink(Box::new(sink))?;
        build_pipeline_on_node(child, input, graph)
    }
}
