//! Filter operator descriptor.
use super::Operator;
use crate::expr::Expr;
use crate::{
    Error, Result,
    exec::FilterExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_node},
};
use arrow::datatypes::{DataType, SchemaRef};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FilterOperator {
    pub predicate: Expr,
    pub input_schema: SchemaRef,
}

impl Operator for FilterOperator {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = node.children() else {
            return Err(Error::Plan("filter operator requires one child".into()));
        };
        if self.predicate.data_type(&self.input_schema)? != DataType::Boolean {
            return Err(Error::Plan("filter predicate must be Boolean".into()));
        }
        graph
            .pipeline_mut(current)?
            .add_processor(Box::new(FilterExec::new(self.clone())));
        build_pipeline_node(child, current, graph)
    }
}
