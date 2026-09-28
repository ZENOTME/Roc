//! Projection operator descriptor.
use super::Operator;
use crate::expr::NamedExpr;
use crate::{
    Error, Result,
    exec::ProjectExec,
    expr,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_node},
};
use arrow::datatypes::SchemaRef;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProjectOperator {
    pub expressions: Vec<NamedExpr>,
    pub input_schema: SchemaRef,
}

impl Operator for ProjectOperator {
    fn name(&self) -> &'static str {
        "project"
    }

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = node.children() else {
            return Err(Error::Plan("project operator requires one child".into()));
        };
        let schema = expr::project_schema(&self.input_schema, &self.expressions)?;
        graph
            .pipeline_mut(current)?
            .add_processor(Box::new(ProjectExec::new(self.clone(), schema)));
        build_pipeline_node(child, current, graph)
    }
}
