//! Projection operator descriptor.
use super::Operator;
use crate::{
    Error, Result,
    exec::ProjectExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use datafusion_physical_expr::projection::Projector;

#[derive(Clone, Debug)]
pub struct ProjectOperator {
    /// Prepared by the host against the child's output schema.
    projector: Projector,
}

impl ProjectOperator {
    pub fn new(projector: Projector) -> Self {
        Self { projector }
    }
}

impl Operator for ProjectOperator {
    fn name(&self) -> &'static str {
        "project"
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::Plan("project operator requires one child".into()));
        };
        graph
            .pipeline_mut(current)?
            .add_processor(Box::new(ProjectExec::new(self.projector.clone())));
        build_pipeline_on_node(child, current, graph)
    }
}
