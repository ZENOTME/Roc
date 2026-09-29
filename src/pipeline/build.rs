use super::{PipelineGraph, PipelineGraphBuilder, PipelineId};
use crate::{
    error::Result,
    operator::{OperatorTree, OperatorTreeNode},
};

/// Builds pipeline graph based on operator tree.
pub fn build_pipeline_graph(tree: OperatorTree) -> Result<PipelineGraph> {
    let mut graph = PipelineGraphBuilder::new();
    build_pipeline_on_node(tree.root(), 0, &mut graph)?;
    graph.finish()
}

/// Builds pipelines for the current operator tree node.
pub fn build_pipeline_on_node(
    current_node: &OperatorTreeNode,
    current: PipelineId,
    graph: &mut PipelineGraphBuilder,
) -> Result<()> {
    current_node
        .operator()
        .build_pipeline(current_node, current, graph)
}
