//! Lowers a bound operator tree into a pipeline graph.
use super::{PipelineGraph, PipelineGraphBuilder, PipelineId};
use crate::{
    Result,
    operator::{OperatorTree, OperatorTreeNode},
};

/// Builds a tree whose operators already hold any runtime services they need.
pub fn build_pipeline_graph(tree: OperatorTree) -> Result<PipelineGraph> {
    let mut graph = PipelineGraphBuilder::new();
    build_pipeline_node(tree.root(), 0, &mut graph)?;
    graph.finish()
}

/// Asks an operator to place itself and its children into the pipeline graph.
pub fn build_pipeline_node(
    node: &OperatorTreeNode,
    current: PipelineId,
    graph: &mut PipelineGraphBuilder,
) -> Result<()> {
    node.operator().build_pipeline(node, current, graph)
}
