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
