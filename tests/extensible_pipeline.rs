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

use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use roc::{
    error::{Error, Result},
    exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SourceExec, SourceExecutor},
    operator::{Operator, OperatorTree, OperatorTreeNode},
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_graph, build_pipeline_on_node},
};
use std::sync::Arc;

struct NoopExec;

impl SourceExec for NoopExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        Err(Error::internal(
            "this test only constructs the graph".into(),
        ))
    }

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl SinkExec for NoopExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }

    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        Err(Error::internal(
            "this test only constructs the graph".into(),
        ))
    }
    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct CustomSource;
#[derive(Debug)]
struct CustomBarrier;
#[derive(Debug)]
struct CustomSink;
#[derive(Debug)]
struct CustomTwoInput;

impl Operator for CustomSource {
    fn name(&self) -> &'static str {
        "custom source"
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        assert!(current_node.children().is_empty());
        graph.pipeline_mut(current)?.set_source(Box::new(NoopExec))
    }
}
impl Operator for CustomBarrier {
    fn name(&self) -> &'static str {
        "custom barrier"
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        assert_eq!(current_node.children().len(), 1);
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(NoopExec))?;
        let producer = graph.new_dependency(current)?;
        graph.pipeline_mut(producer)?.set_sink(Box::new(NoopExec))?;
        build_pipeline_on_node(&current_node.children()[0], producer, graph)
    }
}
impl Operator for CustomSink {
    fn name(&self) -> &'static str {
        "custom sink"
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        assert_eq!(current_node.children().len(), 1);
        graph.pipeline_mut(current)?.set_sink(Box::new(NoopExec))?;
        build_pipeline_on_node(&current_node.children()[0], current, graph)
    }
}

impl Operator for CustomTwoInput {
    fn name(&self) -> &'static str {
        "custom two input"
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(NoopExec))?;
        for child in current_node.children() {
            let input = graph.new_dependency(current)?;
            graph.pipeline_mut(input)?.set_sink(Box::new(NoopExec))?;
            build_pipeline_on_node(child, input, graph)?;
        }
        Ok(())
    }
}

#[test]
fn host_operator_builds_a_dependency_without_core_dispatch() {
    let source = OperatorTreeNode::new(CustomSource, vec![]);
    let barrier = OperatorTreeNode::new(CustomBarrier, vec![source]);
    let sink = OperatorTreeNode::new(CustomSink, vec![barrier]);
    let graph = build_pipeline_graph(OperatorTree::new(sink)).unwrap();
    assert_eq!(graph.len(), 2);
    assert_eq!(graph.dependencies(0), Some([].as_slice()));
    assert_eq!(graph.dependencies(1), Some([0].as_slice()));
}

#[test]
fn two_input_operator_chooses_both_child_pipelines() {
    let tree = OperatorTreeNode::new(
        CustomTwoInput,
        vec![
            OperatorTreeNode::new(CustomSource, vec![]),
            OperatorTreeNode::new(CustomSource, vec![]),
        ],
    );
    let sink = OperatorTreeNode::new(CustomSink, vec![tree]);
    let graph = build_pipeline_graph(OperatorTree::new(sink)).unwrap();
    assert_eq!(graph.len(), 3);
    assert_eq!(graph.dependencies(0), Some([].as_slice()));
    assert_eq!(graph.dependencies(1), Some([].as_slice()));
    assert_eq!(graph.dependencies(2), Some([1, 0].as_slice()));
}

#[test]
fn invalid_project_reports_operator_and_reason() {
    use arrow::datatypes::{DataType, Field, Schema};
    use roc::operator::{ProjectOperator, Projection};

    for child_count in [0, 2] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "customer_id",
            DataType::Int64,
            false,
        )]));
        let project = OperatorTreeNode::new(
            ProjectOperator::new(Projection::from_indices(schema, &[0]).unwrap()),
            (0..child_count)
                .map(|_| OperatorTreeNode::new(CustomSource, vec![]))
                .collect(),
        );
        let barrier = OperatorTreeNode::new(CustomBarrier, vec![project]);
        let sink = OperatorTreeNode::new(CustomSink, vec![barrier]);

        let error = build_pipeline_graph(OperatorTree::new(sink)).unwrap_err();
        assert_eq!(error.kind(), roc::error::ErrorKind::InvalidPlan);
        assert_eq!(
            error.message(),
            format!("project operator requires exactly 1 child, got {child_count}"),
        );
    }
}
