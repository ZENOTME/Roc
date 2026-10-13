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
    column::{ColumnContext, ColumnId},
    error::{Error, Result},
    exec::{SinkExec, SourceExec},
    operator::{Operator, OperatorTree, OperatorTreeNode},
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_graph, build_pipeline_on_node},
};

struct NoopExec;

impl SourceExec for NoopExec {
    fn next_batch<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<arrow::record_batch::RecordBatch>>> {
        Box::pin(async {
            Err(Error::Execution(
                "not executed in graph construction tests".into(),
            ))
        })
    }
}
impl SinkExec for NoopExec {}

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
    fn output_columns(&self) -> Vec<ColumnId> {
        vec![]
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        assert!(current_node.children().is_empty());
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(NoopExec), &[])
    }
}
impl Operator for CustomBarrier {
    fn name(&self) -> &'static str {
        "custom barrier"
    }
    fn output_columns(&self) -> Vec<ColumnId> {
        vec![]
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        assert_eq!(current_node.children().len(), 1);
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(NoopExec), &[])?;
        let producer = graph.new_dependency(current)?;
        graph
            .pipeline_mut(producer)?
            .set_sink(Box::new(NoopExec), None)?;
        build_pipeline_on_node(&current_node.children()[0], producer, graph)
    }
}
impl Operator for CustomSink {
    fn name(&self) -> &'static str {
        "custom sink"
    }
    fn output_columns(&self) -> Vec<ColumnId> {
        vec![]
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        assert_eq!(current_node.children().len(), 1);
        graph
            .pipeline_mut(current)?
            .set_sink(Box::new(NoopExec), None)?;
        build_pipeline_on_node(&current_node.children()[0], current, graph)
    }
}

impl Operator for CustomTwoInput {
    fn name(&self) -> &'static str {
        "custom two input"
    }
    fn output_columns(&self) -> Vec<ColumnId> {
        vec![]
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(NoopExec), &[])?;
        for child in current_node.children() {
            let input = graph.new_dependency(current)?;
            graph
                .pipeline_mut(input)?
                .set_sink(Box::new(NoopExec), None)?;
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
    let graph = build_pipeline_graph(&OperatorTree::new(ColumnContext::new(), sink)).unwrap();
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
    let graph = build_pipeline_graph(&OperatorTree::new(ColumnContext::new(), sink)).unwrap();
    assert_eq!(graph.len(), 3);
    assert_eq!(graph.dependencies(0), Some([].as_slice()));
    assert_eq!(graph.dependencies(1), Some([].as_slice()));
    assert_eq!(graph.dependencies(2), Some([1, 0].as_slice()));
}

#[test]
fn invalid_project_reports_operator_and_reason() {
    use arrow::datatypes::DataType;
    use roc::column::ColumnType;
    use roc::expr::scalar::ReferenceExpression;
    use roc::operator::ProjectOperator;

    for child_count in [0, 2] {
        let mut columns = ColumnContext::new();
        let customer_id =
            columns.allocate_named(ColumnType::new(DataType::Int64, false), "customer_id");
        let project = OperatorTreeNode::new(
            ProjectOperator::new(vec![ReferenceExpression::new(customer_id).into_ref()]),
            (0..child_count)
                .map(|_| OperatorTreeNode::new(CustomSource, vec![]))
                .collect(),
        );
        let barrier = OperatorTreeNode::new(CustomBarrier, vec![project]);
        let sink = OperatorTreeNode::new(CustomSink, vec![barrier]);

        let error = build_pipeline_graph(&OperatorTree::new(columns, sink)).unwrap_err();
        let Error::InvalidPlan(message) = error else {
            panic!("expected InvalidPlan, got {error:?}");
        };
        assert_eq!(
            message,
            format!("project operator requires exactly 1 child, got {child_count}"),
        );
    }
}
