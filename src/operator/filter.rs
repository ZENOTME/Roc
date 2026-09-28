//! Filter operator descriptor.
use super::Operator;
use crate::PhysicalExprRef;
use crate::{
    Error, Result,
    exec::FilterExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};

#[derive(Clone, Debug)]
pub struct FilterOperator {
    /// Bound to the child output. Filtering preserves the input batch schema.
    predicate: PhysicalExprRef,
}

impl FilterOperator {
    pub fn new(predicate: PhysicalExprRef) -> Self {
        Self { predicate }
    }
}

impl Operator for FilterOperator {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::Plan("filter operator requires one child".into()));
        };
        graph
            .pipeline_mut(current)?
            .add_processor(Box::new(FilterExec::new(self.predicate.clone())));
        build_pipeline_on_node(child, current, graph)
    }
}
