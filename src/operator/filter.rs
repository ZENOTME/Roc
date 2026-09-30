//! Filter operator descriptor.
use super::Operator;
use crate::expr::scalar::ScalarExprRef;
use crate::{
    error::{Error, Result},
    exec::FilterExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};

#[derive(Clone, Debug)]
pub struct FilterOperator {
    /// Bound to the child output. Filtering preserves the input batch schema.
    predicate: ScalarExprRef,
}

impl FilterOperator {
    pub fn new(predicate: ScalarExprRef) -> Self {
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
            return Err(Error::InvalidPlan(format!(
                "filter operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        graph
            .pipeline_mut(current)?
            .add_processor(Box::new(FilterExec::new(self.predicate.clone())));
        build_pipeline_on_node(child, current, graph)
    }
}
