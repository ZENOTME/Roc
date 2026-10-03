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
    /// Built from the declared types. Filtering preserves the input batch schema.
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
