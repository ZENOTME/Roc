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

use super::Operator;
use crate::column::ColumnId;
use crate::expr::scalar::ScalarExprRef;
use crate::kernel::KernelBuilder;
use crate::{
    error::{Error, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};

/// Computes one output column per expression; output names come from the
/// column context.
#[derive(Clone, Debug)]
pub struct ProjectOperator {
    expressions: Vec<ScalarExprRef>,
}

impl ProjectOperator {
    pub fn new(expressions: Vec<ScalarExprRef>) -> Self {
        Self { expressions }
    }

    pub fn expressions(&self) -> &[ScalarExprRef] {
        &self.expressions
    }

    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<()> {
        for expression in &self.expressions {
            let errors = expression.build_kernels(builder)?;
            builder.raise(errors)?;
        }
        Ok(())
    }
}

impl Operator for ProjectOperator {
    fn name(&self) -> &'static str {
        "project"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        self.expressions.iter().map(|e| e.output()).collect()
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::InvalidPlan(format!(
                "project operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        build_pipeline_on_node(child, current, graph)?;
        self.build_kernels(&mut graph.pipeline_mut(current)?.kernels)
    }
}
