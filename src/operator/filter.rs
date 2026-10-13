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
use crate::arrow::datatypes::DataType;
use crate::column::ColumnId;
use crate::expr::scalar::ScalarExprRef;
use crate::kernel::KernelBuilder;
use crate::{
    error::{Error, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};

/// Keeps rows whose predicate is TRUE. Downstream kernels of the pipeline run
/// in the row domain of the kept rows.
#[derive(Clone, Debug)]
pub struct FilterOperator {
    predicate: ScalarExprRef,
    outputs: Vec<ColumnId>,
}

impl FilterOperator {
    /// `outputs` lists the child columns visible to the parent.
    pub fn new(predicate: ScalarExprRef, outputs: Vec<ColumnId>) -> Self {
        Self { predicate, outputs }
    }

    pub fn predicate(&self) -> &ScalarExprRef {
        &self.predicate
    }

    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<()> {
        let column = self.predicate.output();
        let ty = builder.columns().column_type(column)?;
        if ty.data_type != DataType::Boolean {
            return Err(Error::InvalidPlan(format!(
                "filter predicate must be Boolean, got {}",
                ty.data_type
            )));
        }
        let errors = self.predicate.build_kernels(builder)?;
        builder.raise(errors)?;
        let binding = builder.column_binding(column)?;
        let predicate = builder.predicate(&binding)?;
        let domain = builder.filter(predicate)?;
        builder.set_current_domain(domain)
    }
}

impl Operator for FilterOperator {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        self.outputs.clone()
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::InvalidPlan(format!(
                "filter operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        build_pipeline_on_node(child, current, graph)?;
        self.build_kernels(&mut graph.pipeline_mut(current)?.kernels)
    }
}
