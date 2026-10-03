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

//! Projection operator descriptor.
use super::Operator;
use crate::expr::ExpressionResultType;
use crate::expr::scalar::{ReferenceExpression, ScalarExprRef};
use crate::{
    error::{Error, Result},
    exec::ProjectExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Debug)]
pub struct ProjectionExpression {
    expression: ScalarExprRef,
    name: String,
}

impl ProjectionExpression {
    pub fn new(expression: ScalarExprRef, name: impl Into<String>) -> Self {
        Self {
            expression,
            name: name.into(),
        }
    }
    pub fn expression(&self) -> &ScalarExprRef {
        &self.expression
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Fully specified projection. Expression descriptions carry their own result
/// metadata, so no input schema is retained; the host supplies output metadata.
#[derive(Clone, Debug)]
pub struct Projection {
    expressions: Vec<ProjectionExpression>,
    metadata: HashMap<String, String>,
}
impl Projection {
    pub fn new(expressions: Vec<ProjectionExpression>) -> Self {
        Self {
            expressions,
            metadata: HashMap::new(),
        }
    }
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
    pub fn from_indices(input_schema: SchemaRef, indices: &[usize]) -> Result<Self> {
        let expressions = indices
            .iter()
            .map(|&index| {
                let field = input_schema.fields().get(index).ok_or_else(|| {
                    Error::InvalidPlan(format!("column index {index} out of bounds"))
                })?;
                Ok(ProjectionExpression::new(
                    ReferenceExpression::new(
                        index,
                        ExpressionResultType::new(field.data_type().clone(), field.is_nullable()),
                    )
                    .into_ref(),
                    field.name(),
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            expressions,
            metadata: input_schema.metadata().clone(),
        })
    }
    pub fn expressions(&self) -> &[ProjectionExpression] {
        &self.expressions
    }
    pub fn output_schema(&self) -> SchemaRef {
        let fields = self
            .expressions
            .iter()
            .map(|e| {
                let result_type = e.expression().result_type();
                Field::new(
                    e.name(),
                    result_type.data_type().clone(),
                    result_type.is_nullable(),
                )
            })
            .collect::<Vec<_>>();
        Arc::new(Schema::new_with_metadata(fields, self.metadata.clone()))
    }
}

#[derive(Clone, Debug)]
pub struct ProjectOperator {
    /// Prepared by the host against the child's output schema.
    projector: Projection,
}

impl ProjectOperator {
    pub fn new(projector: Projection) -> Self {
        Self { projector }
    }
}

impl Operator for ProjectOperator {
    fn name(&self) -> &'static str {
        "project"
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::InvalidPlan(format!(
                "project operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        graph
            .pipeline_mut(current)?
            .add_processor(Box::new(ProjectExec::new(self.projector.clone())));
        build_pipeline_on_node(child, current, graph)
    }
}
