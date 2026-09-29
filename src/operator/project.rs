//! Projection operator descriptor.
use super::Operator;
use crate::expr::scalar::{BoundReferenceExpression, BoundScalarExprRef};
use crate::{
    error::{Error, Result},
    exec::ProjectExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use arrow::datatypes::SchemaRef;

#[derive(Clone, Debug)]
pub struct ProjectionExpression {
    expression: BoundScalarExprRef,
    name: String,
}

impl ProjectionExpression {
    pub fn new(expression: BoundScalarExprRef, name: impl Into<String>) -> Self {
        Self {
            expression,
            name: name.into(),
        }
    }
    pub fn expression(&self) -> &BoundScalarExprRef {
        &self.expression
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct Projection {
    input_schema: SchemaRef,
    expressions: Vec<ProjectionExpression>,
}
impl Projection {
    pub fn new(input_schema: SchemaRef, expressions: Vec<ProjectionExpression>) -> Self {
        Self {
            input_schema,
            expressions,
        }
    }
    pub fn from_indices(input_schema: SchemaRef, indices: &[usize]) -> Result<Self> {
        let expressions = indices
            .iter()
            .map(|&index| {
                let field = input_schema.fields().get(index).ok_or_else(|| {
                    Error::InvalidPlan(format!("column index {index} out of bounds"))
                })?;
                Ok(ProjectionExpression::new(
                    BoundReferenceExpression::new(index).into_ref(),
                    field.name(),
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self::new(input_schema, expressions))
    }
    pub fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
    pub fn expressions(&self) -> &[ProjectionExpression] {
        &self.expressions
    }
    /// Resolve result metadata through the executor; it is not stored in the description.
    pub fn output_schema(&self) -> Result<SchemaRef> {
        Ok(crate::exec::ProjectionExecutor::try_new(self.clone())?
            .output_schema()
            .clone())
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
