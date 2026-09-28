use super::Operator;
use crate::{
    Error, Result,
    expr::{self, Expr, NamedExpr},
};
use crate::{
    exec::aggregate_execs,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_node},
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateExpr {
    pub name: String,
    pub function: AggregateFunction,
    /// None means COUNT(*); other functions require an expression.
    pub expr: Option<Expr>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateOperator {
    pub groups: Vec<NamedExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input_schema: SchemaRef,
}

/// Schema-derived layout shared by the aggregate sink/source execution pair.
#[derive(Clone, Debug)]
pub struct AggregateLayout {
    pub operator: AggregateOperator,
    pub output_schema: SchemaRef,
    pub partial_schema: SchemaRef,
    pub group_schema: SchemaRef,
    pub value_types: Vec<DataType>,
}

impl AggregateOperator {
    pub fn layout(&self, input: &SchemaRef) -> Result<AggregateLayout> {
        if self.aggregates.is_empty() && self.groups.is_empty() {
            return Err(Error::Plan("aggregate needs a group or function".into()));
        }
        let group_schema = expr::project_schema(input, &self.groups)?;
        for field in group_schema.fields() {
            if !matches!(
                field.data_type(),
                DataType::Boolean | DataType::Int32 | DataType::Int64 | DataType::Utf8
            ) {
                return Err(Error::Plan(format!(
                    "unsupported group key type: {}",
                    field.data_type()
                )));
            }
        }
        let mut fields: Vec<Field> = group_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        let mut partial = fields.clone();
        let mut value_types = vec![];
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            let ty = match (&aggregate.expr, aggregate.function) {
                (None, AggregateFunction::Count) => DataType::Int64,
                (None, _) => return Err(Error::Plan("only COUNT accepts no expression".into())),
                (Some(expression), function) => {
                    let ty = expression.data_type(input)?;
                    if function == AggregateFunction::Count {
                        DataType::Int64
                    } else {
                        match ty {
                            DataType::Int32 | DataType::Int64 => DataType::Int64,
                            DataType::Float64 => DataType::Float64,
                            _ => {
                                return Err(Error::Plan(format!(
                                    "unsupported aggregate type: {ty}"
                                )));
                            }
                        }
                    }
                }
            };
            let output_type = if aggregate.function == AggregateFunction::Avg {
                DataType::Float64
            } else {
                ty.clone()
            };
            fields.push(Field::new(
                &aggregate.name,
                output_type,
                aggregate.function != AggregateFunction::Count,
            ));
            let partial_type = if ty == DataType::Int64
                && matches!(
                    aggregate.function,
                    AggregateFunction::Sum | AggregateFunction::Avg
                ) {
                DataType::Decimal128(38, 0)
            } else {
                ty.clone()
            };
            partial.push(Field::new(format!("__a{index}_value"), partial_type, true));
            partial.push(Field::new(
                format!("__a{index}_count"),
                DataType::Int64,
                false,
            ));
            value_types.push(ty);
        }
        let mut names = std::collections::HashSet::new();
        if fields.iter().any(|field| !names.insert(field.name())) {
            return Err(Error::Plan("duplicate aggregate output name".into()));
        }
        Ok(AggregateLayout {
            operator: self.clone(),
            output_schema: Arc::new(Schema::new(fields)),
            partial_schema: Arc::new(Schema::new(partial)),
            group_schema,
            value_types,
        })
    }
}

impl Operator for AggregateOperator {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = node.children() else {
            return Err(Error::Plan("aggregate operator requires one child".into()));
        };
        let (sink, source) = aggregate_execs(self.layout(&self.input_schema)?);
        graph.pipeline_mut(current)?.set_source(Box::new(source))?;
        let input = graph.new_dependency(current)?;
        graph.pipeline_mut(input)?.set_sink(Box::new(sink))?;
        build_pipeline_node(child, input, graph)
    }
}
