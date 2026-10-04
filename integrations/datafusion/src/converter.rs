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

use std::sync::Arc;

use crate::{DataFusionScan, DataFusionStorage};
use arrow::datatypes::{DataType, Schema};
use datafusion::{
    catalog::default_table_source::source_as_provider,
    common::{DFSchema, ScalarValue},
    dataframe::DataFrame,
    error::{DataFusionError, Result},
    execution::SessionState,
    functions_aggregate::{
        average::Avg,
        count::Count,
        covariance::CovariancePopulation,
        min_max::{Max, Min},
        sum::Sum,
    },
    logical_expr::{Expr, LogicalPlan, Operator},
    physical_expr::{
        PhysicalExpr,
        expressions::{
            BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal,
            NegativeExpr, NotExpr, TryCastExpr,
        },
    },
};
use futures::future::BoxFuture;
use roc::{
    expr::{
        ExpressionResultType,
        agg::{AggregateExpression, AggregateFunction, executor::AggregateExpressionExecutor},
        scalar::*,
    },
    operator::{
        AggregateOperator, FilterOperator, OperatorTree, OperatorTreeNode, ProjectOperator,
        Projection, ProjectionExpression, ScanOperator,
    },
};

fn unsupported(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("Roc conversion: {}", message.into()))
}
fn roc_error(error: roc::error::Error) -> DataFusionError {
    unsupported(error.to_string())
}

/// Converts analyzed, optimized logical operators and bound scalar expressions
/// into Roc descriptions. Only table scans are physically planned by DataFusion.
/// Execution, aggregation phases and pipeline scheduling belong to Roc.
///
/// Integer arithmetic and SUM retain Roc's checked overflow semantics.
/// Unsupported nodes/functions fail during conversion; no query subtree is
/// silently executed through DataFusion as a fallback.
pub struct LogicalPlanConverter {
    state: SessionState,
}
impl LogicalPlanConverter {
    pub fn new(state: SessionState) -> Self {
        Self { state }
    }

    /// Preserve the DataFrame's session snapshot while analyzing and optimizing.
    pub async fn convert_dataframe(frame: DataFrame) -> Result<OperatorTree> {
        let (state, plan) = frame.into_parts();
        let plan = state.optimize(&plan)?;
        Self::new(state).convert(&plan).await
    }

    /// Input must be analyzed and optimized using this converter's session.
    pub async fn convert(&self, plan: &LogicalPlan) -> Result<OperatorTree> {
        Ok(OperatorTree::new(self.node(plan).await?))
    }

    fn node<'a>(&'a self, plan: &'a LogicalPlan) -> BoxFuture<'a, Result<OperatorTreeNode>> {
        Box::pin(async move {
            match plan {
                LogicalPlan::TableScan(scan) => {
                    let provider = source_as_provider(&scan.source)?;
                    let physical = provider
                        .scan(
                            &self.state,
                            scan.projection.as_ref(),
                            &scan.filters,
                            scan.fetch,
                        )
                        .await?;
                    if physical.schema().as_ref() != scan.projected_schema.as_arrow() {
                        return Err(unsupported(
                            "table provider returned a different scan schema",
                        ));
                    }
                    Ok(OperatorTreeNode::new(
                        ScanOperator::new(
                            DataFusionScan::new(physical, self.state.task_ctx()),
                            Arc::new(DataFusionStorage),
                        ),
                        vec![],
                    ))
                }
                LogicalPlan::Projection(project) => {
                    let projection =
                        self.projection(&project.expr, project.input.schema(), &project.schema)?;
                    let child = self.node(&project.input).await?;
                    Ok(OperatorTreeNode::new(
                        ProjectOperator::new(projection),
                        vec![child],
                    ))
                }
                LogicalPlan::Filter(filter) => {
                    let predicate = self.scalar(&filter.predicate, filter.input.schema())?;
                    let child = self.node(&filter.input).await?;
                    Ok(OperatorTreeNode::new(
                        FilterOperator::new(predicate),
                        vec![child],
                    ))
                }
                LogicalPlan::SubqueryAlias(alias) => self.node(&alias.input).await,
                LogicalPlan::Aggregate(aggregate) => {
                    if aggregate
                        .group_expr
                        .iter()
                        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
                    {
                        return Err(unsupported("grouping sets"));
                    }
                    let input = aggregate.input.schema();
                    let groups =
                        self.projection(&aggregate.group_expr, input, &aggregate.schema)?;
                    let mut expressions = Vec::new();
                    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
                        let expr = match expr {
                            Expr::Alias(alias) => alias.expr.as_ref(),
                            other => other,
                        };
                        let Expr::AggregateFunction(call) = expr else {
                            return Err(unsupported(format!("aggregate expression {expr}")));
                        };
                        let implementation = call.func.inner();
                        let function = if implementation.downcast_ref::<Count>().is_some() {
                            AggregateFunction::Count
                        } else if implementation.downcast_ref::<Sum>().is_some() {
                            AggregateFunction::Sum
                        } else if implementation.downcast_ref::<Avg>().is_some() {
                            AggregateFunction::Avg
                        } else if implementation.downcast_ref::<Min>().is_some() {
                            AggregateFunction::Min
                        } else if implementation.downcast_ref::<Max>().is_some() {
                            AggregateFunction::Max
                        } else if implementation
                            .downcast_ref::<CovariancePopulation>()
                            .is_some()
                        {
                            AggregateFunction::CovarPop
                        } else {
                            return Err(unsupported(format!(
                                "aggregate function {}",
                                call.func.name()
                            )));
                        };
                        let params = &call.params;
                        let expected_args = if function == AggregateFunction::CovarPop {
                            2
                        } else {
                            1
                        };
                        if params.args.len() != expected_args
                            || !params.order_by.is_empty()
                            || params.null_treatment.is_some()
                        {
                            return Err(unsupported(format!(
                                "arguments/order/null treatment for {}",
                                call.func.name()
                            )));
                        }
                        if params.distinct && function != AggregateFunction::Count {
                            return Err(unsupported("DISTINCT is supported only for COUNT(expr)"));
                        }
                        let args = params
                            .args
                            .iter()
                            .map(|arg| self.scalar(arg, input))
                            .collect::<Result<Vec<_>>>()?;
                        let field = aggregate.schema.field(aggregate.group_expr.len() + index);
                        if !field.metadata().is_empty() {
                            return Err(unsupported("aggregate field metadata"));
                        }
                        if function == AggregateFunction::Sum
                            && !matches!(
                                field.data_type(),
                                DataType::Int64 | DataType::UInt64 | DataType::Float64
                            )
                        {
                            return Err(unsupported(format!(
                                "SUM result type {}",
                                field.data_type()
                            )));
                        }
                        if function == AggregateFunction::Avg
                            && field.data_type() != &DataType::Float64
                        {
                            return Err(unsupported(format!(
                                "AVG result type {}",
                                field.data_type()
                            )));
                        }
                        let mut expression = AggregateExpression::new(
                            function,
                            args,
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                        .with_alias(field.name());
                        if params.distinct {
                            expression = expression.with_distinct();
                        }
                        if let Some(filter) = &params.filter {
                            expression = expression.with_filter(self.scalar(filter, input)?);
                        }
                        let expression = Arc::new(expression);
                        // Validate supported accumulator types before a pipeline starts.
                        AggregateExpressionExecutor::try_new(expression.clone())
                            .map_err(roc_error)?;
                        expressions.push(expression);
                    }
                    let operator =
                        AggregateOperator::try_new(groups, expressions).map_err(roc_error)?;
                    if operator.output_schema().as_ref() != aggregate.schema.as_arrow() {
                        return Err(unsupported("aggregate output schema mismatch"));
                    }
                    let child = self.node(&aggregate.input).await?;
                    Ok(OperatorTreeNode::new(operator, vec![child]))
                }
                other => Err(unsupported(format!("logical operator {}", other.display()))),
            }
        })
    }

    fn projection(
        &self,
        expressions: &[Expr],
        input: &DFSchema,
        output: &DFSchema,
    ) -> Result<Projection> {
        let mut result = Vec::with_capacity(expressions.len());
        for (index, expr) in expressions.iter().enumerate() {
            let field = output.field(index);
            if !field.metadata().is_empty() {
                return Err(unsupported("projection field metadata"));
            }
            let expression = self.scalar(expr, input)?;
            if expression.result_type().data_type() != field.data_type()
                || expression.result_type().is_nullable() != field.is_nullable()
            {
                return Err(unsupported(format!(
                    "expression schema mismatch for {}",
                    field.name()
                )));
            }
            result.push(ProjectionExpression::new(expression, field.name()));
        }
        Ok(Projection::new(result).with_metadata(output.metadata().clone()))
    }

    fn scalar(&self, expr: &Expr, input: &DFSchema) -> Result<ScalarExprRef> {
        let physical = self.state.create_physical_expr(expr.clone(), input)?;
        let expression = convert_expression(&physical, input.as_arrow())?;
        expression.to_evaluation().map_err(roc_error)?;
        Ok(expression)
    }
}

/// Bind a supported DataFusion physical scalar expression to Roc's kernels.
/// This does not retain a DataFusion evaluator in the returned expression.
fn convert_expression(expr: &Arc<dyn PhysicalExpr>, input: &Schema) -> Result<ScalarExprRef> {
    let data_type = expr.data_type(input)?;
    let nullable = expr.nullable(input)?;
    let recurse = |child: &Arc<dyn PhysicalExpr>| convert_expression(child, input);
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Ok(ReferenceExpression::new(
            column.index(),
            ExpressionResultType::new(data_type, nullable),
        )
        .into_ref());
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        return ConstantExpression::try_new(literal.value().to_array()?)
            .map(|e| e.into_ref())
            .map_err(roc_error);
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        let left = recurse(binary.left())?;
        let right = recurse(binary.right())?;
        if matches!(binary.op(), Operator::And | Operator::Or) {
            // DataFusion can skip its RHS; Roc's conjunction evaluates both.
            // Do not change observable errors for an expression that can fail.
            if may_error(binary.right()) {
                return Err(unsupported("AND/OR with a fallible right operand"));
            }
            return Ok(ConjunctionExpression::new(
                if *binary.op() == Operator::And {
                    Conjunction::And
                } else {
                    Conjunction::Or
                },
                vec![left, right],
                nullable,
            )
            .into_ref());
        }
        let function = match binary.op() {
            Operator::Plus => FunctionKind::Add,
            Operator::Minus => FunctionKind::Subtract,
            Operator::Multiply => FunctionKind::Multiply,
            Operator::Divide => FunctionKind::Divide,
            Operator::Modulo => FunctionKind::Remainder,
            Operator::Eq => FunctionKind::Equal,
            Operator::NotEq => FunctionKind::NotEqual,
            Operator::Lt => FunctionKind::LessThan,
            Operator::LtEq => FunctionKind::LessThanOrEqual,
            Operator::Gt => FunctionKind::GreaterThan,
            Operator::GtEq => FunctionKind::GreaterThanOrEqual,
            Operator::IsDistinctFrom => FunctionKind::IsDistinctFrom,
            Operator::IsNotDistinctFrom => FunctionKind::IsNotDistinctFrom,
            other => return Err(unsupported(format!("binary operator {other}"))),
        };
        // Arrow total-order float comparison differs from DataFusion's signed-zero
        // normalization. Reject it until Roc provides matching SQL comparison.
        if data_type == DataType::Boolean
            && matches!(
                left.result_type().data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            )
        {
            return Err(unsupported(
                "floating-point comparison (signed-zero semantics)",
            ));
        }
        return Ok(
            FunctionExpression::binary(function, left, right, data_type, nullable).into_ref(),
        );
    }
    if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        // Only numeric casts are independent of configurable formatting options.
        if !cast.cast_type().is_numeric() || !cast.expr().data_type(input)?.is_numeric() {
            return Err(unsupported("non-numeric CAST"));
        }
        return Ok(CastExpression::new(
            recurse(cast.expr())?,
            data_type,
            if cast.cast_options().safe {
                CastMode::Try
            } else {
                CastMode::Strict
            },
            nullable,
        )
        .into_ref());
    }
    if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        if !cast.cast_type().is_numeric() || !cast.expr().data_type(input)?.is_numeric() {
            return Err(unsupported("non-numeric TRY_CAST"));
        }
        return Ok(
            CastExpression::new(recurse(cast.expr())?, data_type, CastMode::Try, nullable)
                .into_ref(),
        );
    }
    if let Some(not) = expr.downcast_ref::<NotExpr>() {
        return Ok(NotExpression::new(recurse(not.arg())?, nullable).into_ref());
    }
    let unary = if let Some(e) = expr.downcast_ref::<NegativeExpr>() {
        Some((FunctionKind::Negate, e.arg()))
    } else if let Some(e) = expr.downcast_ref::<IsNullExpr>() {
        Some((FunctionKind::IsNull, e.arg()))
    } else if let Some(e) = expr.downcast_ref::<IsNotNullExpr>() {
        Some((FunctionKind::IsNotNull, e.arg()))
    } else {
        None
    };
    if let Some((kind, argument)) = unary {
        return Ok(
            FunctionExpression::unary(kind, recurse(argument)?, data_type, nullable).into_ref(),
        );
    }
    if let Some(case) = expr.downcast_ref::<CaseExpr>() {
        if case.expr().is_some() {
            return Err(unsupported("simple CASE (use searched CASE)"));
        }
        let branches = case
            .when_then_expr()
            .iter()
            .map(|(when, then)| Ok((recurse(when)?, recurse(then)?)))
            .collect::<Result<_>>()?;
        let otherwise = match case.else_expr() {
            Some(other) => recurse(other)?,
            None => ConstantExpression::try_new(ScalarValue::try_from(&data_type)?.to_array()?)
                .map_err(roc_error)?
                .into_ref(),
        };
        return Ok(CaseExpression::new(branches, otherwise, data_type, nullable).into_ref());
    }
    Err(unsupported(format!("scalar expression {expr}")))
}

fn may_error(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        if matches!(
            binary.op(),
            Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo
        ) {
            return true;
        }
    }
    if expr.downcast_ref::<NegativeExpr>().is_some()
        || expr
            .downcast_ref::<CastExpr>()
            .is_some_and(|cast| !cast.cast_options().safe)
    {
        return true;
    }
    expr.children().into_iter().any(may_error)
}
