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

//! Optimized DataFusion logical plans as Roc operator trees.
//!
//! Every plan node becomes Roc operators over Roc columns: one column per
//! field of the node's schema, in order. DataFusion keeps the frontend:
//! names, types, coercions and the optimized plan shape. Its scans read the
//! tables. Anything without a Roc equivalent fails with the node or
//! expression that needs it.
use crate::{DataFusionScan, DataFusionStorage};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::{
    arrow::datatypes::{DataType, IntervalMonthDayNano},
    catalog::default_table_source::source_as_provider,
    common::{DFSchema, NullEquality, ScalarValue},
    execution::SessionState,
    logical_expr::{
        self as df, BinaryExpr, Expr, JoinType as DfJoinType, LogicalPlan, Operator as Op,
        WindowFrameBound, WindowFrameUnits, expr::AggregateFunction as DfAggregate,
        expr::WindowFunction as DfWindow,
    },
    physical_plan::{
        ExecutionPlan,
        statistics::{StatisticsArgs, StatisticsContext},
    },
};
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    error::{Error, ErrorContext, ErrorKind, Result},
    exec::window::{WindowFrame, WindowFunction},
    expr::{
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            CaseExpression, CastExpression, CastMode, Conjunction, ConjunctionExpression,
            ConstantExpression, FunctionExpression, FunctionKind, LikeExpression, NotExpression,
            ReferenceExpression, ScalarExprRef, ScalarExpression,
        },
    },
    operator::{
        AggregateOperator, CollectOperator, FilterOperator, JoinOperator, JoinType, LimitOperator,
        OperatorTree, OperatorTreeNode, ProjectOperator, ScanOperator, SingleRowOperator,
        SortColumn, SortOperator, SpoolOperator, UnionOperator, WindowExpression, WindowOperator,
    },
};
use std::{collections::HashMap, sync::Arc};

/// A translated query: run `tree` and take the result from `collect`.
pub struct Query {
    pub tree: OperatorTree,
    pub collect: CollectOperator,
}

/// A translated plan node: its operators and one column per schema field.
#[derive(Clone)]
struct Rel {
    node: OperatorTreeNode,
    columns: Vec<ColumnId>,
    /// An estimate of its row count, to choose join build sides.
    rows: f64,
}

fn external(e: datafusion::error::DataFusionError) -> Error {
    Error::new(ErrorKind::External, "DataFusion failed").with_source(e)
}

fn unsupported(what: impl std::fmt::Display) -> Error {
    Error::unsupported(format!("unsupported in Roc: {what}"))
}

pub async fn translate(plan: &LogicalPlan, state: &SessionState) -> Result<Query> {
    let mut t = Translator {
        columns: ColumnContext::new(),
        state,
        subqueries: HashMap::new(),
    };
    let rel = t.plan(plan).await?;
    // The result columns in schema order.
    let refs = rel
        .columns
        .iter()
        .map(|c| ReferenceExpression::new(*c).into_ref())
        .collect();
    let root = OperatorTreeNode::new(ProjectOperator::new(refs), vec![rel.node]);
    let collect = CollectOperator::new();
    let root = OperatorTreeNode::new(collect.clone(), vec![root]);
    Ok(Query {
        tree: OperatorTree::new(t.columns, root),
        collect,
    })
}

struct Translator<'a> {
    columns: ColumnContext,
    state: &'a SessionState,
    /// Scalar subqueries of the expressions being translated, by plan.
    subqueries: HashMap<String, ColumnId>,
}

impl Translator<'_> {
    fn column(&mut self, t: &DataType) -> ColumnId {
        // Every column is nullable: Roc checks NULLs against declared
        // nullability, and frontends may claim less than they produce.
        self.columns.allocate(ColumnType::new(t.clone(), true))
    }

    fn columns_of(&mut self, schema: &DFSchema) -> Vec<ColumnId> {
        schema
            .fields()
            .iter()
            .map(|f| {
                self.columns
                    .allocate_named(ColumnType::new(f.data_type().clone(), true), f.name())
            })
            .collect()
    }

    fn plan<'p>(
        &'p mut self,
        plan: &'p LogicalPlan,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Rel>> + 'p>> {
        Box::pin(async move {
            let node = plan.display().to_string();
            self.plan_node(plan).await.map_err(|e| {
                e.with_context(ErrorContext::new("datafusion.translate").field("node", node))
            })
        })
    }

    async fn plan_node(&mut self, plan: &LogicalPlan) -> Result<Rel> {
        match plan {
            LogicalPlan::TableScan(scan) => self.scan(scan).await,
            LogicalPlan::SubqueryAlias(a) => self.plan(&a.input).await,
            LogicalPlan::Projection(p) => {
                let input = self.plan(&p.input).await?;
                let input = self.subquery_inputs(&p.expr, input).await?;
                let schema = p.input.schema();
                if p.expr.iter().all(|e| as_column(e).is_some()) {
                    let columns = p
                        .expr
                        .iter()
                        .map(|e| self.reference(as_column(e).unwrap(), schema, &input))
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(Rel { columns, ..input });
                }
                let exprs = p
                    .expr
                    .iter()
                    .map(|e| self.expr(e, schema, &input))
                    .collect::<Result<Vec<_>>>()?;
                let columns = exprs.iter().map(|e| e.output()).collect();
                let rows = input.rows;
                Ok(Rel {
                    node: OperatorTreeNode::new(ProjectOperator::new(exprs), vec![input.node]),
                    columns,
                    rows,
                })
            }
            LogicalPlan::Filter(f) => {
                let input = self.plan(&f.input).await?;
                let width = input.columns.len();
                let input = self
                    .subquery_inputs(std::slice::from_ref(&f.predicate), input)
                    .await?;
                let predicate = self.expr(&f.predicate, f.input.schema(), &input)?;
                let all = input.columns.clone();
                Ok(Rel {
                    node: OperatorTreeNode::new(
                        FilterOperator::new(predicate, all),
                        vec![input.node],
                    ),
                    columns: input.columns[..width].to_vec(),
                    rows: input.rows * 0.25,
                })
            }
            LogicalPlan::Aggregate(a) => self.aggregate(a).await,
            LogicalPlan::Sort(s) => {
                let input = self.plan(&s.input).await?;
                let schema = s.input.schema();
                let width = input.columns.len();
                let keys = s.expr.iter().map(|k| k.expr.clone()).collect::<Vec<_>>();
                let (input, key_columns) = self.computed(&keys, schema, input)?;
                let keys = s
                    .expr
                    .iter()
                    .zip(key_columns)
                    .map(|(k, column)| SortColumn {
                        column,
                        descending: !k.asc,
                        nulls_first: k.nulls_first,
                    })
                    .collect();
                let sort = SortOperator::try_new(keys, input.columns.clone(), s.fetch)?;
                Ok(Rel {
                    node: OperatorTreeNode::new(sort, vec![input.node]),
                    columns: input.columns[..width].to_vec(),
                    rows: s.fetch.map_or(input.rows, |f| input.rows.min(f as f64)),
                })
            }
            LogicalPlan::Limit(l) => {
                let input = self.plan(&l.input).await?;
                let skip = match l.get_skip_type().map_err(external)? {
                    df::SkipType::Literal(s) => s,
                    _ => return Err(unsupported("a non-constant OFFSET")),
                };
                let fetch = match l.get_fetch_type().map_err(external)? {
                    df::FetchType::Literal(f) => f,
                    _ => return Err(unsupported("a non-constant LIMIT")),
                };
                let limit = LimitOperator::new(skip, fetch, input.columns.clone());
                Ok(Rel {
                    node: OperatorTreeNode::new(limit, vec![input.node]),
                    rows: fetch.map_or(input.rows, |f| input.rows.min(f as f64)),
                    ..input
                })
            }
            LogicalPlan::Join(j) => self.join(j).await,
            LogicalPlan::Union(u) => {
                let mut inputs = vec![];
                let mut nodes = vec![];
                let mut rows = 0.0;
                for input in &u.inputs {
                    let rel = self.plan(input).await?;
                    rows += rel.rows;
                    inputs.push(rel.columns);
                    nodes.push(rel.node);
                }
                let outputs = self.columns_of(&u.schema);
                let union = UnionOperator::try_new(inputs, outputs.clone())?;
                Ok(Rel {
                    node: OperatorTreeNode::new(union, nodes),
                    columns: outputs,
                    rows,
                })
            }
            LogicalPlan::Window(w) => self.window(w).await,
            other => Err(unsupported(format!("plan node {}", other.display()))),
        }
    }

    async fn scan(&mut self, scan: &df::TableScan) -> Result<Rel> {
        let provider = source_as_provider(&scan.source).map_err(external)?;
        let plan: Arc<dyn ExecutionPlan> = provider
            .scan(
                self.state,
                scan.projection.as_ref(),
                &scan.filters,
                scan.fetch,
            )
            .await
            .map_err(external)?;
        // Only an estimate for choosing build sides.
        let rows = StatisticsContext::new()
            .compute(plan.as_ref(), &StatisticsArgs::new())
            .ok()
            .and_then(|s| s.num_rows.get_value().copied())
            .unwrap_or(1_000_000) as f64;
        let columns = self.columns_of(&scan.projected_schema);
        let source = DataFusionScan::new(plan, self.state.task_ctx());
        let node = OperatorTreeNode::new(
            ScanOperator::new(source, Arc::new(DataFusionStorage), columns.clone()),
            vec![],
        );
        let rel = Rel {
            node,
            columns,
            rows,
        };
        // Pushed filters may be inexact; evaluating them again is harmless.
        match scan.filters.iter().cloned().reduce(|a, b| a.and(b)) {
            Some(predicate) => {
                let predicate = self.expr(&predicate, &scan.projected_schema, &rel)?;
                Ok(Rel {
                    node: OperatorTreeNode::new(
                        FilterOperator::new(predicate, rel.columns.clone()),
                        vec![rel.node],
                    ),
                    ..rel
                })
            }
            None => Ok(rel),
        }
    }

    /// The input with one more column per expression that is not a column;
    /// returns the columns of every expression.
    fn computed(
        &mut self,
        exprs: &[Expr],
        schema: &DFSchema,
        input: Rel,
    ) -> Result<(Rel, Vec<ColumnId>)> {
        let mut projected = input
            .columns
            .iter()
            .map(|c| ReferenceExpression::new(*c).into_ref())
            .collect::<Vec<_>>();
        let mut outputs = vec![];
        for e in exprs {
            let x = self.expr(e, schema, &input)?;
            outputs.push(x.output());
            if !matches!(x.as_ref(), ScalarExpression::Reference(_)) {
                projected.push(x);
            }
        }
        if projected.len() == input.columns.len() {
            return Ok((input, outputs));
        }
        let columns = projected.iter().map(|e| e.output()).collect();
        Ok((
            Rel {
                node: OperatorTreeNode::new(ProjectOperator::new(projected), vec![input.node]),
                columns,
                rows: input.rows,
            },
            outputs,
        ))
    }

    async fn aggregate(&mut self, a: &df::Aggregate) -> Result<Rel> {
        let input = self.plan(&a.input).await?;
        let schema = a.input.schema();
        let grouping = a.group_expr.iter().find_map(|e| match e {
            Expr::GroupingSet(g) => Some(g.clone()),
            _ => None,
        });
        let (groups, sets) = match &grouping {
            Some(g) => {
                let all = g.distinct_expr().into_iter().cloned().collect::<Vec<_>>();
                let sets = match g {
                    df::GroupingSet::Rollup(exprs) => {
                        (0..=exprs.len()).rev().map(|n| (0..n).collect()).collect()
                    }
                    df::GroupingSet::Cube(exprs) => (0..1usize << exprs.len())
                        .rev()
                        .map(|mask| {
                            (0..exprs.len())
                                .filter(|i| mask >> (exprs.len() - 1 - i) & 1 != 0)
                                .collect()
                        })
                        .collect(),
                    df::GroupingSet::GroupingSets(sets) => sets
                        .iter()
                        .map(|set| {
                            set.iter()
                                .map(|e| all.iter().position(|x| x == e).unwrap())
                                .collect()
                        })
                        .collect::<Vec<Vec<usize>>>(),
                };
                (all, Some(sets))
            }
            None => (a.group_expr.clone(), None),
        };
        let groups = groups
            .iter()
            .map(|e| self.expr(e, schema, &input))
            .collect::<Result<Vec<_>>>()?;
        let output_fields = a.schema.fields();
        let mut aggregates = vec![];
        let offset = groups.len() + sets.is_some() as usize;
        for (i, e) in a.aggr_expr.iter().enumerate() {
            let output_type = output_fields[offset + i].data_type();
            aggregates.push(Arc::new(self.aggregate_call(
                e,
                output_type,
                schema,
                &input,
            )?));
        }
        let mut columns = groups.iter().map(|g| g.output()).collect::<Vec<_>>();
        let mut operator = AggregateOperator::try_new(groups, aggregates.clone())?;
        if let Some(sets) = sets {
            let id = self.column(output_fields[columns.len()].data_type());
            operator = operator.with_grouping_sets(sets, id)?;
            columns.push(id);
        }
        columns.extend(aggregates.iter().map(|a| a.output()));
        let rows = if a.group_expr.is_empty() {
            1.0
        } else {
            (input.rows * 0.1).max(1.0)
        };
        Ok(Rel {
            node: OperatorTreeNode::new(operator, vec![input.node]),
            columns,
            rows,
        })
    }

    fn aggregate_call(
        &mut self,
        e: &Expr,
        output: &DataType,
        schema: &DFSchema,
        input: &Rel,
    ) -> Result<AggregateExpression> {
        let e = strip_alias(e);
        let Expr::AggregateFunction(DfAggregate { func, params }) = e else {
            return Err(unsupported(format!("aggregate expression {e}")));
        };
        if !params.order_by.is_empty() {
            return Err(unsupported("ordered aggregates"));
        }
        let function = match func.name() {
            "count" => AggregateFunction::Count,
            "sum" => AggregateFunction::Sum,
            "avg" => AggregateFunction::Avg,
            "min" => AggregateFunction::Min,
            "max" => AggregateFunction::Max,
            "stddev" | "stddev_samp" => AggregateFunction::StddevSamp,
            "covar_pop" => AggregateFunction::CovarPop,
            name => return Err(unsupported(format!("aggregate function {name}"))),
        };
        let arguments = params
            .args
            .iter()
            .map(|a| self.expr(a, schema, input))
            .collect::<Result<Vec<_>>>()?;
        let out = self.column(output);
        let mut aggregate = AggregateExpression::new(function, arguments, out);
        if params.distinct {
            aggregate = aggregate.with_distinct();
        }
        if let Some(filter) = &params.filter {
            aggregate = aggregate.with_filter(self.expr(filter, schema, input)?);
        }
        Ok(aggregate)
    }

    async fn join(&mut self, j: &df::Join) -> Result<Rel> {
        if j.null_aware {
            return Err(unsupported("null-aware anti joins"));
        }
        let left = self.plan(&j.left).await?;
        let right = self.plan(&j.right).await?;
        if j.join_type == DfJoinType::Full {
            return self.full_join(j, left, right);
        }
        let nulls_equal = j.null_equality == NullEquality::NullEqualsNull;
        let (lw, rw) = (left.columns.len(), right.columns.len());
        let (lkeys, rkeys): (Vec<Expr>, Vec<Expr>) = j.on.iter().cloned().unzip();
        let (left, lk) = self.computed(&lkeys, j.left.schema(), left)?;
        let (right, rk) = self.computed(&rkeys, j.right.schema(), right)?;
        use DfJoinType as D;
        // The probe side is the side whose rows the join keeps or extends.
        let (probe_left, join_type) = match j.join_type {
            D::Inner => (left.rows >= right.rows, JoinType::Inner),
            D::Left => (true, JoinType::Left),
            D::Right => (false, JoinType::Left),
            D::LeftSemi => (true, JoinType::Semi),
            D::RightSemi => (false, JoinType::Semi),
            D::LeftAnti => (true, JoinType::Anti),
            D::RightAnti => (false, JoinType::Anti),
            D::LeftMark => (true, JoinType::Mark),
            D::RightMark => (false, JoinType::Mark),
            D::Full => unreachable!("rewritten above"),
        };
        // Join columns of the left and right fields.
        let lout = left.columns[..lw]
            .iter()
            .map(|c| (*c, self.renamed(*c)))
            .collect::<Vec<_>>();
        let rout = right.columns[..rw]
            .iter()
            .map(|c| (*c, self.renamed(*c)))
            .collect::<Vec<_>>();
        let filter = match &j.filter {
            Some(f) => {
                let both = Rel {
                    node: left.node.clone(),
                    columns: lout.iter().chain(&rout).map(|c| c.1).collect(),
                    rows: 0.0,
                };
                let schema = j.left.schema().join(j.right.schema()).map_err(external)?;
                Some(self.expr(f, &schema, &both)?)
            }
            None => None,
        };
        let mark = matches!(join_type, JoinType::Mark).then(|| self.column(&DataType::Boolean));
        let keys: Vec<(ColumnId, ColumnId)> = if probe_left {
            lk.iter().copied().zip(rk.iter().copied()).collect()
        } else {
            rk.iter().copied().zip(lk.iter().copied()).collect()
        };
        // The build side stores its keys too, so computed keys stay columns.
        let stored = |rel: &Rel, out: &[(ColumnId, ColumnId)], keys: &[ColumnId], t: &mut Self| {
            let mut stored = out.to_vec();
            for k in keys {
                if !stored.iter().any(|(c, _)| c == k) && rel.columns.contains(k) {
                    stored.push((*k, t.renamed(*k)));
                }
            }
            stored
        };
        let (probe, build, probe_out, build_out) = if probe_left {
            let b = stored(&right, &rout, &rk, self);
            (left, right, lout.clone(), b)
        } else {
            let b = stored(&left, &lout, &lk, self);
            (right, left, rout.clone(), b)
        };
        let rows = match join_type {
            JoinType::Inner => probe.rows.max(build.rows),
            JoinType::Left | JoinType::Mark => probe.rows,
            _ => probe.rows * 0.5,
        };
        let op = JoinOperator::try_new(
            join_type,
            keys,
            nulls_equal,
            probe_out,
            build_out,
            filter,
            mark,
        )?;
        let node = OperatorTreeNode::new(op, vec![probe.node, build.node]);
        let lcols = lout.iter().map(|c| c.1);
        let rcols = rout.iter().map(|c| c.1);
        let columns = match j.join_type {
            D::Inner | D::Left | D::Right => lcols.chain(rcols).collect(),
            D::LeftSemi | D::LeftAnti => lcols.collect(),
            D::RightSemi | D::RightAnti => rcols.collect(),
            D::LeftMark => lcols.chain(mark).collect(),
            D::RightMark => rcols.chain(mark).collect(),
            D::Full => unreachable!(),
        };
        Ok(Rel {
            node,
            columns,
            rows,
        })
    }

    /// FULL JOIN as the left join of both sides, then the right rows
    /// without a match with NULL left columns. Each side is computed once.
    fn full_join(&mut self, j: &df::Join, left: Rel, right: Rel) -> Result<Rel> {
        if j.filter.is_some() {
            return Err(unsupported("full joins with a residual filter"));
        }
        let lkey = SpoolOperator::new_key();
        let rkey = SpoolOperator::new_key();
        let spool = |t: &mut Self, key: u64, rel: &Rel| {
            let outputs = rel
                .columns
                .iter()
                .map(|c| t.renamed(*c))
                .collect::<Vec<_>>();
            Rel {
                node: OperatorTreeNode::new(
                    SpoolOperator::new(key, rel.columns.clone(), outputs.clone()),
                    vec![rel.node.clone()],
                ),
                columns: outputs,
                rows: rel.rows,
            }
        };
        let (l1, r1) = (spool(self, lkey, &left), spool(self, rkey, &right));
        let (l2, r2) = (spool(self, lkey, &left), spool(self, rkey, &right));
        let mut left_join = j.clone();
        left_join.join_type = DfJoinType::Left;
        let mut anti = j.clone();
        anti.join_type = DfJoinType::RightAnti;
        let matched = self.join_rels(&left_join, l1, r1)?;
        let unmatched = self.join_rels(&anti, l2, r2)?;
        // NULL left columns for the right rows without a match.
        let mut exprs = vec![];
        for f in j.left.schema().fields() {
            let out = self.column(f.data_type());
            exprs.push(ConstantExpression::null(f.data_type(), out).into_ref());
        }
        exprs.extend(
            unmatched
                .columns
                .iter()
                .map(|c| ReferenceExpression::new(*c).into_ref()),
        );
        let padded_columns = exprs.iter().map(|e| e.output()).collect::<Vec<_>>();
        let padded = OperatorTreeNode::new(ProjectOperator::new(exprs), vec![unmatched.node]);
        let outputs = self.columns_of(&j.schema);
        let union = UnionOperator::try_new(vec![matched.columns, padded_columns], outputs.clone())?;
        Ok(Rel {
            node: OperatorTreeNode::new(union, vec![matched.node, padded]),
            columns: outputs,
            rows: left.rows.max(right.rows),
        })
    }

    fn join_rels(&mut self, j: &df::Join, left: Rel, right: Rel) -> Result<Rel> {
        let nulls_equal = j.null_equality == NullEquality::NullEqualsNull;
        let (lw, rw) = (left.columns.len(), right.columns.len());
        let (lkeys, rkeys): (Vec<Expr>, Vec<Expr>) = j.on.iter().cloned().unzip();
        let (left, lk) = self.computed(&lkeys, j.left.schema(), left)?;
        let (right, rk) = self.computed(&rkeys, j.right.schema(), right)?;
        let probe_left = j.join_type == DfJoinType::Left;
        let join_type = if probe_left {
            JoinType::Left
        } else {
            JoinType::Anti
        };
        let lout = left.columns[..lw]
            .iter()
            .map(|c| (*c, self.renamed(*c)))
            .collect::<Vec<_>>();
        let rout = right.columns[..rw]
            .iter()
            .map(|c| (*c, self.renamed(*c)))
            .collect::<Vec<_>>();
        let (probe, probe_out, build_keys, probe_keys, build_rel) = if probe_left {
            (left, lout.clone(), rk.clone(), lk.clone(), right)
        } else {
            (right, rout.clone(), lk.clone(), rk.clone(), left)
        };
        let mut build_out = if probe_left {
            rout.clone()
        } else {
            lout.clone()
        };
        for k in &build_keys {
            if !build_out.iter().any(|(c, _)| c == k) && build_rel.columns.contains(k) {
                build_out.push((*k, self.renamed(*k)));
            }
        }
        let keys = probe_keys.into_iter().zip(build_keys).collect();
        let op = JoinOperator::try_new(
            join_type,
            keys,
            nulls_equal,
            probe_out,
            build_out,
            None,
            None,
        )?;
        let node = OperatorTreeNode::new(op, vec![probe.node, build_rel.node]);
        let columns = if probe_left {
            lout.iter().chain(&rout).map(|c| c.1).collect()
        } else {
            rout.iter().map(|c| c.1).collect()
        };
        Ok(Rel {
            node,
            columns,
            rows: probe.rows,
        })
    }

    async fn window(&mut self, w: &df::Window) -> Result<Rel> {
        let input = self.plan(&w.input).await?;
        let schema = w.input.schema();
        let width = input.columns.len();
        let mut windows = vec![];
        let mut input = input;
        let out_fields = w.schema.fields();
        for (i, e) in w.window_expr.iter().enumerate() {
            let e = strip_alias(e);
            let Expr::WindowFunction(f) = e else {
                return Err(unsupported(format!("window expression {e}")));
            };
            let DfWindow { fun, params } = f.as_ref();
            if params.filter.is_some() || params.distinct || params.null_treatment.is_some() {
                return Err(unsupported("window filters, DISTINCT or NULL treatment"));
            }
            let function = match fun.name() {
                "rank" => WindowFunction::Rank,
                "dense_rank" => WindowFunction::DenseRank,
                "row_number" => WindowFunction::RowNumber,
                "count" => WindowFunction::Count,
                "sum" => WindowFunction::Sum,
                "avg" => WindowFunction::Avg,
                "min" => WindowFunction::Min,
                "max" => WindowFunction::Max,
                name => return Err(unsupported(format!("window function {name}"))),
            };
            let mut exprs = params.args.clone();
            exprs.extend(params.partition_by.iter().cloned());
            exprs.extend(params.order_by.iter().map(|o| o.expr.clone()));
            let (next, columns) = self.computed(&exprs, schema, input)?;
            input = next;
            let (args, rest) = columns.split_at(params.args.len());
            let (partition, order) = rest.split_at(params.partition_by.len());
            let frame = &params.window_frame;
            let frame = match (frame.units, &frame.start_bound, &frame.end_bound) {
                (_, WindowFrameBound::Preceding(s), WindowFrameBound::Following(e))
                    if s.is_null() && e.is_null() =>
                {
                    WindowFrame::Partition
                }
                (
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(s),
                    WindowFrameBound::CurrentRow,
                ) if s.is_null() => WindowFrame::RowsToCurrent,
                (
                    WindowFrameUnits::Range,
                    WindowFrameBound::Preceding(s),
                    WindowFrameBound::CurrentRow,
                ) if s.is_null() => {
                    if params.order_by.is_empty() {
                        WindowFrame::Partition
                    } else {
                        WindowFrame::RangeToCurrent
                    }
                }
                _ => return Err(unsupported(format!("window frame {frame}"))),
            };
            let output = self.column(out_fields[width + i].data_type());
            windows.push(WindowExpression {
                function,
                argument: args.first().copied(),
                partition: partition.to_vec(),
                order: params
                    .order_by
                    .iter()
                    .zip(order)
                    .map(|(o, c)| SortColumn {
                        column: *c,
                        descending: !o.asc,
                        nulls_first: o.nulls_first,
                    })
                    .collect(),
                frame,
                output,
            });
        }
        let mut columns = input.columns[..width].to_vec();
        columns.extend(windows.iter().map(|w| w.output));
        let op = WindowOperator::try_new(input.columns.clone(), windows)?;
        Ok(Rel {
            node: OperatorTreeNode::new(op, vec![input.node]),
            columns,
            rows: input.rows,
        })
    }

    /// A new column of the type of `c`, for operators whose outputs are new.
    fn renamed(&mut self, c: ColumnId) -> ColumnId {
        let t = self.columns.column_type(c).unwrap().clone();
        self.columns.allocate(t)
    }

    /// Joins each uncorrelated scalar subquery of `exprs` to `input` as one
    /// more column, recorded for `expr`.
    async fn subquery_inputs(&mut self, exprs: &[Expr], mut input: Rel) -> Result<Rel> {
        let mut found = vec![];
        for e in exprs {
            e.apply(|x| {
                if let Expr::ScalarSubquery(s) = x {
                    found.push(s.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .map_err(external)?;
        }
        // Each node joins its own subqueries: a column joined into another
        // node's pipeline is not visible here. Within the node, one join
        // serves every occurrence.
        let mut joined = std::collections::HashSet::new();
        for s in found {
            let key = s.subquery.display_indent().to_string();
            if !joined.insert(key.clone()) {
                continue;
            }
            if !s.outer_ref_columns.is_empty() {
                return Err(unsupported("correlated scalar subqueries"));
            }
            let sub = self.plan(&s.subquery).await?;
            if sub.columns.len() != 1 {
                return Err(Error::invalid_plan(
                    "a scalar subquery has one column".into(),
                ));
            }
            let single =
                OperatorTreeNode::new(SingleRowOperator::new(sub.columns.clone()), vec![sub.node]);
            let value = self.renamed(sub.columns[0]);
            let probe = input
                .columns
                .iter()
                .map(|c| (*c, self.renamed(*c)))
                .collect::<Vec<_>>();
            let columns = probe.iter().map(|c| c.1).chain([value]).collect();
            let op = JoinOperator::try_new(
                JoinType::Inner,
                vec![],
                false,
                probe,
                vec![(sub.columns[0], value)],
                None,
                None,
            )?;
            self.subqueries.insert(key, value);
            input = Rel {
                node: OperatorTreeNode::new(op, vec![input.node, single]),
                columns,
                rows: input.rows,
            };
        }
        Ok(input)
    }

    fn reference(
        &self,
        c: &datafusion::common::Column,
        schema: &DFSchema,
        input: &Rel,
    ) -> Result<ColumnId> {
        let i = schema.index_of_column(c).map_err(external)?;
        Ok(input.columns[i])
    }

    fn expr(&mut self, e: &Expr, schema: &DFSchema, input: &Rel) -> Result<ScalarExprRef> {
        let text = e.to_string();
        self.expr_node(e, schema, input).map_err(|err| {
            err.with_context(ErrorContext::new("datafusion.expression").field("expr", text))
        })
    }

    fn output(&mut self, e: &Expr, schema: &DFSchema) -> Result<ColumnId> {
        use datafusion::logical_expr::ExprSchemable;
        let t = e.get_type(schema).map_err(external)?;
        Ok(self.column(&t))
    }

    fn expr_node(&mut self, e: &Expr, schema: &DFSchema, input: &Rel) -> Result<ScalarExprRef> {
        Ok(match e {
            Expr::Alias(a) => return self.expr(&a.expr, schema, input),
            Expr::Column(c) => {
                ReferenceExpression::new(self.reference(c, schema, input)?).into_ref()
            }
            Expr::Literal(v, _) => {
                let array = v.to_array().map_err(external)?;
                let out = self.column(array.data_type());
                ConstantExpression::try_new(array, out)?.into_ref()
            }
            Expr::ScalarSubquery(s) => {
                let key = s.subquery.display_indent().to_string();
                let c = self.subqueries.get(&key).ok_or_else(|| {
                    unsupported("a scalar subquery outside a filter or projection")
                })?;
                ReferenceExpression::new(*c).into_ref()
            }
            Expr::BinaryExpr(b) => self.binary(b, e, schema, input)?,
            Expr::Not(x) => {
                let x = self.expr(x, schema, input)?;
                let out = self.output(e, schema)?;
                NotExpression::new(x, out).into_ref()
            }
            Expr::IsNull(x) | Expr::IsNotNull(x) => {
                let f = if matches!(e, Expr::IsNull(_)) {
                    FunctionKind::IsNull
                } else {
                    FunctionKind::IsNotNull
                };
                let x = self.expr(x, schema, input)?;
                let out = self.output(e, schema)?;
                FunctionExpression::unary(f, x, out).into_ref()
            }
            Expr::Negative(x) => {
                let x = self.expr(x, schema, input)?;
                let out = self.output(e, schema)?;
                FunctionExpression::unary(FunctionKind::Negate, x, out).into_ref()
            }
            Expr::Cast(c) => {
                let x = self.expr(&c.expr, schema, input)?;
                let out = self.column(c.field.data_type());
                CastExpression::new(x, CastMode::Strict, out).into_ref()
            }
            Expr::TryCast(c) => {
                let x = self.expr(&c.expr, schema, input)?;
                let out = self.column(c.field.data_type());
                CastExpression::new(x, CastMode::Try, out).into_ref()
            }
            Expr::Case(c) => {
                let out = self.output(e, schema)?;
                let mut branches = vec![];
                for (when, then) in &c.when_then_expr {
                    let condition = match &c.expr {
                        Some(operand) => {
                            let eq = Expr::BinaryExpr(BinaryExpr::new(
                                operand.clone(),
                                Op::Eq,
                                when.clone(),
                            ));
                            self.expr(&eq, schema, input)?
                        }
                        None => self.expr(when, schema, input)?,
                    };
                    branches.push((condition, self.expr(then, schema, input)?));
                }
                let otherwise = match &c.else_expr {
                    Some(x) => self.expr(x, schema, input)?,
                    None => {
                        let t = self.columns.column_type(out)?.data_type.clone();
                        let n = self.column(&t);
                        ConstantExpression::null(&t, n).into_ref()
                    }
                };
                CaseExpression::new(branches, otherwise, out).into_ref()
            }
            Expr::InList(l) => {
                // x IN (a, b) is x = a OR x = b, with the same NULL semantics.
                let mut any = vec![];
                for item in &l.list {
                    let eq = Expr::BinaryExpr(BinaryExpr::new(
                        l.expr.clone(),
                        Op::Eq,
                        Box::new(item.clone()),
                    ));
                    any.push(self.expr(&eq, schema, input)?);
                }
                let out = self.column(&DataType::Boolean);
                let or = ConjunctionExpression::new(Conjunction::Or, any, out).into_ref();
                if l.negated {
                    let out = self.column(&DataType::Boolean);
                    NotExpression::new(or, out).into_ref()
                } else {
                    or
                }
            }
            Expr::Like(l) => {
                if l.case_insensitive {
                    return Err(unsupported("ILIKE"));
                }
                let x = self.expr(&l.expr, schema, input)?;
                let p = self.expr(&l.pattern, schema, input)?;
                let out = self.column(&DataType::Boolean);
                LikeExpression::new(x, p, l.negated, l.escape_char, out).into_ref()
            }
            Expr::ScalarFunction(f) => {
                let function = match f.name() {
                    "abs" => FunctionKind::Abs,
                    "round" => FunctionKind::Round,
                    "substr" | "substring" => FunctionKind::Substring,
                    "upper" => FunctionKind::Upper,
                    name => return Err(unsupported(format!("function {name}"))),
                };
                let mut args = vec![];
                for (i, a) in f.args.iter().enumerate() {
                    let x = self.expr(a, schema, input)?;
                    // Positions are Int64.
                    let x = if function == FunctionKind::Substring && i > 0 {
                        self.cast_to(x, DataType::Int64)?
                    } else {
                        x
                    };
                    args.push(x);
                }
                if function == FunctionKind::Round && args.len() == 1 {
                    let out = self.column(&DataType::Int64);
                    args.push(ConstantExpression::int64(Some(0), out).into_ref());
                }
                let out = self.output(e, schema)?;
                FunctionExpression::new(function, args, out).into_ref()
            }
            other => return Err(unsupported(format!("expression {}", other.variant_name()))),
        })
    }

    fn cast_to(&mut self, x: ScalarExprRef, t: DataType) -> Result<ScalarExprRef> {
        if self.columns.column_type(x.output())?.data_type == t {
            return Ok(x);
        }
        let out = self.column(&t);
        Ok(CastExpression::new(x, CastMode::Strict, out).into_ref())
    }

    fn binary(
        &mut self,
        b: &BinaryExpr,
        e: &Expr,
        schema: &DFSchema,
        input: &Rel,
    ) -> Result<ScalarExprRef> {
        use datafusion::logical_expr::ExprSchemable;
        if matches!(b.op, Op::And | Op::Or) {
            let mut args = vec![];
            for x in [&b.left, &b.right] {
                args.push(self.expr(x, schema, input)?);
            }
            let conjunction = if b.op == Op::And {
                Conjunction::And
            } else {
                Conjunction::Or
            };
            let out = self.column(&DataType::Boolean);
            return Ok(ConjunctionExpression::new(conjunction, args, out).into_ref());
        }
        // date +/- days: the day count of a Date32.
        if let (Ok(DataType::Date32), Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(i)), _)) =
            (b.left.get_type(schema), b.right.as_ref())
            && matches!(b.op, Op::Plus | Op::Minus)
        {
            let IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } = *i;
            if months != 0 || nanoseconds != 0 {
                return Err(unsupported("intervals of months or below days"));
            }
            let date = self.expr(&b.left, schema, input)?;
            let day = self.cast_to(date, DataType::Int32)?;
            let n = self.column(&DataType::Int32);
            let n = ConstantExpression::try_new(
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![days])),
                n,
            )?;
            let f = if b.op == Op::Plus {
                FunctionKind::Add
            } else {
                FunctionKind::Subtract
            };
            let out = self.column(&DataType::Int32);
            let sum = FunctionExpression::binary(f, day, n.into_ref(), out).into_ref();
            return self.cast_to(sum, DataType::Date32);
        }
        let function = match b.op {
            Op::Eq => FunctionKind::Equal,
            Op::NotEq => FunctionKind::NotEqual,
            Op::Lt => FunctionKind::LessThan,
            Op::LtEq => FunctionKind::LessThanOrEqual,
            Op::Gt => FunctionKind::GreaterThan,
            Op::GtEq => FunctionKind::GreaterThanOrEqual,
            Op::Plus => FunctionKind::Add,
            Op::Minus => FunctionKind::Subtract,
            Op::Multiply => FunctionKind::Multiply,
            Op::Divide => FunctionKind::Divide,
            Op::Modulo => FunctionKind::Remainder,
            Op::IsDistinctFrom => FunctionKind::IsDistinctFrom,
            Op::IsNotDistinctFrom => FunctionKind::IsNotDistinctFrom,
            Op::BitwiseAnd => FunctionKind::BitwiseAnd,
            Op::BitwiseOr => FunctionKind::BitwiseOr,
            Op::BitwiseShiftRight => FunctionKind::ShiftRight,
            Op::StringConcat => FunctionKind::Concat,
            op => return Err(unsupported(format!("operator {op}"))),
        };
        let left = self.expr(&b.left, schema, input)?;
        let mut right = self.expr(&b.right, schema, input)?;
        let lt = b.left.get_type(schema).map_err(external)?;
        let rt = b.right.get_type(schema).map_err(external)?;
        // Strings of different representations compare as one.
        let bytes =
            |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
        if lt != rt && bytes(&lt) && bytes(&rt) && function != FunctionKind::Concat {
            right = self.cast_to(right, lt.clone())?;
        }
        if lt != rt && function == FunctionKind::ShiftRight {
            right = self.cast_to(right, lt.clone())?;
        }
        if lt != rt
            && !bytes(&lt)
            && !matches!(lt, DataType::Decimal128(..))
            && function != FunctionKind::ShiftRight
        {
            // Coercion leaves equal types; keep the left one otherwise.
            right = self.cast_to(right, lt.clone())?;
        }
        let out = self.output(e, schema)?;
        Ok(FunctionExpression::binary(function, left, right, out).into_ref())
    }
}

fn as_column(e: &Expr) -> Option<&datafusion::common::Column> {
    match e {
        Expr::Column(c) => Some(c),
        Expr::Alias(a) => as_column(&a.expr),
        _ => None,
    }
}

fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => strip_alias(&a.expr),
        e => e,
    }
}
