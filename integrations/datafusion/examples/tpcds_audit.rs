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

//! Lists the plan nodes, expressions, functions and types of the optimized
//! logical plans of the TPC-DS corpus.
//!
//!     cargo run -p roc-datafusion --example tpcds_audit -- <data-dir> [query...]
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    logical_expr::{Expr, LogicalPlan},
    prelude::SessionContext,
};
use roc_datafusion::tpcds;
use std::collections::{BTreeMap, BTreeSet};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let data = std::path::PathBuf::from(&args[0]);
    let ctx = SessionContext::new();
    tpcds::register_tables(&ctx, &data).await?;
    let mut features: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut failures = vec![];
    let statements = tpcds::statements(&tpcds::query_dir())?;
    for s in &statements {
        if args.len() > 1 && !args[1..].contains(&s.name) {
            continue;
        }
        let plan = match ctx.state().create_logical_plan(&s.sql).await {
            Ok(plan) => ctx.state().optimize(&plan),
            Err(e) => Err(e),
        };
        let plan = match plan {
            Ok(plan) => plan,
            Err(e) => {
                failures.push((s.name.clone(), e.to_string()));
                continue;
            }
        };
        if args.len() > 1 {
            println!("== {}\n{}", s.name, plan.display_indent_schema());
        }
        let mut add = |k: String| {
            features.entry(k).or_default().insert(s.name.clone());
        };
        plan.apply_with_subqueries(|node| {
            add(format!("plan {}", node_name(node)));
            for field in node.schema().fields() {
                add(format!("type {}", field.data_type()));
            }
            node.apply_expressions(|e| {
                e.apply(|e| {
                    add(format!("expr {}", expr_name(e)));
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    for (feature, queries) in &features {
        let list = queries
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        println!("{:4} {feature:60} {list}", queries.len());
    }
    println!(
        "planned {} / {}",
        statements.len() - failures.len(),
        statements.len()
    );
    for (name, error) in failures {
        println!("FAILED {name}: {error}");
    }
    Ok(())
}

fn node_name(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Join(j) => format!(
            "Join {:?} keys={} filter={} null_eq={:?}",
            j.join_type,
            j.on.len().min(2),
            j.filter.is_some(),
            j.null_equality
        ),
        LogicalPlan::Aggregate(a) => format!(
            "Aggregate grouping_sets={}",
            a.group_expr
                .iter()
                .any(|e| matches!(e, Expr::GroupingSet(_)))
        ),
        LogicalPlan::Limit(l) => format!("Limit skip={}", l.skip.is_some()),
        LogicalPlan::Sort(s) => format!("Sort fetch={}", s.fetch.is_some()),
        LogicalPlan::Union(_) => "Union".into(),
        other => other
            .display()
            .to_string()
            .split(':')
            .next()
            .unwrap()
            .to_string(),
    }
}

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::BinaryExpr(b) => format!("Binary {}", b.op),
        Expr::ScalarFunction(f) => format!("Function {}", f.name()),
        Expr::AggregateFunction(f) => format!(
            "Aggregate {} distinct={} filter={} order={}",
            f.func.name(),
            f.params.distinct,
            f.params.filter.is_some(),
            !f.params.order_by.is_empty()
        ),
        Expr::WindowFunction(w) => format!(
            "Window {} frame={} order={}",
            w.fun,
            w.params.window_frame,
            !w.params.order_by.is_empty()
        ),
        Expr::Literal(v, _) => format!("Literal {}", v.data_type()),
        Expr::Cast(c) => format!("Cast {}", c.field.data_type()),
        Expr::TryCast(c) => format!("TryCast {}", c.field.data_type()),
        Expr::Like(l) => format!(
            "Like negated={} case_insensitive={} escape={:?}",
            l.negated, l.case_insensitive, l.escape_char
        ),
        Expr::InList(l) => format!("InList negated={}", l.negated),
        other => other.variant_name().to_string(),
    }
}
