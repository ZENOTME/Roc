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
use crate::exec::TableSinkExec;
use crate::expr::scalar::ScalarExprRef;
use crate::kernel::{
    ExpandKind, StateBuilder,
    table::{SharedTable, TableState},
};
use crate::{
    error::{Error, ErrorContext, Result, ResultExt},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use std::sync::Arc;

/// What a join produces for each probe row, given its matches: build rows
/// whose keys equal its keys and that pass the residual filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// One row per match.
    Inner,
    /// One row per match, or one with NULL build columns without any.
    Left,
    /// The probe row once, if it has a match.
    Semi,
    /// The probe row, if it has no match.
    Anti,
    /// The probe row with a non-null Boolean: whether it has a match.
    Mark,
}

/// A hash join, or a nested-loop join without keys. The build child (the
/// second) becomes a table in a pipeline of its own; the probe child (the
/// first) runs in the current pipeline and expands into its matches.
#[derive(Clone, Debug)]
pub struct JoinOperator {
    join_type: JoinType,
    probe_keys: Vec<ColumnId>,
    build_keys: Vec<ColumnId>,
    nulls_equal: bool,
    /// The probe child's columns the join reads, and their join columns.
    probe_columns: Vec<(ColumnId, ColumnId)>,
    /// The build child's columns the table stores, and their join columns.
    build_columns: Vec<(ColumnId, ColumnId)>,
    /// Over the join columns of matching pairs.
    filter: Option<ScalarExprRef>,
    mark: Option<ColumnId>,
}

impl JoinOperator {
    /// Keys are pairwise equal columns of the probe and build children; without
    /// keys every build row matches. Join columns are new: probe ones first.
    pub fn try_new(
        join_type: JoinType,
        keys: Vec<(ColumnId, ColumnId)>,
        nulls_equal: bool,
        probe_columns: Vec<(ColumnId, ColumnId)>,
        build_columns: Vec<(ColumnId, ColumnId)>,
        filter: Option<ScalarExprRef>,
        mark: Option<ColumnId>,
    ) -> Result<Self> {
        if (join_type == JoinType::Mark) != mark.is_some() {
            return Err(Error::invalid_plan(
                "a mark join, and only a mark join, has a mark column".into(),
            ));
        }
        if join_type == JoinType::Left && filter.is_some() {
            return Err(Error::unsupported(
                "left joins with a residual filter are not supported".into(),
            ));
        }
        let (probe_keys, build_keys) = keys.into_iter().unzip();
        Ok(Self {
            join_type,
            probe_keys,
            build_keys,
            nulls_equal,
            probe_columns,
            build_columns,
            filter,
            mark,
        })
    }
}

impl Operator for JoinOperator {
    fn name(&self) -> &'static str {
        "join"
    }

    /// Probe columns; then build columns for inner and left joins, or the
    /// mark for a mark join.
    fn output_columns(&self) -> Vec<ColumnId> {
        let mut columns = self.probe_columns.iter().map(|c| c.1).collect::<Vec<_>>();
        match self.join_type {
            JoinType::Inner | JoinType::Left => {
                columns.extend(self.build_columns.iter().map(|c| c.1))
            }
            JoinType::Mark => columns.extend(self.mark),
            JoinType::Semi | JoinType::Anti => {}
        }
        columns
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let [probe, build] = current_node.children() else {
            return Err(Error::invalid_plan(format!(
                "join operator requires exactly 2 children, got {}",
                current_node.children().len(),
            )));
        };
        let stored = self.build_columns.iter().map(|c| c.0).collect::<Vec<_>>();
        let keys = self
            .build_keys
            .iter()
            .map(|k| {
                stored.iter().position(|c| c == k).ok_or_else(|| {
                    Error::invalid_plan("build keys must be stored build columns".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let table = Arc::new(SharedTable::default());
        let build_pipeline = graph.new_dependency(current)?;
        build_pipeline_on_node(build, build_pipeline, graph)?;
        let sink = TableSinkExec::new(
            graph.columns().schema(&stored)?,
            keys,
            self.nulls_equal,
            table.clone(),
        );
        graph
            .pipeline_mut(build_pipeline)?
            .set_sink(Box::new(sink), Some(stored))?;
        build_pipeline_on_node(probe, current, graph)?;

        let k = &mut graph.pipeline_mut(current)?.kernels;
        (|| {
            for &(from, to) in &self.probe_columns {
                let binding = k.column_binding(from)?;
                let errors = k.column_errors(from)?;
                k.bind_column(to, binding, errors)?;
            }
            let state = k.state(StateBuilder::new(move || {
                Ok(TableState {
                    table: table.get()?,
                })
            }));
            let kind = if self.probe_keys.is_empty() {
                ExpandKind::Product { state }
            } else {
                ExpandKind::Lookup {
                    state,
                    keys: self
                        .probe_keys
                        .iter()
                        .map(|c| k.column_binding(*c))
                        .collect::<Result<_>>()?,
                    nulls_equal: self.nulls_equal,
                    outer: self.join_type == JoinType::Left,
                }
            };
            let parent = k.current_domain();
            let (expanded, row) = k.expand(kind)?;
            k.set_current_domain(expanded)?;
            for (i, &(_, to)) in self.build_columns.iter().enumerate() {
                let t = k.column_type(to)?.data_type.clone();
                let binding = k.table_gather(state, row, i, &t)?;
                k.bind_column(to, binding, vec![])?;
            }
            if let Some(filter) = &self.filter {
                let errors = filter.build_kernels(k)?;
                k.raise(errors)?;
                let binding = k.column_binding(filter.output())?;
                let predicate = k.predicate(&binding)?;
                let matched = k.filter(predicate)?;
                k.set_current_domain(matched)?;
            }
            if matches!(self.join_type, JoinType::Inner | JoinType::Left) {
                return Ok(());
            }
            let matched = k.current_domain();
            let yes = k.boolean(true)?;
            k.set_current_domain(parent)?;
            let (reduced, any) = k.reduce_any(matched, vec![yes])?;
            k.set_current_domain(reduced)?;
            match self.join_type {
                JoinType::Semi => {
                    let kept = k.filter(any[0])?;
                    k.set_current_domain(kept)
                }
                JoinType::Anti => {
                    let none = k.not(any[0])?;
                    let kept = k.filter(none)?;
                    k.set_current_domain(kept)
                }
                _ => k.bind_column(
                    self.mark.unwrap(),
                    crate::kernel::ColumnVariableBinding::Boolean {
                        value: any[0],
                        validity: None,
                    },
                    vec![],
                ),
            }
        })()
        .with_context(|| {
            ErrorContext::new("join.bind").field("join", format_args!("{:?}", self.join_type))
        })
    }
}
