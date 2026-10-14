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

//! Operators that materialize their input: each child runs in a pipeline of
//! its own that fills a buffer, and the current pipeline reads the result.
use super::Operator;
use crate::arrow::{
    array::new_null_array,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::column::ColumnId;
use crate::exec::{
    Buffer, BufferSinkExec, BufferSourceExec, Finish,
    sort::{SortKey, sort_chunks},
    window::{self, Window, WindowFrame, WindowFunction},
};
use crate::{
    error::{Error, ErrorKind, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Builds `child` into a new pipeline that `current` depends on, sinking
/// `columns` into `buffer`.
fn materialize(
    child: &OperatorTreeNode,
    current: PipelineId,
    graph: &mut PipelineGraphBuilder<'_>,
    columns: &[ColumnId],
    sink: BufferSinkExec,
) -> Result<PipelineId> {
    let input = graph.new_dependency(current)?;
    build_pipeline_on_node(child, input, graph)?;
    graph
        .pipeline_mut(input)?
        .set_sink(Box::new(sink), Some(columns.to_vec()))?;
    Ok(input)
}

/// Reads `buffer` as the source of `current`, binding `columns`.
fn read(
    buffer: Arc<Buffer>,
    current: PipelineId,
    graph: &mut PipelineGraphBuilder<'_>,
    columns: &[ColumnId],
) -> Result<SchemaRef> {
    let schema = graph.columns().schema(columns)?;
    let source = BufferSourceExec::new(buffer, schema.clone());
    graph
        .pipeline_mut(current)?
        .set_source(Box::new(source), columns)?;
    Ok(schema)
}

fn one_child<'a>(node: &'a OperatorTreeNode, name: &str) -> Result<&'a OperatorTreeNode> {
    match node.children() {
        [child] => Ok(child),
        children => Err(Error::invalid_plan(format!(
            "{name} operator requires exactly 1 child, got {}",
            children.len()
        ))),
    }
}

/// One key of a sort.
#[derive(Clone, Copy, Debug)]
pub struct SortColumn {
    pub column: ColumnId,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Orders the rows of its child's `columns` by `keys`, which are among
/// them; with `fetch`, keeps only the first rows. The pipeline reading the
/// result is ordered.
#[derive(Clone, Debug)]
pub struct SortOperator {
    keys: Vec<SortColumn>,
    columns: Vec<ColumnId>,
    fetch: Option<usize>,
}

impl SortOperator {
    pub fn try_new(
        keys: Vec<SortColumn>,
        columns: Vec<ColumnId>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        if keys.iter().any(|k| !columns.contains(&k.column)) {
            return Err(Error::invalid_plan(
                "sort keys must be sorted columns".into(),
            ));
        }
        Ok(Self {
            keys,
            columns,
            fetch,
        })
    }
}

impl Operator for SortOperator {
    fn name(&self) -> &'static str {
        "sort"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        self.columns.clone()
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let child = one_child(current_node, "sort")?;
        let schema = graph.columns().schema(&self.columns)?;
        let keys = self
            .keys
            .iter()
            .map(|k| SortKey {
                column: self.columns.iter().position(|c| *c == k.column).unwrap(),
                descending: k.descending,
                nulls_first: k.nulls_first,
            })
            .collect::<Vec<_>>();
        let fetch = self.fetch;
        let finish: Finish = Box::new(move |chunks, cpu| {
            Box::pin(async move { sort_chunks(&schema, chunks, &keys, fetch, cpu).await })
        });
        let buffer = Arc::new(Buffer::default());
        let sink = BufferSinkExec::new(buffer.clone(), Some(finish));
        materialize(child, current, graph, &self.columns, sink)?;
        read(buffer, current, graph, &self.columns)?;
        graph.pipeline_mut(current)?.set_ordered();
        Ok(())
    }
}

/// Skips `skip` rows of its child's `columns`, then keeps at most `fetch`.
/// The rows are the first ones only when the child is ordered.
#[derive(Clone, Debug)]
pub struct LimitOperator {
    skip: usize,
    fetch: Option<usize>,
    columns: Vec<ColumnId>,
}

impl LimitOperator {
    pub fn new(skip: usize, fetch: Option<usize>, columns: Vec<ColumnId>) -> Self {
        Self {
            skip,
            fetch,
            columns,
        }
    }
}

impl Operator for LimitOperator {
    fn name(&self) -> &'static str {
        "limit"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        self.columns.clone()
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let child = one_child(current_node, "limit")?;
        let (skip, fetch) = (self.skip, self.fetch);
        let finish: Finish = crate::exec::finish(move |chunks| {
            let mut out = vec![];
            let (mut skip, mut left) = (skip, fetch.unwrap_or(usize::MAX));
            for chunk in chunks {
                let s = skip.min(chunk.num_rows());
                skip -= s;
                let n = (chunk.num_rows() - s).min(left);
                left -= n;
                if n != 0 {
                    out.push(chunk.slice(s, n));
                }
            }
            Ok(out)
        });
        let buffer = Arc::new(Buffer::default());
        let mut sink = BufferSinkExec::new(buffer.clone(), Some(finish));
        if let Some(fetch) = fetch {
            sink = sink.with_limit(skip.saturating_add(fetch));
        }
        // An ordered input, such as a sort's, keeps its first rows.
        materialize(child, current, graph, &self.columns, sink)?;
        read(buffer, current, graph, &self.columns)?;
        graph.pipeline_mut(current)?.set_ordered();
        Ok(())
    }
}

/// The rows of every child, each child's `inputs` read as `outputs`.
#[derive(Clone, Debug)]
pub struct UnionOperator {
    inputs: Vec<Vec<ColumnId>>,
    outputs: Vec<ColumnId>,
}

impl UnionOperator {
    pub fn try_new(inputs: Vec<Vec<ColumnId>>, outputs: Vec<ColumnId>) -> Result<Self> {
        if inputs.iter().any(|i| i.len() != outputs.len()) {
            return Err(Error::invalid_plan(
                "union inputs must have the output columns".into(),
            ));
        }
        Ok(Self { inputs, outputs })
    }
}

impl Operator for UnionOperator {
    fn name(&self) -> &'static str {
        "union"
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
        if current_node.children().len() != self.inputs.len() {
            return Err(Error::invalid_plan(
                "a union has one input per child".into(),
            ));
        }
        let buffer = Arc::new(Buffer::default());
        for (child, columns) in current_node.children().iter().zip(&self.inputs) {
            let sink = BufferSinkExec::new(buffer.clone(), None);
            materialize(child, current, graph, columns, sink)?;
        }
        read(buffer, current, graph, &self.outputs)?;
        Ok(())
    }
}

/// Exactly one row of its child's `columns`: NULLs without rows; more than
/// one row fails. The result of a scalar subquery.
#[derive(Clone, Debug)]
pub struct SingleRowOperator {
    columns: Vec<ColumnId>,
}

impl SingleRowOperator {
    pub fn new(columns: Vec<ColumnId>) -> Self {
        Self { columns }
    }
}

impl Operator for SingleRowOperator {
    fn name(&self) -> &'static str {
        "single_row"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        self.columns.clone()
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let child = one_child(current_node, "single_row")?;
        let schema = graph.columns().schema(&self.columns)?;
        let finish: Finish = crate::exec::finish(move |chunks| {
            let rows = chunks.iter().map(RecordBatch::num_rows).sum::<usize>();
            if rows > 1 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "a scalar subquery produced more than one row",
                ));
            }
            if rows == 1 {
                return Ok(chunks.into_iter().filter(|c| c.num_rows() == 1).collect());
            }
            let nulls = schema
                .fields()
                .iter()
                .map(|f| new_null_array(f.data_type(), 1))
                .collect();
            let row = RecordBatch::try_new_with_options(
                schema.clone(),
                nulls,
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )
            .map_err(|source| {
                Error::invalid_plan("scalar subquery columns must be nullable".into())
                    .with_source(source)
            })?;
            Ok(vec![row])
        });
        let buffer = Arc::new(Buffer::default());
        let sink = BufferSinkExec::new(buffer.clone(), Some(finish)).with_limit(2);
        materialize(child, current, graph, &self.columns, sink)?;
        read(buffer, current, graph, &self.columns)?;
        Ok(())
    }
}

/// A shared subplan: every spool node with the same key in a tree is one
/// materialization of its child's `inputs`, read as `outputs` by each node.
/// The child of each node is the same subplan; only the first is built.
#[derive(Clone, Debug)]
pub struct SpoolOperator {
    key: u64,
    inputs: Vec<ColumnId>,
    outputs: Vec<ColumnId>,
}

static SPOOLS: AtomicU64 = AtomicU64::new(0);

impl SpoolOperator {
    /// The key of a new shared subplan.
    pub fn new_key() -> u64 {
        SPOOLS.fetch_add(1, Ordering::Relaxed)
    }

    pub fn new(key: u64, inputs: Vec<ColumnId>, outputs: Vec<ColumnId>) -> Self {
        Self {
            key,
            inputs,
            outputs,
        }
    }
}

impl Operator for SpoolOperator {
    fn name(&self) -> &'static str {
        "spool"
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
        let child = one_child(current_node, "spool")?;
        let buffer = match graph.shared(self.key) {
            Some((pipeline, state)) => {
                graph.add_dependency(current, pipeline)?;
                state
                    .downcast::<Buffer>()
                    .map_err(|_| Error::internal("spool keys name buffers".into()))?
            }
            None => {
                let buffer = Arc::new(Buffer::default());
                let sink = BufferSinkExec::new(buffer.clone(), None);
                let pipeline = materialize(child, current, graph, &self.inputs, sink)?;
                graph.set_shared(self.key, pipeline, buffer.clone());
                buffer
            }
        };
        read(buffer, current, graph, &self.outputs)?;
        Ok(())
    }
}

/// One window function of a WindowOperator, over its child's columns.
#[derive(Clone, Debug)]
pub struct WindowExpression {
    pub function: WindowFunction,
    pub argument: Option<ColumnId>,
    pub partition: Vec<ColumnId>,
    pub order: Vec<SortColumn>,
    pub frame: WindowFrame,
    pub output: ColumnId,
}

/// Its child's `columns`, then one column per window function.
#[derive(Clone, Debug)]
pub struct WindowOperator {
    columns: Vec<ColumnId>,
    windows: Vec<WindowExpression>,
}

impl WindowOperator {
    pub fn try_new(columns: Vec<ColumnId>, windows: Vec<WindowExpression>) -> Result<Self> {
        let read = |c: &ColumnId| columns.contains(c);
        if windows.iter().any(|w| {
            !w.argument.iter().all(read)
                || !w.partition.iter().all(read)
                || !w.order.iter().all(|o| read(&o.column))
        }) {
            return Err(Error::invalid_plan(
                "windows read their child's columns".into(),
            ));
        }
        Ok(Self { columns, windows })
    }
}

impl Operator for WindowOperator {
    fn name(&self) -> &'static str {
        "window"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        let mut columns = self.columns.clone();
        columns.extend(self.windows.iter().map(|w| w.output));
        columns
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let child = one_child(current_node, "window")?;
        let outputs = self.output_columns();
        let schema = graph.columns().schema(&outputs)?;
        let position = |c: &ColumnId| self.columns.iter().position(|x| x == c).unwrap();
        let windows = self
            .windows
            .iter()
            .map(|w| {
                Ok(Window {
                    function: w.function,
                    argument: w.argument.as_ref().map(position),
                    partition: w.partition.iter().map(position).collect(),
                    order: w
                        .order
                        .iter()
                        .map(|o| SortKey {
                            column: position(&o.column),
                            descending: o.descending,
                            nulls_first: o.nulls_first,
                        })
                        .collect(),
                    frame: w.frame,
                    output: graph.columns().column_type(w.output)?.data_type.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let finish: Finish = Box::new(move |chunks, cpu| {
            Box::pin(async move { window::compute(&schema, chunks, &windows, cpu).await })
        });
        let buffer = Arc::new(Buffer::default());
        let sink = BufferSinkExec::new(buffer.clone(), Some(finish));
        materialize(child, current, graph, &self.columns, sink)?;
        read(buffer, current, graph, &outputs)?;
        Ok(())
    }
}
