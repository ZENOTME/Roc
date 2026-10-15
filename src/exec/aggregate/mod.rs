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

pub(crate) mod group_index;

use super::{
    ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, SourceExec, StateContext, downcast_local,
};
use crate::arrow::datatypes::SchemaRef;
use crate::arrow::{
    array::ArrayRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::column::ColumnContext;
use crate::expr::agg::accumulator::Accumulator;
use crate::kernel::StateId;
use crate::kernel::aggregation::{AccumulatorState, GroupState};
use crate::operator::Operator;
use crate::{
    error::{Error, ErrorContext, Result, ResultExt},
    operator::AggregateOperator,
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use group_index::GroupIndex;
use std::sync::{Arc, OnceLock};

// Share output morsels among workers. The pipeline applies its own
// configured batch size to each morsel.
const AGGREGATE_MORSEL_ROWS: usize = 2048;
const PARTITION_GROUPS: usize = 16 * 1024;

// Grouping set -> hash partition -> one partial.
type Partitioned = Vec<Vec<PartialAggregate>>;

/// The merge of local partials. No expression evaluator or executable query
/// plan is needed here: all inputs are accumulator states.
struct AggregateMergeState {
    index: GroupIndex,
    group_ids: Vec<usize>,
    /// The output of an operator without grouping sets, for tests.
    #[cfg(test)]
    output_schema: SchemaRef,
    accumulators: Vec<Accumulator>,
}

/// Each aggregate keeps its own state arrays; there is no flattened
/// partial schema or column-offset mapping between aggregates.
#[derive(Clone)]
struct PartialAggregate {
    groups: RecordBatch,
    states: Vec<Vec<ArrayRef>>,
}

impl PartialAggregate {
    fn num_rows(&self) -> usize {
        self.groups.num_rows()
    }

    /// The rows of each of `parts` partitions by key hash.
    fn partitions(
        &self,
        index: &GroupIndex,
        parts: usize,
        guard: &ShutdownGuard,
    ) -> Result<Vec<Self>> {
        if parts == 1 || self.groups.num_columns() == 0 {
            return Ok(vec![self.clone()]);
        }
        let hashes = index.hash_rows(self.groups.columns(), self.num_rows())?;
        let mut rows = vec![vec![]; parts];
        for (r, h) in hashes.iter().enumerate() {
            // The high bits pick the table slot inside the merge; partition
            // by the low ones.
            rows[(*h % parts as u64) as usize].push(r as u32);
        }
        let take = |array: &ArrayRef, rows: &crate::arrow::array::UInt32Array| {
            arrow_select::take::take(array, rows, None).map_err(|source| {
                Error::internal("aggregate partials do not partition".into()).with_source(source)
            })
        };
        rows.into_iter()
            .map(|rows| {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let rows = crate::arrow::array::UInt32Array::from(rows);
                let groups = RecordBatch::try_new_with_options(
                    self.groups.schema(),
                    self.groups
                        .columns()
                        .iter()
                        .map(|c| take(c, &rows))
                        .collect::<Result<_>>()?,
                    &RecordBatchOptions::new().with_row_count(Some(rows.len())),
                )
                .map_err(|source| {
                    Error::internal("aggregate partials do not partition".into())
                        .with_source(source)
                })?;
                let states = self
                    .states
                    .iter()
                    .map(|s| s.iter().map(|a| take(a, &rows)).collect())
                    .collect::<Result<_>>()?;
                Ok(Self { groups, states })
            })
            .collect()
    }

    #[cfg(test)]
    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            groups: self.groups.slice(offset, len),
            states: self
                .states
                .iter()
                .map(|state| state.iter().map(|array| array.slice(offset, len)).collect())
                .collect(),
        }
    }
}

impl AggregateMergeState {
    #[cfg(test)]
    fn new(operator: &AggregateOperator, columns: &ColumnContext) -> Result<Self> {
        let all = (0..operator.groups().len()).collect::<Vec<_>>();
        Self::for_set(operator, columns, &all)
    }

    /// The merge of one grouping set's partials.
    fn for_set(
        operator: &AggregateOperator,
        columns: &ColumnContext,
        set: &[usize],
    ) -> Result<Self> {
        let group_columns = operator.group_columns();
        let set_columns = set.iter().map(|&g| group_columns[g]).collect::<Vec<_>>();
        let schema = columns.schema(&set_columns)?;
        let mut accumulators = Vec::new();
        for (index, aggregate) in operator.aggregates().iter().enumerate() {
            let mut accumulator = aggregate
                .accumulator(columns)
                .with_context(|| ErrorContext::new("aggregate.bind").field("aggregate", index))?;
            if schema.fields().is_empty() {
                accumulator.resize(1);
            }
            accumulators.push(accumulator);
        }
        Ok(Self {
            index: GroupIndex::new(&schema)?,
            group_ids: vec![],
            #[cfg(test)]
            output_schema: columns.schema(&operator.output_columns())?,
            accumulators,
        })
    }
    fn merge(&mut self, partial: &PartialAggregate) -> Result<()> {
        // Each accumulator owns its partial-state schema. Check the complete
        // bundle before mutating any global group or accumulator state.
        if partial.states.len() != self.accumulators.len()
            || self
                .accumulators
                .iter()
                .zip(&partial.states)
                .any(|(a, columns)| {
                    let types = a.state_types();
                    types.len() != columns.len()
                        || types.iter().zip(columns).any(|(ty, column)| {
                            column.data_type() != ty || column.len() != partial.num_rows()
                        })
                })
        {
            return Err(Error::internal("invalid aggregate partial state".into()));
        }
        if partial.num_rows() == 0 {
            return Ok(());
        }
        self.index.intern(
            partial.groups.columns(),
            partial.num_rows(),
            &mut self.group_ids,
        )?;
        for (index, (accumulator, state)) in self
            .accumulators
            .iter_mut()
            .zip(&partial.states)
            .enumerate()
        {
            accumulator.resize(self.index.len());
            accumulator
                .merge(state, &self.group_ids)
                .with_context(|| ErrorContext::new("aggregate.merge").field("aggregate", index))?;
        }
        Ok(())
    }
    /// The group columns, the aggregate columns and the row count.
    fn finish_columns(self) -> Result<(Vec<ArrayRef>, Vec<ArrayRef>, usize)> {
        let count = self.index.len();
        let groups = self.index.columns()?;
        let mut columns = vec![];
        for (index, accumulator) in self.accumulators.into_iter().enumerate() {
            columns.push(accumulator.evaluate().with_context(|| {
                ErrorContext::new("aggregate.evaluate").field("aggregate", index)
            })?);
        }
        Ok((groups, columns, count))
    }
    #[cfg(test)]
    fn finish(self) -> Result<RecordBatch> {
        let schema = self.output_schema.clone();
        let (mut columns, aggregates, count) = self.finish_columns()?;
        columns.extend(aggregates);
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )
        .map_err(|source| {
            Error::invalid_plan("aggregate output does not match its declared schema".into())
                .with_source(source)
        })
    }
}

#[derive(Debug, Default)]
struct AggregateSharedState {
    finalized: OnceLock<RecordBatch>,
}

/// Created for one execution. Only the result cell is shared across pipelines.
pub struct AggregateExecuteState {
    merges: Vec<AggregateMergeState>,
    factory: Arc<MergeFactory>,
    layout: Layout,
    result: Arc<AggregateSharedState>,
}

/// Makes the merge states of grouping sets, one per partition.
struct MergeFactory {
    operator: AggregateOperator,
    columns: ColumnContext,
    sets: Vec<Vec<usize>>,
}

impl MergeFactory {
    fn merge(&self, set: usize) -> Result<AggregateMergeState> {
        AggregateMergeState::for_set(&self.operator, &self.columns, &self.sets[set])
    }
}

/// How grouping-set results form the output.
struct Layout {
    schema: SchemaRef,
    groups: usize,
    sets: Vec<Vec<usize>>,
    grouping_id: bool,
}

/// Collects prepared partitions, then merges and evaluates each on a CPU worker.
pub struct AggregateSinkExec {
    states: Vec<Vec<StateId>>,
    merges: Option<Vec<AggregateMergeState>>,
    partials: Vec<Partitioned>,
    factory: Arc<MergeFactory>,
    flush_groups: Option<usize>,
    layout: Layout,
    result: Arc<AggregateSharedState>,
}

impl AggregateSinkExec {
    /// Optional capacity preflight for root-domain grouping kernels. A run
    /// after Expand can add more than batch_rows groups; leave this disabled
    /// for those programs until an expansion bound is available.
    pub fn with_flush_groups(mut self, groups: usize) -> Self {
        self.flush_groups = Some(groups);
        self
    }
}

struct AggregateLocalSink {
    partitions: usize,
    states: Vec<Vec<StateId>>,
    batch_rows: usize,
    flush_groups: Option<usize>,
    partials: Partitioned,
}
impl LocalSink for AggregateLocalSink {
    fn consume(
        &mut self,
        _: Option<RecordBatch>,
        states: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(limit) = self.flush_groups {
            for set in &self.states {
                if states
                    .get_mut::<GroupState>(set[0])?
                    .index
                    .len()
                    .saturating_add(self.batch_rows)
                    > limit
                {
                    return Ok(ConsumeStatus::Flush);
                }
            }
        }
        Ok(ConsumeStatus::End)
    }
    fn finish(
        &mut self,
        status: ConsumeStatus,
        states_context: &mut StateContext<'_>,
        guard: &ShutdownGuard,
    ) -> Result<()> {
        self.partials.clear();
        for set in &self.states {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let (groups, states) = crate::kernel::aggregation::partial(states_context, set)?;
            let partial = PartialAggregate { groups, states };
            let index = &states_context.get_mut::<GroupState>(set[0])?.index;
            self.partials
                .push(partial.partitions(index, self.partitions, guard)?);
            if status == ConsumeStatus::End {
                drop(states_context.take::<GroupState>(set[0])?);
                for &id in &set[1..] {
                    drop(states_context.take::<AccumulatorState>(id)?);
                }
            }
        }
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct AggregateSourceExec {
    result: Arc<AggregateSharedState>,
    next_row: usize,
}
impl AggregateOperator {
    pub fn new_execute_state(&self, columns: &ColumnContext) -> Result<AggregateExecuteState> {
        let sets = self.sets();
        Ok(AggregateExecuteState {
            merges: sets
                .iter()
                .map(|set| AggregateMergeState::for_set(self, columns, set))
                .collect::<Result<_>>()?,
            factory: Arc::new(MergeFactory {
                operator: self.clone(),
                columns: columns.clone(),
                sets: sets.clone(),
            }),
            layout: Layout {
                schema: columns.schema(&self.output_columns())?,
                groups: self.groups().len(),
                sets,
                grouping_id: self.grouping_id().is_some(),
            },
            result: Arc::new(AggregateSharedState::default()),
        })
    }
    /// `locals` are the states of each grouping set, from `build_set_kernels`.
    pub fn build_exec(
        &self,
        state: AggregateExecuteState,
        locals: Vec<Vec<StateId>>,
    ) -> (AggregateSourceExec, AggregateSinkExec) {
        let source = AggregateSourceExec {
            result: state.result.clone(),
            next_row: 0,
        };
        let sink = AggregateSinkExec {
            states: locals,
            merges: Some(state.merges),
            factory: state.factory,
            partials: vec![],
            flush_groups: None,
            layout: state.layout,
            result: state.result,
        };
        (source, sink)
    }
}

impl Layout {
    /// The rows of every set, with NULL group columns outside each set.
    fn assemble(&self, results: Vec<(Vec<ArrayRef>, Vec<ArrayRef>, usize)>) -> Result<RecordBatch> {
        use crate::arrow::array::{UInt64Array, new_null_array};
        let fields = self.schema.fields();
        let mut columns: Vec<Vec<ArrayRef>> = vec![vec![]; fields.len()];
        let mut rows = 0;
        for (set, (groups, aggregates, count)) in self.sets.iter().zip(results) {
            rows += count;
            let mut groups = groups.into_iter();
            for g in 0..self.groups {
                columns[g].push(if set.contains(&g) {
                    groups.next().unwrap()
                } else {
                    new_null_array(fields[g].data_type(), count)
                });
            }
            let mut next = self.groups;
            if self.grouping_id {
                let id = (0..self.groups)
                    .filter(|g| !set.contains(g))
                    .fold(0u64, |id, g| id | 1 << (self.groups - 1 - g));
                let ids: ArrayRef = Arc::new(UInt64Array::from(vec![id; count]));
                let t = fields[next].data_type();
                let cast = crate::kernel::scalar::encode(t, &crate::kernel::scalar::decode(&ids)?)?;
                columns[next].push(cast);
                next += 1;
            }
            for (i, a) in aggregates.into_iter().enumerate() {
                columns[next + i].push(a);
            }
        }
        let arrays = columns
            .into_iter()
            .zip(fields.iter())
            .map(|(parts, f)| crate::kernel::table::concat(parts.iter(), f.data_type()))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|source| {
            Error::invalid_plan("aggregate output does not match its declared schema".into())
                .with_source(source)
        })
    }
}

impl SinkExec for AggregateSinkExec {
    fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        if !ctx.partitions.is_power_of_two() {
            return Err(Error::invalid_plan(
                "aggregate partitions must be a power of two".into(),
            ));
        }
        Ok(Box::new(AggregateLocalSink {
            partitions: ctx.partitions,
            states: self.states.clone(),
            batch_rows: ctx.batch_rows,
            flush_groups: self.flush_groups,
            partials: vec![],
        }))
    }
    fn consume<'a>(
        &'a mut self,
        status: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        mut states: StateContext<'a>,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let local = downcast_local::<AggregateLocalSink>(local)?;
            self.partials.push(std::mem::take(&mut local.partials));
            if status != ConsumeStatus::End {
                for set in &self.states {
                    if guard.is_shutdown_requested() {
                        return Err(Error::cancelled());
                    }
                    states.get_mut::<GroupState>(set[0])?.index.clear();
                    for &id in &set[1..] {
                        states.get_mut::<AccumulatorState>(id)?.accumulator.clear();
                    }
                }
            }
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn super::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let merges = self
                .merges
                .take()
                .ok_or_else(|| Error::internal("aggregate already finished".into()))?;
            let partials = std::mem::take(&mut self.partials);
            let groups = partials
                .iter()
                .flatten()
                .flatten()
                .map(PartialAggregate::num_rows)
                .sum::<usize>();
            if partials.iter().any(|p| p.len() != merges.len()) {
                return Err(Error::internal("missing grouping-set partial".into()));
            }
            let mut results = vec![];
            if groups < PARTITION_GROUPS {
                let mut merges = merges;
                for local in partials {
                    for (merge, parts) in merges.iter_mut().zip(local) {
                        for partial in parts {
                            if guard.is_shutdown_requested() {
                                return Err(Error::cancelled());
                            }
                            merge.merge(&partial)?;
                        }
                    }
                }
                for merge in merges {
                    if guard.is_shutdown_requested() {
                        return Err(Error::cancelled());
                    }
                    results.push(merge.finish_columns()?);
                }
            } else {
                let mut inputs = (0..merges.len())
                    .map(|_| Vec::<Vec<PartialAggregate>>::new())
                    .collect::<Vec<_>>();
                for local in partials {
                    for (buckets, parts) in inputs.iter_mut().zip(local) {
                        if buckets.is_empty() {
                            buckets.resize_with(parts.len(), Vec::new);
                        }
                        if buckets.len() != parts.len() {
                            return Err(Error::internal(
                                "inconsistent aggregate partitions".into(),
                            ));
                        }
                        for (bucket, part) in buckets.iter_mut().zip(parts) {
                            bucket.push(part);
                        }
                    }
                }
                let sets = merges.len();
                let mut jobs: Vec<Box<dyn FnOnce() -> Result<(usize, Merged)> + Send>> = vec![];
                for (set, (merge, buckets)) in merges.into_iter().zip(inputs).enumerate() {
                    let mut merge = Some(merge);
                    let mut buckets = buckets
                        .into_iter()
                        .filter(|b| b.iter().any(|p| p.num_rows() != 0))
                        .collect::<Vec<_>>();
                    // Empty keyed sets still need typed empty columns; an empty
                    // global set has exactly one initialized aggregate group.
                    if buckets.is_empty() {
                        buckets.push(vec![]);
                    }
                    for bucket in buckets {
                        let first = merge.take();
                        let factory = self.factory.clone();
                        let guard = guard.clone();
                        jobs.push(Box::new(move || {
                            if guard.is_shutdown_requested() {
                                return Err(Error::cancelled());
                            }
                            let mut merge = match first {
                                Some(m) => m,
                                None => factory.merge(set)?,
                            };
                            for partial in bucket {
                                if guard.is_shutdown_requested() {
                                    return Err(Error::cancelled());
                                }
                                merge.merge(&partial)?;
                            }
                            Ok((set, merge.finish_columns()?))
                        }));
                    }
                }
                let mut by_set = vec![vec![]; sets];
                for (set, result) in super::run_all(cpu, jobs).await? {
                    by_set[set].push(result);
                }
                for parts in by_set {
                    if guard.is_shutdown_requested() {
                        return Err(Error::cancelled());
                    }
                    results.push(concat_parts(&parts)?);
                }
            }
            let batch = self.layout.assemble(results)?;
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.result
                .finalized
                .set(batch)
                .map_err(|_| Error::internal("aggregate result already published".into()))
        })
    }
}

/// The group columns, the aggregate columns and the row count of a merge.
type Merged = (Vec<ArrayRef>, Vec<ArrayRef>, usize);

/// The groups of a set's partitions, one after another.
fn concat_parts(parts: &[Merged]) -> Result<Merged> {
    if let [one] = parts {
        return Ok(one.clone());
    }
    let column = |i: usize, f: fn(&Merged) -> &Vec<ArrayRef>| {
        let arrays = parts.iter().map(|p| &f(p)[i]);
        let t = f(&parts[0])[i].data_type().clone();
        crate::kernel::table::concat(arrays, &t)
    };
    let groups = (0..parts[0].0.len())
        .map(|i| column(i, |p| &p.0))
        .collect::<Result<_>>()?;
    let aggregates = (0..parts[0].1.len())
        .map(|i| column(i, |p| &p.1))
        .collect::<Result<_>>()?;
    Ok((groups, aggregates, parts.iter().map(|p| p.2).sum()))
}

impl SourceExec for AggregateSourceExec {
    fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
        if self.result.finalized.get().is_none() {
            return Err(Error::internal(
                "aggregate source started before sink finished".into(),
            ));
        }
        Ok(())
    }
    fn next_batch<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let batch = self
                .result
                .finalized
                .get()
                .ok_or_else(|| Error::internal("aggregate result is not ready".into()))?;
            if self.next_row >= batch.num_rows() {
                return Ok(None);
            }
            let rows = AGGREGATE_MORSEL_ROWS.min(batch.num_rows() - self.next_row);
            let output = batch.slice(self.next_row, rows);
            self.next_row += rows;
            Ok(Some(output))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{
        array::{Array, Float64Array, Int64Array, StringArray},
        datatypes::DataType,
    };
    use crate::column::ColumnId;
    use crate::error::ErrorKind;
    use crate::expr::{
        agg::{AggregateExpression, AggregateFunction},
        scalar::{
            ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
            ScalarExprRef, test_support::TestPlan,
        },
    };
    use crate::kernel::{ExecutionMode, KernelBuilder, LocalState, MODES, Program, agree};
    use asyncband::shutdown::Shutdown;

    /// One worker per execution mode: the aggregate's input program and its
    /// local state, plus a merge state standing in for the sink. Every input
    /// goes to all workers; their results must agree.
    struct AggregateState {
        workers: Vec<(ExecutionMode, Program, LocalState, AggregateMergeState)>,
        states: Vec<StateId>,
        _shutdown: Shutdown,
        guard: ShutdownGuard,
    }

    /// DISTINCT states list hash set members in unspecified order.
    fn same_partial(a: &PartialAggregate, b: &PartialAggregate) -> bool {
        use crate::arrow::array::AsArray;
        let members = |array: &ArrayRef| {
            let list = array.as_list::<i32>();
            (0..list.len())
                .map(|i| {
                    let mut values = (0..list.value(i).len())
                        .map(|j| list.value(i).as_binary::<i32>().value(j).to_vec())
                        .collect::<Vec<_>>();
                    values.sort();
                    values
                })
                .collect::<Vec<_>>()
        };
        let same = |x: &ArrayRef, y: &ArrayRef| match x.data_type() {
            DataType::List(_) => x.data_type() == y.data_type() && members(x) == members(y),
            _ => x == y,
        };
        a.groups == b.groups
            && a.states.len() == b.states.len()
            && a.states
                .iter()
                .zip(&b.states)
                .all(|(x, y)| x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y)))
    }

    impl AggregateState {
        fn new(plan: &TestPlan, operator: &AggregateOperator) -> Result<Self> {
            let mut workers = vec![];
            let mut states = vec![];
            for mode in MODES {
                let merged = AggregateMergeState::new(operator, &plan.columns)?;
                let mut builder = KernelBuilder::new(&plan.columns);
                builder.bind_inputs(&plan.inputs)?;
                states = operator.build_kernels(&mut builder)?;
                let program = Program::new(builder.finish(None)?, mode)?;
                let local = program.new_local()?;
                workers.push((mode, program, local, merged));
            }
            let (shutdown, guard) = asyncband::shutdown::new();
            Ok(Self {
                workers,
                states,
                _shutdown: shutdown,
                guard,
            })
        }
        fn update(&mut self, batch: &RecordBatch) -> Result<()> {
            let results = self
                .workers
                .iter_mut()
                .map(|(mode, program, local, _)| {
                    (*mode, program.run(batch, local, &self.guard).map(|_| ()))
                })
                .collect();
            agree(results, |_, _| true)
        }
        fn finish_partial(mut self) -> Result<PartialAggregate> {
            let results = self
                .workers
                .iter_mut()
                .map(|(mode, _, local, _)| {
                    let partial = crate::kernel::aggregation::partial(
                        &mut StateContext::new(local),
                        &self.states,
                    )
                    .map(|(groups, states)| PartialAggregate { groups, states });
                    (*mode, partial)
                })
                .collect();
            agree(results, same_partial)
        }
        fn merge(&mut self, partial: &PartialAggregate) -> Result<()> {
            let results = self
                .workers
                .iter_mut()
                .map(|(mode, _, _, merged)| (*mode, merged.merge(partial)))
                .collect();
            agree(results, |_, _| true)
        }
        fn finish(self) -> Result<RecordBatch> {
            let states = self.states;
            let results = self
                .workers
                .into_iter()
                .map(|(mode, _, mut local, mut merged)| {
                    let result = (|| {
                        let (groups, states) = crate::kernel::aggregation::partial(
                            &mut StateContext::new(&mut local),
                            &states,
                        )?;
                        merged.merge(&PartialAggregate { groups, states })?;
                        merged.finish()
                    })();
                    (mode, result)
                })
                .collect();
            agree(results, |a, b| a == b)
        }
    }

    /// Input columns `key: Utf8?` and `value: Float64?`.
    fn plan() -> TestPlan {
        TestPlan::new(&[(DataType::Utf8, true), (DataType::Float64, true)])
    }
    fn value(plan: &TestPlan) -> ScalarExprRef {
        ReferenceExpression::new(plan.input(1)).into_ref()
    }
    fn groups(plan: &TestPlan, grouped: bool) -> Vec<ScalarExprRef> {
        if grouped {
            vec![ReferenceExpression::new(plan.input(0)).into_ref()]
        } else {
            vec![]
        }
    }
    fn aggregate(
        plan: &mut TestPlan,
        function: AggregateFunction,
        arguments: Vec<ScalarExprRef>,
        data_type: DataType,
        nullable: bool,
    ) -> AggregateExpression {
        let output = plan.column(data_type, nullable);
        AggregateExpression::new(function, arguments, output)
    }
    /// Result types and nullability are host-supplied: SUM over Float64 is
    /// Float64, AVG and COVAR_POP are Float64, and only COUNT is non-nullable.
    fn aggregates(plan: &mut TestPlan) -> Vec<Arc<AggregateExpression>> {
        use AggregateFunction::*;
        let v = value(plan);
        vec![
            Arc::new(aggregate(
                plan,
                Sum,
                vec![v.clone()],
                DataType::Float64,
                true,
            )),
            Arc::new(aggregate(
                plan,
                Avg,
                vec![v.clone()],
                DataType::Float64,
                true,
            )),
            Arc::new(aggregate(
                plan,
                Count,
                vec![v.clone()],
                DataType::Int64,
                false,
            )),
            Arc::new(
                aggregate(plan, Count, vec![v.clone()], DataType::Int64, false).with_distinct(),
            ),
            Arc::new(aggregate(
                plan,
                CovarPop,
                vec![v.clone(), v],
                DataType::Float64,
                true,
            )),
            Arc::new(aggregate(plan, Count, vec![], DataType::Int64, false)),
        ]
    }
    fn input(plan: &TestPlan, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(plan.columns.schema(&plan.inputs).unwrap(), columns).unwrap()
    }
    fn batch(plan: &TestPlan, keys: Vec<Option<&str>>, values: Vec<Option<f64>>) -> RecordBatch {
        input(
            plan,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Float64Array::from(values)),
            ],
        )
    }
    fn empty(plan: &TestPlan) -> RecordBatch {
        RecordBatch::new_empty(plan.columns.schema(&plan.inputs).unwrap())
    }
    fn int(plan: &mut TestPlan, v: i64) -> ScalarExprRef {
        let output = plan.column(DataType::Int64, false);
        ConstantExpression::int64(Some(v), output).into_ref()
    }
    fn output_schema(plan: &TestPlan, operator: &AggregateOperator) -> SchemaRef {
        plan.columns.schema(&operator.output_columns()).unwrap()
    }
    fn group_schema(plan: &TestPlan, operator: &AggregateOperator) -> SchemaRef {
        let columns: Vec<ColumnId> = operator.group_columns();
        plan.columns.schema(&columns).unwrap()
    }

    /// `SELECT k, COUNT(*), SUM(x) WHERE x + 1 > 5 GROUP BY k`: group IDs only
    /// exist on rows the filter keeps; the slots of rejected rows hold other
    /// values, here huge negative ones.
    #[test]
    fn grouped_updates_under_a_filter_never_read_ids_of_rejected_rows() {
        use crate::expr::agg::AggregateFunction::{Count, Sum};
        use crate::operator::FilterOperator;
        let mut plan = TestPlan::new(&[(DataType::Int64, false), (DataType::Int64, false)]);
        let k = ReferenceExpression::new(plan.input(0)).into_ref();
        let x = ReferenceExpression::new(plan.input(1)).into_ref();
        let one = int(&mut plan, 1);
        let five = int(&mut plan, 5);
        let sum = plan.column(DataType::Int64, false);
        let add = FunctionExpression::binary(FunctionKind::Add, x.clone(), one, sum).into_ref();
        let above = plan.column(DataType::Boolean, false);
        let predicate =
            FunctionExpression::binary(FunctionKind::GreaterThan, add, five, above).into_ref();
        let filter = FilterOperator::new(predicate, plan.inputs.clone());
        let count = aggregate(&mut plan, Count, vec![], DataType::Int64, false);
        let total = aggregate(&mut plan, Sum, vec![x], DataType::Int64, true);
        let operator =
            AggregateOperator::try_new(vec![k], vec![Arc::new(count), Arc::new(total)]).unwrap();
        let n = 1024;
        let keys = Int64Array::from_iter_values((0..n).map(|i| i % 3));
        let xs = Int64Array::from_iter_values((0..n).map(|i| {
            if i % 2 == 0 {
                10
            } else {
                -1_000_000_000_000 - i
            }
        }));
        let batch = input(&plan, vec![Arc::new(keys), Arc::new(xs)]);
        let (_shutdown, guard) = asyncband::shutdown::new();
        for mode in MODES {
            let mut builder = KernelBuilder::new(&plan.columns);
            builder.bind_inputs(&plan.inputs).unwrap();
            filter.build_kernels(&mut builder).unwrap();
            let states = operator.build_kernels(&mut builder).unwrap();
            let program = Program::new(builder.finish(None).unwrap(), mode).unwrap();
            let mut local = program.new_local().unwrap();
            for _ in 0..3 {
                program.run(&batch, &mut local, &guard).unwrap();
            }
            let (groups, values) =
                crate::kernel::aggregation::partial(&mut StateContext::new(&mut local), &states)
                    .unwrap();
            let keys = groups
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = values[0][0].as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = values[1][0].as_any().downcast_ref::<Int64Array>().unwrap();
            let mut seen = (0..keys.len())
                .map(|g| (keys.value(g), counts.value(g), sums.value(g)))
                .collect::<Vec<_>>();
            seen.sort();
            // Even rows pass: per key 0 / 1 / 2, rows i = 6j / 6j + 4 / 6j + 2.
            assert_eq!(
                seen,
                [(0, 513, 5130), (1, 510, 5100), (2, 513, 5130)],
                "{mode:?}"
            );
        }
    }

    #[test]
    fn generated_global_reductions_handle_sliced_bitmaps_arbitrary_null_payloads_and_tails() {
        use crate::arrow::buffer::NullBuffer;
        use crate::expr::agg::AggregateFunction::{Count, Sum};
        let mut plan = TestPlan::new(&[(DataType::Int64, true)]);
        let reference = ReferenceExpression::new(plan.input(0)).into_ref();
        let sum = aggregate(
            &mut plan,
            Sum,
            vec![reference.clone()],
            DataType::Int64,
            true,
        );
        let count = aggregate(&mut plan, Count, vec![reference], DataType::Int64, false);
        let operator =
            AggregateOperator::try_new(vec![], vec![Arc::new(sum), Arc::new(count)]).unwrap();
        for pattern in [0, 1, 2, 17] {
            for offset in [0, 1, 7, 9] {
                let values = (0..150)
                    .map(|i| if i % 3 == 0 { i64::MAX } else { -(i as i64) })
                    .collect::<Vec<_>>();
                let valid = (0..150)
                    .map(|i| pattern == 0 || (pattern != 1 && i % pattern != 0))
                    .collect::<Vec<_>>();
                let values = Int64Array::new(values.into(), Some(NullBuffer::from(valid)))
                    .slice(offset, 140);
                for len in [0, 1, 7, 8, 63, 64, 65, 127, 129] {
                    let values = values.slice(0, len);
                    let expected = values.iter().flatten().fold(0i64, i64::wrapping_add);
                    let count = values.len() - values.null_count();
                    let batch = input(&plan, vec![Arc::new(values)]);
                    let mut state = AggregateState::new(&plan, &operator).unwrap();
                    state.update(&batch).unwrap();
                    // Repeated calls also exercise persisted state and wrapping merges.
                    state.update(&batch).unwrap();
                    let out = state.finish().unwrap();
                    let sum = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let actual_count = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(sum.is_null(0), count == 0);
                    if count != 0 {
                        assert_eq!(sum.value(0), expected.wrapping_mul(2));
                    }
                    assert_eq!(actual_count.value(0), 2 * count as i64);
                }
            }
        }
    }

    #[test]
    fn merges_distinct_and_multi_column_states_across_workers() {
        let mut plan = plan();
        let aggregates = aggregates(&mut plan);
        let operator = AggregateOperator::try_new(groups(&plan, true), aggregates).unwrap();
        let batches = [
            batch(
                &plan,
                vec![Some("a"), None, Some("a"), Some("b")],
                vec![Some(1.), Some(2.), Some(3.), None],
            ),
            batch(
                &plan,
                vec![Some("b"), Some("a"), None],
                vec![Some(8.), Some(3.), Some(4.)],
            ),
        ];
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        for batch in batches {
            let mut worker = AggregateState::new(&plan, &operator).unwrap();
            // Exercise repeated updates and one-row partial merge chunks.
            for row in 0..batch.num_rows() {
                worker.update(&batch.slice(row, 1)).unwrap();
            }
            let partial = worker.finish_partial().unwrap();
            assert_eq!(partial.groups.schema(), group_schema(&plan, &operator));
            for row in 0..partial.num_rows() {
                merged.merge(&partial.slice(row, 1)).unwrap();
            }
        }
        let result = merged.finish().unwrap();
        assert_eq!(result.schema(), output_schema(&plan, &operator));
        assert_eq!(result.num_rows(), 3);
        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..3 {
            let (sum, avg, count, distinct, covar, rows) = if keys.is_null(row) {
                (6., 3., 2, 2, 1., 2)
            } else if keys.value(row) == "a" {
                (7., 7. / 3., 3, 2, 8. / 9., 3)
            } else {
                assert_eq!(keys.value(row), "b");
                (8., 8., 1, 1, 0., 2)
            };
            for (column, expected) in [(1, sum), (2, avg), (5, covar)] {
                let array = result
                    .column(column)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert!(array.is_valid(row));
                assert!((array.value(row) - expected).abs() < 1e-10);
            }
            for (column, expected) in [(3, count), (4, distinct), (6, rows)] {
                let array = result
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(array.value(row), expected);
            }
        }
    }

    #[test]
    fn empty_global_input_has_one_row() {
        let mut plan = plan();
        let aggregates = aggregates(&mut plan);
        let operator = AggregateOperator::try_new(vec![], aggregates).unwrap();
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        for update_empty in [false, true] {
            let mut worker = AggregateState::new(&plan, &operator).unwrap();
            if update_empty {
                worker.update(&empty(&plan)).unwrap();
            }
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        for result in [
            merged.finish().unwrap(),
            AggregateState::new(&plan, &operator)
                .unwrap()
                .finish()
                .unwrap(),
        ] {
            assert_eq!(result.num_rows(), 1);
            for col in [0, 1, 4] {
                assert!(
                    result.column(col).is_null(0),
                    "column {col}: {:?}",
                    result.column(col)
                );
            }
            for col in [2, 3, 5] {
                assert_eq!(
                    result
                        .column(col)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    0
                );
            }
        }
    }

    #[test]
    fn all_null_group_stays_null_across_empty_batches_and_merges() {
        let mut plan = plan();
        let aggregates = aggregates(&mut plan);
        let operator = AggregateOperator::try_new(groups(&plan, true), aggregates).unwrap();
        let mut worker = AggregateState::new(&plan, &operator).unwrap();
        worker
            .update(&batch(&plan, vec![Some("a"); 2], vec![None; 2]))
            .unwrap();
        worker.update(&empty(&plan)).unwrap();
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        merged.merge(&worker.finish_partial().unwrap()).unwrap();
        merged
            .merge(
                &AggregateState::new(&plan, &operator)
                    .unwrap()
                    .finish_partial()
                    .unwrap(),
            )
            .unwrap();
        let result = merged.finish().unwrap();
        assert_eq!(result.num_rows(), 1);
        for col in [1, 2, 5] {
            assert!(result.column(col).is_null(0));
        }
        for (col, expected) in [(3, 0), (4, 0), (6, 2)] {
            assert_eq!(
                result
                    .column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }

    #[test]
    fn global_input_merges_distinct_and_multi_column_states() {
        let mut plan = plan();
        let aggregates = aggregates(&mut plan);
        let operator = AggregateOperator::try_new(vec![], aggregates).unwrap();
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        for values in [vec![Some(2.), None], vec![Some(2.), Some(4.)]] {
            let mut worker = AggregateState::new(&plan, &operator).unwrap();
            worker
                .update(&batch(&plan, vec![None; values.len()], values))
                .unwrap();
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        let result = merged.finish().unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            8.
        );
        assert_eq!(
            result
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            result
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4
        );
    }

    #[test]
    fn empty_grouped_input_and_grouping_without_aggregates() {
        let mut plan = plan();
        for aggregates in [aggregates(&mut plan), vec![]] {
            let operator = AggregateOperator::try_new(groups(&plan, true), aggregates).unwrap();
            let mut state = AggregateState::new(&plan, &operator).unwrap();
            state.update(&empty(&plan)).unwrap();
            let partial = state.finish_partial().unwrap();
            assert_eq!(partial.num_rows(), 0);
            assert_eq!(partial.groups.schema(), group_schema(&plan, &operator));
            for (arrays, aggregate) in partial.states.iter().zip(operator.aggregates()) {
                let fields = aggregate.accumulator(&plan.columns).unwrap().state_types();
                assert_eq!(arrays.len(), fields.len());
                for (array, field) in arrays.iter().zip(fields) {
                    assert_eq!(array.len(), 0);
                    assert_eq!(array.data_type(), &field);
                }
            }
            let mut merged = AggregateState::new(&plan, &operator).unwrap();
            merged.merge(&partial).unwrap();
            let result = merged.finish().unwrap();
            assert_eq!(result.num_rows(), 0);
            assert_eq!(result.schema(), output_schema(&plan, &operator));
        }

        let operator = AggregateOperator::try_new(groups(&plan, true), vec![]).unwrap();
        let mut worker = AggregateState::new(&plan, &operator).unwrap();
        worker
            .update(&batch(
                &plan,
                vec![Some("a"), None, Some("a")],
                vec![None; 3],
            ))
            .unwrap();
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        merged.merge(&worker.finish_partial().unwrap()).unwrap();
        let result = merged.finish().unwrap();
        assert_eq!((result.num_rows(), result.num_columns()), (2, 1));
        assert_eq!(result.column(0).null_count(), 1);
    }

    #[test]
    fn rejects_empty_specification() {
        assert!(AggregateOperator::try_new(vec![], vec![]).is_err());
    }

    #[test]
    fn aggregate_filter_runs_before_arguments_and_preserves_empty_groups() {
        let mut plan = TestPlan::new(&[(DataType::Utf8, false), (DataType::Int64, false)]);
        let value = ReferenceExpression::new(plan.input(1)).into_ref();
        let zero = int(&mut plan, 0);
        let hundred = int(&mut plan, 100);
        let ne = plan.column(DataType::Boolean, false);
        let predicate =
            FunctionExpression::binary(FunctionKind::NotEqual, value.clone(), zero, ne).into_ref();
        let quotient = plan.column(DataType::Int64, true);
        let division =
            FunctionExpression::binary(FunctionKind::Divide, hundred, value.clone(), quotient)
                .into_ref();
        let sum = aggregate(
            &mut plan,
            AggregateFunction::Sum,
            vec![division],
            DataType::Int64,
            true,
        )
        .with_filter(predicate.clone());
        let distinct = aggregate(
            &mut plan,
            AggregateFunction::Count,
            vec![value],
            DataType::Int64,
            false,
        )
        .with_distinct()
        .with_filter(predicate);
        let operator = AggregateOperator::try_new(
            vec![ReferenceExpression::new(plan.input(0)).into_ref()],
            vec![Arc::new(sum), Arc::new(distinct)],
        )
        .unwrap();
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        for _ in 0..2 {
            let input = input(
                &plan,
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "a"])),
                    Arc::new(Int64Array::from(vec![2, 0, 4])),
                ],
            );
            let mut worker = AggregateState::new(&plan, &operator).unwrap();
            worker.update(&input).unwrap();
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        let result = merged.finish().unwrap();
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(150), None]
        );
        assert_eq!(
            result
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2, 0]
        );
    }

    #[test]
    fn extrema_merge_values_and_keep_string_null_semantics() {
        let mut plan = TestPlan::new(&[(DataType::Utf8, true)]);
        let expr = ReferenceExpression::new(plan.input(0)).into_ref();
        let aggregates = [AggregateFunction::Min, AggregateFunction::Max]
            .into_iter()
            .map(|f| {
                Arc::new(aggregate(
                    &mut plan,
                    f,
                    vec![expr.clone()],
                    DataType::Utf8,
                    true,
                ))
            })
            .collect();
        let operator = AggregateOperator::try_new(vec![], aggregates).unwrap();
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        for values in [vec![Some("z"), None], vec![None, Some("a")], vec![None]] {
            let input = input(&plan, vec![Arc::new(StringArray::from(values))]);
            let mut worker = AggregateState::new(&plan, &operator).unwrap();
            worker.update(&input).unwrap();
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        let result = merged.finish().unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "z"
        );
        let empty = AggregateState::new(&plan, &operator)
            .unwrap()
            .finish()
            .unwrap();
        assert!(empty.column(0).is_null(0));
        assert!(empty.column(1).is_null(0));
    }

    #[test]
    fn integer_sum_stays_exact_and_wraps_on_update_and_partial_merge() {
        use crate::arrow::array::UInt64Array;
        for (array, expected) in [
            (
                Arc::new(UInt64Array::from(vec![
                    Some(9_007_199_254_740_993),
                    None,
                    Some(2),
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![9_007_199_254_740_995])) as ArrayRef,
            ),
            (
                Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef,
            ),
            (
                Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(-1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef,
            ),
            (
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(1)])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
            ),
        ] {
            let data_type = array.data_type().clone();
            let mut plan = TestPlan::new(&[(data_type.clone(), true)]);
            let reference = ReferenceExpression::new(plan.input(0)).into_ref();
            let sum = aggregate(
                &mut plan,
                AggregateFunction::Sum,
                vec![reference],
                data_type.clone(),
                true,
            );
            let operator = AggregateOperator::try_new(vec![], vec![Arc::new(sum)]).unwrap();
            let mut state = AggregateState::new(&plan, &operator).unwrap();
            let input = input(&plan, vec![array]);
            state.update(&input).unwrap();

            // Each worker's value fits by itself; overflow occurs only when
            // partial states combine. Include a NULL-only worker as well.
            let mut merged = AggregateState::new(&plan, &operator).unwrap();
            for row in 0..input.num_rows() {
                let mut worker = AggregateState::new(&plan, &operator).unwrap();
                worker.update(&input.slice(row, 1)).unwrap();
                merged.merge(&worker.finish_partial().unwrap()).unwrap();
            }
            for result in [state.finish().unwrap(), merged.finish().unwrap()] {
                assert_eq!(
                    result.column(0).to_data(),
                    expected.to_data(),
                    "SUM over {data_type}"
                );
            }
        }
    }

    #[test]
    fn filtered_count_star_handles_zero_column_batches() {
        let mut plan = TestPlan::new(&[]);
        let expressions = [true, false]
            .into_iter()
            .map(|v| {
                let predicate = plan.column(DataType::Boolean, false);
                let filter = ConstantExpression::boolean(Some(v), predicate).into_ref();
                Arc::new(
                    aggregate(
                        &mut plan,
                        AggregateFunction::Count,
                        vec![],
                        DataType::Int64,
                        false,
                    )
                    .with_filter(filter),
                )
            })
            .collect();
        let operator = AggregateOperator::try_new(vec![], expressions).unwrap();
        let mut state = AggregateState::new(&plan, &operator).unwrap();
        let batch = RecordBatch::try_new_with_options(
            plan.columns.schema(&[]).unwrap(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        state.update(&batch).unwrap();
        let result = state.finish().unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }

    #[test]
    fn aggregate_signatures_without_usable_kernels_are_plan_errors() {
        // DISTINCT and MIN/MAX derive their state key type from an argument, so
        // a missing argument has no kernel to build. The operator itself only
        // assembles descriptions; building its kernels is the first point that
        // builds an accumulator.
        use AggregateFunction::*;
        let mut plan = plan();
        let v = value(&plan);
        for expr in [
            aggregate(&mut plan, Count, vec![], DataType::Int64, false).with_distinct(),
            aggregate(&mut plan, Min, vec![], DataType::Utf8, true),
            aggregate(&mut plan, Max, vec![], DataType::Utf8, true),
        ] {
            let operator = AggregateOperator::try_new(vec![], vec![Arc::new(expr)])
                .expect("operator construction does not validate aggregates");
            let error = AggregateState::new(&plan, &operator).err().unwrap();
            assert_eq!(error.kind(), ErrorKind::InvalidPlan, "{error:?}");
        }
        // Only COUNT keeps a DISTINCT set; the others would silently drop it.
        let sum = aggregate(&mut plan, Sum, vec![v], DataType::Float64, true).with_distinct();
        let operator = AggregateOperator::try_new(vec![], vec![Arc::new(sum)]).unwrap();
        let error = AggregateState::new(&plan, &operator).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{error:?}");
    }

    #[test]
    fn aggregate_signatures_are_checked_before_code_generation() {
        use AggregateFunction::*;
        let mut plan = plan();
        let v = value(&plan);
        for expr in [
            aggregate(
                &mut plan,
                Count,
                vec![v.clone(), v.clone()],
                DataType::Int64,
                false,
            ),
            aggregate(&mut plan, Sum, vec![], DataType::Float64, true),
            aggregate(&mut plan, Avg, vec![], DataType::Float64, true),
            aggregate(
                &mut plan,
                CovarPop,
                vec![v.clone()],
                DataType::Float64,
                true,
            ),
            aggregate(&mut plan, Min, vec![v.clone()], DataType::Utf8, true),
            aggregate(&mut plan, Count, vec![], DataType::Float64, false),
        ] {
            let operator = AggregateOperator::try_new(vec![], vec![Arc::new(expr)]).unwrap();
            let error = AggregateState::new(&plan, &operator).err().unwrap();
            assert_eq!(error.kind(), ErrorKind::InvalidPlan, "{error:?}");
        }

        // SUM inserts a TRY cast; unparseable text becomes NULL.
        let text = plan.column(DataType::Utf8, false);
        let text = ConstantExpression::string(Some("x"), text).into_ref();
        let sum = aggregate(&mut plan, Sum, vec![text], DataType::Int64, true);
        let operator = AggregateOperator::try_new(vec![], vec![Arc::new(sum)]).unwrap();
        let mut state = AggregateState::new(&plan, &operator).unwrap();
        state
            .update(&batch(&plan, vec![Some("a")], vec![Some(1.)]))
            .expect("an unparseable value is skipped, not rejected");
        assert!(state.finish().unwrap().column(0).is_null(0));

        // A filter must evaluate to Boolean before rows can be selected.
        let rows = aggregate(&mut plan, Count, vec![], DataType::Int64, false).with_filter(v);
        let operator = AggregateOperator::try_new(vec![], vec![Arc::new(rows)]).unwrap();
        let error = AggregateState::new(&plan, &operator).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidPlan, "{error:?}");
    }

    #[test]
    fn unsupported_sum_result_type_is_rejected_before_codegen() {
        // The host declares the SUM result type; an unusable one must not panic.
        let mut plan = plan();
        let v = value(&plan);
        let sum = aggregate(
            &mut plan,
            AggregateFunction::Sum,
            vec![v],
            DataType::Utf8,
            true,
        );
        let operator = AggregateOperator::try_new(vec![], vec![Arc::new(sum)]).unwrap();
        let error = AggregateState::new(&plan, &operator).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert_eq!(error.message(), "unsupported sum result type: Utf8");
        assert_eq!(
            error.frames()[0].context.as_ref().unwrap().operation,
            "aggregate.bind"
        );
    }

    #[test]
    fn integer_groups_remap_partial_ids_and_keep_null_and_wrapping_sum() {
        use std::collections::BTreeMap;
        let mut plan = TestPlan::new(&[(DataType::Int64, true), (DataType::Int64, true)]);
        let value = ReferenceExpression::new(plan.input(1)).into_ref();
        let sum = aggregate(
            &mut plan,
            AggregateFunction::Sum,
            vec![value.clone()],
            DataType::Int64,
            true,
        );
        let count = aggregate(
            &mut plan,
            AggregateFunction::Count,
            vec![value],
            DataType::Int64,
            false,
        );
        let operator = AggregateOperator::try_new(
            vec![ReferenceExpression::new(plan.input(0)).into_ref()],
            vec![Arc::new(sum), Arc::new(count)],
        )
        .unwrap();
        let inputs = [
            (
                vec![Some(i64::MIN), None, Some(0), Some(i64::MAX)],
                vec![Some(i64::MAX), None, Some(2), Some(4)],
            ),
            (
                vec![Some(i64::MAX), Some(i64::MIN), None, Some(1)],
                vec![Some(3), Some(1), None, Some(-1)],
            ),
        ];
        let mut merged = AggregateState::new(&plan, &operator).unwrap();
        for (keys, values) in inputs {
            let input = input(
                &plan,
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            );
            let mut partial = AggregateState::new(&plan, &operator).unwrap();
            partial.update(&input).unwrap();
            merged.merge(&partial.finish_partial().unwrap()).unwrap();
        }
        let output = merged.finish().unwrap();
        let arrays = output
            .columns()
            .iter()
            .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect::<Vec<_>>();
        let actual = arrays[0]
            .iter()
            .zip(arrays[1].iter().zip(arrays[2].iter()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            actual,
            BTreeMap::from([
                (None, (None, Some(0))),
                (Some(i64::MIN), (Some(i64::MIN), Some(2))),
                (Some(0), (Some(2), Some(1))),
                (Some(1), (Some(-1), Some(1))),
                (Some(i64::MAX), (Some(7), Some(2))),
            ])
        );
    }
    #[test]
    fn repeated_flush_resets_all_grouping_sets_and_accumulators() {
        use crate::expr::agg::AggregateFunction::{Count, Min, Sum};
        use futures::executor::block_on;
        for key_type in [DataType::Int64, DataType::Utf8] {
            let mut plan = TestPlan::new(&[(key_type.clone(), true), (DataType::Int64, true)]);
            let key = ReferenceExpression::new(plan.input(0)).into_ref();
            let value = ReferenceExpression::new(plan.input(1)).into_ref();
            let count = aggregate(&mut plan, Count, vec![], DataType::Int64, false);
            let sum = aggregate(&mut plan, Sum, vec![value.clone()], DataType::Int64, true);
            let min = aggregate(&mut plan, Min, vec![key.clone()], key_type.clone(), true);
            let distinct = aggregate(
                &mut plan,
                Count,
                vec![value.clone()],
                DataType::Int64,
                false,
            )
            .with_distinct();
            let id = plan.column(DataType::UInt64, false);
            let operator = AggregateOperator::try_new(
                vec![key, value],
                vec![
                    Arc::new(count),
                    Arc::new(sum),
                    Arc::new(min),
                    Arc::new(distinct),
                ],
            )
            .unwrap()
            .with_grouping_sets(vec![vec![0], vec![0, 1], vec![]], id)
            .unwrap();
            let keys: ArrayRef = if key_type == DataType::Int64 {
                Arc::new(Int64Array::from_iter(
                    (0..2048).map(|i| (i % 19 != 0).then_some(i % 257)),
                ))
            } else {
                Arc::new(StringArray::from_iter(
                    (0..2048).map(|i| (i % 19 != 0).then(|| format!("key{}", i % 257))),
                ))
            };
            let batch = input(
                &plan,
                vec![
                    keys,
                    Arc::new(Int64Array::from_iter_values((0..2048).map(|i| i % 7))),
                ],
            );
            for mode in MODES {
                let mut expected = None;
                for (threshold, partitions) in [None, Some(64)]
                    .into_iter()
                    .flat_map(|t| [1, 2, 8].map(|p| (t, p)))
                {
                    let mut builder = KernelBuilder::new(&plan.columns);
                    builder.bind_inputs(&plan.inputs).unwrap();
                    let states = operator.build_set_kernels(&mut builder).unwrap();
                    let program = Program::new(builder.finish(None).unwrap(), mode).unwrap();
                    let execute = operator.new_execute_state(&plan.columns).unwrap();
                    let (_, mut sink) = operator.build_exec(execute, states.clone());
                    if let Some(limit) = threshold {
                        sink = sink.with_flush_groups(limit);
                    }
                    let guard = asyncband::shutdown::new().1;
                    let mut pairs = (0..3)
                        .map(|_| {
                            (
                                program.new_local().unwrap(),
                                sink.local_sink(&LocalSinkContext {
                                    batch_rows: 32,
                                    partitions,
                                })
                                .unwrap(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut flushes = 0;
                    for (i, offset) in (0..batch.num_rows()).step_by(32).enumerate() {
                        let (state, local) = &mut pairs[i % 3];
                        let chunk = program
                            .run(&batch.slice(offset, 32), state, &guard)
                            .unwrap();
                        assert!(chunk.is_none());
                        let status = local.consume(chunk, &mut StateContext::new(state)).unwrap();
                        if status == ConsumeStatus::Flush {
                            flushes += 1;
                            local
                                .finish(status, &mut StateContext::new(state), &guard)
                                .unwrap();
                            // Export is borrowed: group slots are still populated.
                            assert!(
                                StateContext::new(state)
                                    .get_mut::<GroupState>(states[0][0])
                                    .unwrap()
                                    .index
                                    .len()
                                    > 0
                            );
                            block_on(sink.consume(
                                status,
                                &mut **local,
                                StateContext::new(state),
                                &guard,
                            ))
                            .unwrap();
                            for (set, ids) in operator.sets().iter().zip(&states) {
                                assert_eq!(
                                    StateContext::new(state)
                                        .get_mut::<GroupState>(ids[0])
                                        .unwrap()
                                        .index
                                        .len(),
                                    usize::from(set.is_empty())
                                );
                            }
                        }
                    }
                    for (state, local) in &mut pairs {
                        local
                            .finish(ConsumeStatus::End, &mut StateContext::new(state), &guard)
                            .unwrap();
                        for set in &states {
                            assert!(
                                StateContext::new(state)
                                    .get_mut::<GroupState>(set[0])
                                    .is_err()
                            );
                            for &id in &set[1..] {
                                assert!(
                                    StateContext::new(state)
                                        .get_mut::<AccumulatorState>(id)
                                        .is_err()
                                );
                            }
                        }
                    }
                    for (state, local) in &mut pairs {
                        block_on(sink.consume(
                            ConsumeStatus::End,
                            &mut **local,
                            StateContext::new(state),
                            &guard,
                        ))
                        .unwrap();
                    }
                    block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
                    assert_eq!(flushes > 1, threshold.is_some());
                    let result = sink.result.finalized.get().unwrap();
                    let mut rows = (0..result.num_rows())
                        .map(|r| {
                            result
                                .columns()
                                .iter()
                                .map(|c| {
                                    arrow::util::display::array_value_to_string(c.as_ref(), r)
                                        .unwrap()
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    rows.sort();
                    if let Some(expected) = &expected {
                        assert_eq!(&rows, expected, "{key_type:?} {mode:?}");
                    } else {
                        expected = Some(rows);
                    }
                }
            }
        }
    }
    #[test]
    fn partitioned_rollup_crosses_merge_threshold_without_extra_global_groups() {
        use futures::executor::block_on;
        for key_type in [DataType::Int64, DataType::Utf8] {
            let mut plan = TestPlan::new(&[(key_type.clone(), true)]);
            let count = aggregate(
                &mut plan,
                AggregateFunction::Count,
                vec![],
                DataType::Int64,
                false,
            );
            let id = plan.column(DataType::UInt64, false);
            let operator = AggregateOperator::try_new(
                vec![ReferenceExpression::new(plan.input(0)).into_ref()],
                vec![Arc::new(count)],
            )
            .unwrap()
            .with_grouping_sets(vec![vec![0], vec![]], id)
            .unwrap();
            for n in [0, 100, 9000] {
                for partitions in [1, 2, 8] {
                    let execute = operator.new_execute_state(&plan.columns).unwrap();
                    let (_, mut sink) = operator.build_exec(execute, vec![]);
                    let guard = asyncband::shutdown::new().1;
                    for _ in 0..2 {
                        let mut local = vec![];
                        for set in 0..2 {
                            let mut merge = sink.factory.merge(set).unwrap();
                            let schema = plan
                                .columns
                                .schema(if set == 0 { &plan.inputs } else { &[] })
                                .unwrap();
                            let rows = if set == 0 { n } else { 1 };
                            let cols: Vec<ArrayRef> = if set != 0 {
                                vec![]
                            } else if key_type == DataType::Int64 {
                                vec![Arc::new(Int64Array::from_iter(
                                    (0..n).map(|i| (i != 0).then_some(i as i64)),
                                ))]
                            } else {
                                vec![Arc::new(StringArray::from_iter(
                                    (0..n).map(|i| (i != 0).then(|| format!("k{i}"))),
                                ))]
                            };
                            let groups = RecordBatch::try_new_with_options(
                                schema,
                                cols,
                                &RecordBatchOptions::new().with_row_count(Some(rows)),
                            )
                            .unwrap();
                            let count = if set == 0 { 1 } else { n as i64 };
                            let partial = PartialAggregate {
                                groups,
                                states: vec![vec![Arc::new(Int64Array::from(vec![count; rows]))]],
                            };
                            // Both instances map the same key to the same bucket.
                            local.push(
                                partial
                                    .partitions(&merge.index, partitions, &guard)
                                    .unwrap(),
                            );
                            // Direct merge is also a check of the hand-built COUNT state.
                            merge.merge(&partial).unwrap();
                        }
                        sink.partials.push(local);
                    }
                    block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
                    let out = sink.result.finalized.get().unwrap();
                    assert_eq!(out.num_rows(), n + 1);
                    let count = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                    let grouping = out
                        .column(1)
                        .as_any()
                        .downcast_ref::<crate::arrow::array::UInt64Array>()
                        .unwrap();
                    assert_eq!(grouping.iter().filter(|v| *v == Some(1)).count(), 1);
                    for r in 0..out.num_rows() {
                        assert_eq!(
                            count.value(r),
                            if grouping.value(r) == 1 {
                                2 * n as i64
                            } else {
                                2
                            }
                        );
                    }
                }
            }
        }
    }
}
