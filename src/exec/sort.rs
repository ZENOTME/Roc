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

//! Sorting by keys as byte strings that compare like the keys.
use super::{
    ConsumeStatus, LocalSink, LocalSinkContext, PartitionedResult, SinkExec, StateContext,
    downcast_local,
};
use crate::arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, Result};
use crate::kernel::scalar;
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::{cmp::Reverse, collections::BinaryHeap};

/// One sort key: a column of the sorted chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SortKey {
    pub column: usize,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Row keys of `columns` encoded so that byte order is key order: per key a
/// NULL marker, then the value's order-preserving bytes, inverted when
/// descending. Floats order totally, strings by bytes.
pub(crate) struct RowKeys {
    data: Vec<u8>,
    offsets: Vec<usize>,
}

impl Default for RowKeys {
    fn default() -> Self {
        Self {
            data: vec![],
            offsets: vec![0],
        }
    }
}

impl RowKeys {
    pub fn push(&mut self, columns: &[ArrayRef], keys: &[SortKey], rows: usize) -> Result<()> {
        let decoded = keys
            .iter()
            .map(|k| scalar::decode(&columns[k.column]))
            .collect::<Result<Vec<_>>>()?;
        let encoders = keys
            .iter()
            .map(|k| scalar::bind_key(columns[k.column].data_type()))
            .collect::<Result<Vec<_>>>()?;
        let (data, offsets) = (&mut self.data, &mut self.offsets);
        offsets.reserve(rows);
        let mut value = vec![];
        for r in 0..rows {
            for ((key, cells), encode) in keys.iter().zip(&decoded).zip(&encoders) {
                let cell = cells[r];
                if cell.valid == 0 {
                    data.push(if key.nulls_first { 0 } else { 2 });
                    continue;
                }
                data.push(1);
                value.clear();
                encode(cell, &mut value);
                // The encoder's own validity byte is redundant here.
                let payload = &value[1..];
                if key.descending {
                    data.extend(payload.iter().map(|b| !b));
                } else {
                    data.extend_from_slice(payload);
                }
            }
            offsets.push(data.len());
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn row(&self, r: usize) -> &[u8] {
        &self.data[self.offsets[r]..self.offsets[r + 1]]
    }
}

/// One chunk of the columns of `chunks`.
pub(crate) async fn concat_chunks_on(
    schema: &SchemaRef,
    chunks: Arc<Vec<RecordBatch>>,
    guard: &ShutdownGuard,
    cpu: &dyn super::Cpu,
) -> Result<RecordBatch> {
    let jobs = (0..schema.fields().len())
        .map(|i| {
            let chunks = chunks.clone();
            let ty = schema.field(i).data_type().clone();
            let guard = guard.clone();
            Box::new(move || {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                crate::kernel::table::concat(chunks.iter().map(|c| c.column(i)), &ty)
            }) as Box<dyn FnOnce() -> Result<crate::arrow::array::ArrayRef> + Send>
        })
        .collect();
    let columns = super::run_all(cpu, jobs).await?;
    let rows = chunks.iter().map(RecordBatch::num_rows).sum();
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|source| Error::internal("chunks do not concatenate".into()).with_source(source))
}

/// Gather payload columns exactly once, directly from their original chunks.
pub(crate) fn interleave_chunks(
    schema: &SchemaRef,
    chunks: &[RecordBatch],
    order: &[(usize, usize)],
    guard: &ShutdownGuard,
) -> Result<RecordBatch> {
    let columns = (0..schema.fields().len())
        .map(|i| {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let arrays = chunks
                .iter()
                .map(|c| c.column(i).as_ref())
                .collect::<Vec<_>>();
            if arrays.is_empty() {
                return Ok(crate::arrow::array::new_empty_array(
                    schema.field(i).data_type(),
                ));
            }
            arrow_select::interleave::interleave(&arrays, order).map_err(|source| {
                Error::internal("rows cannot be interleaved".into()).with_source(source)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(order.len())),
    )
    .map_err(|source| {
        Error::internal("interleaved columns do not match their schema".into()).with_source(source)
    })
}

/// Heap merge by encoded keys, run number and position within the run.
/// Borrow keys in the heap instead of copying them for every output row.
pub(crate) fn merge_sorted<'a>(
    orders: &[&[usize]],
    key: impl Fn(usize, usize) -> (&'a [u8], &'a [u8]),
    limit: usize,
    guard: &ShutdownGuard,
) -> Result<Vec<(usize, usize)>> {
    let mut heap = BinaryHeap::new();
    for (run, rows) in orders.iter().enumerate() {
        if let Some(&row) = rows.first() {
            let (part, order) = key(run, row);
            heap.push(Reverse((part, order, run, 0usize, row)));
        }
    }
    let total = orders.iter().map(|r| r.len()).sum::<usize>().min(limit);
    let mut out = Vec::with_capacity(total);
    while out.len() < total {
        if out.len() % 4096 == 0 && guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        let Reverse((_, _, run, pos, row)) = heap.pop().expect("a nonempty run");
        out.push((run, row));
        if let Some(&row) = orders[run].get(pos + 1) {
            let (part, order) = key(run, row);
            heap.push(Reverse((part, order, run, pos + 1, row)));
        }
    }
    Ok(out)
}

struct SortedRun {
    chunks: Vec<RecordBatch>,
    keys: RowKeys,
    order: Vec<usize>,
}

struct SortLocalSink {
    sort_keys: Vec<SortKey>,
    fetch: Option<usize>,
    chunks: Vec<RecordBatch>,
    keys: RowKeys,
    run: Option<SortedRun>,
}

impl LocalSink for SortLocalSink {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            self.keys
                .push(chunk.columns(), &self.sort_keys, chunk.num_rows())?;
            self.chunks.push(chunk);
        }
        Ok(ConsumeStatus::End)
    }
    fn finish(
        &mut self,
        _: ConsumeStatus,
        _: &mut StateContext<'_>,
        guard: &ShutdownGuard,
    ) -> Result<()> {
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        let mut order = (0..self.keys.len()).collect::<Vec<_>>();
        let compare =
            |a: &usize, b: &usize| self.keys.row(*a).cmp(self.keys.row(*b)).then(a.cmp(b));
        if let Some(n) = self.fetch.filter(|n| *n < order.len()) {
            if n != 0 {
                order.select_nth_unstable_by(n, compare);
            }
            order.truncate(n);
        }
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        order.sort_unstable_by(compare);
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        self.run = Some(SortedRun {
            chunks: std::mem::take(&mut self.chunks),
            keys: std::mem::take(&mut self.keys),
            order,
        });
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct SortSinkExec {
    buffer: Arc<PartitionedResult>,
    schema: SchemaRef,
    keys: Vec<SortKey>,
    fetch: Option<usize>,
    runs: Vec<SortedRun>,
}
impl SortSinkExec {
    pub fn new(
        buffer: Arc<PartitionedResult>,
        schema: SchemaRef,
        keys: Vec<SortKey>,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            buffer,
            schema,
            keys,
            fetch,
            runs: vec![],
        }
    }
}
impl SinkExec for SortSinkExec {
    fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        Ok(Box::new(SortLocalSink {
            sort_keys: self.keys.clone(),
            fetch: self.fetch,
            chunks: vec![],
            keys: RowKeys::default(),
            run: None,
        }))
    }
    fn consume<'a>(
        &'a mut self,
        _: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        _: StateContext<'a>,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let run = downcast_local::<SortLocalSink>(local)?
                .run
                .take()
                .ok_or_else(|| Error::internal("sort local was not finished".into()))?;
            self.runs.push(run);
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn crate::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let runs = std::mem::take(&mut self.runs)
                .into_iter()
                .filter(|r| !r.order.is_empty())
                .map(|run| {
                    let mut rows = 0;
                    let ends = run
                        .chunks
                        .iter()
                        .map(|c| {
                            rows += c.num_rows();
                            rows
                        })
                        .collect();
                    Arc::new(SortData { run, ends })
                })
                .collect::<Vec<_>>();
            let ranges = (4 * cpu.parallelism().max(1)).next_power_of_two();
            let mut samples = vec![];
            let total = runs.iter().map(|r| r.run.order.len()).sum::<usize>();
            let stride = (total / (ranges * 32)).max(1);
            for data in &runs {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let run = &data.run;
                samples.extend(
                    run.order
                        .iter()
                        .step_by(stride)
                        .map(|&r| run.keys.row(r).to_vec()),
                );
            }
            samples.sort_unstable();
            let mut bounds = (1..ranges)
                .filter_map(|i| samples.get(i * samples.len() / ranges).cloned())
                .collect::<Vec<_>>();
            bounds.dedup();
            let mut buckets = (0..=bounds.len()).map(|_| vec![]).collect::<Vec<_>>();
            for data in runs {
                let mut start = 0;
                for (i, bound) in bounds.iter().enumerate() {
                    let end = data
                        .run
                        .order
                        .partition_point(|&r| data.run.keys.row(r) < bound.as_slice());
                    if end != start {
                        buckets[i].push((data.clone(), start..end));
                    }
                    start = end;
                }
                if start != data.run.order.len() {
                    let end = data.run.order.len();
                    buckets.last_mut().unwrap().push((data, start..end));
                }
            }
            let mut remaining = self.fetch.unwrap_or(usize::MAX);
            let mut work: Vec<super::partitioned::PartitionWork> = vec![];
            for segments in buckets {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                if remaining == 0 {
                    break;
                }
                let rows = segments.iter().map(|(_, r)| r.len()).sum::<usize>();
                if rows == 0 {
                    continue;
                }
                let limit = rows.min(remaining);
                remaining -= limit;
                let range = SortRange {
                    segments,
                    schema: self.schema.clone(),
                    limit,
                };
                work.push(Box::new(move |guard| Ok(vec![range.compute(guard)?])));
            }
            self.buffer.publish(work, guard)
        })
    }
}

struct SortData {
    run: SortedRun,
    ends: Vec<usize>,
}
struct SortRange {
    segments: Vec<(Arc<SortData>, std::ops::Range<usize>)>,
    schema: SchemaRef,
    limit: usize,
}
impl SortRange {
    fn compute(self, guard: &ShutdownGuard) -> Result<RecordBatch> {
        let orders = self
            .segments
            .iter()
            .map(|(data, range)| &data.run.order[range.clone()])
            .collect::<Vec<_>>();
        let merged = merge_sorted(
            &orders,
            |run, row| (self.segments[run].0.run.keys.row(row), &[]),
            self.limit,
            guard,
        )?;
        let mut chunks = vec![];
        let mut bases = vec![];
        for (data, _) in &self.segments {
            bases.push(chunks.len());
            chunks.extend(data.run.chunks.iter().cloned());
        }
        let mut locations = Vec::with_capacity(merged.len());
        for (i, (run, row)) in merged.into_iter().enumerate() {
            if i % 4096 == 0 && guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let ends = &self.segments[run].0.ends;
            let chunk = ends.partition_point(|&end| end <= row);
            locations.push((
                bases[run] + chunk,
                row - if chunk == 0 { 0 } else { ends[chunk - 1] },
            ));
        }
        interleave_chunks(&self.schema, &chunks, &locations, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema},
        },
        kernel::LocalState,
    };
    use futures::executor::block_on;

    #[test]
    fn multiple_runs_append_keys_and_merge_ties_nulls_and_topk() {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                for fetch in [None, Some(0), Some(1), Some(5), Some(100)] {
                    let buffer = Arc::new(PartitionedResult::default());
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("k", DataType::Int64, true),
                        Field::new("id", DataType::Int64, false),
                    ]));
                    let mut sink = SortSinkExec::new(
                        buffer.clone(),
                        schema.clone(),
                        vec![SortKey {
                            column: 0,
                            descending,
                            nulls_first,
                        }],
                        fetch,
                    );
                    let guard = asyncband::shutdown::new().1;
                    let mut expected = vec![];
                    for run in 0..3 {
                        let mut state = LocalState::default();
                        let mut local = sink
                            .local_sink(&LocalSinkContext {
                                batch_rows: 2,
                                partitions: 1,
                            })
                            .unwrap();
                        for chunk in 0..2 {
                            let keys = [Some(1), None, Some(2), Some(1)];
                            let ids = (0..4)
                                .map(|r| run * 8 + chunk * 4 + r)
                                .collect::<Vec<i64>>();
                            expected.extend(keys.into_iter().zip(ids.iter().copied()));
                            let batch = RecordBatch::try_new(
                                schema.clone(),
                                vec![
                                    Arc::new(Int64Array::from(keys.to_vec())),
                                    Arc::new(Int64Array::from(ids)),
                                ],
                            )
                            .unwrap();
                            local
                                .consume(Some(batch), &mut StateContext::new(&mut state))
                                .unwrap();
                        }
                        local
                            .finish(
                                ConsumeStatus::End,
                                &mut StateContext::new(&mut state),
                                &guard,
                            )
                            .unwrap();
                        block_on(sink.consume(
                            ConsumeStatus::End,
                            &mut *local,
                            StateContext::new(&mut state),
                            &guard,
                        ))
                        .unwrap();
                    }
                    expected.sort_by(|(a, _), (b, _)| match (a, b) {
                        (None, None) => std::cmp::Ordering::Equal,
                        (None, _) => {
                            if nulls_first {
                                std::cmp::Ordering::Less
                            } else {
                                std::cmp::Ordering::Greater
                            }
                        }
                        (_, None) => {
                            if nulls_first {
                                std::cmp::Ordering::Greater
                            } else {
                                std::cmp::Ordering::Less
                            }
                        }
                        (Some(a), Some(b)) => {
                            if descending {
                                b.cmp(a)
                            } else {
                                a.cmp(b)
                            }
                        }
                    });
                    expected.truncate(fetch.unwrap_or(usize::MAX));
                    block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
                    let chunks = buffer.read_all(&guard);
                    let actual = chunks
                        .iter()
                        .flat_map(|c| {
                            c.column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .to_vec()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual,
                        expected.into_iter().map(|r| r.1).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    #[test]
    fn ordered_source_only_merges_claimed_ranges_and_fetch_prunes_the_tail() {
        use crate::exec::{LocalSourceContext, PartitionedSourceExec, SourceExec};
        for fetch in [None, Some(0), Some(17), Some(400)] {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
            let result = Arc::new(PartitionedResult::default());
            let mut sink = SortSinkExec::new(
                result.clone(),
                schema.clone(),
                vec![SortKey {
                    column: 0,
                    descending: false,
                    nulls_first: false,
                }],
                fetch,
            );
            let guard = asyncband::shutdown::new().1;
            let mut state = LocalState::default();
            for run in 0..4 {
                let mut local = sink
                    .local_sink(&LocalSinkContext {
                        batch_rows: 25,
                        partitions: 8,
                    })
                    .unwrap();
                let chunk = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(
                        (0..100).rev().map(|i| i * 4 + run),
                    ))],
                )
                .unwrap();
                local
                    .consume(Some(chunk), &mut StateContext::new(&mut state))
                    .unwrap();
                local
                    .finish(
                        ConsumeStatus::End,
                        &mut StateContext::new(&mut state),
                        &guard,
                    )
                    .unwrap();
                block_on(sink.consume(
                    ConsumeStatus::End,
                    &mut *local,
                    StateContext::new(&mut state),
                    &guard,
                ))
                .unwrap();
            }
            block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
            assert_eq!(result.computed_parts(), 0);
            if fetch != Some(0) {
                let mut source = PartitionedSourceExec::new(result.clone());
                source.start(&guard).unwrap();
                let mut local = source
                    .local_source(&LocalSourceContext {
                        batch_rows: 7,
                        instances: 1,
                    })
                    .unwrap();
                let first = block_on(local.next(&guard)).unwrap().unwrap();
                assert_eq!(first.num_rows(), 7);
                assert_eq!(result.computed_parts(), 1);
                if fetch.is_none() {
                    assert!(result.part_count() > 1);
                }
                drop(local); // A downstream Stop must leave all later ranges untouched.
                assert_eq!(result.computed_parts(), 1);
            } else {
                assert_eq!(result.part_count(), 0);
            }
            let actual = result
                .read_all(&guard)
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, (0..fetch.unwrap_or(400) as i64).collect::<Vec<_>>());
        }
    }
}
