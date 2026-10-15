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

//! Window functions over materialized rows: each window sorts the rows by
//! its partition and order keys, computes one value per row and returns the
//! values in the original row order.
use super::sort::{RowKeys, SortKey};
use super::{
    Buffer, ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, StateContext, downcast_local,
};
use crate::arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, ErrorKind, Result};
use crate::kernel::scalar::{self, Cell};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowFunction {
    /// One plus the rows before the row's first peer in its partition.
    Rank,
    /// One plus the distinct peer groups before the row's.
    DenseRank,
    RowNumber,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// The rows of the partition an aggregate window function reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowFrame {
    Partition,
    /// From the first row of the partition to the current row.
    RowsToCurrent,
    /// From the first row of the partition to the current row's last peer.
    RangeToCurrent,
}

/// One window over columns of the materialized chunk.
#[derive(Clone, Debug)]
pub struct Window {
    pub function: WindowFunction,
    pub argument: Option<usize>,
    pub partition: Vec<usize>,
    pub order: Vec<SortKey>,
    pub frame: WindowFrame,
    pub output: DataType,
}

#[derive(Clone)]
struct Spec {
    partition: Vec<SortKey>,
    order: Vec<SortKey>,
    members: Vec<usize>,
}

#[derive(Default)]
struct LocalSpec {
    parts: RowKeys,
    orders: RowKeys,
    sorted: Vec<Vec<usize>>,
}

struct WindowLocalSink {
    partitions: usize,
    specs: Arc<Vec<Spec>>,
    chunks: Vec<RecordBatch>,
    keys: Vec<LocalSpec>,
}
impl LocalSink for WindowLocalSink {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            for (spec, keys) in self.specs.iter().zip(&mut self.keys) {
                keys.parts
                    .push(chunk.columns(), &spec.partition, chunk.num_rows())?;
                keys.orders
                    .push(chunk.columns(), &spec.order, chunk.num_rows())?;
            }
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
        for (spec, keys) in self.specs.iter().zip(&mut self.keys) {
            keys.sorted = vec![vec![]; self.partitions];
            for row in 0..keys.parts.len() {
                if row % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let bucket = if self.partitions == 1 || spec.partition.is_empty() {
                    0
                } else {
                    (crate::kernel::table::hash_bytes(keys.parts.row(row)) as usize)
                        & (self.partitions - 1)
                };
                keys.sorted[bucket].push(row);
            }
            for sorted in &mut keys.sorted {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                sorted.sort_unstable_by(|&a, &b| {
                    keys.parts
                        .row(a)
                        .cmp(keys.parts.row(b))
                        .then_with(|| keys.orders.row(a).cmp(keys.orders.row(b)))
                        .then(a.cmp(&b))
                });
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

struct WindowRun {
    chunks: Vec<RecordBatch>,
    keys: Vec<LocalSpec>,
}

pub struct WindowSinkExec {
    buffer: Arc<Buffer>,
    schema: SchemaRef,
    windows: Vec<Window>,
    specs: Arc<Vec<Spec>>,
    runs: Vec<WindowRun>,
}
impl WindowSinkExec {
    pub fn new(buffer: Arc<Buffer>, schema: SchemaRef, windows: Vec<Window>) -> Self {
        let mut specs: Vec<Spec> = vec![];
        for (i, w) in windows.iter().enumerate() {
            let partition = w
                .partition
                .iter()
                .map(|&column| SortKey {
                    column,
                    descending: false,
                    nulls_first: true,
                })
                .collect::<Vec<_>>();
            if let Some(spec) = specs
                .iter_mut()
                .find(|s| s.partition == partition && s.order == w.order)
            {
                spec.members.push(i);
            } else {
                specs.push(Spec {
                    partition,
                    order: w.order.clone(),
                    members: vec![i],
                });
            }
        }
        Self {
            buffer,
            schema,
            windows,
            specs: Arc::new(specs),
            runs: vec![],
        }
    }
}
impl SinkExec for WindowSinkExec {
    fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        if !ctx.partitions.is_power_of_two() {
            return Err(Error::invalid_plan(
                "window partitions must be a power of two".into(),
            ));
        }
        Ok(Box::new(WindowLocalSink {
            partitions: ctx.partitions,
            specs: self.specs.clone(),
            chunks: vec![],
            keys: (0..self.specs.len())
                .map(|_| LocalSpec::default())
                .collect(),
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
            let local = downcast_local::<WindowLocalSink>(local)?;
            let keys = local.keys.iter_mut().map(std::mem::take).collect();
            self.runs.push(WindowRun {
                chunks: std::mem::take(&mut local.chunks),
                keys,
            });
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn super::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let runs = Arc::new(std::mem::take(&mut self.runs));
            let mut chunks = vec![];
            let mut bases = vec![];
            let mut locations = vec![];
            for (run, data) in runs.iter().enumerate() {
                bases.push(locations.len());
                let rows = data.chunks.iter().map(RecordBatch::num_rows).sum::<usize>();
                locations.extend((0..rows).map(|r| (run, r)));
                chunks.extend(data.chunks.iter().cloned());
            }
            let rows = locations.len();
            if rows == 0 {
                return Ok(());
            }
            let bases = Arc::new(bases);
            let locations = Arc::new(locations);
            let inputs = self.schema.fields().len() - self.windows.len();
            let input_schema = Arc::new(crate::arrow::datatypes::Schema::new(
                self.schema.fields()[..inputs].to_vec(),
            ));
            let batch = Arc::new(
                super::sort::concat_chunks_on(&input_schema, Arc::new(chunks), guard, cpu).await?,
            );
            let mut decode_jobs: Vec<Box<dyn FnOnce() -> Result<Option<Vec<Cell>>> + Send>> =
                vec![];
            for w in &self.windows {
                let batch = batch.clone();
                let argument = w.argument;
                let guard = guard.clone();
                decode_jobs.push(Box::new(move || {
                    if guard.is_shutdown_requested() {
                        return Err(Error::cancelled());
                    }
                    argument
                        .map(|c| scalar::decode(batch.column(c)))
                        .transpose()
                }));
            }
            let arguments = Arc::new(super::run_all(cpu, decode_jobs).await?);
            let windows = Arc::new(self.windows.clone());
            let mut outputs: Vec<Option<ArrayRef>> = vec![None; windows.len()];
            for (s, spec) in self.specs.iter().enumerate() {
                let partitions = runs[0].keys[s].sorted.len();
                if runs.iter().any(|r| r.keys[s].sorted.len() != partitions) {
                    return Err(Error::internal("inconsistent window partitions".into()));
                }
                type Computed = (Vec<usize>, Vec<Vec<Cell>>);
                let mut jobs: Vec<Box<dyn FnOnce() -> Result<Computed> + Send>> = vec![];
                for bucket in 0..partitions {
                    if runs.iter().all(|r| r.keys[s].sorted[bucket].is_empty()) {
                        continue;
                    }
                    let (runs, bases, locations, arguments, windows, batch) = (
                        runs.clone(),
                        bases.clone(),
                        locations.clone(),
                        arguments.clone(),
                        windows.clone(),
                        batch.clone(),
                    );
                    let members = spec.members.clone();
                    let guard = guard.clone();
                    jobs.push(Box::new(move || {
                        let orders = runs
                            .iter()
                            .map(|r| r.keys[s].sorted[bucket].as_slice())
                            .collect::<Vec<_>>();
                        let sorted = super::sort::merge_sorted(
                            &orders,
                            |run, row| {
                                let k = &runs[run].keys[s];
                                (k.parts.row(row), k.orders.row(row))
                            },
                            rows,
                            &guard,
                        )?
                        .into_iter()
                        .map(|(run, row)| bases[run] + row)
                        .collect::<Vec<_>>();
                        let parts = |r: usize| {
                            let (run, row) = locations[r];
                            runs[run].keys[s].parts.row(row)
                        };
                        let orders = |r: usize| {
                            let (run, row) = locations[r];
                            runs[run].keys[s].orders.row(row)
                        };
                        let computed = members
                            .iter()
                            .map(|&i| {
                                let w = &windows[i];
                                values(
                                    &sorted,
                                    &parts,
                                    &orders,
                                    w,
                                    arguments[i].as_deref(),
                                    w.argument.map(|c| batch.column(c).data_type()),
                                    &guard,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok((sorted, computed))
                    }));
                }
                let done = super::run_all(cpu, jobs).await?;
                let mut cells = vec![vec![Cell::default(); rows]; spec.members.len()];
                for (sorted, values) in done {
                    for (out, column) in cells.iter_mut().zip(values) {
                        for (j, (&row, value)) in sorted.iter().zip(column).enumerate() {
                            if j % 4096 == 0 && guard.is_shutdown_requested() {
                                return Err(Error::cancelled());
                            }
                            out[row] = value;
                        }
                    }
                }
                let mut jobs: Vec<Box<dyn FnOnce() -> Result<(usize, ArrayRef)> + Send>> = vec![];
                for (&i, cells) in spec.members.iter().zip(cells) {
                    let output = windows[i].output.clone();
                    // Byte cells borrow the payload. Each detached job must
                    // retain it even if the waiting finish future is cancelled.
                    let owner = batch.clone();
                    let guard = guard.clone();
                    jobs.push(Box::new(move || {
                        if guard.is_shutdown_requested() {
                            return Err(Error::cancelled());
                        }
                        let array = scalar::encode(&output, &cells)?;
                        drop(owner);
                        Ok((i, array))
                    }));
                }
                for (i, array) in super::run_all(cpu, jobs).await? {
                    outputs[i] = Some(array);
                }
            }
            let mut columns = batch.columns().to_vec();
            columns.extend(outputs.into_iter().map(Option::unwrap));
            let out = RecordBatch::try_new_with_options(
                self.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .map_err(|source| {
                Error::invalid_plan("window outputs do not match their columns".into())
                    .with_source(source)
            })?;
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.buffer.0.lock().unwrap().push(out);
            Ok(())
        })
    }
}

/// A window's value for every row of `sorted`, rows sorted by partition
/// and order keys, in that order.
fn values<'a>(
    sorted: &[usize],
    parts: &impl Fn(usize) -> &'a [u8],
    orders: &impl Fn(usize) -> &'a [u8],
    w: &Window,
    argument: Option<&[Cell]>,
    argument_type: Option<&DataType>,
    guard: &ShutdownGuard,
) -> Result<Vec<Cell>> {
    let rows = sorted.len();
    let mut out = vec![Cell::default(); rows];
    let mut start = 0;
    while start < rows {
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        let mut end = start + 1;
        while end < rows && parts(sorted[end]) == parts(sorted[start]) {
            if end % 4096 == 0 && guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            end += 1;
        }
        let part = &sorted[start..end];
        let out = &mut out[start..end];
        let peers = |i: usize| -> Result<usize> {
            let mut j = i + 1;
            while j < part.len() && orders(part[j]) == orders(part[i]) {
                if j % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                j += 1;
            }
            Ok(j)
        };
        match w.function {
            WindowFunction::Rank | WindowFunction::DenseRank | WindowFunction::RowNumber => {
                let (mut i, mut dense) = (0, 0u64);
                while i < part.len() {
                    let j = peers(i)?;
                    dense += 1;
                    for (k, o) in out.iter_mut().enumerate().take(j).skip(i) {
                        if k % 4096 == 0 && guard.is_shutdown_requested() {
                            return Err(Error::cancelled());
                        }
                        let v = match w.function {
                            WindowFunction::Rank => i as u64 + 1,
                            WindowFunction::DenseRank => dense,
                            _ => k as u64 + 1,
                        };
                        *o = Cell::word(v);
                    }
                    i = j;
                }
            }
            _ => {
                let mut acc = Running::new(w.function, argument_type, &w.output)?;
                let value = |r: usize| argument.map(|v| v[r]);
                match w.frame {
                    WindowFrame::Partition => {
                        for (i, &r) in part.iter().enumerate() {
                            if i % 4096 == 0 && guard.is_shutdown_requested() {
                                return Err(Error::cancelled());
                            }
                            acc.add(value(r))?;
                        }
                        out.fill(acc.value()?);
                    }
                    WindowFrame::RowsToCurrent => {
                        for (i, (o, &r)) in out.iter_mut().zip(part).enumerate() {
                            if i % 4096 == 0 && guard.is_shutdown_requested() {
                                return Err(Error::cancelled());
                            }
                            acc.add(value(r))?;
                            *o = acc.value()?;
                        }
                    }
                    WindowFrame::RangeToCurrent => {
                        let mut i = 0;
                        while i < part.len() {
                            let j = peers(i)?;
                            for (k, &r) in part[i..j].iter().enumerate() {
                                if k % 4096 == 0 && guard.is_shutdown_requested() {
                                    return Err(Error::cancelled());
                                }
                                acc.add(value(r))?;
                            }
                            out[i..j].fill(acc.value()?);
                            i = j;
                        }
                    }
                }
            }
        }
        start = end;
    }
    Ok(out)
}

/// A running aggregate of one partition.
struct Running {
    function: WindowFunction,
    output: DataType,
    input_scale: i8,
    count: i64,
    int: i64,
    wide: i128,
    float: f64,
    extremum: Option<(Vec<u8>, Cell)>,
    key: scalar::KeyEncoder,
    scratch: Vec<u8>,
}

impl Running {
    fn new(function: WindowFunction, input: Option<&DataType>, output: &DataType) -> Result<Self> {
        let input_scale = match input {
            Some(DataType::Decimal128(_, s)) => *s,
            _ => 0,
        };
        let supported = match function {
            WindowFunction::Count => true,
            WindowFunction::Sum | WindowFunction::Avg => matches!(
                output,
                DataType::Int64 | DataType::Float64 | DataType::Decimal128(..)
            ),
            WindowFunction::Min | WindowFunction::Max => input == Some(output),
            _ => false,
        };
        if !supported || (function != WindowFunction::Count && input.is_none()) {
            return Err(Error::unsupported(format!(
                "unsupported window function {function:?} of {input:?} as {output}"
            )));
        }
        Ok(Self {
            function,
            output: output.clone(),
            input_scale,
            count: 0,
            int: 0,
            wide: 0,
            float: 0.0,
            extremum: None,
            key: scalar::bind_key(input.unwrap_or(&DataType::Null))?,
            scratch: vec![],
        })
    }

    fn add(&mut self, value: Option<Cell>) -> Result<()> {
        let Some(v) = value else {
            self.count += 1;
            return Ok(());
        };
        if v.valid == 0 {
            return Ok(());
        }
        self.count += 1;
        match self.function {
            WindowFunction::Sum | WindowFunction::Avg => match self.output {
                DataType::Decimal128(..) => self.wide = self.wide.wrapping_add(v.value128()),
                DataType::Float64 => self.float += f64::from_bits(v.bits),
                _ => self.int = self.int.wrapping_add(v.bits as i64),
            },
            WindowFunction::Min | WindowFunction::Max => {
                let mut key = std::mem::take(&mut self.scratch);
                key.clear();
                (self.key)(v, &mut key);
                let better = self.extremum.as_ref().is_none_or(|(old, _)| {
                    if self.function == WindowFunction::Min {
                        key < *old
                    } else {
                        key > *old
                    }
                });
                // String cells borrow the materialized chunk, alive until
                // the output is encoded.
                if better {
                    match &mut self.extremum {
                        Some((old, cell)) => {
                            std::mem::swap(old, &mut key);
                            *cell = v;
                        }
                        None => self.extremum = Some((std::mem::take(&mut key), v)),
                    }
                }
                self.scratch = key;
            }
            _ => {}
        }
        Ok(())
    }

    fn value(&self) -> Result<Cell> {
        Ok(match self.function {
            WindowFunction::Count => Cell::word(self.count as u64),
            _ if self.count == 0 => Cell::default(),
            WindowFunction::Sum => match self.output {
                DataType::Decimal128(..) => Cell::wide(self.wide),
                DataType::Float64 => Cell::word(self.float.to_bits()),
                _ => Cell::word(self.int as u64),
            },
            WindowFunction::Avg => match self.output {
                DataType::Decimal128(p, s) => {
                    let shift = 10i128.pow((s - self.input_scale).max(0) as u32);
                    let overflow =
                        || Error::new(ErrorKind::ArithmeticOverflow, "window average overflow");
                    let v = self.wide.checked_mul(shift).ok_or_else(overflow)? / self.count as i128;
                    if v.unsigned_abs() >= 10u128.pow(p as u32) {
                        return Err(overflow());
                    }
                    Cell::wide(v)
                }
                _ => Cell::word((self.float / self.count as f64).to_bits()),
            },
            WindowFunction::Min | WindowFunction::Max => {
                self.extremum.as_ref().map_or(Cell::default(), |(_, c)| *c)
            }
            _ => unreachable!("ranking functions are not aggregates"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arrow::{
            array::Int64Array,
            datatypes::{Field, Schema},
        },
        kernel::LocalState,
    };
    use futures::executor::block_on;

    #[test]
    fn partitions_spanning_instances_share_specs_and_compute_all_frames() {
        let input_schema = Arc::new(Schema::new(
            ["p", "o", "v", "id"]
                .map(|n| Field::new(n, DataType::Int64, false))
                .to_vec(),
        ));
        let order = vec![SortKey {
            column: 1,
            descending: false,
            nulls_first: true,
        }];
        let unique = vec![SortKey {
            column: 3,
            descending: false,
            nulls_first: true,
        }];
        let windows = [
            (
                WindowFunction::Rank,
                WindowFrame::RangeToCurrent,
                order.clone(),
            ),
            (
                WindowFunction::DenseRank,
                WindowFrame::RangeToCurrent,
                order.clone(),
            ),
            (WindowFunction::Sum, WindowFrame::Partition, vec![]),
            (WindowFunction::Sum, WindowFrame::RangeToCurrent, order),
            (WindowFunction::Sum, WindowFrame::RowsToCurrent, unique),
        ]
        .into_iter()
        .map(|(function, frame, order)| Window {
            function,
            frame,
            order,
            partition: vec![0],
            argument: (function == WindowFunction::Sum).then_some(2),
            output: DataType::Int64,
        })
        .collect::<Vec<_>>();
        let mut fields = input_schema.fields().to_vec();
        fields.extend(
            (0..windows.len())
                .map(|i| Arc::new(Field::new(format!("w{i}"), DataType::Int64, true))),
        );
        let schema = Arc::new(Schema::new(fields));
        let input = RecordBatch::try_new(
            input_schema,
            (0..4)
                .map(|c| {
                    Arc::new(Int64Array::from_iter_values((0..36).map(|i| match c {
                        0 => i % 3,
                        1 => i % 4,
                        2 => i + 1,
                        _ => i,
                    }))) as ArrayRef
                })
                .collect(),
        )
        .unwrap();
        for rows in [0, 36] {
            for (instances, partitions) in
                [1, 3].into_iter().flat_map(|i| [1, 2, 8].map(|p| (i, p)))
            {
                let buffer = Arc::new(Buffer::default());
                let mut sink = WindowSinkExec::new(buffer.clone(), schema.clone(), windows.clone());
                let guard = asyncband::shutdown::new().1;
                let mut locals = (0..instances)
                    .map(|_| {
                        sink.local_sink(&LocalSinkContext {
                            batch_rows: 2,
                            partitions,
                        })
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                let mut state = LocalState::default();
                for (i, offset) in (0..rows).step_by(2).enumerate() {
                    locals[i % instances]
                        .consume(
                            Some(input.slice(offset, 2)),
                            &mut StateContext::new(&mut state),
                        )
                        .unwrap();
                }
                for local in &mut locals {
                    local
                        .finish(
                            ConsumeStatus::End,
                            &mut StateContext::new(&mut state),
                            &guard,
                        )
                        .unwrap();
                    block_on(sink.consume(
                        ConsumeStatus::End,
                        &mut **local,
                        StateContext::new(&mut state),
                        &guard,
                    ))
                    .unwrap();
                }
                block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
                let chunks = buffer.0.lock().unwrap();
                if rows == 0 {
                    assert!(chunks.is_empty());
                    continue;
                }
                let batch = &chunks[0];
                let col = |c: usize, r| {
                    batch
                        .column(c)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(r)
                };
                for r in 0..rows {
                    let id = col(3, r);
                    let part = (0..36).filter(|i| i % 3 == id % 3).collect::<Vec<i64>>();
                    let rank = 1 + part.iter().filter(|&&i| i % 4 < id % 4).count() as i64;
                    assert_eq!(col(4, r), rank);
                    assert_eq!(col(5, r), id % 4 + 1);
                    assert_eq!(col(6, r), part.iter().map(|i| i + 1).sum::<i64>());
                    assert_eq!(
                        col(7, r),
                        part.iter()
                            .filter(|&&i| i % 4 <= id % 4)
                            .map(|i| i + 1)
                            .sum::<i64>()
                    );
                    assert_eq!(
                        col(8, r),
                        part.iter()
                            .filter(|&&i| i <= id)
                            .map(|i| i + 1)
                            .sum::<i64>()
                    );
                }
            }
        }
    }
    #[test]
    fn shared_string_max_specs_and_global_running_sum_cross_instances() {
        use crate::arrow::array::{Array, StringArray};
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, true),
            Field::new("v", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let part = |i: i64| (i % 3 != 0).then_some(i % 3);
        let value = |i: i64| (i % 5 != 0).then(|| format!("v{i:02}"));
        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter((0..24).map(part))),
                Arc::new(StringArray::from_iter((0..24).map(value))),
                Arc::new(Int64Array::from_iter_values(0..24)),
            ],
        )
        .unwrap();
        let order = vec![SortKey {
            column: 2,
            descending: false,
            nulls_first: true,
        }];
        let max = Window {
            function: WindowFunction::Max,
            argument: Some(1),
            partition: vec![0],
            order: order.clone(),
            frame: WindowFrame::RowsToCurrent,
            output: DataType::Utf8,
        };
        let windows = vec![
            max.clone(),
            max,
            Window {
                function: WindowFunction::Sum,
                argument: Some(2),
                partition: vec![],
                order,
                frame: WindowFrame::RowsToCurrent,
                output: DataType::Int64,
            },
        ];
        let mut fields = input_schema.fields().to_vec();
        fields.extend(
            [
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("sum", DataType::Int64, true),
            ]
            .map(Arc::new),
        );
        let schema = Arc::new(Schema::new(fields));
        for partitions in [1, 2, 8] {
            let buffer = Arc::new(Buffer::default());
            let mut sink = WindowSinkExec::new(buffer.clone(), schema.clone(), windows.clone());
            assert_eq!(sink.specs.len(), 2);
            let guard = asyncband::shutdown::new().1;
            let mut state = LocalState::default();
            let mut locals = (0..3)
                .map(|_| {
                    sink.local_sink(&LocalSinkContext {
                        batch_rows: 3,
                        partitions,
                    })
                    .unwrap()
                })
                .collect::<Vec<_>>();
            for (i, offset) in (0..24).step_by(3).enumerate() {
                locals[i % 3]
                    .consume(
                        Some(batch.slice(offset, 3)),
                        &mut StateContext::new(&mut state),
                    )
                    .unwrap();
            }
            for mut local in locals {
                local
                    .finish(
                        ConsumeStatus::End,
                        &mut StateContext::new(&mut state),
                        &guard,
                    )
                    .unwrap();
                let data = downcast_local::<WindowLocalSink>(&mut *local).unwrap();
                assert!(data.keys[1].sorted.iter().skip(1).all(Vec::is_empty));
                block_on(sink.consume(
                    ConsumeStatus::End,
                    &mut *local,
                    StateContext::new(&mut state),
                    &guard,
                ))
                .unwrap();
            }
            block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
            let output = buffer.0.lock().unwrap();
            let out = &output[0];
            let ids = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = out.column(5).as_any().downcast_ref::<Int64Array>().unwrap();
            for r in 0..24 {
                let id = ids.value(r);
                let expected = (0..=id)
                    .filter(|&i| part(i) == part(id))
                    .filter_map(value)
                    .max();
                for c in [3, 4] {
                    let a = out
                        .column(c)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!((!a.is_null(r)).then(|| a.value(r)), expected.as_deref());
                }
                assert_eq!(sums.value(r), id * (id + 1) / 2);
            }
        }
    }
}
