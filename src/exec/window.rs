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
use super::{Cpu, run_all};
use crate::arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, ErrorKind, Result};
use crate::kernel::scalar::{self, Cell};
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

/// Rows hash-partitioned by partition key among the jobs of one window
/// specification: below this many, one job.
const PARALLEL_ROWS: usize = 32 * 1024;

/// The chunks with one more column per window, of `schema`. Windows with
/// the same partition and order keys share one sort; rows split by a hash
/// of their partition key, and each part sorts and computes in a CPU job.
pub async fn compute(
    schema: &SchemaRef,
    chunks: Vec<RecordBatch>,
    windows: &[Window],
    cpu: &dyn Cpu,
) -> Result<Vec<RecordBatch>> {
    let inputs = schema.fields().len() - windows.len();
    let input_schema = Arc::new(crate::arrow::datatypes::Schema::new(
        schema.fields()[..inputs].to_vec(),
    ));
    let batch = Arc::new(super::sort::concat_chunks(&input_schema, &chunks)?);
    drop(chunks);
    let rows = batch.num_rows();
    let mut outputs: Vec<Option<ArrayRef>> = vec![None; windows.len()];
    let mut specs: Vec<(Vec<usize>, Vec<SortKey>, Vec<usize>)> = vec![];
    for (i, w) in windows.iter().enumerate() {
        match specs
            .iter_mut()
            .find(|(p, o, _)| *p == w.partition && *o == w.order)
        {
            Some(spec) => spec.2.push(i),
            None => specs.push((w.partition.clone(), w.order.clone(), vec![i])),
        }
    }
    let parts = if rows < PARALLEL_ROWS {
        1
    } else {
        (2 * cpu.parallelism()).max(1)
    };
    for (partition, order, members) in specs {
        let members = members
            .into_iter()
            .map(|i| (i, windows[i].clone()))
            .collect::<Vec<_>>();
        for (i, array) in spec(&batch, rows, &partition, &order, &members, parts, cpu).await? {
            outputs[i] = Some(array);
        }
    }
    let mut columns = batch.columns().to_vec();
    columns.extend(outputs.into_iter().map(Option::unwrap));
    let out = RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|source| {
        Error::invalid_plan("window outputs do not match their columns".into()).with_source(source)
    })?;
    Ok(if rows == 0 { vec![] } else { vec![out] })
}

/// The output of every window of one partition and order specification.
async fn spec(
    batch: &Arc<RecordBatch>,
    rows: usize,
    partition: &[usize],
    order: &[SortKey],
    windows: &[(usize, Window)],
    parts: usize,
    cpu: &dyn Cpu,
) -> Result<Vec<(usize, ArrayRef)>> {
    let partition = partition
        .iter()
        .map(|&column| SortKey {
            column,
            descending: false,
            nulls_first: true,
        })
        .collect::<Vec<_>>();
    let keys = Arc::new((
        RowKeys::new(batch.columns(), &partition, rows)?,
        RowKeys::new(batch.columns(), order, rows)?,
    ));
    // Each window's argument, decoded once.
    let arguments = Arc::new(
        windows
            .iter()
            .map(|(_, w)| w.argument.map(|c| scalar::decode(batch.column(c))).transpose())
            .collect::<Result<Vec<_>>>()?,
    );
    let mut buckets = vec![vec![]; parts];
    for r in 0..rows {
        let b = if parts == 1 {
            0
        } else {
            (hash(keys.0.row(r)) % parts as u64) as usize
        };
        buckets[b].push(r);
    }
    let windows = Arc::new(windows.to_vec());
    let jobs = buckets
        .into_iter()
        .map(|rows| {
            let (keys, arguments, windows, batch) =
                (keys.clone(), arguments.clone(), windows.clone(), batch.clone());
            Box::new(move || -> Result<(Vec<usize>, Vec<Vec<Cell>>)> {
                let mut sorted = rows;
                let (parts, orders) = (&keys.0, &keys.1);
                sorted.sort_unstable_by(|&a, &b| {
                    parts
                        .row(a)
                        .cmp(parts.row(b))
                        .then_with(|| orders.row(a).cmp(orders.row(b)))
                        .then(a.cmp(&b))
                });
                let values = windows
                    .iter()
                    .zip(arguments.iter())
                    .map(|((_, w), argument)| {
                        let t = w.argument.map(|c| batch.column(c).data_type().clone());
                        values(&sorted, parts, orders, w, argument.as_deref(), t.as_ref())
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((sorted, values))
            }) as Box<dyn FnOnce() -> Result<(Vec<usize>, Vec<Vec<Cell>>)> + Send>
        })
        .collect();
    let done = run_all(cpu, jobs).await?;
    let mut out = vec![vec![Cell::default(); rows]; windows.len()];
    for (sorted, values) in &done {
        for (column, values) in out.iter_mut().zip(values) {
            for (&r, v) in sorted.iter().zip(values) {
                column[r] = *v;
            }
        }
    }
    // Byte cells point into the batch, alive until here.
    let arrays = windows
        .iter()
        .zip(&out)
        .map(|((i, w), cells)| Ok((*i, scalar::encode(&w.output, cells)?)))
        .collect::<Result<Vec<_>>>()?;
    drop(done);
    Ok(arrays)
}

fn hash(key: &[u8]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = foldhash::fast::FixedState::with_seed(0x1357_9bdf_2468_ace0).build_hasher();
    h.write(key);
    h.finish()
}

/// A window's value for every row of `sorted`, rows sorted by partition
/// and order keys, in that order.
fn values(
    sorted: &[usize],
    parts: &RowKeys,
    orders: &RowKeys,
    w: &Window,
    argument: Option<&[Cell]>,
    argument_type: Option<&DataType>,
) -> Result<Vec<Cell>> {
    let rows = sorted.len();
    let mut out = vec![Cell::default(); rows];
    let mut start = 0;
    while start < rows {
        let mut end = start + 1;
        while end < rows && parts.row(sorted[end]) == parts.row(sorted[start]) {
            end += 1;
        }
        let part = &sorted[start..end];
        let out = &mut out[start..end];
        let peers = |i: usize| {
            let mut j = i + 1;
            while j < part.len() && orders.row(part[j]) == orders.row(part[i]) {
                j += 1;
            }
            j
        };
        match w.function {
            WindowFunction::Rank | WindowFunction::DenseRank | WindowFunction::RowNumber => {
                let (mut i, mut dense) = (0, 0u64);
                while i < part.len() {
                    let j = peers(i);
                    dense += 1;
                    for (k, o) in out.iter_mut().enumerate().take(j).skip(i) {
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
                        for &r in part {
                            acc.add(value(r))?;
                        }
                        out.fill(acc.value()?);
                    }
                    WindowFrame::RowsToCurrent => {
                        for (o, &r) in out.iter_mut().zip(part) {
                            acc.add(value(r))?;
                            *o = acc.value()?;
                        }
                    }
                    WindowFrame::RangeToCurrent => {
                        let mut i = 0;
                        while i < part.len() {
                            let j = peers(i);
                            for &r in &part[i..j] {
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
