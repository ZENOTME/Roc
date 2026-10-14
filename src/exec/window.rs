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
use crate::arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, ErrorKind, Result};
use crate::kernel::scalar::{self, Cell};

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

/// The chunks with one more column per window, of `schema`.
pub fn compute(
    schema: &SchemaRef,
    chunks: Vec<RecordBatch>,
    windows: &[Window],
) -> Result<Vec<RecordBatch>> {
    let inputs = schema.fields().len() - windows.len();
    let input_schema = std::sync::Arc::new(crate::arrow::datatypes::Schema::new(
        schema.fields()[..inputs].to_vec(),
    ));
    let batch = super::sort::concat_chunks(&input_schema, &chunks)?;
    drop(chunks);
    let rows = batch.num_rows();
    let mut columns = batch.columns().to_vec();
    for w in windows {
        columns.push(window(&batch, rows, w)?);
    }
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

fn window(batch: &RecordBatch, rows: usize, w: &Window) -> Result<ArrayRef> {
    let partition = w
        .partition
        .iter()
        .map(|&column| SortKey {
            column,
            descending: false,
            nulls_first: true,
        })
        .collect::<Vec<_>>();
    let parts = RowKeys::new(batch.columns(), &partition, rows)?;
    let orders = RowKeys::new(batch.columns(), &w.order, rows)?;
    let mut sorted = (0..rows).collect::<Vec<_>>();
    sorted.sort_by(|&a, &b| {
        parts
            .row(a)
            .cmp(parts.row(b))
            .then_with(|| orders.row(a).cmp(orders.row(b)))
            .then(a.cmp(&b))
    });
    let argument = w
        .argument
        .map(|c| scalar::decode(batch.column(c)))
        .transpose()?;
    let argument_type = w.argument.map(|c| batch.column(c).data_type().clone());
    let mut out = vec![Cell::default(); rows];
    let mut start = 0;
    while start < rows {
        let mut end = start + 1;
        while end < rows && parts.row(sorted[end]) == parts.row(sorted[start]) {
            end += 1;
        }
        let part = &sorted[start..end];
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
                    for (k, &r) in part.iter().enumerate().take(j).skip(i) {
                        let v = match w.function {
                            WindowFunction::Rank => i as u64 + 1,
                            WindowFunction::DenseRank => dense,
                            _ => k as u64 + 1,
                        };
                        out[r] = Cell::word(v);
                    }
                    i = j;
                }
            }
            _ => {
                let values = argument.as_deref();
                let t = argument_type.as_ref();
                let mut acc = Running::new(w.function, t, &w.output)?;
                match w.frame {
                    WindowFrame::Partition => {
                        for &r in part {
                            acc.add(values.map(|v| v[r]))?;
                        }
                        let v = acc.value()?;
                        for &r in part {
                            out[r] = v;
                        }
                    }
                    WindowFrame::RowsToCurrent => {
                        for &r in part {
                            acc.add(values.map(|v| v[r]))?;
                            out[r] = acc.value()?;
                        }
                    }
                    WindowFrame::RangeToCurrent => {
                        let mut i = 0;
                        while i < part.len() {
                            let j = peers(i);
                            for &r in &part[i..j] {
                                acc.add(values.map(|v| v[r]))?;
                            }
                            let v = acc.value()?;
                            for &r in &part[i..j] {
                                out[r] = v;
                            }
                            i = j;
                        }
                    }
                }
            }
        }
        start = end;
    }
    scalar::encode(&w.output, &out)
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
                let mut key = vec![];
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
                    self.extremum = Some((key, v));
                }
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
