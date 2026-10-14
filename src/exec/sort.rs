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
use crate::arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use super::{Cpu, run_all};
use crate::error::{Error, Result};
use crate::kernel::scalar;
use std::sync::Arc;

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

impl RowKeys {
    pub fn new(columns: &[ArrayRef], keys: &[SortKey], rows: usize) -> Result<Self> {
        let decoded = keys
            .iter()
            .map(|k| scalar::decode(&columns[k.column]))
            .collect::<Result<Vec<_>>>()?;
        let encoders = keys
            .iter()
            .map(|k| scalar::bind_key(columns[k.column].data_type()))
            .collect::<Result<Vec<_>>>()?;
        let mut data = vec![];
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
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
        Ok(Self { data, offsets })
    }

    pub fn row(&self, r: usize) -> &[u8] {
        &self.data[self.offsets[r]..self.offsets[r + 1]]
    }
}

/// One chunk of the columns of `chunks`.
pub(crate) fn concat_chunks(schema: &SchemaRef, chunks: &[RecordBatch]) -> Result<RecordBatch> {
    let columns = (0..schema.fields().len())
        .map(|i| {
            crate::kernel::table::concat(
                chunks.iter().map(|c| c.column(i)),
                schema.field(i).data_type(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let rows = chunks.iter().map(RecordBatch::num_rows).sum();
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|source| Error::internal("chunks do not concatenate".into()).with_source(source))
}

/// The rows at `indices`.
pub(crate) fn take(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let indices = crate::arrow::array::UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow_select::take::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|source| Error::internal("rows cannot be taken".into()).with_source(source))?;
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )
    .map_err(|source| Error::internal("rows cannot be taken".into()).with_source(source))
}

/// Rows below which one job sorts them all.
const PARALLEL_ROWS: usize = 64 * 1024;

/// The rows of `chunks` ordered by `keys`; with `fetch`, only the first.
/// Rows with equal keys keep their order. Ranges of rows sort in CPU jobs
/// of their own; their runs merge at the end.
pub async fn sort_chunks(
    schema: &SchemaRef,
    chunks: Vec<RecordBatch>,
    keys: &[SortKey],
    fetch: Option<usize>,
    cpu: &dyn Cpu,
) -> Result<Vec<RecordBatch>> {
    let batch = concat_chunks(schema, &chunks)?;
    drop(chunks);
    let rows = batch.num_rows();
    if rows == 0 || fetch == Some(0) {
        return Ok(vec![]);
    }
    let encoded = Arc::new(RowKeys::new(batch.columns(), keys, rows)?);
    let runs = if rows < PARALLEL_ROWS {
        1
    } else {
        (2 * cpu.parallelism()).max(1)
    };
    let size = rows.div_ceil(runs);
    let jobs = (0..runs)
        .map(|i| {
            let encoded = encoded.clone();
            let range = i * size..((i + 1) * size).min(rows);
            Box::new(move || -> Result<Vec<u32>> {
                let mut order = range.map(|r| r as u32).collect::<Vec<_>>();
                let compare = |a: &u32, b: &u32| {
                    encoded
                        .row(*a as usize)
                        .cmp(encoded.row(*b as usize))
                        .then(a.cmp(b))
                };
                match fetch {
                    Some(n) if n < order.len() => {
                        order.select_nth_unstable_by(n, compare);
                        order.truncate(n);
                        order.sort_unstable_by(compare);
                    }
                    _ => order.sort_unstable_by(compare),
                }
                Ok(order)
            }) as Box<dyn FnOnce() -> Result<Vec<u32>> + Send>
        })
        .collect();
    let runs = run_all(cpu, jobs).await?;
    let order = merge(&encoded, runs, fetch.unwrap_or(rows));
    Ok(vec![take(&batch, &order)?])
}

/// The first `limit` rows of sorted runs, merged by key and then row.
fn merge(keys: &RowKeys, mut runs: Vec<Vec<u32>>, limit: usize) -> Vec<u32> {
    if runs.len() == 1 {
        let mut run = runs.pop().unwrap();
        run.truncate(limit);
        return run;
    }
    let total = runs.iter().map(Vec::len).sum::<usize>().min(limit);
    let mut at = vec![0; runs.len()];
    let mut out = Vec::with_capacity(total);
    while out.len() < total {
        let mut best: Option<(usize, u32)> = None;
        for (i, run) in runs.iter().enumerate() {
            let Some(&r) = run.get(at[i]) else { continue };
            let better = best.is_none_or(|(_, b)| {
                keys.row(r as usize)
                    .cmp(keys.row(b as usize))
                    .then(r.cmp(&b))
                    .is_lt()
            });
            if better {
                best = Some((i, r));
            }
        }
        let (i, r) = best.expect("rows are left");
        at[i] += 1;
        out.push(r);
    }
    out
}
