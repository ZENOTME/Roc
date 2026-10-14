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

//! Table states: immutable rows, optionally indexed by key columns, that
//! Expand looks rows up in and TableGather reads values from. A table is
//! shared by every execution instance of the pipelines reading it.
use super::scalar::{self, Cell, KeyEncoder};
use crate::arrow::{
    array::{Array, ArrayRef, make_array},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use crate::error::{Error, Result};
use arrow_data::transform::MutableArrayData;
use std::sync::{Arc, OnceLock};

/// No row: the end of a chain.
const END: u32 = u32::MAX;

pub struct Table {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
    index: Option<Index>,
}

/// A chained hash index; each chain lists its rows in table order.
struct Index {
    heads: Vec<u32>,
    next: Vec<u32>,
    shift: u32,
    keys: Keys,
}

enum Keys {
    /// One fixed-width key without NULL matches: its canonical word per row.
    Word(Vec<u64>),
    /// Encoded keys of any types, in `offsets` order.
    Encoded {
        encoders: Vec<KeyEncoder>,
        data: Vec<u8>,
        offsets: Vec<usize>,
    },
}

/// The key of one probe row.
pub(crate) enum Probe<'a> {
    Word(u64),
    Encoded(&'a [u8]),
}

#[inline(always)]
fn hash_word(key: u64) -> u64 {
    (key ^ (key >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9)
}

fn hash_bytes(key: &[u8]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = foldhash::fast::FixedState::with_seed(0x51_7cc1_b727_220a).build_hasher();
    h.write(key);
    h.finish()
}

/// Whether a key type has one canonical word: integers and dates.
fn word_key(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
    )
}

impl Table {
    /// The rows of `batches`, indexed by the columns `keys` when there are
    /// any. A row with a NULL key is not indexed unless `nulls_equal`.
    pub fn new(
        schema: SchemaRef,
        batches: &[RecordBatch],
        keys: &[usize],
        nulls_equal: bool,
    ) -> Result<Self> {
        let columns = (0..schema.fields().len())
            .map(|i| {
                concat(
                    batches.iter().map(|b| b.column(i)),
                    schema.field(i).data_type(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if rows >= END as usize {
            return Err(Error::new(
                crate::error::ErrorKind::ResourceExhausted,
                format!("a table holds at most {} rows", END - 1),
            ));
        }
        let index = (!keys.is_empty())
            .then(|| Index::new(&columns, rows, keys, nulls_equal))
            .transpose()?;
        Ok(Self {
            schema,
            columns,
            rows,
            index,
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn column(&self, i: usize) -> &ArrayRef {
        &self.columns[i]
    }

    /// Whether probes use one word per key.
    pub(crate) fn word_keys(&self) -> bool {
        matches!(self.index.as_ref().map(|i| &i.keys), Some(Keys::Word(_)))
    }

    /// Encodes the key cells of a probe row; `false` when it matches nothing.
    pub(crate) fn encode(&self, cells: &[Cell], nulls_equal: bool, out: &mut Vec<u8>) -> bool {
        out.clear();
        let Some(Index {
            keys: Keys::Encoded { encoders, .. },
            ..
        }) = &self.index
        else {
            return false;
        };
        for (cell, encode) in cells.iter().zip(encoders) {
            if cell.valid == 0 && !nulls_equal {
                return false;
            }
            encode(*cell, out);
        }
        true
    }

    /// Calls `f` with each table row whose key equals `probe`, in order.
    #[inline]
    pub(crate) fn find(&self, probe: Probe<'_>, mut f: impl FnMut(u32)) {
        let Some(index) = &self.index else {
            return;
        };
        let hash = match probe {
            Probe::Word(k) => hash_word(k),
            Probe::Encoded(k) => hash_bytes(k),
        };
        let mut row = index.heads[(hash >> index.shift) as usize];
        match (&index.keys, probe) {
            (Keys::Word(words), Probe::Word(k)) => {
                while row != END {
                    if words[row as usize] == k {
                        f(row);
                    }
                    row = index.next[row as usize];
                }
            }
            (Keys::Encoded { data, offsets, .. }, Probe::Encoded(k)) => {
                while row != END {
                    let r = row as usize;
                    if &data[offsets[r]..offsets[r + 1]] == k {
                        f(row);
                    }
                    row = index.next[r];
                }
            }
            _ => {}
        }
    }
}

impl Index {
    fn new(columns: &[ArrayRef], rows: usize, keys: &[usize], nulls_equal: bool) -> Result<Self> {
        let types = keys
            .iter()
            .map(|&k| columns[k].data_type().clone())
            .collect::<Vec<_>>();
        let decoded = keys
            .iter()
            .map(|&k| scalar::decode(&columns[k]))
            .collect::<Result<Vec<_>>>()?;
        let buckets = rows.max(1).next_power_of_two() * 2;
        let shift = 64 - buckets.trailing_zeros();
        let mut heads = vec![END; buckets];
        let mut next = vec![END; rows];
        let indexed = |r: usize| nulls_equal || decoded.iter().all(|c| c[r].valid != 0);
        // Rows are pushed onto chains last first: chains list them in order.
        let keys = if types.len() == 1 && word_key(&types[0]) && !nulls_equal {
            let words = decoded[0].iter().map(|c| c.bits).collect::<Vec<_>>();
            for r in (0..rows).rev() {
                if indexed(r) {
                    let b = (hash_word(words[r]) >> shift) as usize;
                    next[r] = heads[b];
                    heads[b] = r as u32;
                }
            }
            Keys::Word(words)
        } else {
            let encoders = types
                .iter()
                .map(scalar::bind_key)
                .collect::<Result<Vec<_>>>()?;
            let mut data = vec![];
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            for r in 0..rows {
                for (c, encode) in decoded.iter().zip(&encoders) {
                    encode(c[r], &mut data);
                }
                offsets.push(data.len());
            }
            for r in (0..rows).rev() {
                if indexed(r) {
                    let b = (hash_bytes(&data[offsets[r]..offsets[r + 1]]) >> shift) as usize;
                    next[r] = heads[b];
                    heads[b] = r as u32;
                }
            }
            Keys::Encoded {
                encoders,
                data,
                offsets,
            }
        };
        Ok(Self {
            heads,
            next,
            shift,
            keys,
        })
    }
}

/// The arrays one after another.
pub(crate) fn concat<'a>(
    arrays: impl Iterator<Item = &'a ArrayRef>,
    t: &DataType,
) -> Result<ArrayRef> {
    let data = arrays.map(|a| a.to_data()).collect::<Vec<_>>();
    if data.is_empty() {
        return Ok(crate::arrow::array::new_empty_array(t));
    }
    if data.len() == 1 {
        return Ok(make_array(data.into_iter().next().unwrap()));
    }
    let rows = data.iter().map(|d| d.len()).sum();
    let mut out = MutableArrayData::new(data.iter().collect(), false, rows);
    for (i, d) in data.iter().enumerate() {
        out.try_extend(i, 0, d.len()).map_err(|source| {
            Error::new(
                crate::error::ErrorKind::ResourceExhausted,
                "a column exceeds the offsets of its type",
            )
            .with_source(source)
        })?;
    }
    Ok(make_array(out.freeze()))
}

/// A table published by the pipeline that builds it, read by the pipelines
/// that depend on that one.
#[derive(Default)]
pub struct SharedTable(OnceLock<Arc<Table>>);

impl SharedTable {
    pub fn publish(&self, table: Table) -> Result<()> {
        self.0
            .set(Arc::new(table))
            .map_err(|_| Error::internal("table published twice".into()))
    }

    pub fn get(&self) -> Result<Arc<Table>> {
        self.0
            .get()
            .cloned()
            .ok_or_else(|| Error::internal("table read before it is built".into()))
    }
}

/// The state of an Expand or TableGather.
pub struct TableState {
    pub table: Arc<Table>,
}
