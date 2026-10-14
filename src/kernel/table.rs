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
//!
//! An indexed table stores its rows grouped by hash bucket, so the rows a
//! probe matches are adjacent: a probe reads one bucket range, and gathers
//! of its matches read neighbouring values. Within a bucket rows keep their
//! input order, so the matches of a key come in input order.
use super::scalar::{self, Cell, KeyEncoder};
use crate::arrow::{
    array::{Array, ArrayRef, make_array},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use crate::error::{Error, Result};
use arrow_data::transform::MutableArrayData;
use std::sync::{Arc, OnceLock};

/// Tables hold fewer rows than this.
const END: u32 = u32::MAX;

pub struct Table {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
    index: Option<Index>,
}

/// A hash index over the table's rows, which are grouped by bucket: the
/// rows of bucket `b` are `starts[b]..starts[b + 1]`. Rows with a NULL key
/// that matches nothing follow the last bucket.
struct Index {
    starts: Vec<u32>,
    shift: u32,
    keys: Keys,
}

enum Keys {
    /// One fixed-width key without NULL matches: its canonical word per
    /// indexed row.
    Word(Vec<u64>),
    /// Encoded keys of any types per indexed row.
    Encoded {
        encoders: Vec<KeyEncoder>,
        data: Vec<u8>,
        offsets: Vec<usize>,
    },
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
        let mut columns = (0..schema.fields().len())
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
        let index = if keys.is_empty() {
            None
        } else {
            let (index, order) = Index::new(&columns, rows, keys, nulls_equal)?;
            if let Some(order) = order {
                let order = crate::arrow::array::UInt32Array::from(order);
                for c in &mut columns {
                    *c = arrow_select::take::take(c, &order, None).map_err(|source| {
                        Error::new(
                            crate::error::ErrorKind::ResourceExhausted,
                            "a column exceeds the offsets of its type",
                        )
                        .with_source(source)
                    })?;
                }
            }
            Some(index)
        };
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

    /// The index of a table with one word key.
    pub(crate) fn word_index(&self) -> Option<WordIndex<'_>> {
        match self.index.as_ref()? {
            Index {
                starts,
                shift,
                keys: Keys::Word(words),
            } => Some(WordIndex {
                starts,
                shift: *shift,
                words,
            }),
            _ => None,
        }
    }

    /// The rows of the bucket of an encoded key: every row whose key may
    /// equal it, in order. Compare them with `equal`.
    #[inline]
    pub(crate) fn bucket(&self, key: &[u8]) -> std::ops::Range<u32> {
        let Some(index) = &self.index else {
            return 0..0;
        };
        let b = (hash_bytes(key) >> index.shift) as usize;
        index.starts[b]..index.starts[b + 1]
    }

    /// Whether the encoded key of an indexed row equals `key`.
    #[inline(always)]
    pub(crate) fn equal(&self, row: u32, key: &[u8]) -> bool {
        let r = row as usize;
        match &self.index.as_ref().unwrap().keys {
            Keys::Encoded { data, offsets, .. } => &data[offsets[r]..offsets[r + 1]] == key,
            Keys::Word(_) => false,
        }
    }
}

/// A word-key index: the key of every indexed row, grouped by bucket.
#[derive(Clone, Copy)]
pub(crate) struct WordIndex<'a> {
    starts: &'a [u32],
    shift: u32,
    pub words: &'a [u64],
}

impl WordIndex<'_> {
    /// The rows whose key may equal `key`.
    #[inline(always)]
    pub fn bucket(&self, key: u64) -> std::ops::Range<u32> {
        let b = (hash_word(key) >> self.shift) as usize;
        self.starts[b]..self.starts[b + 1]
    }
}

impl Index {
    /// The index of the rows, and the order the table stores them in
    /// (`None` when they already are in it).
    fn new(
        columns: &[ArrayRef],
        rows: usize,
        keys: &[usize],
        nulls_equal: bool,
    ) -> Result<(Self, Option<Vec<u32>>)> {
        let types = keys
            .iter()
            .map(|&k| columns[k].data_type().clone())
            .collect::<Vec<_>>();
        let decoded = keys
            .iter()
            .map(|&k| scalar::decode(&columns[k]))
            .collect::<Result<Vec<_>>>()?;
        let buckets = rows.max(2).next_power_of_two();
        let shift = 64 - buckets.trailing_zeros();
        let indexed = |r: usize| nulls_equal || decoded.iter().all(|c| c[r].valid != 0);
        let word = types.len() == 1 && word_key(&types[0]) && !nulls_equal;
        let (words, encoders, mut data, mut offsets) = if word {
            let words = decoded[0].iter().map(|c| c.bits).collect::<Vec<_>>();
            (words, vec![], vec![], vec![])
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
            (vec![], encoders, data, offsets)
        };
        // The bucket of each row; unindexed rows go after the last one.
        let bucket = (0..rows)
            .map(|r| match indexed(r) {
                false => buckets as u32,
                true if word => (hash_word(words[r]) >> shift) as u32,
                true => (hash_bytes(&data[offsets[r]..offsets[r + 1]]) >> shift) as u32,
            })
            .collect::<Vec<_>>();
        // A stable counting sort of the rows by bucket.
        let mut starts = vec![0u32; buckets + 2];
        for &b in &bucket {
            starts[b as usize + 1] += 1;
        }
        for b in 1..starts.len() {
            starts[b] += starts[b - 1];
        }
        let mut order = vec![0u32; rows];
        let mut at = starts.clone();
        for (r, &b) in bucket.iter().enumerate() {
            order[at[b as usize] as usize] = r as u32;
            at[b as usize] += 1;
        }
        starts.truncate(buckets + 1);
        let sorted = order.iter().enumerate().all(|(i, &r)| i == r as usize);
        let keys = if word {
            Keys::Word(order.iter().map(|&r| words[r as usize]).collect())
        } else {
            let (old, old_offsets) = (std::mem::take(&mut data), std::mem::take(&mut offsets));
            offsets.push(0);
            for &r in &order {
                let r = r as usize;
                data.extend_from_slice(&old[old_offsets[r]..old_offsets[r + 1]]);
                offsets.push(data.len());
            }
            Keys::Encoded {
                encoders,
                data,
                offsets,
            }
        };
        let index = Self {
            starts,
            shift,
            keys,
        };
        Ok((index, (!sorted).then_some(order)))
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
