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
use asyncband::shutdown::ShutdownGuard;
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

pub(crate) fn hash_bytes(key: &[u8]) -> u64 {
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
        let plan = KeyPlan::new(&schema, keys.to_vec(), nulls_equal)?;
        let prepared = batches
            .iter()
            .map(|b| plan.prepare(b))
            .collect::<Result<Vec<_>>>()?;
        plan.assemble(schema, batches, &prepared, None)
    }

    /// A table of columns already arranged by KeyPlan::assemble.
    pub(crate) fn from_parts(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        index: Option<TableIndex>,
    ) -> Self {
        let rows = columns.first().map_or(0, |c| c.len());
        Self {
            schema,
            columns,
            rows,
            index: index.map(|i| i.0),
        }
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

/// Key binding is fixed for the whole build; row preparation is local to a job.
#[derive(Clone)]
pub(crate) struct KeyPlan {
    keys: Vec<usize>,
    nulls_equal: bool,
    word: bool,
    encoders: Vec<KeyEncoder>,
}

pub(crate) struct PreparedKeys {
    hashes: Vec<Option<u64>>,
    words: Vec<u64>,
    data: Vec<u8>,
    offsets: Vec<usize>,
}

impl KeyPlan {
    pub fn new(schema: &SchemaRef, keys: Vec<usize>, nulls_equal: bool) -> Result<Self> {
        let types = keys
            .iter()
            .map(|&k| schema.field(k).data_type())
            .collect::<Vec<_>>();
        let word = types.len() == 1 && word_key(types[0]) && !nulls_equal;
        let encoders = if word {
            vec![]
        } else {
            types
                .into_iter()
                .map(scalar::bind_key)
                .collect::<Result<_>>()?
        };
        Ok(Self {
            keys,
            nulls_equal,
            word,
            encoders,
        })
    }

    pub fn prepare(&self, batch: &RecordBatch) -> Result<PreparedKeys> {
        let mut prepared = PreparedKeys {
            hashes: vec![],
            words: vec![],
            data: vec![],
            offsets: vec![0],
        };
        if self.keys.is_empty() {
            return Ok(prepared);
        }
        let columns = batch.columns();
        let decoded = if self.word {
            vec![]
        } else {
            self.keys
                .iter()
                .map(|&k| scalar::decode(&columns[k]))
                .collect::<Result<Vec<_>>>()?
        };
        if self.word {
            prepared.words = key_words(&columns[self.keys[0]]).expect("word keys have word types");
        }
        let nulls = self
            .keys
            .iter()
            .map(|&k| columns[k].logical_nulls())
            .collect::<Vec<_>>();
        for r in 0..batch.num_rows() {
            let hash = if self.word {
                hash_word(prepared.words[r])
            } else {
                let start = prepared.data.len();
                for (cells, encode) in decoded.iter().zip(&self.encoders) {
                    encode(cells[r], &mut prepared.data);
                }
                prepared.offsets.push(prepared.data.len());
                hash_bytes(&prepared.data[start..])
            };
            let indexed = self.nulls_equal
                || nulls
                    .iter()
                    .all(|n| n.as_ref().is_none_or(|n| n.is_valid(r)));
            prepared.hashes.push(indexed.then_some(hash));
        }
        Ok(prepared)
    }

    /// Stable counting sort by the existing bucket hash, followed by one
    /// interleave per payload column. No concatenated payload is reordered.
    pub fn assemble(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
        prepared: &[PreparedKeys],
        guard: Option<&ShutdownGuard>,
    ) -> Result<Table> {
        let check = || {
            if guard.is_some_and(|g| g.is_shutdown_requested()) {
                Err(Error::cancelled())
            } else {
                Ok(())
            }
        };
        check()?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if rows >= END as usize {
            return Err(Error::new(
                crate::error::ErrorKind::ResourceExhausted,
                format!("a table holds at most {} rows", END - 1),
            ));
        }
        if self.keys.is_empty() {
            let columns = (0..schema.fields().len())
                .map(|i| {
                    check()?;
                    concat(
                        batches.iter().map(|b| b.column(i)),
                        schema.field(i).data_type(),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let mut table = Table::from_parts(schema, columns, None);
            table.rows = rows;
            return Ok(table);
        }
        let buckets = rows.max(2).next_power_of_two();
        let shift = 64 - buckets.trailing_zeros();
        let bucket = |hash: Option<u64>| hash.map_or(buckets, |h| (h >> shift) as usize);
        let mut starts = vec![0u32; buckets + 2];
        for chunk in prepared {
            check()?;
            for (r, &hash) in chunk.hashes.iter().enumerate() {
                if r % 4096 == 0 {
                    check()?;
                }
                starts[bucket(hash) + 1] += 1;
            }
        }
        for b in 1..starts.len() {
            if b % 4096 == 0 {
                check()?;
            }
            starts[b] += starts[b - 1];
        }
        let mut order = vec![(0, 0); rows];
        let mut at = starts.clone();
        for (chunk, keys) in prepared.iter().enumerate() {
            for (r, &hash) in keys.hashes.iter().enumerate() {
                if r % 4096 == 0 {
                    check()?;
                }
                let b = bucket(hash);
                order[at[b] as usize] = (chunk, r);
                at[b] += 1;
            }
        }
        starts.truncate(buckets + 1);
        let keys = if self.word {
            let mut words = Vec::with_capacity(rows);
            for (i, &(c, r)) in order.iter().enumerate() {
                if i % 4096 == 0 {
                    check()?;
                }
                words.push(prepared[c].words[r]);
            }
            Keys::Word(words)
        } else {
            let mut data = vec![];
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            for (i, &(c, r)) in order.iter().enumerate() {
                if i % 4096 == 0 {
                    check()?;
                }
                let p = &prepared[c];
                data.extend_from_slice(&p.data[p.offsets[r]..p.offsets[r + 1]]);
                offsets.push(data.len());
            }
            Keys::Encoded {
                encoders: self.encoders.clone(),
                data,
                offsets,
            }
        };
        let columns = (0..schema.fields().len())
            .map(|i| {
                check()?;
                let arrays = batches
                    .iter()
                    .map(|b| b.column(i).as_ref())
                    .collect::<Vec<_>>();
                if arrays.is_empty() {
                    return Ok(crate::arrow::array::new_empty_array(
                        schema.field(i).data_type(),
                    ));
                }
                arrow_select::interleave::interleave(&arrays, &order).map_err(|source| {
                    Error::new(
                        crate::error::ErrorKind::ResourceExhausted,
                        "a column exceeds the offsets of its type",
                    )
                    .with_source(source)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        check()?;
        Ok(Table::from_parts(
            schema,
            columns,
            Some(TableIndex(Index {
                starts,
                shift,
                keys,
            })),
        ))
    }
}

/// One contiguous bucket range, or the unindexed tail.
struct TablePartition {
    starts: Vec<u32>,
    keys: Keys,
    columns: Vec<ArrayRef>,
    rows: usize,
}

impl KeyPlan {
    /// High hash bits select contiguous bucket ranges. The last partition
    /// retains non-indexed NULL rows in input order.
    pub(crate) fn partitions(
        &self,
        prepared: &[PreparedKeys],
        count: usize,
        guard: &ShutdownGuard,
    ) -> Result<Vec<Vec<(usize, usize)>>> {
        if !count.is_power_of_two() {
            return Err(Error::invalid_plan(
                "table partitions must be a power of two".into(),
            ));
        }
        let mut out = vec![vec![]; count + 1];
        for (chunk, keys) in prepared.iter().enumerate() {
            for (row, hash) in keys.hashes.iter().enumerate() {
                if row % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let p = match hash {
                    None => count,
                    Some(_) if count == 1 => 0,
                    Some(h) => (h >> (64 - count.trailing_zeros())) as usize,
                };
                out[p].push((chunk, row));
            }
        }
        Ok(out)
    }

    fn gather_keys(
        &self,
        prepared: &[PreparedKeys],
        order: &[(usize, usize)],
        guard: &ShutdownGuard,
    ) -> Result<Keys> {
        if self.word {
            let mut words = Vec::with_capacity(order.len());
            for (i, &(c, r)) in order.iter().enumerate() {
                if i % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                words.push(prepared[c].words[r]);
            }
            Ok(Keys::Word(words))
        } else {
            let mut data = vec![];
            let mut offsets = Vec::with_capacity(order.len() + 1);
            offsets.push(0);
            for (i, &(c, r)) in order.iter().enumerate() {
                if i % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let p = &prepared[c];
                data.extend_from_slice(&p.data[p.offsets[r]..p.offsets[r + 1]]);
                offsets.push(data.len());
            }
            Ok(Keys::Encoded {
                encoders: self.encoders.clone(),
                data,
                offsets,
            })
        }
    }

    fn assemble_partition(
        &self,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        prepared: &[PreparedKeys],
        rows: Vec<(usize, usize)>,
        first: usize,
        buckets: usize,
        shift: u32,
        guard: &ShutdownGuard,
    ) -> Result<TablePartition> {
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        let mut starts = vec![0u32; buckets + 1];
        let order = if buckets == 0 {
            rows
        } else {
            for (i, &(c, r)) in rows.iter().enumerate() {
                if i % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let b =
                    (prepared[c].hashes[r].expect("indexed partition") >> shift) as usize - first;
                starts[b + 1] += 1;
            }
            for b in 1..starts.len() {
                starts[b] += starts[b - 1];
            }
            let mut at = starts.clone();
            let mut order = vec![(0, 0); rows.len()];
            for (i, &(c, r)) in rows.iter().enumerate() {
                if i % 4096 == 0 && guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let b = (prepared[c].hashes[r].unwrap() >> shift) as usize - first;
                order[at[b] as usize] = (c, r);
                at[b] += 1;
            }
            order
        };
        let keys = self.gather_keys(prepared, &order, guard)?;
        let batch = crate::exec::sort::interleave_chunks(schema, batches, &order, guard)?;
        Ok(TablePartition {
            starts,
            keys,
            columns: batch.columns().to_vec(),
            rows: order.len(),
        })
    }

    pub(crate) async fn assemble_on(
        self: &Arc<Self>,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        prepared: Vec<PreparedKeys>,
        partitions: Vec<Vec<(usize, usize)>>,
        guard: &ShutdownGuard,
        cpu: &dyn crate::exec::Cpu,
    ) -> Result<Table> {
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if rows >= END as usize {
            return Err(Error::new(
                crate::error::ErrorKind::ResourceExhausted,
                format!("a table holds at most {} rows", END - 1),
            ));
        }
        let count = partitions.len().saturating_sub(1);
        let buckets = rows.max(2).next_power_of_two();
        if self.keys.is_empty() || count <= 1 || buckets < count {
            return self.assemble(schema, &batches, &prepared, Some(guard));
        }
        let shift = 64 - buckets.trailing_zeros();
        let width = buckets / count;
        let batches = Arc::new(batches);
        let prepared = Arc::new(prepared);
        let jobs = partitions
            .into_iter()
            .enumerate()
            .map(|(k, rows)| {
                let (plan, schema, batches, prepared, guard) = (
                    self.clone(),
                    schema.clone(),
                    batches.clone(),
                    prepared.clone(),
                    guard.clone(),
                );
                Box::new(move || {
                    plan.assemble_partition(
                        &schema,
                        &batches,
                        &prepared,
                        rows,
                        k * width,
                        if k == count { 0 } else { width },
                        shift,
                        &guard,
                    )
                }) as Box<dyn FnOnce() -> Result<TablePartition> + Send>
            })
            .collect();
        let mut parts = crate::exec::run_all(cpu, jobs).await?;
        let mut starts = Vec::with_capacity(buckets + 1);
        let mut base = 0u32;
        for part in &parts[..count] {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            starts.extend(part.starts[..width].iter().map(|s| s + base));
            base += part.rows as u32;
        }
        starts.push(base);
        let keys = if self.word {
            let mut words = Vec::with_capacity(rows);
            for part in &mut parts {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let Keys::Word(mut values) = std::mem::replace(&mut part.keys, Keys::Word(vec![]))
                else {
                    unreachable!("word segment")
                };
                words.append(&mut values);
            }
            Keys::Word(words)
        } else {
            let mut data = vec![];
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            for part in &mut parts {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                let Keys::Encoded {
                    data: mut bytes,
                    offsets: local,
                    ..
                } = std::mem::replace(&mut part.keys, Keys::Word(vec![]))
                else {
                    unreachable!("encoded segment")
                };
                let base = data.len();
                offsets.extend(local.into_iter().skip(1).map(|o| o + base));
                data.append(&mut bytes);
            }
            Keys::Encoded {
                encoders: self.encoders.clone(),
                data,
                offsets,
            }
        };
        let parts = Arc::new(parts);
        let jobs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let parts = parts.clone();
                let ty = f.data_type().clone();
                let guard = guard.clone();
                Box::new(move || {
                    if guard.is_shutdown_requested() {
                        return Err(Error::cancelled());
                    }
                    concat(parts.iter().map(|p| &p.columns[i]), &ty)
                }) as Box<dyn FnOnce() -> Result<ArrayRef> + Send>
            })
            .collect();
        let columns = crate::exec::run_all(cpu, jobs).await?;
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        Ok(Table::from_parts(
            schema,
            columns,
            Some(TableIndex(Index {
                starts,
                shift,
                keys,
            })),
        ))
    }
}

/// The index of a table under construction.
pub(crate) struct TableIndex(Index);

/// The canonical word of every row of a key column of a word type, as
/// `scalar::decode` gives them, NULL rows zero; None for other types.
fn key_words(a: &ArrayRef) -> Option<Vec<u64>> {
    use crate::arrow::array::{AsArray, PrimitiveArray};
    use crate::arrow::datatypes::*;
    fn words<T: ArrowPrimitiveType>(
        a: &PrimitiveArray<T>,
        f: impl Fn(T::Native) -> u64,
    ) -> Vec<u64> {
        match a.nulls() {
            None => a.values().iter().map(|v| f(*v)).collect(),
            Some(n) => a
                .values()
                .iter()
                .enumerate()
                .map(|(i, v)| if n.is_valid(i) { f(*v) } else { 0 })
                .collect(),
        }
    }
    Some(match a.data_type() {
        DataType::Int8 => words(a.as_primitive::<Int8Type>(), |v| v as u64),
        DataType::Int16 => words(a.as_primitive::<Int16Type>(), |v| v as u64),
        DataType::Int32 => words(a.as_primitive::<Int32Type>(), |v| v as u64),
        DataType::Int64 => words(a.as_primitive::<Int64Type>(), |v| v as u64),
        DataType::UInt8 => words(a.as_primitive::<UInt8Type>(), |v| v as u64),
        DataType::UInt16 => words(a.as_primitive::<UInt16Type>(), |v| v as u64),
        DataType::UInt32 => words(a.as_primitive::<UInt32Type>(), |v| v as u64),
        DataType::UInt64 => words(a.as_primitive::<UInt64Type>(), |v| v),
        DataType::Date32 => words(a.as_primitive::<Date32Type>(), |v| v as u64),
        DataType::Date64 => words(a.as_primitive::<Date64Type>(), |v| v as u64),
        _ => return None,
    })
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

#[cfg(test)]
mod partition_tests {
    use super::*;
    use crate::arrow::{
        array::{Int64Array, StringArray},
        datatypes::{Field, Schema},
    };

    #[test]
    fn parallel_segments_preserve_every_bucket_key_and_payload_row() {
        for word in [false, true] {
            for nulls_equal in [false, true] {
                let schema = Arc::new(Schema::new(vec![
                    Field::new(
                        "key",
                        if word {
                            DataType::Int64
                        } else {
                            DataType::Utf8
                        },
                        true,
                    ),
                    Field::new("row", DataType::Int64, false),
                ]));
                let plan = Arc::new(KeyPlan::new(&schema, vec![0], nulls_equal).unwrap());
                for rows in [0, 1, 3, 513] {
                    let keys: ArrayRef = if word {
                        Arc::new(Int64Array::from_iter(
                            (0..rows).map(|i| (i % 7 != 0).then_some(i % 17)),
                        ))
                    } else {
                        Arc::new(StringArray::from_iter(
                            (0..rows).map(|i| (i % 7 != 0).then(|| format!("key{}", i % 17))),
                        ))
                    };
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![keys, Arc::new(Int64Array::from_iter_values(0..rows))],
                    )
                    .unwrap();
                    let batches = (0..rows as usize)
                        .step_by(19)
                        .map(|i| batch.slice(i, 19.min(rows as usize - i)))
                        .collect::<Vec<_>>();
                    let prepared = batches
                        .iter()
                        .map(|b| plan.prepare(b).unwrap())
                        .collect::<Vec<_>>();
                    let serial = plan
                        .assemble(schema.clone(), &batches, &prepared, None)
                        .unwrap();
                    for k in [1, 2, 8] {
                        let guard = asyncband::shutdown::new().1;
                        let prepared = batches
                            .iter()
                            .map(|b| plan.prepare(b).unwrap())
                            .collect::<Vec<_>>();
                        let parts = plan.partitions(&prepared, k, &guard).unwrap();
                        let parallel = futures::executor::block_on(plan.assemble_on(
                            schema.clone(),
                            batches.clone(),
                            prepared,
                            parts,
                            &guard,
                            &crate::exec::Inline,
                        ))
                        .unwrap();
                        assert_eq!(serial.rows, parallel.rows);
                        for (a, b) in serial.columns.iter().zip(&parallel.columns) {
                            assert_eq!(a.to_data(), b.to_data());
                        }
                        let (a, b) = (
                            serial.index.as_ref().unwrap(),
                            parallel.index.as_ref().unwrap(),
                        );
                        assert_eq!(a.starts, b.starts);
                        assert_eq!(a.shift, b.shift);
                        match (&a.keys, &b.keys) {
                            (Keys::Word(a), Keys::Word(b)) => assert_eq!(a, b),
                            (
                                Keys::Encoded {
                                    data: a,
                                    offsets: ao,
                                    ..
                                },
                                Keys::Encoded {
                                    data: b,
                                    offsets: bo,
                                    ..
                                },
                            ) => {
                                assert_eq!(a, b);
                                assert_eq!(ao, bo);
                            }
                            _ => panic!("different key representations"),
                        }
                    }
                }
            }
        }
    }
}
