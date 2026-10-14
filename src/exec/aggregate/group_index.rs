//! Query-owned hash table of group keys: group IDs in insertion order.
use crate::{
    arrow::{array::ArrayRef, datatypes::*},
    error::{Error, Result},
    kernel::scalar::{self, Cell, OwnedCell},
};

/// An open-addressing table of group IDs: one fixed-width key compares its
/// canonical word, several fixed-width keys their words, others their
/// encoded bytes.
pub(crate) struct GroupIndex {
    types: Vec<DataType>,
    encoders: Vec<scalar::KeyEncoder>,
    owners: Vec<scalar::OwnValue>,
    /// The key cells of every group, one per key column.
    cells: Vec<OwnedCell>,
    groups: usize,
    /// Per slot: a group ID plus one, or 0 when empty. A power of two at most
    /// half full.
    slots: Vec<u32>,
    keys: Keys,
    scratch: Vec<u8>,
}

enum Keys {
    /// No key columns: one group.
    None,
    /// One fixed-width key: the canonical word of every group but the NULL
    /// one.
    Word { words: Vec<u64>, null: Option<u32> },
    /// Fixed-width keys: per group `width` words, the canonical word of each
    /// column (two for 128-bit ones, zero when NULL), then a word with a bit
    /// per valid column.
    Fixed {
        width: usize,
        words: Vec<u64>,
        hashes: Vec<u64>,
    },
    /// Encoded keys of every group, with their hashes.
    Encoded {
        hashes: Vec<u64>,
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
    let mut h = foldhash::fast::FixedState::with_seed(0x2d35_8dcc_aa6c_78a5).build_hasher();
    h.write(key);
    h.finish()
}

/// The hash of a fixed-width key's words.
#[inline(always)]
pub(crate) fn hash_words(words: &[u64]) -> u64 {
    let mut h = 0x243f_6a88_85a3_08d3u64;
    for &w in words {
        h = (h.rotate_left(23) ^ w).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    h ^ (h >> 31)
}

/// The words of a fixed-width key column: 2 for 128-bit types, 1 for
/// other integers, dates and timestamps, None for any other type.
pub(crate) fn fixed_words(t: &DataType) -> Option<usize> {
    match t {
        DataType::Decimal128(..) => Some(2),
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(..) => Some(1),
        _ => None,
    }
}

/// The slot of a hash in a table of `len` slots.
#[inline(always)]
fn home(hash: u64, len: usize) -> usize {
    (hash >> (64 - len.trailing_zeros())) as usize
}

impl GroupIndex {
    pub fn new(schema: &Schema) -> Result<Self> {
        let types = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        if types.iter().any(|t| !scalar::supported(t)) {
            return Err(Error::unsupported("unsupported group key type".into()));
        }
        let encoders = types.iter().map(scalar::bind_key).collect::<Result<_>>()?;
        let owners = types.iter().map(scalar::bind_owner).collect();
        let word = types.len() == 1
            && matches!(
                types[0],
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(..)
            );
        let fixed = types.iter().map(fixed_words).sum::<Option<usize>>();
        let keys = if types.is_empty() {
            Keys::None
        } else if word {
            Keys::Word {
                words: vec![],
                null: None,
            }
        } else if let Some(width) = fixed.filter(|_| types.len() <= 64) {
            Keys::Fixed {
                width: width + 1,
                words: vec![],
                hashes: vec![],
            }
        } else {
            Keys::Encoded {
                hashes: vec![],
                data: vec![],
                offsets: vec![0],
            }
        };
        Ok(Self {
            types,
            encoders,
            owners,
            cells: vec![],
            groups: 0,
            slots: vec![0; 16],
            keys,
            scratch: vec![],
        })
    }

    pub fn len(&self) -> usize {
        match self.keys {
            Keys::None => 1,
            _ => self.groups,
        }
    }

    /// Whether the key is one fixed-width column, interned by `intern_word`.
    pub fn word_keys(&self) -> bool {
        matches!(self.keys, Keys::Word { .. })
    }

    /// The words of a key interned by `intern_fixed`, if it is one.
    pub fn fixed_width(&self) -> Option<usize> {
        match self.keys {
            Keys::Fixed { width, .. } => Some(width),
            _ => None,
        }
    }

    pub fn intern_row(&mut self, values: &[Cell]) -> usize {
        match self.keys {
            Keys::None => 0,
            Keys::Word { .. } => self.intern_word(values[0].bits, values[0].valid != 0),
            Keys::Fixed { width, .. } => {
                let (mut small, mut large) = ([0u64; 8], vec![]);
                let words: &mut [u64] = if width <= 8 {
                    &mut small[..width]
                } else {
                    large.resize(width, 0);
                    &mut large
                };
                let mut at = 0;
                for (i, (v, t)) in values.iter().zip(&self.types).enumerate() {
                    let valid = v.valid != 0;
                    words[at] = if valid { v.bits } else { 0 };
                    if fixed_words(t) == Some(2) {
                        words[at + 1] = if valid { v.len } else { 0 };
                        at += 1;
                    }
                    at += 1;
                    words[width - 1] |= (valid as u64) << i;
                }
                self.intern_fixed(words)
            }
            Keys::Encoded { .. } => self.intern_encoded(values),
        }
    }

    /// The group of a fixed-width key's words, laid out as `Keys::Fixed`
    /// describes. A new group gets the next ID.
    #[inline]
    pub fn intern_fixed(&mut self, key: &[u64]) -> usize {
        let Keys::Fixed {
            width,
            words,
            hashes,
        } = &mut self.keys
        else {
            unreachable!("fixed keys")
        };
        let width = *width;
        let hash = hash_words(key);
        let len = self.slots.len();
        let mut s = home(hash, len);
        loop {
            let slot = self.slots[s];
            if slot == 0 {
                break;
            }
            let id = slot as usize - 1;
            if hashes[id] == hash && words[id * width..(id + 1) * width] == *key {
                return id;
            }
            s = (s + 1) & (len - 1);
        }
        let id = self.groups;
        words.extend_from_slice(key);
        hashes.push(hash);
        self.slots[s] = id as u32 + 1;
        self.groups += 1;
        self.grow();
        id
    }

    /// The group of a word key, the canonical word of a fixed-width value;
    /// `valid` false for NULL. A new group gets the next ID.
    #[inline(always)]
    pub fn intern_word(&mut self, key: u64, valid: bool) -> usize {
        let Keys::Word { words, null } = &mut self.keys else {
            unreachable!("word keys")
        };
        let id = self.groups;
        if !valid {
            if let Some(id) = null {
                return *id as usize;
            }
            *null = Some(id as u32);
            words.push(0);
            self.groups += 1;
            return id;
        }
        let len = self.slots.len();
        let mut s = home(hash_word(key), len);
        loop {
            let slot = self.slots[s];
            if slot == 0 {
                break;
            }
            if words[slot as usize - 1] == key {
                return slot as usize - 1;
            }
            s = (s + 1) & (len - 1);
        }
        words.push(key);
        self.slots[s] = id as u32 + 1;
        self.groups += 1;
        self.grow();
        id
    }

    /// Records the key cells of a new group.
    fn add(&mut self, values: &[Cell]) {
        self.cells
            .extend(values.iter().zip(&self.owners).map(|(v, own)| own(*v)));
        self.groups += 1;
    }

    fn intern_encoded(&mut self, values: &[Cell]) -> usize {
        let mut key = std::mem::take(&mut self.scratch);
        key.clear();
        for (v, encode) in values.iter().zip(&self.encoders) {
            encode(*v, &mut key);
        }
        let hash = hash_bytes(&key);
        let Keys::Encoded {
            hashes,
            data,
            offsets,
        } = &mut self.keys
        else {
            unreachable!("encoded keys")
        };
        let len = self.slots.len();
        let mut s = home(hash, len);
        loop {
            let slot = self.slots[s];
            if slot == 0 {
                break;
            }
            let id = slot as usize - 1;
            if hashes[id] == hash && data[offsets[id]..offsets[id + 1]] == key[..] {
                self.scratch = key;
                return id;
            }
            s = (s + 1) & (len - 1);
        }
        let id = self.groups;
        hashes.push(hash);
        data.extend_from_slice(&key);
        offsets.push(data.len());
        self.slots[s] = id as u32 + 1;
        self.scratch = key;
        self.add(values);
        self.grow();
        id
    }

    /// Doubles the table once the groups fill half of it.
    fn grow(&mut self) {
        if self.groups * 2 <= self.slots.len() {
            return;
        }
        let len = self.slots.len() * 2;
        let mut slots = vec![0u32; len];
        let mut place = |hash: u64, id: usize| {
            let mut s = home(hash, len);
            while slots[s] != 0 {
                s = (s + 1) & (len - 1);
            }
            slots[s] = id as u32 + 1;
        };
        match &self.keys {
            Keys::Word { words, null } => {
                for (id, &w) in words.iter().enumerate() {
                    if Some(id as u32) != *null {
                        place(hash_word(w), id);
                    }
                }
            }
            Keys::Encoded { hashes, .. } | Keys::Fixed { hashes, .. } => {
                for (id, &h) in hashes.iter().enumerate() {
                    place(h, id);
                }
            }
            Keys::None => {}
        }
        self.slots = slots;
    }

    /// The hash of every row's key, as this index hashes keys: equal keys
    /// hash equally in every index of the same key types.
    pub fn hash_rows(&self, columns: &[ArrayRef], rows: usize) -> Result<Vec<u64>> {
        let columns = columns
            .iter()
            .map(scalar::decode)
            .collect::<Result<Vec<_>>>()?;
        Ok(match self.keys {
            Keys::None => vec![0; rows],
            Keys::Word { .. } => columns[0]
                .iter()
                .take(rows)
                .map(|c| if c.valid == 0 { 0 } else { hash_word(c.bits) })
                .collect(),
            Keys::Fixed { width, .. } => {
                let mut words = vec![0u64; width];
                (0..rows)
                    .map(|r| {
                        words.fill(0);
                        let mut at = 0;
                        for (i, (c, t)) in columns.iter().zip(&self.types).enumerate() {
                            let v = c[r];
                            let valid = v.valid != 0;
                            words[at] = if valid { v.bits } else { 0 };
                            if fixed_words(t) == Some(2) {
                                words[at + 1] = if valid { v.len } else { 0 };
                                at += 1;
                            }
                            at += 1;
                            words[width - 1] |= (valid as u64) << i;
                        }
                        hash_words(&words)
                    })
                    .collect()
            }
            Keys::Encoded { .. } => {
                let mut key = vec![];
                (0..rows)
                    .map(|r| {
                        key.clear();
                        for (c, encode) in columns.iter().zip(&self.encoders) {
                            encode(c[r], &mut key);
                        }
                        hash_bytes(&key)
                    })
                    .collect()
            }
        })
    }

    pub fn intern(
        &mut self,
        columns: &[ArrayRef],
        rows: usize,
        ids: &mut Vec<usize>,
    ) -> Result<()> {
        let columns = columns
            .iter()
            .map(scalar::decode)
            .collect::<Result<Vec<_>>>()?;
        ids.clear();
        let mut values = Vec::with_capacity(columns.len());
        for row in 0..rows {
            values.clear();
            values.extend(columns.iter().map(|c| c[row]));
            ids.push(self.intern_row(&values));
        }
        Ok(())
    }

    pub fn columns(&self) -> Result<Vec<ArrayRef>> {
        let columns = self.types.len();
        match &self.keys {
            Keys::None => Ok(vec![]),
            Keys::Word { words, null } => {
                let cells = words
                    .iter()
                    .enumerate()
                    .map(|(g, &w)| match Some(g as u32) == *null {
                        true => Cell::default(),
                        false => Cell::word(w),
                    })
                    .collect::<Vec<_>>();
                Ok(vec![scalar::encode(&self.types[0], &cells)?])
            }
            Keys::Fixed { width, words, .. } => {
                let mut at = 0;
                self.types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        let wide = fixed_words(t) == Some(2);
                        let cells = words
                            .chunks_exact(*width)
                            .map(|k| match k[width - 1] >> i & 1 {
                                0 => Cell::default(),
                                _ => Cell {
                                    bits: k[at],
                                    len: if wide { k[at + 1] } else { 0 },
                                    valid: 1,
                                },
                            })
                            .collect::<Vec<_>>();
                        at += 1 + wide as usize;
                        scalar::encode(t, &cells)
                    })
                    .collect()
            }
            Keys::Encoded { .. } => self
                .types
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let cells = (0..self.groups)
                        .map(|g| self.cells[g * columns + i].get())
                        .collect::<Vec<_>>();
                    scalar::encode(t, &cells)
                })
                .collect(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::Array;
    use crate::arrow::array::{Decimal128Array, Int32Array, Int64Array, StringArray, UInt64Array};
    use std::sync::Arc;

    #[test]
    fn colliding_word_keys_keep_full_keys_and_stable_ids() {
        let schema = Schema::new(vec![Field::new("key", DataType::UInt64, true)]);
        let mut index = GroupIndex::new(&schema).unwrap();
        let values = [
            Cell::word(0),
            Cell::word(1024),
            Cell::word(u64::MAX),
            Cell::default(),
            Cell::word(2048),
        ];
        // Enough keys to grow the table several times.
        let more = (0..1000u64).map(|k| Cell::word(k * 4096 + 7)).collect::<Vec<_>>();
        for _ in 0..3 {
            for (expected, value) in values.iter().chain(&more).enumerate() {
                assert_eq!(index.intern_row(&[*value]), expected);
            }
        }
        assert_eq!(index.len(), values.len() + more.len());
        let column = index.columns().unwrap().remove(0);
        assert_eq!(
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .take(5)
                .collect::<Vec<_>>(),
            [Some(0), Some(1024), Some(u64::MAX), None, Some(2048)]
        );
    }

    #[test]
    fn signed_keys_keep_null_extremes_and_reuse_ids_across_batches() {
        let schema = Schema::new(vec![Field::new("key", DataType::Int64, true)]);
        let mut index = GroupIndex::new(&schema).unwrap();
        let mut ids = vec![];
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(0),
            Some(i64::MAX),
            None,
            Some(i64::MIN),
        ]));
        index.intern(&[array], 6, &mut ids).unwrap();
        assert_eq!(ids, [0, 1, 2, 3, 1, 0]);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(-1)]));
        index.intern(&[array], 3, &mut ids).unwrap();
        assert_eq!(ids, [3, 1, 4]);
        assert_eq!(index.len(), 5);
        let columns = index.columns().unwrap();
        let values = columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            [Some(i64::MIN), None, Some(0), Some(i64::MAX), Some(-1)]
        );
    }

    #[test]
    fn unsigned_slices_keep_full_width_values_and_null_offsets() {
        let schema = Schema::new(vec![Field::new("key", DataType::UInt64, true)]);
        let mut index = GroupIndex::new(&schema).unwrap();
        let mut ids = vec![];
        let array: ArrayRef = Arc::new(UInt64Array::from(vec![
            Some(13),
            None,
            Some(u64::MAX),
            Some(0),
            Some(u64::MAX),
        ]));
        index.intern(&[array.slice(1, 4)], 4, &mut ids).unwrap();
        assert_eq!(ids, [0, 1, 2, 1]);
        let columns = index.columns().unwrap();
        assert_eq!(
            columns[0]
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [None, Some(u64::MAX), Some(0)]
        );
    }

    #[test]
    fn compound_keys_use_row_semantics() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ]);
        let mut index = GroupIndex::new(&schema).unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(1)])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("a"),
                Some("a"),
            ])),
        ];
        let mut ids = vec![];
        index.intern(&columns, 4, &mut ids).unwrap();
        assert_eq!(ids, [0, 1, 2, 0]);
        let emitted = index.columns().unwrap();
        assert_eq!(emitted[0].to_data(), columns[0].slice(0, 3).to_data());
        assert_eq!(emitted[1].to_data(), columns[1].slice(0, 3).to_data());
    }

    #[test]
    fn fixed_width_compound_keys_keep_nulls_and_wide_values() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("amount", DataType::Decimal128(20, 2), true),
        ]);
        let mut index = GroupIndex::new(&schema).unwrap();
        assert_eq!(index.fixed_width(), Some(4));
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(-1),
            None,
            Some(-1),
            Some(7),
            None,
            Some(-1),
        ]));
        let amounts: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(i128::MIN + 5),
                Some(3),
                Some(i128::MIN + 5),
                None,
                Some(3),
                Some(0),
            ])
            .with_precision_and_scale(20, 2)
            .unwrap(),
        );
        let mut ids = vec![];
        index.intern(&[keys.clone(), amounts.clone()], 6, &mut ids).unwrap();
        assert_eq!(ids, [0, 1, 0, 2, 1, 3]);
        let columns = index.columns().unwrap();
        assert_eq!(columns[0].to_data(), Int32Array::from(vec![Some(-1), None, Some(7), Some(-1)]).to_data());
        let amounts = columns[1].as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(
            amounts.iter().collect::<Vec<_>>(),
            [Some(i128::MIN + 5), Some(3), None, Some(0)]
        );
    }

    #[test]
    fn many_compound_keys_survive_growth() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ]);
        let mut index = GroupIndex::new(&schema).unwrap();
        let n = 5000i64;
        let keys: ArrayRef = Arc::new(Int64Array::from_iter_values((0..n).map(|i| i % 1000)));
        let text: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("t{}", i % 7)),
        ));
        let mut ids = vec![];
        index.intern(&[keys.clone(), text.clone()], n as usize, &mut ids).unwrap();
        // (i % 1000, i % 7) repeats with period 7000: every row is new.
        assert_eq!(index.len(), n as usize);
        let first = ids.clone();
        index.intern(&[keys, text], n as usize, &mut ids).unwrap();
        assert_eq!(ids, first);
        assert_eq!(index.len(), n as usize);
    }
}
