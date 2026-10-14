//! Query-owned hash table; keys are scalar values supplied by generated code.
use crate::{
    arrow::{array::ArrayRef, datatypes::*},
    error::{Error, Result},
    kernel::scalar::{self, Cell, OwnedCell},
};
use std::collections::HashMap;
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub(crate) struct WordEntry {
    pub key: u64,
    pub id: u64,
    pub tag: u64,
}
#[repr(C)]
pub(crate) struct GroupIndex {
    pub cache: Option<Box<[WordEntry; 1024]>>,
    types: Vec<DataType>,
    encoders: Vec<scalar::KeyEncoder>,
    owners: Vec<scalar::OwnValue>,
    index: HashMap<Vec<u8>, usize, foldhash::fast::RandomState>,
    rows: Vec<Vec<OwnedCell>>,
    words: HashMap<(u64, bool), usize, foldhash::fast::RandomState>,
    scratch: Vec<u8>,
    word_key: bool,
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
        let word_key = schema.fields().len() == 1
            && matches!(
                schema.field(0).data_type(),
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
        Ok(Self {
            cache: word_key.then(|| Box::new([WordEntry::default(); 1024])),
            types,
            encoders,
            owners,
            index: HashMap::default(),
            rows: vec![],
            words: HashMap::default(),
            scratch: vec![],
            word_key,
        })
    }
    pub fn len(&self) -> usize {
        if self.types.is_empty() {
            1
        } else {
            self.rows.len()
        }
    }
    pub fn intern_row(&mut self, values: &[Cell]) -> usize {
        if self.types.is_empty() {
            return 0;
        }
        if self.word_key {
            return self.intern_word(values[0]);
        }
        self.scratch.clear();
        let mut key = std::mem::take(&mut self.scratch);
        for (v, encode) in values.iter().zip(&self.encoders) {
            encode(*v, &mut key);
        }
        if let Some(&id) = self.index.get(&key) {
            self.scratch = key;
            return id;
        }
        let id = self.rows.len();
        self.rows.push(
            values
                .iter()
                .zip(&self.owners)
                .map(|(v, own)| own(*v))
                .collect(),
        );
        self.index.insert(key, id);
        id
    }
    pub fn intern_word(&mut self, value: Cell) -> usize {
        let key = (
            if value.valid == 0 { 0 } else { value.bits },
            value.valid != 0,
        );
        let slot = key.0 as usize & 1023;
        let tag = if key.1 { 2 } else { 1 };
        let cached = self.cache.as_ref().unwrap()[slot];
        if cached.tag == tag && cached.key == key.0 {
            return cached.id as usize;
        }
        let id = if let Some(&id) = self.words.get(&key) {
            id
        } else {
            let id = self.rows.len();
            self.rows.push(vec![(self.owners[0])(value)]);
            self.words.insert(key, id);
            id
        };
        self.cache.as_mut().unwrap()[slot] = WordEntry {
            key: key.0,
            id: id as u64,
            tag,
        };
        id
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
        for row in 0..rows {
            let values = columns.iter().map(|c| c[row]).collect::<Vec<_>>();
            ids.push(self.intern_row(&values));
        }
        Ok(())
    }
    pub fn columns(&self) -> Result<Vec<ArrayRef>> {
        self.types
            .iter()
            .enumerate()
            .map(|(i, t)| {
                scalar::encode(t, &self.rows.iter().map(|r| r[i].get()).collect::<Vec<_>>())
            })
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::Array;
    use crate::arrow::array::{Int64Array, StringArray, UInt64Array};
    use std::sync::Arc;

    #[test]
    fn word_cache_collisions_keep_full_keys_and_stable_ids() {
        let schema = Schema::new(vec![Field::new("key", DataType::UInt64, true)]);
        let mut index = GroupIndex::new(&schema).unwrap();
        let values = [
            Cell::word(0),
            Cell::word(1024),
            Cell::word(u64::MAX),
            Cell::default(),
            Cell::word(2048),
        ];
        for _ in 0..3 {
            for (expected, value) in values.iter().enumerate() {
                assert_eq!(index.intern_word(*value), expected);
            }
        }
        assert_eq!(index.len(), values.len());
        let column = index.columns().unwrap().remove(0);
        assert_eq!(
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
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
}
