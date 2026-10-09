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

use crate::error::{Error, Result};
use arrow::{
    array::{Array, ArrayRef, PrimitiveArray},
    buffer::NullBuffer,
    datatypes::*,
    row::{RowConverter, SortField},
};
use std::{collections::HashMap, hash::Hash, sync::Arc};

type NativeMap<K> = HashMap<K, usize, foldhash::fast::RandomState>;

/// Integer keys already have an equality-preserving representation. Keep it
/// instead of encoding every input row and decoding the output through Rows.
/// NULL has a separate group so every possible integer remains a usable key.
pub(super) struct IntegerIndex<T: ArrowPrimitiveType>
where
    T::Native: Eq + Hash,
{
    index: NativeMap<T::Native>,
    values: Vec<T::Native>,
    null_group: Option<usize>,
}

impl<T: ArrowPrimitiveType> IntegerIndex<T>
where
    T::Native: Eq + Hash,
{
    fn new() -> Self {
        Self {
            index: NativeMap::default(),
            values: vec![],
            null_group: None,
        }
    }

    fn intern(&mut self, array: &ArrayRef, ids: &mut Vec<usize>) -> Result<()> {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                Error::invalid_input("group key type changed during execution".into())
            })?;
        ids.clear();
        ids.reserve(array.len());
        if array.null_count() == 0 {
            for &value in array.values() {
                ids.push(self.intern_value(value));
            }
        } else {
            for value in array {
                ids.push(match value {
                    Some(value) => self.intern_value(value),
                    None => *self.null_group.get_or_insert_with(|| {
                        let id = self.values.len();
                        self.values.push(T::Native::default());
                        id
                    }),
                });
            }
        }
        Ok(())
    }

    #[inline]
    fn intern_value(&mut self, value: T::Native) -> usize {
        *self.index.entry(value).or_insert_with(|| {
            let id = self.values.len();
            self.values.push(value);
            id
        })
    }

    fn column(&self) -> ArrayRef {
        let nulls = self.null_group.map(|null| {
            NullBuffer::from(
                (0..self.values.len())
                    .map(|i| i != null)
                    .collect::<Vec<_>>(),
            )
        });
        Arc::new(PrimitiveArray::<T>::new(self.values.clone().into(), nulls))
    }
}

/// The row format remains the fallback for compound keys and other Arrow types,
/// preserving their established equality/NULL semantics.
pub(super) struct RowIndex {
    converter: RowConverter,
    index: HashMap<Vec<u8>, usize>,
    keys: Vec<Vec<u8>>,
}

macro_rules! group_index {
    ($($variant:ident => $ty:ty),* $(,)?) => {
        pub(super) enum GroupIndex {
            Global,
            $($variant(IntegerIndex<$ty>),)*
            Rows(RowIndex),
        }

        impl GroupIndex {
            pub(super) fn new(schema: &Schema) -> Result<Self> {
                if schema.fields().is_empty() {
                    return Ok(Self::Global);
                }
                if schema.fields().len() == 1 {
                    match schema.field(0).data_type() {
                        $(DataType::$variant => return Ok(Self::$variant(IntegerIndex::new())),)*
                        _ => {}
                    }
                }
                Ok(Self::Rows(RowIndex {
                    converter: RowConverter::new(schema.fields().iter()
                        .map(|f| SortField::new(f.data_type().clone())).collect())?,
                    index: HashMap::new(),
                    keys: vec![],
                }))
            }

            pub(super) fn len(&self) -> usize {
                match self {
                    Self::Global => 1,
                    $(Self::$variant(index) => index.values.len(),)*
                    Self::Rows(index) => index.keys.len(),
                }
            }

            pub(super) fn intern(&mut self, columns: &[ArrayRef], rows: usize, ids: &mut Vec<usize>) -> Result<()> {
                match self {
                    Self::Global => {
                        // AggregateState owns this ID buffer for a fixed index.
                        // Its existing entries are already zero; initialize only growth.
                        debug_assert!(ids.iter().all(|&id| id == 0));
                        ids.resize(rows, 0);
                        Ok(())
                    }
                    $(Self::$variant(index) => index.intern(&columns[0], ids),)*
                    Self::Rows(index) => {
                        let rows = index.converter.convert_columns(columns)?;
                        ids.clear();
                        ids.reserve(rows.num_rows());
                        for row in rows.iter() {
                            let id = match index.index.get(row.as_ref()) {
                                Some(id) => *id,
                                None => {
                                    let id = index.keys.len();
                                    let key = row.as_ref().to_vec();
                                    index.index.insert(key.clone(), id);
                                    index.keys.push(key);
                                    id
                                }
                            };
                            ids.push(id);
                        }
                        Ok(())
                    }
                }
            }

            pub(super) fn columns(&self) -> Result<Vec<ArrayRef>> {
                match self {
                    Self::Global => Ok(vec![]),
                    $(Self::$variant(index) => Ok(vec![index.column()]),)*
                    Self::Rows(index) => {
                        let parser = index.converter.parser();
                        Ok(index.converter.convert_rows(index.keys.iter().map(|key| parser.parse(key)))?)
                    }
                }
            }
        }
    }
}

group_index! {
    Int8 => Int8Type,
    Int16 => Int16Type,
    Int32 => Int32Type,
    Int64 => Int64Type,
    UInt8 => UInt8Type,
    UInt16 => UInt16Type,
    UInt32 => UInt32Type,
    UInt64 => UInt64Type,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, UInt64Array};

    #[test]
    fn global_ids_remain_zero_across_input_and_merge_batch_sizes() {
        let mut index = GroupIndex::new(&Schema::empty()).unwrap();
        let mut ids = vec![];
        for rows in [8192, 8192, 0, 1, 65536, 3, 8192] {
            index.intern(&[], rows, &mut ids).unwrap();
            assert_eq!(ids, vec![0; rows]);
        }
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
