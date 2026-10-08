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

//! Logical rows over shared Arrow columns. Selection bits address physical rows.
use crate::error::{Error, Result};
use arrow::{
    array::BooleanArray, buffer::BooleanBuffer, compute::filter_record_batch, datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use std::{borrow::Cow,sync::Arc};

#[derive(Clone, Debug)]
pub struct Batch {
    data: Arc<RecordBatch>,
    selection: Option<BooleanBuffer>,
    rows: usize,
}
impl From<RecordBatch> for Batch {
    fn from(data: RecordBatch) -> Self {
        let rows = data.num_rows();
        Self {
            data:Arc::new(data),
            selection: None,
            rows,
        }
    }
}
impl Batch {
    /// `selection` is relative to `data`, including when its arrays are sliced.
    pub fn try_new(data: impl Into<Arc<RecordBatch>>, selection: BooleanBuffer) -> Result<Self> {
        let data=data.into();
        if selection.len() != data.num_rows() {
            return Err(Error::Execution(
                "selection length differs from physical batch".into(),
            ));
        }
        let rows = selection.count_set_bits();
        let selection = (rows != data.num_rows()).then_some(selection);
        Ok(Self {
            data,
            selection,
            rows,
        })
    }
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.rows
    }
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.data.num_columns()
    }
    #[inline]
    pub fn schema(&self) -> SchemaRef {
        self.data.schema()
    }
    /// Physical columns may contain unselected rows. Do not treat them as logical output.
    #[inline]
    pub fn physical(&self) -> &RecordBatch {
        &self.data
    }
    #[inline]
    pub fn selection(&self) -> Option<&BooleanBuffer> {
        self.selection.as_ref()
    }
    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        Ok(Self {
            data: Arc::new(self.data.project(indices)?),
            selection: self.selection.clone(),
            rows: self.rows,
        })
    }
    /// Select logical rows, preserving physical row order and shared value buffers.
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset <= self.rows && len <= self.rows - offset);
        let Some(selection) = &self.selection else {
            return self.data.slice(offset, len).into();
        };
        if len == 0 {
            return self.data.slice(0, 0).into();
        }
        let mut rows = selection.set_indices().skip(offset);
        let first = rows.next().unwrap();
        let last = if len == 1 {
            first
        } else {
            rows.nth(len - 2).unwrap()
        };
        Self::try_new(
            self.data.slice(first, last - first + 1),
            selection.slice(first, last - first + 1),
        )
        .unwrap()
    }
    /// Explicit boundary for consumers which require dense Arrow columns.
    pub fn materialize(&self) -> Result<Cow<'_, RecordBatch>> {
        match &self.selection {
            None => Ok(Cow::Borrowed(&self.data)),
            Some(selection) => Ok(Cow::Owned(filter_record_batch(
                &self.data,
                &BooleanArray::new(selection.clone(), None),
            )?)),
        }
    }
    pub fn into_record_batch(self) -> Result<RecordBatch> {
        if self.selection.is_none() {
            return Ok(Arc::unwrap_or_clone(self.data));
        }
        Ok(self.materialize()?.into_owned())
    }
    /// `mask` describes current logical rows, not the underlying physical rows.
    pub fn filter(&self, mask: &BooleanArray) -> Result<Self> {
        use arrow::array::Array;
        if mask.len() != self.rows {
            return Err(Error::Execution(
                "filter length differs from logical batch".into(),
            ));
        }
        let active = match mask.nulls() {
            Some(nulls) => mask.values() & nulls.inner(),
            None => mask.values().clone(),
        };
        let selection = match &self.selection {
            None => active,
            Some(previous) => {
                let mut bits = arrow::array::BooleanBufferBuilder::new(previous.len());
                bits.append_n(previous.len(), false);
                for (logical, physical) in previous.set_indices().enumerate() {
                    if active.value(logical) {
                        bits.set_bit(physical, true);
                    }
                }
                bits.finish()
            }
        };
        Self::try_new(self.data.clone(), selection)
    }
}
