//! Arrow owns imported memory; Variables name columns, never raw allocations.
use crate::error::{Error, Result};
use arrow::{array::ArrayRef, buffer::Buffer, datatypes::DataType};

/// A batch value retaining every Arrow owner (including dictionaries and children).
/// Cloning/slicing this view does not copy value buffers. Validity and bit offsets
/// stay attached to the owner instead of being lost at the execution boundary.
#[derive(Clone, Debug)]
pub struct ColumnBuffer {
    array: ArrayRef,
}
impl ColumnBuffer {
    pub fn from_arrow(array: ArrayRef) -> Self {
        Self { array }
    }
    pub fn as_arrow(&self) -> &ArrayRef {
        &self.array
    }
    pub fn into_arrow(self) -> ArrayRef {
        self.array
    }
    pub fn len(&self) -> usize {
        self.array.len()
    }
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }
    pub fn data_type(&self) -> &DataType {
        self.array.data_type()
    }
    pub fn slice(&self, offset: usize, len: usize) -> Result<Self> {
        if offset > self.len() || len > self.len() - offset {
            return Err(Error::Execution("buffer slice out of bounds".into()));
        }
        Ok(Self::from_arrow(self.array.slice(offset, len)))
    }
    pub fn buffers(&self) -> Vec<Buffer> {
        self.array.to_data().buffers().to_vec()
    }
}
