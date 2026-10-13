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

//! Columns: the value identities of one physical plan.
//!
//! The caller submits a fully bound and typed plan. Operators and expressions
//! refer to logical columns by ColumnId, and every value-producing node declares
//! which column receives its result. Kernel construction later expands each
//! column into internal Variables; the two identity spaces are connected by the
//! per-pipeline bindings only.
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use crate::error::{Error, Result};
use std::sync::Arc;

/// Meaningful only within its ColumnContext; not a global identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(usize);

impl ColumnId {
    pub fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnType {
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnType {
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}

/// A logical column declaration: type, nullability and an optional display
/// name. It never holds data.
#[derive(Clone, Debug)]
pub struct Column {
    pub ty: ColumnType,
    pub name: Option<String>,
}

/// One context per physical plan; the index into `columns` is the ID.
#[derive(Clone, Debug, Default)]
pub struct ColumnContext {
    columns: Vec<Column>,
}

impl ColumnContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate(&mut self, ty: ColumnType) -> ColumnId {
        self.columns.push(Column { ty, name: None });
        ColumnId(self.columns.len() - 1)
    }

    pub fn allocate_named(&mut self, ty: ColumnType, name: impl Into<String>) -> ColumnId {
        self.columns.push(Column {
            ty,
            name: Some(name.into()),
        });
        ColumnId(self.columns.len() - 1)
    }

    /// Allocates one column per field, named after the field.
    pub fn allocate_schema(&mut self, schema: &Schema) -> Vec<ColumnId> {
        schema
            .fields()
            .iter()
            .map(|f| {
                self.allocate_named(
                    ColumnType::new(f.data_type().clone(), f.is_nullable()),
                    f.name(),
                )
            })
            .collect()
    }

    pub fn get(&self, id: ColumnId) -> Result<&Column> {
        self.columns
            .get(id.0)
            .ok_or_else(|| Error::InvalidPlan(format!("undefined column {}", id.0)))
    }

    pub fn column_type(&self, id: ColumnId) -> Result<&ColumnType> {
        Ok(&self.get(id)?.ty)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Output field; unnamed columns are called `column_<id>`.
    pub fn field(&self, id: ColumnId) -> Result<Field> {
        let column = self.get(id)?;
        let name = column
            .name
            .clone()
            .unwrap_or_else(|| format!("column_{}", id.0));
        Ok(Field::new(
            name,
            column.ty.data_type.clone(),
            column.ty.nullable,
        ))
    }

    pub fn schema(&self, ids: &[ColumnId]) -> Result<SchemaRef> {
        let fields = ids
            .iter()
            .map(|&id| self.field(id))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }
}
