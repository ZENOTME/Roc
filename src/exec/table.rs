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

//! Builds a table state from the output chunks of a pipeline.
use super::SinkExec;
use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::error::Result;
use crate::exec::SinkStatus;
use crate::arrow::array::ArrayRef;
use crate::kernel::table::{self, SharedTable, Table};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::Arc;

pub struct TableSinkExec {
    schema: SchemaRef,
    keys: Vec<usize>,
    nulls_equal: bool,
    batches: Vec<RecordBatch>,
    shared: Arc<SharedTable>,
}

impl TableSinkExec {
    /// Indexes the columns `keys`; see `Table::new`.
    pub fn new(
        schema: SchemaRef,
        keys: Vec<usize>,
        nulls_equal: bool,
        shared: Arc<SharedTable>,
    ) -> Self {
        Self {
            schema,
            keys,
            nulls_equal,
            batches: vec![],
            shared,
        }
    }
}

impl SinkExec for TableSinkExec {
    fn consume<'a>(
        &'a mut self,
        chunk: RecordBatch,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        self.batches.push(chunk);
        Box::pin(async { Ok(SinkStatus::Continue) })
    }

    fn finish<'a>(&'a mut self, guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        self.finish_on(guard, &crate::exec::Inline)
    }

    /// Concatenates the columns, indexes the keys and reorders the columns
    /// for the index, each column a CPU job of its own.
    fn finish_on<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn crate::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if guard.is_shutdown_requested() {
                return Err(crate::error::Error::cancelled());
            }
            let batches = Arc::new(std::mem::take(&mut self.batches));
            let width = self.schema.fields().len();
            let jobs = (0..width)
                .map(|i| {
                    let (batches, schema) = (batches.clone(), self.schema.clone());
                    Box::new(move || table::column(&batches, &schema, i))
                        as Box<dyn FnOnce() -> Result<ArrayRef> + Send>
                })
                .collect();
            let columns = crate::exec::run_all(cpu, jobs).await?;
            drop(batches);
            let (index, order) = table::index(&columns, &self.keys, self.nulls_equal)?;
            let columns = match order {
                Some(order) => {
                    let order = Arc::new(order);
                    let jobs = columns
                        .into_iter()
                        .map(|c| {
                            let order = order.clone();
                            Box::new(move || table::reorder(&c, &order))
                                as Box<dyn FnOnce() -> Result<ArrayRef> + Send>
                        })
                        .collect();
                    crate::exec::run_all(cpu, jobs).await?
                }
                None => columns,
            };
            self.shared
                .publish(Table::from_parts(self.schema.clone(), columns, index))
        })
    }
}
