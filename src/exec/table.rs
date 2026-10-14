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
use crate::kernel::table::{SharedTable, Table};
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
        Box::pin(async move {
            if guard.is_shutdown_requested() {
                return Err(crate::error::Error::cancelled());
            }
            let batches = std::mem::take(&mut self.batches);
            let table = Table::new(self.schema.clone(), &batches, &self.keys, self.nulls_equal)?;
            self.shared.publish(table)
        })
    }
}
