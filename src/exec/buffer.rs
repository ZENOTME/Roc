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

//! Materialized results: pipelines sink chunks into a buffer, optionally
//! transformed once their input ends, and dependent pipelines read it.
use super::{SinkExec, SinkStatus, SourceExec};
use crate::arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, Result};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Rows per chunk a buffer source emits.
const MORSEL_ROWS: usize = 8192;

/// The chunks of one or more producing pipelines.
#[derive(Default)]
pub struct Buffer(Mutex<Vec<RecordBatch>>);

/// Transforms the chunks of one producing pipeline once its input ends.
pub type Finish = Box<dyn FnOnce(Vec<RecordBatch>) -> Result<Vec<RecordBatch>> + Send>;

pub struct BufferSinkExec {
    buffer: Arc<Buffer>,
    chunks: Vec<RecordBatch>,
    finish: Option<Finish>,
    /// Stop after this many rows.
    limit: Option<usize>,
    rows: usize,
}

impl BufferSinkExec {
    pub fn new(buffer: Arc<Buffer>, finish: Option<Finish>) -> Self {
        Self {
            buffer,
            chunks: vec![],
            finish,
            limit: None,
            rows: 0,
        }
    }

    /// The pipeline stops reading its source after `rows` rows.
    pub fn with_limit(mut self, rows: usize) -> Self {
        self.limit = Some(rows);
        self
    }
}

impl SinkExec for BufferSinkExec {
    fn consume<'a>(
        &'a mut self,
        chunk: RecordBatch,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        self.rows += chunk.num_rows();
        self.chunks.push(chunk);
        let status = if self.limit.is_some_and(|l| self.rows >= l) {
            SinkStatus::Finished
        } else {
            SinkStatus::Continue
        };
        Box::pin(async move { Ok(status) })
    }

    fn finish<'a>(&'a mut self, guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let chunks = std::mem::take(&mut self.chunks);
            let chunks = match self.finish.take() {
                Some(f) => f(chunks)?,
                None => chunks,
            };
            self.buffer.0.lock().unwrap().extend(chunks);
            Ok(())
        })
    }
}

/// Reads a buffer filled by the pipelines it depends on, as chunks of
/// `schema`.
pub struct BufferSourceExec {
    buffer: Arc<Buffer>,
    schema: SchemaRef,
    chunks: VecDeque<RecordBatch>,
}

impl BufferSourceExec {
    pub fn new(buffer: Arc<Buffer>, schema: SchemaRef) -> Self {
        Self {
            buffer,
            schema,
            chunks: VecDeque::new(),
        }
    }
}

impl SourceExec for BufferSourceExec {
    fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
        let chunks = self.buffer.0.lock().unwrap().clone();
        for chunk in chunks {
            let chunk = RecordBatch::try_new_with_options(
                self.schema.clone(),
                chunk.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(chunk.num_rows())),
            )
            .map_err(|source| {
                Error::invalid_plan("buffered chunks do not match the columns read".into())
                    .with_source(source)
            })?;
            for offset in (0..chunk.num_rows()).step_by(MORSEL_ROWS) {
                self.chunks
                    .push_back(chunk.slice(offset, MORSEL_ROWS.min(chunk.num_rows() - offset)));
            }
        }
        Ok(())
    }

    fn next_batch<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            Ok(self.chunks.pop_front())
        })
    }
}
