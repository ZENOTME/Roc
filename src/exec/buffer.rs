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
use super::{
    ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, SourceExec, StateContext, downcast_local,
};
use crate::arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, Result};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Rows per chunk a buffer source emits.
const MORSEL_ROWS: usize = 8192;

/// The chunks of one or more producing pipelines.
#[derive(Default)]
pub struct Buffer(pub(super) Mutex<Vec<RecordBatch>>);

/// Transforms one producing pipeline's chunks on the coordinator.
pub type Finish = Box<dyn FnOnce(Vec<RecordBatch>) -> Result<Vec<RecordBatch>> + Send>;

pub fn finish(
    f: impl FnOnce(Vec<RecordBatch>) -> Result<Vec<RecordBatch>> + Send + 'static,
) -> Finish {
    Box::new(f)
}

pub struct BufferSinkExec {
    buffer: Arc<Buffer>,
    chunks: Vec<Vec<RecordBatch>>,
    finish: Option<Finish>,
    /// Stop after this many rows.
    limit: Option<usize>,
    rows: Arc<AtomicUsize>,
}

impl BufferSinkExec {
    pub fn new(buffer: Arc<Buffer>, finish: Option<Finish>) -> Self {
        Self {
            buffer,
            chunks: vec![],
            finish,
            limit: None,
            rows: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The pipeline stops reading its source after `rows` rows.
    pub fn with_limit(mut self, rows: usize) -> Self {
        self.limit = Some(rows);
        self
    }
}

struct BufferLocalSink {
    chunks: Vec<RecordBatch>,
    limit: Option<usize>,
    rows: Arc<AtomicUsize>,
}

impl LocalSink for BufferLocalSink {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            let rows = chunk.num_rows();
            self.chunks.push(chunk);
            if let Some(limit) = self.limit {
                let before = self.rows.fetch_add(rows, Ordering::Relaxed);
                if before.saturating_add(rows) >= limit {
                    return Ok(ConsumeStatus::Stop);
                }
            }
        }
        Ok(ConsumeStatus::End)
    }
    fn finish(
        &mut self,
        _: ConsumeStatus,
        _: &mut StateContext<'_>,
        _: &ShutdownGuard,
    ) -> Result<()> {
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl SinkExec for BufferSinkExec {
    fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        Ok(Box::new(BufferLocalSink {
            chunks: vec![],
            limit: self.limit,
            rows: self.rows.clone(),
        }))
    }
    fn consume<'a>(
        &'a mut self,
        _: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        _: StateContext<'a>,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.chunks.push(std::mem::take(
                &mut downcast_local::<BufferLocalSink>(local)?.chunks,
            ));
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        _cpu: &'a dyn crate::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            let chunks = std::mem::take(&mut self.chunks)
                .into_iter()
                .flatten()
                .collect();
            let chunks = match self.finish.take() {
                Some(f) => f(chunks)?,
                None => chunks,
            };
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
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
