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
    BatchSlices, ConsumeStatus, LocalSink, LocalSinkContext, LocalSource, LocalSourceContext,
    SinkExec, SourceExec, StateContext, downcast_local,
};
use crate::arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::error::{Error, Result};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// The chunks of one or more producing pipelines.
#[derive(Default)]
pub struct Buffer(pub(super) Mutex<Arc<Vec<RecordBatch>>>);

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
            Arc::make_mut(&mut self.buffer.0.lock().unwrap()).extend(chunks);
            Ok(())
        })
    }
}

/// Reads a buffer filled by the pipelines it depends on, as chunks of
/// `schema`.
pub struct BufferSourceExec {
    buffer: Arc<Buffer>,
    schema: SchemaRef,
    chunks: Option<Arc<Vec<RecordBatch>>>,
    next: Arc<AtomicUsize>,
}
impl BufferSourceExec {
    pub fn new(buffer: Arc<Buffer>, schema: SchemaRef) -> Self {
        Self {
            buffer,
            schema,
            chunks: None,
            next: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl SourceExec for BufferSourceExec {
    fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
        self.chunks = Some(self.buffer.0.lock().unwrap().clone());
        Ok(())
    }
    fn local_source(&self, ctx: &LocalSourceContext) -> Result<Box<dyn LocalSource>> {
        Ok(Box::new(BufferLocalSource {
            chunks: self
                .chunks
                .clone()
                .ok_or_else(|| Error::internal("buffer is not started".into()))?,
            schema: self.schema.clone(),
            next: self.next.clone(),
            slices: BatchSlices::default(),
            batch_rows: ctx.batch_rows,
        }))
    }
}
struct BufferLocalSource {
    chunks: Arc<Vec<RecordBatch>>,
    schema: SchemaRef,
    next: Arc<AtomicUsize>,
    slices: BatchSlices,
    batch_rows: usize,
}
impl LocalSource for BufferLocalSource {
    fn next<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            loop {
                if guard.is_shutdown_requested() {
                    return Err(Error::cancelled());
                }
                if let Some(batch) = self.slices.next(self.batch_rows) {
                    return Ok(Some(batch));
                }
                let index = self.next.fetch_add(1, Ordering::Relaxed);
                let Some(chunk) = self.chunks.get(index) else {
                    return Ok(None);
                };
                self.slices.set(
                    RecordBatch::try_new_with_options(
                        self.schema.clone(),
                        chunk.columns().to_vec(),
                        &RecordBatchOptions::new().with_row_count(Some(chunk.num_rows())),
                    )
                    .map_err(|source| {
                        Error::invalid_plan("buffered chunks do not match the columns read".into())
                            .with_source(source)
                    })?,
                );
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    #[test]
    fn concurrent_buffer_readers_share_claims_and_spool_readers_start_fresh() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "input",
            DataType::Int64,
            false,
        )]));
        let chunks = (0..30)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(i * 73..(i + 1) * 73))],
                )
                .unwrap()
            })
            .collect();
        let buffer = Arc::new(Buffer(Mutex::new(Arc::new(chunks))));
        let output = Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int64,
            false,
        )]));
        for _ in 0..2 {
            let guard = asyncband::shutdown::new().1;
            let mut source = BufferSourceExec::new(buffer.clone(), output.clone());
            source.start(&guard).unwrap();
            assert!(Arc::ptr_eq(
                source.chunks.as_ref().unwrap(),
                &buffer.0.lock().unwrap()
            ));
            let mut tasks = vec![];
            for _ in 0..4 {
                let mut local = source
                    .local_source(&LocalSourceContext {
                        batch_rows: 11,
                        instances: 4,
                    })
                    .unwrap();
                let (guard, output) = (guard.clone(), output.clone());
                tasks.push(std::thread::spawn(move || {
                    futures::executor::block_on(async move {
                        let mut values = vec![];
                        while let Some(chunk) = local.next(&guard).await.unwrap() {
                            assert!(chunk.num_rows() <= 11);
                            assert_eq!(chunk.schema(), output);
                            values.extend_from_slice(
                                chunk
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .values(),
                            );
                        }
                        values
                    })
                }));
            }
            let mut values = tasks
                .into_iter()
                .flat_map(|t| t.join().unwrap())
                .collect::<Vec<i64>>();
            values.sort();
            assert_eq!(values, (0..30 * 73).collect::<Vec<_>>());
        }
    }
}
