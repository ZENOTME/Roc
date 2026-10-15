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

//! Lazy, shared results. Claims belong to a reading pipeline, while partition
//! computations and their cached output belong to the producing pipeline.
use super::{BatchSlices, LocalSource, LocalSourceContext, SourceExec};
use crate::{
    arrow::record_batch::RecordBatch,
    error::{Error, ErrorKind, Result},
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

struct Partitioned<P> {
    parts: Vec<Mutex<Option<P>>>,
    done: Vec<OnceLock<std::result::Result<Arc<Vec<RecordBatch>>, Arc<Error>>>>,
}
impl<P> Partitioned<P> {
    fn new(parts: Vec<P>) -> Self {
        Self {
            done: (0..parts.len()).map(|_| OnceLock::new()).collect(),
            parts: parts.into_iter().map(|p| Mutex::new(Some(p))).collect(),
        }
    }
    fn get(
        &self,
        index: usize,
        compute: impl FnOnce(P) -> Result<Vec<RecordBatch>>,
    ) -> Result<Arc<Vec<RecordBatch>>> {
        let result = self.done[index].get_or_init(|| {
            // The first reader consumes the partials; even failures are cached,
            // so concurrent readers neither repeat computation nor lose inputs.
            let part = self.parts[index]
                .lock()
                .unwrap()
                .take()
                .expect("uncomputed partition");
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| compute(part)))
                .unwrap_or_else(|_| {
                    Err(Error::new(
                        ErrorKind::TaskFailed,
                        "source partition panicked",
                    ))
                })
                .map(Arc::new)
                .map_err(Arc::new)
        });
        result
            .as_ref()
            .map(Arc::clone)
            .map_err(|error| Error::new(error.kind(), error.message()).with_source(error.clone()))
    }
}

#[derive(Default)]
struct Claims(AtomicUsize);
impl Claims {
    fn next(&self) -> usize {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

pub(crate) type PartitionWork = Box<dyn FnOnce(&ShutdownGuard) -> Result<Vec<RecordBatch>> + Send>;

/// A sink publishes independent work units without materializing their output.
/// Multiple source executions can read the same result; each partition runs once.
#[derive(Default)]
pub struct PartitionedResult {
    ready: OnceLock<Arc<Partitioned<PartitionWork>>>,
}
impl PartitionedResult {
    pub(crate) fn publish(&self, parts: Vec<PartitionWork>, guard: &ShutdownGuard) -> Result<()> {
        if guard.is_shutdown_requested() {
            return Err(Error::cancelled());
        }
        self.ready
            .set(Arc::new(Partitioned::new(parts)))
            .map_err(|_| Error::internal("partitioned result already published".into()))
    }

    #[cfg(test)]
    pub(crate) fn computed_parts(&self) -> usize {
        self.ready
            .get()
            .unwrap()
            .done
            .iter()
            .filter(|p| p.get().is_some())
            .count()
    }
    #[cfg(test)]
    pub(crate) fn part_count(&self) -> usize {
        self.ready.get().unwrap().parts.len()
    }

    #[cfg(test)]
    pub(crate) fn read_all(self: &Arc<Self>, guard: &ShutdownGuard) -> Vec<RecordBatch> {
        let mut source = PartitionedSourceExec::new(self.clone());
        source.start(guard).unwrap();
        let mut local = source
            .local_source(&LocalSourceContext {
                batch_rows: 2048,
                instances: 1,
            })
            .unwrap();
        futures::executor::block_on(async {
            let mut out = vec![];
            while let Some(batch) = local.next(guard).await.unwrap() {
                out.push(batch);
            }
            out
        })
    }
}

pub struct PartitionedSourceExec {
    result: Arc<PartitionedResult>,
    claims: Arc<Claims>,
}
impl PartitionedSourceExec {
    pub fn new(result: Arc<PartitionedResult>) -> Self {
        Self {
            result,
            claims: Arc::new(Claims::default()),
        }
    }
}
impl SourceExec for PartitionedSourceExec {
    fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
        if self.result.ready.get().is_none() {
            return Err(Error::internal(
                "source started before sink finished".into(),
            ));
        }
        Ok(())
    }
    fn local_source(&self, ctx: &LocalSourceContext) -> Result<Box<dyn LocalSource>> {
        Ok(Box::new(PartitionedLocalSource {
            result: self
                .result
                .ready
                .get()
                .cloned()
                .ok_or_else(|| Error::internal("partitioned result is not ready".into()))?,
            claims: self.claims.clone(),
            batch_rows: ctx.batch_rows,
            chunks: Arc::new(vec![]),
            chunk: 0,
            slices: BatchSlices::default(),
        }))
    }
}
struct PartitionedLocalSource {
    result: Arc<Partitioned<PartitionWork>>,
    claims: Arc<Claims>,
    batch_rows: usize,
    chunks: Arc<Vec<RecordBatch>>,
    chunk: usize,
    slices: BatchSlices,
}
impl LocalSource for PartitionedLocalSource {
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
                if let Some(batch) = self.chunks.get(self.chunk) {
                    self.slices.set(batch.clone());
                    self.chunk += 1;
                    continue;
                }
                let index = self.claims.next();
                if index >= self.result.parts.len() {
                    return Ok(None);
                }
                self.chunks = self.result.get(index, |work| work(guard))?;
                self.chunk = 0;
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
    fn independent_readers_claim_every_partition_but_compute_and_release_it_once() {
        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let result = Arc::new(PartitionedResult::default());
        let guard = asyncband::shutdown::new().1;
        let parts = (0..17)
            .map(|i| {
                let calls = calls.clone();
                let held = DropCount(drops.clone());
                Box::new(move |_: &ShutdownGuard| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    drop(held);
                    Ok(vec![
                        RecordBatch::try_new(
                            Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)])),
                            vec![Arc::new(Int64Array::from(vec![i; 5]))],
                        )
                        .unwrap(),
                    ])
                }) as PartitionWork
            })
            .collect();
        result.publish(parts, &guard).unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let mut tasks = vec![];
        for _ in 0..2 {
            let mut source = PartitionedSourceExec::new(result.clone());
            source.start(&guard).unwrap();
            let mut readers = vec![];
            for _ in 0..4 {
                let mut local = source
                    .local_source(&LocalSourceContext {
                        batch_rows: 2,
                        instances: 4,
                    })
                    .unwrap();
                let guard = guard.clone();
                readers.push(std::thread::spawn(move || {
                    futures::executor::block_on(async move {
                        let mut values = vec![];
                        while let Some(batch) = local.next(&guard).await.unwrap() {
                            assert!(batch.num_rows() <= 2);
                            values.extend_from_slice(
                                batch
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
            tasks.push(readers);
        }
        for readers in tasks {
            let mut values = readers
                .into_iter()
                .flat_map(|r| r.join().unwrap())
                .collect::<Vec<i64>>();
            values.sort();
            assert_eq!(values, (0..17).flat_map(|i| [i; 5]).collect::<Vec<_>>());
        }
        assert_eq!(calls.load(Ordering::Relaxed), 17);
        assert_eq!(drops.load(Ordering::Relaxed), 17);
    }

    #[test]
    fn errors_and_panics_are_cached_without_repeating_work() {
        for panic in [false, true] {
            let parts = Partitioned::new(vec![()]);
            let error = parts
                .get(0, |_| {
                    if panic {
                        panic!("partition panic");
                    }
                    Err(Error::invalid_input("bad partial".into()))
                })
                .unwrap_err();
            let again = parts
                .get(0, |_| unreachable!("cached failure"))
                .unwrap_err();
            assert_eq!(error.kind(), again.kind());
        }
    }
}
