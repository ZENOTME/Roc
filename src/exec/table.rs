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

//! Builds immutable tables from keys prepared in execution instances.
use super::{ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, StateContext, downcast_local};
use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::error::{Error, Result};
use crate::kernel::table::{KeyPlan, PreparedKeys, SharedTable};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::Arc;

pub struct TableSinkExec {
    schema: SchemaRef,
    plan: Arc<KeyPlan>,
    chunks: Vec<Vec<RecordBatch>>,
    prepared: Vec<Vec<PreparedKeys>>,
    partitions: Vec<Vec<(usize, usize)>>,
    shared: Arc<SharedTable>,
}
impl TableSinkExec {
    pub fn new(
        schema: SchemaRef,
        keys: Vec<usize>,
        nulls_equal: bool,
        shared: Arc<SharedTable>,
    ) -> Result<Self> {
        let plan = Arc::new(KeyPlan::new(&schema, keys, nulls_equal)?);
        Ok(Self {
            schema,
            plan,
            chunks: vec![],
            prepared: vec![],
            partitions: vec![],
            shared,
        })
    }
}
struct TableLocalSink {
    partition_count: usize,
    partitions: Vec<Vec<(usize, usize)>>,
    plan: Arc<KeyPlan>,
    chunks: Vec<RecordBatch>,
    prepared: Vec<PreparedKeys>,
}
impl LocalSink for TableLocalSink {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            self.prepared.push(self.plan.prepare(&chunk)?);
            self.chunks.push(chunk);
        }
        Ok(ConsumeStatus::End)
    }
    fn finish(
        &mut self,
        _: ConsumeStatus,
        _: &mut StateContext<'_>,
        guard: &ShutdownGuard,
    ) -> Result<()> {
        self.partitions = if self.partition_count == 1 {
            // assemble_on uses the serial layout; no row lists are needed.
            vec![vec![], vec![]]
        } else {
            self.plan
                .partitions(&self.prepared, self.partition_count, guard)?
        };
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
impl SinkExec for TableSinkExec {
    fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        if !ctx.partitions.is_power_of_two() {
            return Err(Error::invalid_plan(
                "table partitions must be a power of two".into(),
            ));
        }
        Ok(Box::new(TableLocalSink {
            partition_count: ctx.partitions,
            partitions: vec![],
            plan: self.plan.clone(),
            chunks: vec![],
            prepared: vec![],
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
            let local = downcast_local::<TableLocalSink>(local)?;
            let base = self.chunks.iter().map(Vec::len).sum::<usize>();
            if self.partitions.is_empty() {
                self.partitions
                    .resize_with(local.partitions.len(), Vec::new);
            }
            if self.partitions.len() != local.partitions.len() {
                return Err(Error::internal("inconsistent table partitions".into()));
            }
            for (bucket, rows) in self
                .partitions
                .iter_mut()
                .zip(std::mem::take(&mut local.partitions))
            {
                bucket.extend(rows.into_iter().map(|(c, r)| (base + c, r)));
            }
            self.chunks.push(std::mem::take(&mut local.chunks));
            self.prepared.push(std::mem::take(&mut local.prepared));
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn crate::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let chunks = std::mem::take(&mut self.chunks)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let prepared = std::mem::take(&mut self.prepared)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let table = self
                .plan
                .assemble_on(
                    self.schema.clone(),
                    chunks,
                    prepared,
                    std::mem::take(&mut self.partitions),
                    guard,
                    cpu,
                )
                .await?;
            if guard.is_shutdown_requested() {
                return Err(Error::cancelled());
            }
            self.shared.publish(table)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arrow::{
            array::{ArrayRef, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        kernel::{LocalState, scalar::Cell},
    };
    use futures::executor::block_on;

    #[test]
    fn local_key_preparation_preserves_probe_buckets_and_null_semantics() {
        for (word, partitions) in [false, true]
            .into_iter()
            .flat_map(|w| [1, 2, 8].map(|p| (w, p)))
        {
            for nulls_equal in [false, true] {
                let key_type = if word {
                    DataType::Int64
                } else {
                    DataType::Utf8
                };
                let schema = Arc::new(Schema::new(vec![
                    Field::new("key", key_type, true),
                    Field::new("id", DataType::Int64, false),
                ]));
                let shared = Arc::new(SharedTable::default());
                let mut sink =
                    TableSinkExec::new(schema.clone(), vec![0], nulls_equal, shared.clone())
                        .unwrap();
                let guard = asyncband::shutdown::new().1;
                let mut state = LocalState::default();
                for run in 0..3 {
                    let mut local = sink
                        .local_sink(&LocalSinkContext {
                            batch_rows: 3,
                            partitions,
                        })
                        .unwrap();
                    for chunk in 0..2 {
                        let keys: ArrayRef = if word {
                            Arc::new(Int64Array::from(vec![Some(1), None, Some(1)]))
                        } else {
                            Arc::new(StringArray::from(vec![Some("one"), None, Some("one")]))
                        };
                        let ids = Arc::new(Int64Array::from_iter_values(
                            (0..3).map(|r| run * 6 + chunk * 3 + r),
                        ));
                        local
                            .consume(
                                Some(
                                    RecordBatch::try_new(schema.clone(), vec![keys, ids]).unwrap(),
                                ),
                                &mut StateContext::new(&mut state),
                            )
                            .unwrap();
                    }
                    local
                        .finish(
                            ConsumeStatus::End,
                            &mut StateContext::new(&mut state),
                            &guard,
                        )
                        .unwrap();
                    block_on(sink.consume(
                        ConsumeStatus::End,
                        &mut *local,
                        StateContext::new(&mut state),
                        &guard,
                    ))
                    .unwrap();
                }
                block_on(sink.finish(&guard, &crate::exec::Inline)).unwrap();
                let table = shared.get().unwrap();
                assert_eq!(table.rows(), 18);
                for null in [false, true] {
                    let matched = if let Some(index) = table.word_index() {
                        if null {
                            vec![]
                        } else {
                            index
                                .bucket(1)
                                .filter(|&r| index.words[r as usize] == 1)
                                .collect::<Vec<_>>()
                        }
                    } else {
                        let owner: ArrayRef = Arc::new(StringArray::from(vec!["one"]));
                        let cells = if null {
                            vec![Cell::default()]
                        } else if word {
                            vec![Cell::word(1)]
                        } else {
                            crate::kernel::scalar::decode(&owner).unwrap()
                        };
                        let mut encoded = vec![];
                        if table.encode(&cells, nulls_equal, &mut encoded) {
                            table
                                .bucket(&encoded)
                                .filter(|&r| table.equal(r, &encoded))
                                .collect()
                        } else {
                            vec![]
                        }
                    };
                    let ids = table
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let actual = matched
                        .into_iter()
                        .map(|r| ids.value(r as usize))
                        .collect::<Vec<_>>();
                    let expected = (0..18)
                        .filter(|i| {
                            if null {
                                nulls_equal && i % 3 == 1
                            } else {
                                i % 3 != 1
                            }
                        })
                        .collect::<Vec<i64>>();
                    assert_eq!(actual, expected);
                }
            }
        }
    }
}
