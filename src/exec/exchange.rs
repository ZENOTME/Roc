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

use super::{SinkExec, SinkStatus, SourceExec};
use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::{
    error::{Error, Result},
    operator::ExchangeId,
    operator::{ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink},
};
use asyncband::shutdown::ShutdownGuard;
use futures::{FutureExt, future::BoxFuture};
use std::sync::Arc;

pub struct ExchangeSinkExec {
    exchanges: Vec<ExchangeId>,
    schema: SchemaRef,
    service: Arc<dyn ExchangeService>,
    outputs: Vec<Box<dyn ExchangeSink>>,
}
pub struct ExchangeSourceExec {
    exchange: ExchangeId,
    service: Arc<dyn ExchangeService>,
    handle: Option<Arc<dyn ExchangeHandle>>,
    consumer: Option<Box<dyn ExchangeConsumer>>,
}

impl ExchangeSourceExec {
    pub fn new(exchange: ExchangeId, service: Arc<dyn ExchangeService>) -> Self {
        Self {
            exchange,
            service,
            handle: None,
            consumer: None,
        }
    }
}

impl SourceExec for ExchangeSourceExec {
    fn start(&mut self, guard: &ShutdownGuard) -> Result<()> {
        if self.handle.is_some() {
            return Err(Error::Execution("exchange source already started".into()));
        }
        let handle = self.service.start_input(self.exchange, guard)?;
        self.consumer = Some(handle.consumer());
        self.handle = Some(handle);
        Ok(())
    }
    fn finish<'a>(&'a mut self, _: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.consumer.take();
            if let Some(handle) = self.handle.take() {
                handle.finish().await?;
            }
            Ok(())
        })
    }
    fn next_batch<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let cancelled = shutdown_guard.shutdown_requested().fuse();
            let next = self
                .consumer
                .as_mut()
                .ok_or_else(|| Error::Execution("exchange source is not started".into()))?
                .next()
                .fuse();
            futures::pin_mut!(cancelled, next);
            futures::select_biased! {
                _ = cancelled => {
                    Err(Error::Cancelled)
                },
                batch = next => batch,
            }
        })
    }
}

impl ExchangeSinkExec {
    pub fn new(
        exchanges: Vec<ExchangeId>,
        schema: SchemaRef,
        service: Arc<dyn ExchangeService>,
    ) -> Self {
        Self {
            exchanges,
            schema,
            service,
            outputs: vec![],
        }
    }
}

impl SinkExec for ExchangeSinkExec {
    fn start(&mut self, _: &ShutdownGuard) -> Result<()> {
        if !self.outputs.is_empty() {
            return Err(Error::Execution("exchange sink already started".into()));
        }
        self.outputs = self
            .exchanges
            .iter()
            .map(|id| self.service.create_sink(*id, &self.schema))
            .collect::<Result<_>>()?;
        Ok(())
    }
    fn consume<'a>(
        &'a mut self,
        chunk: RecordBatch,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        Box::pin(async move {
            for output in &mut self.outputs {
                output.send(&chunk, guard).await?;
            }
            Ok(SinkStatus::Continue)
        })
    }
}
