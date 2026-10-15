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

use super::{
    ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, SourceExec, StateContext, downcast_local,
};
use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::{
    error::{Error, ErrorContext, Result, ResultExt},
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
            return Err(Error::internal("exchange source already started".into()));
        }
        let handle = self
            .service
            .start_input(self.exchange, guard)
            .with_context(|| {
                ErrorContext::new("exchange.start").field("exchange", self.exchange)
            })?;
        self.consumer = Some(handle.consumer());
        self.handle = Some(handle);
        Ok(())
    }
    fn finish<'a>(&'a mut self, _: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.consumer.take();
            if let Some(handle) = self.handle.take() {
                handle.finish().await.with_context(|| {
                    ErrorContext::new("exchange.finish").field("exchange", self.exchange)
                })?;
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
                .ok_or_else(|| Error::internal("exchange source is not started".into()))?
                .next()
                .fuse();
            futures::pin_mut!(cancelled, next);
            futures::select_biased! {
                _ = cancelled => {
                    Err(Error::cancelled())
                },
                batch = next => batch.with_context(|| ErrorContext::new("exchange.next").field("exchange", self.exchange)),
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
            return Err(Error::internal("exchange sink already started".into()));
        }
        self.outputs = self
            .exchanges
            .iter()
            .map(|exchange| {
                self.service
                    .create_sink(*exchange, &self.schema)
                    .with_context(|| {
                        ErrorContext::new("exchange.create_sink").field("exchange", exchange)
                    })
            })
            .collect::<Result<_>>()?;
        Ok(())
    }
    fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        Ok(Box::new(ExchangeLocalSink {
            chunks: vec![],
            rows: 0,
            flush_rows: EXCHANGE_FLUSH_ROWS.saturating_mul(ctx.batch_rows),
        }))
    }
    fn consume<'a>(
        &'a mut self,
        _: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        _: StateContext<'a>,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let local = downcast_local::<ExchangeLocalSink>(local)?;
            local.rows = 0;
            for chunk in std::mem::take(&mut local.chunks) {
                for (exchange, output) in self.exchanges.iter().zip(&mut self.outputs) {
                    output.send(&chunk, guard).await.with_context(|| {
                        ErrorContext::new("exchange.send").field("exchange", exchange)
                    })?;
                }
            }
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        _cpu: &'a dyn crate::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Number of input-sized batches buffered before sending to an exchange.
const EXCHANGE_FLUSH_ROWS: usize = 8;

struct ExchangeLocalSink {
    chunks: Vec<RecordBatch>,
    rows: usize,
    flush_rows: usize,
}
impl LocalSink for ExchangeLocalSink {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            self.rows += chunk.num_rows();
            self.chunks.push(chunk);
        }
        Ok(if self.rows >= self.flush_rows {
            ConsumeStatus::Flush
        } else {
            ConsumeStatus::End
        })
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
