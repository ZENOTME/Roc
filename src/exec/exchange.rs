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

use super::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult, SourceExec, SourceExecutor};
use crate::{
    error::{Error, ErrorContext, Result, ResultExt},
    operator::{ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink},
    operator::{ExchangeId, ExchangeSinkOperator, ExchangeSourceOperator},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use futures::{FutureExt, future::BoxFuture};
use std::sync::Arc;

pub struct ExchangeSinkExec {
    exchanges: Vec<ExchangeId>,
    service: Arc<dyn ExchangeService>,
    schema: SchemaRef,
}

pub struct ExchangeSourceExec {
    exchange: ExchangeId,
    service: Arc<dyn ExchangeService>,
}

impl ExchangeSourceExec {
    pub fn new(operator: ExchangeSourceOperator) -> Self {
        let (exchange, service) = operator.into_parts();
        Self { exchange, service }
    }
}

impl SourceExec for ExchangeSourceExec {
    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        let handle = self
            .service
            .start_input(self.exchange, shutdown_guard)
            .with_context(|| {
                ErrorContext::new("exchange.start").field("exchange", self.exchange)
            })?;
        Ok(Arc::new(ExchangeSourceGlobalContext { handle }))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global
            .downcast::<ExchangeSourceGlobalContext>()
            .map_err(|_| {
                Error::internal("exchange source received an invalid global context".into())
            })?;
        Ok(Box::new(ExchangeSourceExecutor {
            consumer: global.handle.consumer(),
            exchange: self.exchange,
            _global: global,
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global
                .downcast::<ExchangeSourceGlobalContext>()
                .map_err(|_| {
                    Error::internal("exchange source received an invalid global context".into())
                })?;
            global.handle.finish().await.with_context(|| {
                ErrorContext::new("exchange.finish").field("exchange", self.exchange)
            })
        })
    }
}

struct ExchangeSourceGlobalContext {
    handle: Arc<dyn ExchangeHandle>,
}

struct ExchangeSourceExecutor {
    exchange: ExchangeId,
    consumer: Box<dyn ExchangeConsumer>,
    _global: Arc<ExchangeSourceGlobalContext>,
}

impl SourceExecutor for ExchangeSourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let cancelled = shutdown_guard.shutdown_requested().fuse();
            let next = self.consumer.next().fuse();
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
    pub fn new(operator: ExchangeSinkOperator) -> Self {
        let (exchanges, schema, service) = operator.into_parts();
        Self {
            exchanges,
            schema,
            service,
        }
    }
}

impl SinkExec for ExchangeSinkExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        global.downcast::<()>().map_err(|_| {
            Error::internal("exchange sink received an invalid global context".into())
        })?;
        let outputs = self
            .exchanges
            .iter()
            .map(|exchange| {
                self.service
                    .create_sink(*exchange, &self.schema)
                    .map(|sink| (*exchange, sink))
                    .with_context(|| {
                        ErrorContext::new("exchange.create_sink").field("exchange", exchange)
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(ExchangeSinkExecutor { outputs }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            global.downcast::<()>().map_err(|_| {
                Error::internal("exchange sink received an invalid global context".into())
            })?;
            Ok(())
        })
    }
}

struct ExchangeSinkExecutor {
    outputs: Vec<(ExchangeId, Box<dyn ExchangeSink>)>,
}

impl SinkExecutor for ExchangeSinkExecutor {
    fn sink<'a>(
        &'a mut self,
        shutdown_guard: &'a ShutdownGuard,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            for (exchange, output) in &mut self.outputs {
                output.send(input, shutdown_guard).await.with_context(|| {
                    ErrorContext::new("exchange.send").field("exchange", *exchange)
                })?;
            }
            Ok(SinkResult::NeedMoreInput)
        })
    }

    fn combine(self: Box<Self>, _shutdown_guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
