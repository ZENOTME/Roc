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

use super::{SinkExec, SourceExec};
use crate::arrow::record_batch::RecordBatch;
use crate::{
    error::{Error, Result},
    operator::{ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink},
    operator::{ExchangeId, ExchangeSinkOperator, ExchangeSourceOperator},
};
use asyncband::shutdown::ShutdownGuard;
use futures::{FutureExt, future::BoxFuture};
use std::sync::Arc;

pub struct ExchangeSinkExec {
    state: crate::suboperator::StateId,
}
pub struct ExchangeSourceExec {
    exchange: ExchangeId,
    service: Arc<dyn ExchangeService>,
    handle: Option<Arc<dyn ExchangeHandle>>,
    consumer: Option<Box<dyn ExchangeConsumer>>,
}

impl ExchangeSourceExec {
    pub fn new(operator: ExchangeSourceOperator) -> Self {
        let (exchange, service) = operator.into_parts();
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
    pub fn new(state: crate::suboperator::StateId) -> Self {
        Self { state }
    }
}
impl ExchangeSinkOperator {
    pub fn build_suboperator(
        &self,
        builder: &mut crate::suboperator::SuboperatorScopeBuilder,
    ) -> Result<crate::suboperator::StateId> {
        let (exchanges, schema, service) = self.clone().into_parts();
        let state = builder.state(crate::suboperator::StateBuilder::new(move || {
            let outputs = exchanges
                .iter()
                .map(|id| service.create_sink(*id, &schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(ExchangeLocalState {
                outputs,
                pending: vec![],
            })
        }));
        builder.batch_update(state, |state, batch| {
            state
                .get_mut::<ExchangeLocalState>()?
                .pending
                .push(batch.clone());
            Ok(crate::suboperator::ProgramControl::Continue)
        })?;
        Ok(state)
    }
}
impl SinkExec for ExchangeSinkExec {
    fn flush<'a>(
        &'a mut self,
        local: &'a mut crate::suboperator::ProgramExecutionContext,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let state = local
                .state_mut(self.state)?
                .get_mut::<ExchangeLocalState>()?;
            for batch in state.pending.drain(..) {
                for output in &mut state.outputs {
                    output.send(&batch, guard).await?;
                }
            }
            Ok(())
        })
    }
}

/// Data only: one queue and transport set per exclusive Program instance.
struct ExchangeLocalState {
    pending: Vec<RecordBatch>,
    outputs: Vec<Box<dyn ExchangeSink>>,
}
