use super::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult, SourceExec, SourceExecutor};
use crate::Cancel;
use crate::{
    Error, Result,
    operator::{ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink},
    operator::{ExchangeId, ExchangeSinkOperator, ExchangeSourceOperator},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
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
    fn init_global_context(&self, cancel: &Cancel) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(self.service.start_input(self.exchange, cancel)?))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global.downcast::<Arc<dyn ExchangeHandle>>().map_err(|_| {
            Error::Execution("exchange source received an invalid global context".into())
        })?;
        Ok(Box::new(ExchangeSourceExecutor {
            consumer: global.consumer(),
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global.downcast::<Arc<dyn ExchangeHandle>>().map_err(|_| {
                Error::Execution("exchange source received an invalid global context".into())
            })?;
            global.finish().await
        })
    }
}

struct ExchangeSourceExecutor {
    consumer: Box<dyn ExchangeConsumer>,
}

impl SourceExecutor for ExchangeSourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            let cancelled = cancel.cancelled().fuse();
            let next = self.consumer.next().fuse();
            futures::pin_mut!(cancelled, next);
            futures::select_biased! {
                _ = cancelled => Err(Error::Cancelled),
                batch = next => Ok(batch),
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
    fn init_global_context(&self, _cancel: &Cancel) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        global.downcast::<()>().map_err(|_| {
            Error::Execution("exchange sink received an invalid global context".into())
        })?;
        let outputs = self
            .exchanges
            .iter()
            .map(|exchange| self.service.create_sink(*exchange, &self.schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(ExchangeSinkExecutor { outputs }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            global.downcast::<()>().map_err(|_| {
                Error::Execution("exchange sink received an invalid global context".into())
            })?;
            Ok(())
        })
    }
}

struct ExchangeSinkExecutor {
    outputs: Vec<Box<dyn ExchangeSink>>,
}

impl SinkExecutor for ExchangeSinkExecutor {
    fn sink<'a>(
        &'a mut self,
        cancel: &'a Cancel,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            for output in &mut self.outputs {
                output.send(input, cancel).await?;
            }
            Ok(SinkResult::NeedMoreInput)
        })
    }

    fn combine(self: Box<Self>, _cancel: &Cancel) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
