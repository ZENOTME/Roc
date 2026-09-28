use crate::CancellationToken;
use crate::{Result, operator::ExchangeId};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::future::BoxFuture;
use std::sync::Arc;

/// Host-provided exchange endpoints for the fragment being built.
pub trait ExchangeService: Send + Sync + 'static {
    fn start_input(
        &self,
        exchange: ExchangeId,
        cancel: &CancellationToken,
    ) -> Result<Arc<dyn ExchangeHandle>>;

    fn create_sink(
        &self,
        exchange: ExchangeId,
        schema: &SchemaRef,
    ) -> Result<Box<dyn ExchangeSink>>;
}

pub trait ExchangeHandle: Send + Sync + 'static {
    fn consumer(&self) -> Box<dyn ExchangeConsumer>;
    fn finish<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
}

pub trait ExchangeConsumer: Send + 'static {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<RecordBatch>>;
}

pub trait ExchangeSink: Send + 'static {
    fn send<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>>;
}
