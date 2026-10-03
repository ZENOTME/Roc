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

use crate::{error::Result, operator::ExchangeId};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::Arc;

/// Host-provided exchange endpoints for the fragment being built.
pub trait ExchangeService: Send + Sync + 'static {
    /// Starts input with a caller-owned shutdown observer. Background work must
    /// retain a guard clone until it has finished.
    fn start_input(
        &self,
        exchange: ExchangeId,
        shutdown_guard: &ShutdownGuard,
    ) -> Result<Arc<dyn ExchangeHandle>>;

    fn create_sink(
        &self,
        exchange: ExchangeId,
        schema: &SchemaRef,
    ) -> Result<Box<dyn ExchangeSink>>;
}

pub trait ExchangeHandle: Send + Sync + 'static {
    fn consumer(&self) -> Box<dyn ExchangeConsumer>;
    /// Stops and joins input work on normal completion, including early stop.
    /// This must not require a shutdown request. Dropping the handle must release
    /// owned resources; background tasks must observe their shutdown guards.
    fn finish(&self) -> BoxFuture<'_, Result<()>>;
}

pub trait ExchangeConsumer: Send + 'static {
    /// Returns the next batch, normal end of input, or a read failure.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>>;
}

pub trait ExchangeSink: Send + 'static {
    fn send<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>>;
}
