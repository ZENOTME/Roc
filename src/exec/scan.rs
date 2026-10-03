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

use super::{GlobalExecContextRef, SourceExec, SourceExecutor};
use crate::{
    error::{Error, Result},
    operator::{ScanConsumer, ScanHandle, ScanOperator, ScanRequest},
};
use arrow::record_batch::RecordBatch;
use asyncband::shutdown::ShutdownGuard;
use futures::{FutureExt, future::BoxFuture};
use std::sync::Arc;

pub struct ScanExec<StorageTaskDesc> {
    operator: ScanOperator<StorageTaskDesc>,
}

impl<StorageTaskDesc> ScanExec<StorageTaskDesc> {
    pub fn new(operator: ScanOperator<StorageTaskDesc>) -> Self {
        Self { operator }
    }
}

impl<StorageTaskDesc> SourceExec for ScanExec<StorageTaskDesc>
where
    StorageTaskDesc: Clone + Send + Sync + 'static,
{
    fn init_global_context(&self, shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        let handle = self.operator.storage().start_scan(ScanRequest::new(
            self.operator.source().clone(),
            shutdown_guard.clone(),
        ))?;
        Ok(Arc::new(ScanGlobalContext { handle }))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global.downcast::<ScanGlobalContext>().map_err(|_| {
            Error::Execution("scan source received an invalid global context".into())
        })?;
        Ok(Box::new(ScanExecutor {
            consumer: global.handle.consumer(),
            _global: global,
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        _shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global.downcast::<ScanGlobalContext>().map_err(|_| {
                Error::Execution("scan source received an invalid global context".into())
            })?;
            global.handle.finish().await
        })
    }
}

struct ScanGlobalContext {
    handle: Arc<dyn ScanHandle>,
}

struct ScanExecutor {
    consumer: Box<dyn ScanConsumer>,
    _global: Arc<ScanGlobalContext>,
}

impl SourceExecutor for ScanExecutor {
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
                    Err(Error::Cancelled)
                },
                batch = next => batch,
            }
        })
    }
}
