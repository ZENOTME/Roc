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

//! Runs operator trees over in-memory tables in every execution mode, with
//! one and several workers, and checks that the results agree.
use arrow::{array::Array, record_batch::RecordBatch, util::display::ArrayFormatter};
use futures::future::BoxFuture;
use roc::{
    column::{ColumnContext, ColumnId},
    error::Result,
    kernel::ExecutionMode,
    operator::{
        CollectOperator, OperatorTree, OperatorTreeNode, ScanConsumer, ScanHandle, ScanOperator,
        ScanRequest, ScanStorage,
    },
    pipeline::{Executor, PipelineGraphExecutor, build_pipeline_graph_with_mode},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

/// Batches served once per scan.
struct MemoryStorage(Vec<RecordBatch>);
#[derive(Clone)]
struct MemoryScan(Arc<Mutex<VecDeque<RecordBatch>>>);

impl ScanStorage for MemoryStorage {
    type StorageTaskDesc = ();
    fn start_scan(&self, _: ScanRequest<()>) -> Result<Arc<dyn ScanHandle>> {
        Ok(Arc::new(MemoryScan(Arc::new(Mutex::new(
            self.0.iter().cloned().collect(),
        )))))
    }
}
impl ScanHandle for MemoryScan {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        Box::new(self.clone())
    }
    fn finish(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl ScanConsumer for MemoryScan {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async { Ok(self.0.lock().unwrap().pop_front()) })
    }
}

/// Runs each task on a thread of its own.
pub struct ThreadExecutor;
impl Executor for ThreadExecutor {
    type JoinError = futures::channel::oneshot::Canceled;
    type Handle<T>
        = futures::channel::oneshot::Receiver<T>
    where
        T: Send + 'static;
    fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (send, receive) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            let _ = send.send(futures::executor::block_on(task));
        });
        receive
    }
}

/// A scan of `batches` binding `columns` in order.
pub fn scan(batches: Vec<RecordBatch>, columns: Vec<ColumnId>) -> OperatorTreeNode {
    OperatorTreeNode::new(
        ScanOperator::new((), Arc::new(MemoryStorage(batches)), columns),
        vec![],
    )
}

/// Collects the output of `root` with `parallelism` workers.
pub fn execute(
    columns: &ColumnContext,
    root: &OperatorTreeNode,
    mode: ExecutionMode,
    parallelism: usize,
) -> Result<Vec<RecordBatch>> {
    let collect = CollectOperator::new();
    let tree = OperatorTree::new(
        columns.clone(),
        OperatorTreeNode::new(collect.clone(), vec![root.clone()]),
    );
    let graph = build_pipeline_graph_with_mode(&tree, mode)?;
    let executor = PipelineGraphExecutor::new(graph)
        .with_task_executor(ThreadExecutor)
        .with_parallelism(parallelism);
    let shutdown = executor.shutdown();
    let result = futures::executor::block_on(executor.execute());
    if result.is_err() {
        shutdown.request_shutdown();
    }
    futures::executor::block_on(shutdown);
    result?;
    Ok(collect.take())
}

/// The rows of `batches`, each formatted as one string per column.
pub fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut out = vec![];
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &Default::default()).unwrap())
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            out.push(
                formatters
                    .iter()
                    .zip(batch.columns())
                    .map(|(f, c)| {
                        if c.is_null(row) {
                            "NULL".to_string()
                        } else {
                            f.value(row).to_string()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

/// Runs `root` in every mode with one and four workers; the row multisets
/// must agree. Returns the rows of the ordered single-worker run.
pub fn execute_all(columns: &ColumnContext, root: &OperatorTreeNode) -> Result<Vec<Vec<String>>> {
    let mut first: Option<Vec<Vec<String>>> = None;
    for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
        for parallelism in [1, 4] {
            let rows = rows(&execute(columns, root, mode, parallelism)?);
            match &first {
                None => first = Some(rows),
                Some(expected) => {
                    let (mut a, mut b) = (expected.clone(), rows);
                    a.sort();
                    b.sort();
                    assert_eq!(a, b, "{mode:?} with {parallelism} workers differs");
                }
            }
        }
    }
    Ok(first.unwrap())
}
