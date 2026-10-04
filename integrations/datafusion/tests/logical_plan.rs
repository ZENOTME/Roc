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

use arrow::{
    array::{ArrayRef, Int32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use asyncband::shutdown::ShutdownGuard;
use datafusion::{
    datasource::MemTable,
    physical_plan::collect,
    prelude::{SessionConfig, SessionContext},
};
use futures::future::BoxFuture;
use roc::{
    error::Result as RocResult,
    exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult},
    operator::OperatorTreeNode,
    pipeline::{
        Executor, PipelineExecutionConfig, PipelineGraphBuilder, PipelineGraphExecutor,
        build_pipeline_on_node,
    },
};
use roc_datafusion::LogicalPlanConverter;
use std::sync::{Arc, Mutex};
use std::time::Instant;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const BATCH_ROWS: usize = 8192;
struct TokioExecutor(tokio::runtime::Handle);
impl Executor for TokioExecutor {
    type JoinError = tokio::task::JoinError;
    type Handle<T>
        = tokio::task::JoinHandle<T>
    where
        T: Send + 'static;
    fn spawn<F>(&self, task: F) -> Self::Handle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.spawn(task)
    }
}

struct CollectSink(Arc<Mutex<Vec<RecordBatch>>>);
struct CollectSinkExecutor {
    output: Arc<Mutex<Vec<RecordBatch>>>,
    local: Vec<RecordBatch>,
}

impl SinkExec for CollectSink {
    fn init_global_context(&self, _: &ShutdownGuard) -> RocResult<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _: GlobalExecContextRef) -> RocResult<Box<dyn SinkExecutor>> {
        Ok(Box::new(CollectSinkExecutor {
            output: self.0.clone(),
            local: vec![],
        }))
    }
    fn finalize<'a>(
        &'a self,
        _: GlobalExecContextRef,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, RocResult<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl SinkExecutor for CollectSinkExecutor {
    fn sink<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        batch: &'a RecordBatch,
    ) -> BoxFuture<'a, RocResult<SinkResult>> {
        Box::pin(async move {
            self.local.push(batch.clone());
            Ok(SinkResult::NeedMoreInput)
        })
    }
    fn combine(self: Box<Self>, _: &ShutdownGuard) -> BoxFuture<'_, RocResult<()>> {
        Box::pin(async move {
            self.output.lock().unwrap().extend(self.local);
            Ok(())
        })
    }
}

async fn run_roc(tree: &OperatorTreeNode, parallelism: usize) -> Result<(f64, Vec<RecordBatch>)> {
    let output = Arc::new(Mutex::new(vec![]));
    let mut graph = PipelineGraphBuilder::new();
    graph
        .pipeline_mut(0)?
        .set_sink(Box::new(CollectSink(output.clone())))?;
    build_pipeline_on_node(tree, 0, &mut graph)?;
    let executor = PipelineGraphExecutor::new(graph.finish()?)
        .with_task_executor(TokioExecutor(tokio::runtime::Handle::current()))
        .with_parallelism(parallelism)
        .with_config(PipelineExecutionConfig {
            batch_rows: BATCH_ROWS,
            ..Default::default()
        });
    let shutdown = executor.shutdown();
    let started = Instant::now();
    let result = executor.execute().await;
    if result.is_err() {
        shutdown.request_shutdown();
    }
    shutdown.await;
    result?;
    let batches = std::mem::take(&mut *output.lock().unwrap());
    Ok((started.elapsed().as_secs_f64() * 1000.0, batches))
}

fn context(empty: bool) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let schema = Arc::new(Schema::new(vec![
        Field::new("unused", DataType::Int64, false),
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int32, true),
    ]));
    let arrays: Vec<ArrayRef> = if empty {
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
        ]
    } else {
        vec![
            Arc::new(Int64Array::from(vec![9; 6])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                None,
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(2),
                None,
                Some(4),
                Some(6),
                None,
                None,
            ])),
        ]
    };
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(
            schema,
            vec![vec![batch.clone()], vec![], vec![batch]],
        )?),
    )?;
    Ok(ctx)
}
fn rows(batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let converter = RowConverter::new(
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect(),
    )?;
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(
            converter
                .convert_columns(batch.columns())?
                .iter()
                .map(|row| row.as_ref().to_vec()),
        );
    }
    rows.sort_unstable();
    Ok(rows)
}
async fn compare(ctx: &SessionContext, sql: &str) -> Result<()> {
    let frame = ctx.sql(sql).await?;
    let (state, plan) = frame.into_parts();
    let logical = state.optimize(&plan)?;
    let schema = logical.schema().as_arrow();
    let physical = state.create_physical_plan(&logical).await?;
    let expected = collect(physical, state.task_ctx()).await?;
    let tree = LogicalPlanConverter::new(state).convert(&logical).await?;
    for workers in [1, 4] {
        // Reuse the converted plan to verify fresh scan and aggregate state.
        for _ in 0..2 {
            let (_, actual) = run_roc(tree.root(), workers).await?;
            for batch in &actual {
                assert_eq!(batch.schema().as_ref(), schema, "{sql}");
            }
            assert_eq!(rows(&actual)?, rows(&expected)?, "{sql} workers={workers}");
        }
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_queries_match_datafusion() -> Result<()> {
    let ctx = context(false)?;
    for sql in [
        "SELECT v + 1 AS x, g FROM t WHERE v > 2",
        "SELECT a.g, a.v FROM t AS a WHERE a.g IS NOT NULL AND NOT (a.v = 2)",
        "SELECT g, SUM(v) AS s, COUNT(v) AS c, COUNT(*) AS n, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g",
        "SELECT SUM(v) AS s, COUNT(v) AS c, COUNT(*) AS n, AVG(v) AS a FROM t",
        "SELECT g, COUNT(DISTINCT v) AS c, SUM(v) FILTER (WHERE v > 3) AS s FROM t GROUP BY g",
        "SELECT g, COUNT(*) AS n FROM t GROUP BY g HAVING COUNT(*) > 2",
        "SELECT DISTINCT g FROM t",
        "SELECT g, COVAR_POP(v, v) AS c FROM t GROUP BY g",
        "SELECT CASE WHEN v > 3 THEN v + 1 ELSE v - 1 END AS x FROM t",
        "SELECT CAST(v AS BIGINT) AS x, TRY_CAST(v AS SMALLINT) AS y FROM t",
    ] {
        compare(&ctx, sql).await?;
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_aggregates_and_dataframe_entry() -> Result<()> {
    let ctx = context(true)?;
    compare(
        &ctx,
        "SELECT SUM(v) AS s, COUNT(v) AS c, COUNT(*) AS n, AVG(v) AS a FROM t",
    )
    .await?;
    compare(&ctx, "SELECT g, AVG(v) AS a FROM t GROUP BY g").await?;
    let tree = LogicalPlanConverter::convert_dataframe(ctx.sql("SELECT v FROM t").await?).await?;
    assert_eq!(
        run_roc(tree.root(), 4)
            .await?
            .1
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    Ok(())
}
#[tokio::test]
async fn unsupported_queries_fail_at_conversion() -> Result<()> {
    let ctx = context(false)?;
    for sql in [
        "SELECT v FROM t ORDER BY v",
        "SELECT v FROM t LIMIT 1",
        "SELECT abs(v) FROM t",
        "SELECT a.v FROM t a JOIN t b ON a.g = b.g",
        "SELECT g, SUM(v) FROM t GROUP BY ROLLUP(g)",
        "SELECT SUM(CAST(v AS DECIMAL(20, 0))) FROM t",
        "SELECT AVG(CAST(v AS DECIMAL(20, 0))) FROM t",
        "SELECT CAST(v AS DOUBLE) > 0 FROM t",
        "SELECT (v > 0) AND ((1 / v) > 0) AS x FROM t",
    ] {
        let error = LogicalPlanConverter::convert_dataframe(ctx.sql(sql).await?)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Roc conversion"),
            "{sql}: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn integer_sum_keeps_checked_overflow() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![i64::MAX, 1, -1]))],
    )?;
    ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
    let tree =
        LogicalPlanConverter::convert_dataframe(ctx.sql("SELECT SUM(v) FROM t").await?).await?;
    let error = run_roc(tree.root(), 1).await.unwrap_err();
    assert!(error.to_string().contains("overflow"), "{error}");
    Ok(())
}
