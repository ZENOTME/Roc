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

//! Translated statements against DataFusion's results over in-memory
//! tables, with the plans the translator chose.
use datafusion::{
    arrow::{
        array::{ArrayRef, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    prelude::SessionContext,
};
use roc::kernel::ExecutionMode;
use roc_datafusion::run::{Stage, explain, plan, run};
use std::sync::Arc;

fn table(ctx: &SessionContext, name: &str, columns: Vec<(&str, Vec<Option<i64>>)>) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let arrays = columns
        .into_iter()
        .map(|(_, v)| Arc::new(Int64Array::from(v)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let rows = batch.num_rows();
    // A few partitions, so scans run in parallel.
    let parts = (0..4)
        .map(|p| vec![batch.slice(p * rows / 4, (p + 1) * rows / 4 - p * rows / 4)])
        .collect();
    ctx.register_table(name, Arc::new(MemTable::try_new(schema, parts).unwrap()))
        .unwrap();
}

/// `fact` and `other` share keys many to many; `dim` is a filtered table
/// joined on another `fact` column. With `fact` the smaller side, the join
/// with `dim` semi-joins `fact` before `fact` is stored for `other`.
fn tables() -> SessionContext {
    let ctx = SessionContext::new();
    let n = 3000i64;
    table(
        &ctx,
        "fact",
        vec![
            ("f_key", (0..n).map(|i| Some(i % 50)).collect()),
            ("f_dim", (0..n).map(|i| (i % 17 != 0).then_some(i % 40)).collect()),
            ("f_value", (0..n).map(Some).collect()),
        ],
    );
    table(
        &ctx,
        "other",
        vec![
            ("o_key", (0..4 * n).map(|i| Some(i % 60)).collect()),
            ("o_value", (0..4 * n).map(|i| Some(i * 7)).collect()),
        ],
    );
    table(
        &ctx,
        "dim",
        vec![
            ("d_key", (0..40).map(Some).collect()),
            ("d_flag", (0..40).map(|i| Some(i % 3)).collect()),
        ],
    );
    ctx
}

const QUERY: &str = "select f_key, count(*), sum(f_value + o_value) \
    from fact join other on f_key = o_key join dim on f_dim = d_key \
    where d_flag = 1 group by f_key";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_tables_reduce_the_build_sides_below_their_joins() {
    let ctx = tables();
    let explained = explain(&ctx, &plan(&ctx, QUERY).await.unwrap(), ExecutionMode::ZeroCompile)
        .await
        .unwrap();
    // The join with dim and its semi join on the stored fact rows: two
    // tables of dim rows.
    assert_eq!(explained.matches("Expand").count(), 3, "{explained}");
    for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
        for workers in [1, 4] {
            let outcome = run(&ctx, QUERY, mode, workers).await;
            assert!(
                matches!(outcome.stage, Stage::Passed { .. }),
                "{mode:?} {workers}: {:?}",
                outcome.stage
            );
            assert!(outcome.rows > 0, "{mode:?} {workers}");
        }
    }
}
