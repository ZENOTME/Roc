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

//! The pinned TPC-DS corpus: DataFusion 55.1.0's 99 query files, split into
//! their 103 statements, over one Parquet file per table.
use datafusion::{
    error::{DataFusionError, Result},
    prelude::{ParquetReadOptions, SessionContext},
};
use std::path::{Path, PathBuf};

pub const TABLES: [&str; 24] = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// One SQL statement of the corpus. Files with several statements name them
/// `q14a`, `q14b`, ...
#[derive(Clone, Debug)]
pub struct Statement {
    pub name: String,
    pub sql: String,
}

/// The query directory of this crate.
pub fn query_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tpcds/queries")
}

/// Every statement in query order.
pub fn statements(dir: &Path) -> Result<Vec<Statement>> {
    let mut out = vec![];
    for q in 1..=99 {
        let path = dir.join(format!("q{q}.sql"));
        let text = std::fs::read_to_string(&path)
            .map_err(|e| DataFusionError::Plan(format!("{}: {e}", path.display())))?;
        // The corpus has no semicolons inside literals or comments.
        let parts = text
            .split(';')
            .map(strip_comments)
            .filter(|s| !s.trim().is_empty())
            .collect::<Vec<_>>();
        for (i, sql) in parts.iter().enumerate() {
            let name = if parts.len() == 1 {
                format!("q{q}")
            } else {
                format!("q{q}{}", (b'a' + i as u8) as char)
            };
            out.push(Statement {
                name,
                sql: sql.trim().to_string(),
            });
        }
    }
    Ok(out)
}

fn strip_comments(sql: &str) -> String {
    sql.lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Registers `<table>.parquet` of `dir` for every TPC-DS table.
pub async fn register_tables(ctx: &SessionContext, dir: &Path) -> Result<()> {
    for table in TABLES {
        let path = dir.join(format!("{table}.parquet"));
        ctx.register_parquet(table, path.to_str().unwrap(), ParquetReadOptions::default())
            .await?;
    }
    Ok(())
}
