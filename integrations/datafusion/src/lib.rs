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

//! DataFusion SQL and logical planning with native Roc execution.
//!
//! Convert an analyzed, optimized logical plan into Roc operators. DataFusion
//! binds scalar expressions and plans table scans; Roc owns aggregation phases,
//! kernels and pipeline scheduling. Unsupported features fail at conversion.
//!
//! ```no_run
//! # async fn example() -> datafusion::error::Result<()> {
//! use datafusion::prelude::SessionContext;
//! use roc_datafusion::LogicalPlanConverter;
//!
//! let ctx = SessionContext::new();
//! // Register tables through DataFusion as usual.
//! let frame = ctx.sql("SELECT group_key, SUM(value) FROM t GROUP BY group_key").await?;
//! let tree = LogicalPlanConverter::convert_dataframe(frame).await?;
//! // Install a Roc result sink, build_pipeline_on_node(tree.root(), ...),
//! // and execute the graph using PipelineGraphExecutor.
//! # Ok(())
//! # }
//! ```

mod converter;
mod storage;

pub use converter::LogicalPlanConverter;
pub use storage::{DataFusionScan, DataFusionStorage};
