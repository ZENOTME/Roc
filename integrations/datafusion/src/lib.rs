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

//! DataFusion's planned Arrow streams as Roc storage inputs.
//!
//! DataFusion retains responsibility for Parquet I/O, decoding, projection and
//! predicate pruning. Roc consumers pull directly from its output partitions.
//!
//! ```no_run
//! # async fn example() -> datafusion::error::Result<()> {
//! use std::sync::Arc;
//! use datafusion::prelude::{ParquetReadOptions, SessionContext};
//! use roc::operator::ScanOperator;
//! use roc_datafusion::{DataFusionScan, DataFusionStorage};
//!
//! let ctx = SessionContext::new();
//! let frame = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;
//! // Apply DataFrame filters/projection before planning to preserve pushdown.
//! let plan = frame.create_physical_plan().await?;
//! let source = DataFusionScan::new(plan, ctx.task_ctx());
//! let scan = ScanOperator::new(source, Arc::new(DataFusionStorage));
//! # Ok(())
//! # }
//! ```

mod storage;

pub use storage::{DataFusionScan, DataFusionStorage};
