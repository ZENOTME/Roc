//! Execution model for Roc hosts.
//!
//! Logical planning, SQL, storage implementations, and exchange transports
//! stay outside this crate. Roc owns operator trees, pipeline construction,
//! task execution, and pipeline scheduling.

pub mod error;
pub mod exec;
pub mod expr;
pub mod operator;
pub mod pipeline;

pub use error::{Error, Result};
pub use tokio_util::sync::CancellationToken;
