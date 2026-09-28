//! Execution model for Roc hosts.
//!
//! Logical planning, SQL, storage implementations, and exchange transports
//! stay outside this crate. Roc owns operator trees, pipeline construction,
//! task execution, and pipeline scheduling.
//!
//! Hosts supply fully bound physical plans: schemas, column indices and casts
//! are fixed before constructing the tree. Scalar evaluation uses DataFusion.

mod cancel;
pub mod error;
pub mod exec;
pub mod operator;
pub mod pipeline;

pub use cancel::Cancel;
pub use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
pub use error::{Error, Result};
