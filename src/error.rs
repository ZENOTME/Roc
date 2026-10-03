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

use std::sync::Arc;

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid plan: {0}")]
    InvalidPlan(String),
    #[error(transparent)]
    Arrow(Arc<arrow::error::ArrowError>),
    #[error(transparent)]
    Io(Arc<std::io::Error>),
    #[error("query cancelled")]
    Cancelled,
    #[error("execution failed: {0}")]
    Execution(String),
}
impl From<arrow::error::ArrowError> for Error {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::Arrow(Arc::new(error))
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}
pub type Result<T> = std::result::Result<T, Error>;
