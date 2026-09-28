use std::sync::Arc;

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid plan: {0}")]
    Plan(String),
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
