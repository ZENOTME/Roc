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

//! Errors describe the failed contract; context explains the execution path.
//!
//! Match `kind()`, never message text or a dependency's error type, for control
//! flow. No kind promises that replay is safe: sinks may have produced output.
//! The host owns retry policy, shutdown, and logging the final diagnostic.

use std::{error::Error as StdError, fmt, panic::Location, sync::Arc};

/// Stable failure semantics, independent of the dependency that detected them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The physical IR or execution configuration violates its contract.
    InvalidPlan,
    /// The requested operation or type is not supported.
    Unsupported,
    /// Runtime data or arguments violate the receiving API's contract.
    InvalidInput,
    ArithmeticOverflow,
    DivisionByZero,
    Cancelled,
    /// An explicitly checked capacity or resource limit was exceeded.
    ResourceExhausted,
    /// An adapter reports that a dependency is unavailable; replay is not implied.
    Unavailable,
    /// An unclassified dependency or host failure. Adapters should classify when known.
    External,
    /// A spawned task failed to complete normally (for example, panic or abort).
    TaskFailed,
    /// An engine or extension implementation invariant was violated.
    Internal,
}

/// One logical operation, with diagnostic fields. Values are not a wire protocol.
#[derive(Clone, Debug)]
pub struct ErrorContext {
    pub operation: &'static str,
    pub fields: Vec<(&'static str, String)>,
}
impl ErrorContext {
    pub fn new(operation: &'static str) -> Self {
        Self {
            operation,
            fields: Vec::new(),
        }
    }
    pub fn field(mut self, name: &'static str, value: impl fmt::Display) -> Self {
        self.fields.push((name, value.to_string()));
        self
    }
}

#[derive(Clone, Debug)]
pub struct ErrorFrame {
    pub context: ErrorContext,
    pub location: &'static Location<'static>,
}

/// Cheap to clone and safe to pass between workers. Appending context to a clone
/// does not mutate the original. Success paths do not allocate diagnostic frames.
#[derive(Clone)]
pub struct Error(Arc<ErrorInner>);
#[derive(Clone)]
struct ErrorInner {
    kind: ErrorKind,
    message: String,
    location: &'static Location<'static>,
    source: Option<Arc<dyn StdError + Send + Sync>>,
    frames: Vec<ErrorFrame>,
}
impl Error {
    #[track_caller]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self(Arc::new(ErrorInner {
            kind,
            message: message.into(),
            location: Location::caller(),
            source: None,
            frames: Vec::new(),
        }))
    }
    #[track_caller]
    pub fn invalid_plan(message: String) -> Self {
        Self::new(ErrorKind::InvalidPlan, message)
    }
    #[track_caller]
    pub fn invalid_input(message: String) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
    }
    #[track_caller]
    pub fn unsupported(message: String) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }
    #[track_caller]
    pub fn internal(message: String) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
    #[track_caller]
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "query cancelled")
    }

    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        Arc::make_mut(&mut self.0).source = Some(Arc::new(source));
        self
    }
    pub fn kind(&self) -> ErrorKind {
        self.0.kind
    }
    pub fn message(&self) -> &str {
        &self.0.message
    }
    pub fn location(&self) -> &'static Location<'static> {
        self.0.location
    }
    /// Frames in propagation order, from the innermost operation outward.
    pub fn frames(&self) -> &[ErrorFrame] {
        &self.0.frames
    }
    #[track_caller]
    pub fn context(self, context: ErrorContext) -> Self {
        self.context_at(context, Location::caller())
    }
    #[cold]
    fn context_at(mut self, context: ErrorContext, location: &'static Location<'static>) -> Self {
        Arc::make_mut(&mut self.0)
            .frames
            .push(ErrorFrame { context, location });
        self
    }
}
impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0
            .source
            .as_deref()
            .map(|source| source as &dyn StdError)
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind(), self.message())
    }
}
/// Debug renders the complete diagnostic. Display is deliberately concise.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{self} at {}", self.location())?;
        for frame in self.frames().iter().rev() {
            write!(f, "  while {}", frame.context.operation)?;
            for (name, value) in &frame.context.fields {
                write!(f, " {name}={value:?}")?;
            }
            writeln!(f, " at {}", frame.location)?;
        }
        let mut source = StdError::source(self);
        while let Some(cause) = source {
            writeln!(f, "  caused by: {cause}")?;
            source = cause.source();
        }
        Ok(())
    }
}

/// Adds context only on failure, without changing the machine-readable kind.
pub trait ResultExt<T> {
    #[track_caller]
    fn with_context(self, context: impl FnOnce() -> ErrorContext) -> Result<T>;
}
impl<T> ResultExt<T> for Result<T> {
    #[inline]
    #[track_caller]
    fn with_context(self, context: impl FnOnce() -> ErrorContext) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(error.context_at(context(), Location::caller())),
        }
    }
}

// Only classify dependency errors whose semantics are unambiguous. Other Arrow
// failures require knowledge of the operation; boundaries should map explicitly.
impl From<arrow::error::ArrowError> for Error {
    #[track_caller]
    fn from(source: arrow::error::ArrowError) -> Self {
        use arrow::error::ArrowError;
        let (kind, message) = match &source {
            ArrowError::DivideByZero => (ErrorKind::DivisionByZero, "division by zero"),
            ArrowError::ArithmeticOverflow(_) => {
                (ErrorKind::ArithmeticOverflow, "arithmetic overflow")
            }
            _ => (ErrorKind::External, "Arrow operation failed"),
        };
        Self::new(kind, message).with_source(source)
    }
}
impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        Self::new(ErrorKind::External, "I/O operation failed").with_source(source)
    }
}
pub type Result<T> = std::result::Result<T, Error>;
