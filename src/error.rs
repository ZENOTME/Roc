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

use std::{error::Error as StdError, fmt, panic::Location};

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

/// Runtime diagnostic fields for one logical operation. Values are not a wire protocol.
/// Use no context when the caller's location alone identifies the failed operation.
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
    pub context: Option<ErrorContext>,
    pub location: &'static Location<'static>,
}

/// An owned error that can move between workers.
pub struct Error {
    kind: ErrorKind,
    message: String,
    location: &'static Location<'static>,
    source: Option<Box<dyn StdError + Send + Sync>>,
    frames: Vec<ErrorFrame>,
}
impl Error {
    #[track_caller]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            location: Location::caller(),
            source: None,
            frames: Vec::new(),
        }
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
        self.source = Some(Box::new(source));
        self
    }
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn location(&self) -> &'static Location<'static> {
        self.location
    }
    /// Frames in propagation order, from the innermost operation outward.
    pub fn frames(&self) -> &[ErrorFrame] {
        &self.frames
    }
    /// Records the caller's location and diagnostic context.
    #[track_caller]
    pub fn with_context(self, context: ErrorContext) -> Self {
        self.with_frame(Some(context), Location::caller())
    }
    /// Records the caller's location without constructing a context.
    #[track_caller]
    pub fn with_location(self) -> Self {
        self.with_frame(None, Location::caller())
    }
    #[cold]
    fn with_frame(
        mut self,
        context: Option<ErrorContext>,
        location: &'static Location<'static>,
    ) -> Self {
        self.frames.push(ErrorFrame { context, location });
        self
    }
}
impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as &dyn StdError)
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
            if let Some(context) = &frame.context {
                write!(f, "  while {}", context.operation)?;
                for (name, value) in &context.fields {
                    write!(f, " {name}={value:?}")?;
                }
                writeln!(f, " at {}", frame.location)?;
            } else {
                writeln!(f, "  at {}", frame.location)?;
            }
        }
        let mut source = StdError::source(self);
        while let Some(cause) = source {
            writeln!(f, "  caused by: {cause}")?;
            source = cause.source();
        }
        Ok(())
    }
}

/// Records propagation sites only on failure, with optional diagnostic context.
pub trait ResultExt<T> {
    /// Records the caller's location without constructing a context.
    #[track_caller]
    fn with_location(self) -> Result<T>;

    #[track_caller]
    fn with_context(self, context: impl FnOnce() -> ErrorContext) -> Result<T>;
}
impl<T> ResultExt<T> for Result<T> {
    #[inline]
    #[track_caller]
    fn with_location(self) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(error.with_frame(None, Location::caller())),
        }
    }

    #[inline]
    #[track_caller]
    fn with_context(self, context: impl FnOnce() -> ErrorContext) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => Err(error.with_frame(Some(context()), Location::caller())),
        }
    }
}

// Dependency errors must be translated by the operation that receives them.
// This module only owns the error carrier and diagnostic propagation.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn context_is_lazy_and_captures_the_propagating_call_site() {
        let called = Cell::new(false);
        let result: Result<u32> = Ok(42);
        assert_eq!(
            result
                .with_context(|| {
                    called.set(true);
                    ErrorContext::new("unused").field("column", 0)
                })
                .unwrap(),
            42
        );
        assert!(!called.get());

        let origin_line = line!() + 1;
        let error = Error::invalid_input("wrong schema".into());
        assert_eq!(error.location().line(), origin_line);
        let result: Result<()> = Err(error);
        let context = || ErrorContext::new("scan.decode").field("column", 0);
        let propagation_line = line!() + 1;
        let error = result.with_context(context).unwrap_err();
        assert_eq!(error.frames()[0].location.line(), propagation_line);
        assert_eq!(error.frames()[0].location.file(), file!());
        assert_eq!(error.kind(), ErrorKind::InvalidInput);

        let propagation_line = line!() + 1;
        let error = error.with_location();
        let result: Result<()> = Err(error);
        let outer_line = line!() + 1;
        let error = result.with_location().unwrap_err();
        assert_eq!(error.location().line(), origin_line);
        assert_eq!(error.frames().len(), 3);
        for (frame, line) in error.frames()[1..]
            .iter()
            .zip([propagation_line, outer_line])
        {
            assert!(frame.context.is_none());
            assert_eq!(frame.location.file(), file!());
            assert_eq!(frame.location.line(), line);
            assert!(format!("{error:?}").contains(&format!("  at {}", frame.location)));
        }
        let success: Result<u32> = Ok(42);
        assert_eq!(success.with_location().unwrap(), 42);
    }

    #[test]
    fn context_preserves_the_source_and_diagnostic_report() {
        let error = Error::new(ErrorKind::Unavailable, "read failed").with_source(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset"),
        );
        let error = error.with_context(ErrorContext::new("worker.execute").field("worker", 2));
        assert_eq!(
            error.frames()[0].context.as_ref().unwrap().fields,
            vec![("worker", "2".into())]
        );
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::ConnectionReset,
        );
        let report = format!("{error:?}");
        assert!(report.contains("worker.execute"));
        assert!(report.contains("peer reset"));
        assert!(!error.to_string().contains("peer reset"));
    }

    #[test]
    fn errors_can_move_between_workers() {
        fn assert_traits<T: Send + Sync + std::error::Error + 'static>() {}
        assert_traits::<Error>();
        let error = Error::cancelled();
        assert_eq!(
            std::thread::spawn(move || error).join().unwrap().kind(),
            ErrorKind::Cancelled
        );
    }
}
