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

use arrow::error::ArrowError;
use roc::error::{Error, ErrorContext, ErrorKind, Result, ResultExt};
use std::{cell::Cell, error::Error as _, sync::Arc};

#[test]
fn context_is_lazy_and_captures_the_propagating_call_site() {
    let called = Cell::new(false);
    let result: Result<u32> = Ok(42);
    assert_eq!(
        result
            .with_context(|| {
                called.set(true);
                ErrorContext::new("unused")
            })
            .unwrap(),
        42
    );
    assert!(!called.get());

    let origin_line = line!() + 1;
    let error = Error::invalid_input("wrong schema".into());
    assert_eq!(error.location().line(), origin_line);
    let result: Result<()> = Err(error);
    let context = || ErrorContext::new("scan.decode");
    let propagation_line = line!() + 1;
    let error = result.with_context(context).unwrap_err();
    assert_eq!(error.frames()[0].location.line(), propagation_line);
    assert_eq!(error.frames()[0].location.file(), file!());
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[test]
fn worker_context_does_not_change_a_shared_error_or_its_source() {
    let original = Error::new(ErrorKind::Unavailable, "read failed").with_source(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset"),
    );
    let worker = original
        .clone()
        .context(ErrorContext::new("worker.execute").field("worker", 2));
    assert!(original.frames().is_empty());
    assert_eq!(
        worker.frames()[0].context.fields,
        vec![("worker", "2".into())]
    );
    assert!(std::ptr::eq(
        original
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap(),
        worker
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap(),
    ));
    let report = format!("{worker:?}");
    assert!(report.contains("worker.execute"));
    assert!(report.contains("peer reset"));
    assert!(!worker.to_string().contains("peer reset"));
}

#[test]
fn dependency_conversion_classifies_only_unambiguous_semantics() {
    for (source, kind) in [
        (ArrowError::DivideByZero, ErrorKind::DivisionByZero),
        (
            ArrowError::ArithmeticOverflow("overflow".into()),
            ErrorKind::ArithmeticOverflow,
        ),
        (
            ArrowError::SchemaError("mismatch".into()),
            ErrorKind::External,
        ),
    ] {
        let error = Error::from(source);
        assert_eq!(error.kind(), kind);
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<ArrowError>()
                .is_some()
        );
    }
    let error = Error::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
    assert_eq!(error.kind(), ErrorKind::External);
}

#[test]
fn errors_remain_send_sync_and_cloneable() {
    fn assert_traits<T: Send + Sync + Clone + std::error::Error + 'static>() {}
    assert_traits::<Error>();
    let error = Arc::new(Error::cancelled());
    let clone = error.clone();
    assert_eq!(
        std::thread::spawn(move || clone.kind()).join().unwrap(),
        ErrorKind::Cancelled
    );
}

#[test]
fn projection_binding_preserves_plan_classification() {
    use arrow::datatypes::DataType;
    use roc::{
        exec::{ProcessExec, ProjectExec},
        expr::scalar::{ConstantExpression, FunctionExpression, FunctionKind},
        operator::{Projection, ProjectionExpression},
    };
    let malformed = FunctionExpression::unary(
        FunctionKind::Add,
        ConstantExpression::int64(Some(1)).into_ref(),
        DataType::Int64,
        false,
    )
    .into_ref();
    let project = ProjectExec::new(Projection::new(vec![ProjectionExpression::new(
        malformed, "bad",
    )]));
    let error = match project.new_executor(Arc::new(())) {
        Err(error) => error,
        Ok(_) => panic!("invalid unary Add should fail binding"),
    };
    assert_eq!(error.kind(), ErrorKind::InvalidPlan);
    assert_eq!(
        error.frames().last().unwrap().context.operation,
        "projection.bind"
    );
}
