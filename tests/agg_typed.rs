// Copyright 2026 The Roc Contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::expr::{
    ExpressionResultType,
    agg::{AggregateExpression, AggregateFunction},
    scalar::ReferenceExpression,
};

fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap()
}

fn expression(
    function: AggregateFunction,
    input: DataType,
    output: DataType,
) -> AggregateExpression {
    AggregateExpression::new(
        function,
        Some(ReferenceExpression::new(0, ExpressionResultType::new(input, true)).into_ref()),
        output,
        true,
    )
}

fn executor(expression: AggregateExpression) -> AggregateHarness {
    AggregateHarness::try_new(Arc::new(expression)).unwrap()
}

#[test]
fn grouped_sums_preserve_typed_state_and_sliced_null_bitmaps() {
    for array in [
        Arc::new(Int64Array::from(vec![
            Some(999),
            Some(i64::MAX - 1),
            None,
            Some(1),
            Some(7),
            Some(-7),
        ])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![
            Some(999),
            Some(u64::MAX - 1),
            None,
            Some(1),
            Some(7),
            Some(1),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(999.0),
            Some(4.0),
            None,
            Some(-4.0),
            Some(7.0),
            Some(1.0),
        ])),
    ] {
        let kind = array.data_type().clone();
        let mut sums = executor(expression(AggregateFunction::Sum, kind.clone(), kind));
        sums.update(&batch(array.slice(1, 5)), &[0, 1, 0, 2, 2], 4)
            .unwrap();
        let state = sums.state().unwrap();
        assert!(state[0].is_null(1));
        assert!(state[0].is_null(3));
        let output = sums.evaluate().unwrap();
        match output.data_type() {
            DataType::Int64 => assert_eq!(
                output.as_any().downcast_ref::<Int64Array>().unwrap(),
                &Int64Array::from(vec![Some(i64::MAX), None, Some(0), None])
            ),
            DataType::UInt64 => assert_eq!(
                output.as_any().downcast_ref::<UInt64Array>().unwrap(),
                &UInt64Array::from(vec![Some(u64::MAX), None, Some(8), None])
            ),
            DataType::Float64 => assert_eq!(
                output.as_any().downcast_ref::<Float64Array>().unwrap(),
                &Float64Array::from(vec![Some(0.0), None, Some(8.0), None])
            ),
            _ => unreachable!(),
        }
        let mut merged = executor(expression(
            AggregateFunction::Sum,
            state[0].data_type().clone(),
            state[0].data_type().clone(),
        ));
        merged.merge(&state, &[3, 2, 1, 0], 4).unwrap();
        assert!(merged.evaluate().unwrap().is_null(0));
        assert!(merged.evaluate().unwrap().is_null(2));
    }
}

#[test]
fn public_grouped_update_still_rejects_invalid_ids() {
    let mut sum = executor(expression(
        AggregateFunction::Sum,
        DataType::Int64,
        DataType::Int64,
    ));
    let input = batch(Arc::new(Int64Array::from(vec![1, 2])));
    assert!(sum.update(&input, &[0], 1).is_err());
    assert!(sum.update(&input, &[0, 1], 1).is_err());
    assert!(sum.update(&input, &[0, usize::MAX], 1).is_err());
}

#[test]
fn grouped_count_and_sum_cross_bitmap_words_without_reading_null_payloads() {
    use arrow::buffer::NullBuffer;

    let values = (0..151).map(|index| index as i64 - 70).collect::<Vec<_>>();
    let valid = (0..151).map(|index| index % 7 != 0).collect::<Vec<_>>();
    let array = Arc::new(Int64Array::new(
        values.clone().into(),
        Some(NullBuffer::from(valid.clone())),
    )) as ArrayRef;
    let input = batch(array.slice(3, 143));
    let ids = (0..143).map(|index| index % 5).collect::<Vec<_>>();
    let mut expected_sums = [0i64; 5];
    let mut expected_counts = [0i64; 5];
    for (index, &id) in ids.iter().enumerate() {
        if valid[index + 3] {
            expected_sums[id] = expected_sums[id].checked_add(values[index + 3]).unwrap();
            expected_counts[id] += 1;
        }
    }
    for (function, expected) in [
        (AggregateFunction::Sum, expected_sums),
        (AggregateFunction::Count, expected_counts),
    ] {
        let mut aggregate = executor(expression(function, DataType::Int64, DataType::Int64));
        aggregate.update(&input, &ids, 5).unwrap();
        assert_eq!(
            aggregate
                .evaluate()
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(expected.to_vec())
        );
    }
}

#[test]
fn typed_sum_checks_overflow_within_batches_and_partial_merges() {
    for array in [
        Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            None,
            Some(1),
            Some(-1),
        ])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(-1)])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(1)])),
    ] {
        let kind = array.data_type().clone();
        let expr = expression(AggregateFunction::Sum, kind.clone(), kind);
        let mut sum = executor(expr.clone());
        let input = batch(array.clone());
        assert!(sum.update(&input, &vec![0; input.num_rows()], 1).is_err());
        // Overflow must retain the successful prefix, never wrap or process the tail.
        assert_eq!(
            sum.evaluate().unwrap().to_data(),
            array.slice(0, 1).to_data()
        );
        let mut between = executor(expr.clone());
        between.update(&batch(array.slice(0, 1)), &[0], 1).unwrap();
        let tail = batch(array.slice(1, array.len() - 1));
        assert!(between.update(&tail, &vec![0; tail.num_rows()], 1).is_err());
        let mut merged = executor(expr.clone());
        let mut failed = false;
        for row in 0..array.len() {
            let mut partial = executor(expr.clone());
            partial
                .update(&batch(array.slice(row, 1)), &[0], 1)
                .unwrap();
            if merged.merge(&partial.state().unwrap(), &[0], 1).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
    }
}

#[test]
fn typed_float_sum_preserves_signed_zero_order_and_null_payloads() {
    use arrow::buffer::NullBuffer;
    let mut sum = executor(expression(
        AggregateFunction::Sum,
        DataType::Float64,
        DataType::Float64,
    ));
    let array: ArrayRef = Arc::new(Float64Array::new(
        vec![123.0, f64::NAN, -0.0, f64::NAN].into(),
        Some(NullBuffer::from(vec![true, false, true, false])),
    ));
    sum.update(&batch(array.slice(1, 3)), &[0, 0, 0], 2)
        .unwrap();
    let result = sum.evaluate().unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (-0.0f64).to_bits()
    );
    assert!(result.is_null(1));
    let mut ordered = executor(expression(
        AggregateFunction::Sum,
        DataType::Float64,
        DataType::Float64,
    ));
    for values in [vec![Some(1e16)], vec![Some(1.0), None, Some(-1e16)]] {
        let input = batch(Arc::new(Float64Array::from(values)));
        ordered
            .update(&input, &vec![0; input.num_rows()], 1)
            .unwrap();
    }
    assert_eq!(
        ordered
            .evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
}

#[test]
fn unsupported_sum_result_types_are_rejected_during_construction() {
    for output in [
        DataType::Int32,
        DataType::UInt32,
        DataType::Float32,
        DataType::Decimal128(20, 2),
        DataType::Decimal256(40, 4),
        DataType::Boolean,
        DataType::Utf8,
        DataType::Null,
    ] {
        let result = AggregateHarness::try_new(Arc::new(expression(
            AggregateFunction::Sum,
            DataType::Int64,
            output.clone(),
        )));
        match result {
            Err(roc::error::Error::InvalidPlan(message)) => {
                assert_eq!(message, format!("unsupported sum result type: {output}"));
            }
            Err(error) => panic!("expected invalid plan for {output}, got {error}"),
            Ok(_) => panic!("SUM result type {output} must be rejected before execution"),
        }
    }
}

#[test]
fn sum_result_type_validation_preserves_narrow_input_casts() {
    use arrow::array::{Float32Array, Int32Array, UInt32Array};
    for (input, output, expected) in [
        (
            Arc::new(Int32Array::from(vec![Some(3), None, Some(-3)])) as ArrayRef,
            DataType::Int64,
            Arc::new(Int64Array::from(vec![Some(0), None])) as ArrayRef,
        ),
        (
            Arc::new(UInt32Array::from(vec![Some(3), None, Some(4)])),
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![Some(7), None])),
        ),
        (
            Arc::new(Float32Array::from(vec![Some(1.5), None, Some(2.25)])),
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(3.75), None])),
        ),
    ] {
        let mut sum = executor(expression(
            AggregateFunction::Sum,
            input.data_type().clone(),
            output,
        ));
        sum.update(&batch(input), &[0, 1, 0], 2).unwrap();
        assert_eq!(sum.evaluate().unwrap().to_data(), expected.to_data());
    }
}

#[path="support/aggregate.rs"]
mod aggregate_support;
use aggregate_support::AggregateHarness;
