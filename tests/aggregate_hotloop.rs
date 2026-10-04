// Copyright 2026 The Roc Contributors
// SPDX-License-Identifier: Apache-2.0

use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::expr::{
    ExpressionResultType,
    agg::{AggregateExpression, AggregateFunction, executor::AggregateExpressionExecutor},
    scalar::ReferenceExpression,
};
use std::sync::Arc;

fn execute(
    function: AggregateFunction,
    values: ArrayRef,
    ids: &[usize],
    groups: usize,
) -> ArrayRef {
    let data_type = values.data_type().clone();
    let output_type = if function == AggregateFunction::Count {
        DataType::Int64
    } else {
        data_type.clone()
    };
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            true,
        )])),
        vec![values],
    )
    .unwrap();
    let mut executor = AggregateExpressionExecutor::try_new(Arc::new(AggregateExpression::new(
        function,
        Some(ReferenceExpression::new(0, ExpressionResultType::new(data_type, true)).into_ref()),
        output_type,
        true,
    )))
    .unwrap();
    executor.update(&input, ids, groups).unwrap();
    executor.evaluate().unwrap()
}

#[test]
fn dense_and_sparse_null_masks_match_reference_at_word_and_slice_boundaries() {
    for offset in 0..9 {
        for len in [0, 1, 2, 63, 64, 65, 127, 128, 129] {
            for pattern in 0..4 {
                let valid = (0..offset + len + 3)
                    .map(|index| match pattern {
                        0 => true,
                        1 => index % 17 != 0,
                        2 => index % 17 == 0,
                        _ => false,
                    })
                    .collect::<Vec<_>>();
                let payload = (0..valid.len())
                    .map(|index| {
                        if valid[index] {
                            index as i64 - 73
                        } else {
                            i64::MAX
                        }
                    })
                    .collect::<Vec<_>>();
                let array = Arc::new(Int64Array::new(
                    payload.clone().into(),
                    Some(NullBuffer::from(valid.clone())),
                )) as ArrayRef;
                let array = array.slice(offset, len);
                let ids = (0..len).map(|index| index % 7).collect::<Vec<_>>();
                let mut sums = [None::<i64>; 7];
                let mut counts = [0i64; 7];
                for (index, &id) in ids.iter().enumerate() {
                    if valid[offset + index] {
                        sums[id] =
                            Some(sums[id].unwrap_or(0).wrapping_add(payload[offset + index]));
                        counts[id] += 1;
                    }
                }
                for (function, expected) in [
                    (AggregateFunction::Sum, Int64Array::from(sums.to_vec())),
                    (AggregateFunction::Count, Int64Array::from(counts.to_vec())),
                ] {
                    let result = execute(function, array.clone(), &ids, 7);
                    assert_eq!(
                        result.as_any().downcast_ref::<Int64Array>().unwrap(),
                        &expected,
                        "offset={offset} len={len} pattern={pattern} aggregate={function:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn dense_float_masks_ignore_nan_payloads_and_preserve_first_signed_zero() {
    let len = 131;
    let valid = (0..len + 4)
        .map(|index| index % 19 != 0)
        .collect::<Vec<_>>();
    let payload = (0..valid.len())
        .map(|index| if valid[index] { -0.0 } else { f64::NAN })
        .collect::<Vec<_>>();
    let array = Arc::new(Float64Array::new(
        payload.into(),
        Some(NullBuffer::from(valid.clone())),
    )) as ArrayRef;
    let result = execute(
        AggregateFunction::Sum,
        array.slice(3, len),
        &(0..len).collect::<Vec<_>>(),
        len,
    );
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    for index in 0..len {
        if valid[index + 3] {
            assert!(result.is_valid(index));
            assert_eq!(result.value(index).to_bits(), (-0.0f64).to_bits());
        } else {
            assert!(result.is_null(index));
        }
    }
}
