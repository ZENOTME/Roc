// Copyright 2026 The Roc Contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::expr::{
    ExpressionResultType,
    agg::{AggregateExpression, AggregateFunction, executor::AggregateExpressionExecutor},
    scalar::{ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression},
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

fn executor(expression: AggregateExpression) -> AggregateExpressionExecutor {
    AggregateExpressionExecutor::try_new(Arc::new(expression)).unwrap()
}

#[test]
fn global_reduction_matches_grouped_path_across_batches_and_nulls() {
    let batches = [
        batch(Arc::new(Int64Array::from(vec![
            None,
            Some(4),
            Some(-4),
            None,
        ]))),
        batch(Arc::new(Int64Array::from(vec![Some(9), Some(9), None]))),
        batch(Arc::new(Int64Array::from(vec![None, None]))),
        batch(Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))),
    ];
    for (function, output) in [
        (AggregateFunction::Count, DataType::Int64),
        (AggregateFunction::Sum, DataType::Int64),
        (AggregateFunction::Avg, DataType::Float64),
        (AggregateFunction::Min, DataType::Int64),
        (AggregateFunction::Max, DataType::Int64),
    ] {
        let expr = expression(function, DataType::Int64, output);
        let mut single = executor(expr.clone());
        let mut grouped = executor(expr);
        for batch in &batches {
            single.update_single(batch).unwrap();
            grouped
                .update(batch, &vec![0; batch.num_rows()], 1)
                .unwrap();
            assert_eq!(
                single.evaluate().unwrap().to_data(),
                grouped.evaluate().unwrap().to_data()
            );
            for (left, right) in single.state().unwrap().iter().zip(grouped.state().unwrap()) {
                assert_eq!(left.to_data(), right.to_data());
            }
        }
    }
    let mut distinct = executor(
        expression(AggregateFunction::Count, DataType::Int64, DataType::Int64).with_distinct(),
    );
    for batch in &batches {
        distinct.update_single(batch).unwrap();
    }
    assert_eq!(
        distinct
            .evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[test]
fn global_integer_sums_check_overflow_within_between_and_across_partial_states() {
    for array in [
        Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(1)])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(-1)])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(1)])),
        // A representable final result cannot hide an overflowing prefix.
        Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(1), Some(-1)])),
    ] {
        let kind = array.data_type().clone();
        let expr = expression(AggregateFunction::Sum, kind.clone(), kind);
        let mut single = executor(expr.clone());
        assert!(single.update_single(&batch(array.clone())).is_err());
        let mut grouped = executor(expr.clone());
        assert!(
            grouped
                .update(&batch(array.clone()), &vec![0; array.len()], 1)
                .is_err()
        );
        let mut between = executor(expr.clone());
        between.update_single(&batch(array.slice(0, 1))).unwrap();
        assert!(
            between
                .update_single(&batch(array.slice(1, array.len() - 1)))
                .is_err()
        );
        let mut merged = executor(expr.clone());
        let mut failed = false;
        for row in 0..array.len() {
            let mut partial = executor(expr.clone());
            partial.update_single(&batch(array.slice(row, 1))).unwrap();
            if merged.merge(&partial.state().unwrap(), &[0], 1).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
    }
}

#[test]
fn global_empty_states_and_checked_counts_are_preserved() {
    let nulls = batch(Arc::new(Int64Array::from(vec![None, None])));
    let mut sum = executor(expression(
        AggregateFunction::Sum,
        DataType::Int64,
        DataType::Int64,
    ));
    sum.update_single(&nulls).unwrap();
    assert!(sum.evaluate().unwrap().is_null(0));
    let mut avg = executor(expression(
        AggregateFunction::Avg,
        DataType::Int64,
        DataType::Float64,
    ));
    avg.update_single(&nulls).unwrap();
    assert!(avg.evaluate().unwrap().is_null(0));
    let state = avg.state().unwrap();
    assert_eq!(
        state[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        state[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
    let mut count = executor(expression(
        AggregateFunction::Count,
        DataType::Int64,
        DataType::Int64,
    ));
    count
        .merge(&[Arc::new(Int64Array::from(vec![i64::MAX]))], &[0], 1)
        .unwrap();
    count.update_single(&nulls).unwrap();
    assert!(
        count
            .update_single(&batch(Arc::new(Int64Array::from(vec![1]))))
            .is_err()
    );
    let mut count_star = executor(AggregateExpression::new(
        AggregateFunction::Count,
        None,
        DataType::Int64,
        false,
    ));
    count_star.update_single(&nulls).unwrap();
    assert_eq!(
        count_star
            .evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    let mut overflowing_avg = executor(expression(
        AggregateFunction::Avg,
        DataType::Int64,
        DataType::Float64,
    ));
    overflowing_avg
        .merge(
            &[
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                Arc::new(Float64Array::from(vec![0.0])),
            ],
            &[0],
            1,
        )
        .unwrap();
    overflowing_avg.update_single(&nulls).unwrap();
    assert!(
        overflowing_avg
            .update_single(&batch(Arc::new(Int64Array::from(vec![1]))))
            .is_err()
    );
}

#[test]
fn global_filter_excludes_errors_before_arguments_are_evaluated() {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("divisor", DataType::Int64, false),
            Field::new("keep", DataType::Boolean, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0, 2, 0, 4])),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(true),
            ])),
        ],
    )
    .unwrap();
    let argument = FunctionExpression::binary(
        FunctionKind::Divide,
        ConstantExpression::int64(Some(8)).into_ref(),
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, false)).into_ref(),
        DataType::Int64,
        false,
    )
    .into_ref();
    let mut sum = executor(
        AggregateExpression::new(
            AggregateFunction::Sum,
            Some(argument),
            DataType::Int64,
            true,
        )
        .with_filter(
            ReferenceExpression::new(1, ExpressionResultType::new(DataType::Boolean, true))
                .into_ref(),
        ),
    );
    sum.update_single(&input).unwrap();
    assert_eq!(
        sum.evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6
    );
}

fn assert_float_aggregate_state_eq(
    single: &AggregateExpressionExecutor,
    grouped: &AggregateExpressionExecutor,
) {
    let bit_values = |values: ArrayRef| {
        values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|value| value.map(f64::to_bits))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        bit_values(single.evaluate().unwrap()),
        bit_values(grouped.evaluate().unwrap())
    );
    for (single, grouped) in single
        .state()
        .unwrap()
        .into_iter()
        .zip(grouped.state().unwrap())
    {
        if single.data_type() == &DataType::Float64 {
            assert_eq!(bit_values(single), bit_values(grouped));
        } else {
            assert_eq!(single.to_data(), grouped.to_data());
        }
    }
}

#[test]
fn global_float_sum_keeps_first_negative_zero_and_ignores_null_payloads() {
    use arrow::buffer::NullBuffer;

    let expr = expression(AggregateFunction::Sum, DataType::Float64, DataType::Float64);
    let mut single = executor(expr.clone());
    let mut grouped = executor(expr);
    let nulls = batch(Arc::new(Float64Array::from(vec![None, None])));
    single.update_single(&nulls).unwrap();
    grouped.update(&nulls, &[0, 0], 1).unwrap();
    assert!(single.evaluate().unwrap().is_null(0));
    assert_float_aggregate_state_eq(&single, &grouped);

    // NULL payloads contain NaNs and may not participate in the reduction.
    let array: ArrayRef = Arc::new(Float64Array::new(
        vec![123.0, f64::NAN, -0.0, f64::NAN].into(),
        Some(NullBuffer::from(vec![true, false, true, false])),
    ));
    let input = batch(array.slice(1, 3));
    single.update_single(&input).unwrap();
    grouped.update(&input, &[0, 0, 0], 1).unwrap();
    assert_float_aggregate_state_eq(&single, &grouped);
    assert_eq!(
        single
            .evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        (-0.0f64).to_bits()
    );

    for input in [nulls, input.slice(0, 0)] {
        single.update_single(&input).unwrap();
        grouped
            .update(&input, &vec![0; input.num_rows()], 1)
            .unwrap();
        assert_float_aggregate_state_eq(&single, &grouped);
    }
}

#[test]
fn global_float_sum_and_avg_keep_grouped_order_across_batches_and_merge() {
    for function in [AggregateFunction::Sum, AggregateFunction::Avg] {
        for chunks in [
            vec![vec![Some(1e16), Some(1.0), Some(-1e16)]],
            vec![vec![Some(1e16)], vec![Some(1.0), None, Some(-1e16)]],
            vec![vec![Some(-0.0)], vec![None, Some(-0.0)]],
            vec![vec![
                Some(1e16),
                Some(1.0),
                Some(-1e16),
                Some(2.0),
                Some(1e16),
                None,
                Some(1.0),
                Some(-1e16),
                Some(4.0),
            ]],
        ] {
            let expr = expression(function, DataType::Float64, DataType::Float64);
            let mut single = executor(expr.clone());
            let mut grouped = executor(expr.clone());
            for chunk in chunks {
                let input = batch(Arc::new(Float64Array::from(chunk)));
                single.update_single(&input).unwrap();
                grouped
                    .update(&input, &vec![0; input.num_rows()], 1)
                    .unwrap();
                assert_float_aggregate_state_eq(&single, &grouped);
            }
            let mut merged_single = executor(expr.clone());
            let mut merged_grouped = executor(expr);
            merged_single
                .merge(&single.state().unwrap(), &[0], 1)
                .unwrap();
            merged_grouped
                .merge(&grouped.state().unwrap(), &[0], 1)
                .unwrap();
            let tail = batch(Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(-1e16),
            ])));
            merged_single.update_single(&tail).unwrap();
            merged_grouped.update(&tail, &[0, 0, 0], 1).unwrap();
            assert_float_aggregate_state_eq(&merged_single, &merged_grouped);
        }
    }
}

#[test]
fn global_sum_preserves_successful_prefix_after_overflow() {
    let mut sum = executor(expression(
        AggregateFunction::Sum,
        DataType::Int64,
        DataType::Int64,
    ));
    assert!(
        sum.update_single(&batch(Arc::new(Int64Array::from(vec![i64::MAX, 1, -1]))))
            .is_err()
    );
    assert_eq!(
        sum.evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MAX
    );
    sum.update_single(&batch(Arc::new(Int64Array::from(vec![-1]))))
        .unwrap();
    assert_eq!(
        sum.evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MAX - 1
    );
}

#[test]
fn global_sum_matches_grouped_for_sliced_dense_sparse_and_all_null_inputs() {
    use arrow::buffer::NullBuffer;
    for (stride, invert) in [(1, false), (2, false), (7, false), (7, true), (1000, false)] {
        let values = (0..151).map(|i| i as i64 - 70).collect::<Vec<_>>();
        let valid = (0..151)
            .map(|i| (i % stride == 0) ^ invert)
            .collect::<Vec<_>>();
        let array = Arc::new(Int64Array::new(
            values.into(),
            Some(NullBuffer::from(valid)),
        )) as ArrayRef;
        let input = batch(array.slice(3, 143));
        let expr = expression(AggregateFunction::Sum, DataType::Int64, DataType::Int64);
        let mut single = executor(expr.clone());
        let mut grouped = executor(expr);
        for _ in 0..2 {
            single.update_single(&input).unwrap();
            grouped
                .update(&input, &vec![0; input.num_rows()], 1)
                .unwrap();
            assert_eq!(
                single.evaluate().unwrap().to_data(),
                grouped.evaluate().unwrap().to_data()
            );
        }
    }
}
