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

use arrow::{
    array::{Array, ArrayRef, Float64Array, UInt64Array},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::expr::{
    ExpressionResultType,
    agg::{AggregateExpression, AggregateFunction, executor::AggregateExpressionExecutor},
    scalar::ReferenceExpression,
};

fn avg_executor() -> AggregateExpressionExecutor {
    AggregateExpressionExecutor::try_new(Arc::new(AggregateExpression::new(
        AggregateFunction::Avg,
        vec![
            ReferenceExpression::new(0, ExpressionResultType::new(DataType::Float64, true))
                .into_ref(),
        ],
        DataType::Float64,
        true,
    )))
    .unwrap()
}

fn input(values: Vec<Option<f64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )])),
        vec![Arc::new(Float64Array::from(values))],
    )
    .unwrap()
}

fn assert_avg_state(
    executor: &AggregateExpressionExecutor,
    counts: &[Option<u64>],
    sums: &[Option<f64>],
    averages: &[Option<f64>],
) {
    assert_eq!(
        executor.state_types(),
        [DataType::UInt64, DataType::Float64]
    );
    let state = executor.state().unwrap();
    assert_eq!(state.len(), 2);
    assert_eq!(
        state[0].as_any().downcast_ref::<UInt64Array>().unwrap(),
        &UInt64Array::from(counts.to_vec())
    );
    assert_eq!(
        state[1].as_any().downcast_ref::<Float64Array>().unwrap(),
        &Float64Array::from(sums.to_vec())
    );
    assert_eq!(
        executor
            .evaluate()
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap(),
        &Float64Array::from(averages.to_vec())
    );
}

#[test]
fn avg_empty_and_all_null_states_remain_null_after_round_trip() {
    for values in [vec![], vec![None, None]] {
        let mut worker = avg_executor();
        // Global aggregation has one group even when no update is called.
        worker.resize(1);
        assert_avg_state(&worker, &[None], &[None], &[None]);
        worker
            .update(&input(values.clone()), &vec![0; values.len()], 1)
            .unwrap();
        assert_avg_state(&worker, &[None], &[None], &[None]);

        let mut merged = avg_executor();
        merged.merge(&worker.state().unwrap(), &[0], 1).unwrap();
        assert_avg_state(&merged, &[None], &[None], &[None]);
    }
}

#[test]
fn avg_grouped_states_distinguish_real_zero_from_empty_and_round_trip() {
    let mut worker = avg_executor();
    worker
        .update(
            &input(vec![Some(2.0), Some(9.0), Some(-2.0), None, Some(3.0)]),
            &[1, 2, 1, 3, 2],
            5,
        )
        .unwrap();
    assert_avg_state(
        &worker,
        &[None, Some(2), Some(2), None, None],
        &[None, Some(0.0), Some(12.0), None, None],
        &[None, Some(0.0), Some(6.0), None, None],
    );

    let mut merged = avg_executor();
    // Partial group order may differ from the destination group order.
    merged
        .merge(&worker.state().unwrap(), &[4, 3, 2, 1, 0], 5)
        .unwrap();
    assert_avg_state(
        &merged,
        &[None, None, Some(2), Some(2), None],
        &[None, None, Some(12.0), Some(0.0), None],
        &[None, None, Some(6.0), Some(0.0), None],
    );
}

#[test]
fn avg_partial_merge_ignores_payloads_under_null_counts_and_sums() {
    let mut merged = avg_executor();
    for (counts, counts_valid, sums, sums_valid) in [
        (
            vec![u64::MAX, 2, u64::MAX, 0],
            vec![false, true, false, true],
            vec![f64::NAN, 8.0, 777.0, f64::NAN],
            vec![false, true, false, false],
        ),
        (
            vec![2, u64::MAX, 2, u64::MAX],
            vec![true, false, true, false],
            vec![6.0, f64::NAN, 0.0, 123.0],
            vec![true, false, true, false],
        ),
        (
            vec![0, 1, u64::MAX, 0],
            vec![true, true, false, true],
            vec![456.0, 1.0, f64::NAN, 789.0],
            vec![false, true, false, false],
        ),
    ] {
        // Arrow NULLs can retain arbitrary buffer contents. Reading value(i)
        // without consulting validity would corrupt these merged averages.
        // Both scalar (0, NULL) and grouped (NULL, NULL) empty states are accepted.
        let counts = UInt64Array::new(counts.into(), Some(NullBuffer::from(counts_valid)));
        let sums = Float64Array::new(sums.into(), Some(NullBuffer::from(sums_valid)));
        let partial: Vec<ArrayRef> = vec![Arc::new(counts), Arc::new(sums)];
        merged.merge(&partial, &[0, 1, 2, 3], 4).unwrap();
    }
    assert_avg_state(
        &merged,
        &[Some(2), Some(3), Some(2), None],
        &[Some(6.0), Some(9.0), Some(0.0), None],
        &[Some(3.0), Some(3.0), Some(0.0), None],
    );
}
