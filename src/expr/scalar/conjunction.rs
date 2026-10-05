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

use super::ExpressionResultType;
use super::ScalarExprRef;
use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::error::{Error, Result};
use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, new_empty_array},
    buffer::{BooleanBuffer, MutableBuffer, NullBuffer},
    datatypes::DataType,
    util::bit_util::apply_bitwise_binary_op,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}
/// A variadic Boolean AND or OR over its arguments.
#[derive(Clone, Debug)]
pub struct ConjunctionExpression {
    conjunction: Conjunction,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}

#[derive(Debug)]
pub struct AndExpressionEvaluation {
    arguments: Vec<ScalarExpressionEvaluation>,
}

#[derive(Debug)]
pub struct OrExpressionEvaluation {
    arguments: Vec<ScalarExpressionEvaluation>,
}

impl ConjunctionExpression {
    pub fn new(conjunction: Conjunction, arguments: Vec<ScalarExprRef>, nullable: bool) -> Self {
        Self {
            conjunction,
            arguments,
            result_type: ExpressionResultType {
                data_type: DataType::Boolean,
                nullable,
            },
        }
    }
    pub fn conjunction(&self) -> Conjunction {
        self.conjunction
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        let arguments = self
            .arguments
            .iter()
            .map(|argument| argument.to_evaluation())
            .collect::<Result<Vec<_>>>()?;
        Ok(match self.conjunction {
            Conjunction::And => {
                ScalarExpressionEvaluation::And(AndExpressionEvaluation { arguments })
            }
            Conjunction::Or => ScalarExpressionEvaluation::Or(OrExpressionEvaluation { arguments }),
        })
    }
}

impl AndExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        evaluate::<true>(&self.arguments, executor)
    }
}

impl OrExpressionEvaluation {
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ArrayRef> {
        evaluate::<false>(&self.arguments, executor)
    }
}

fn evaluate<const AND: bool>(
    arguments: &[ScalarExpressionEvaluation],
    executor: &ScalarExpressionExecutor,
) -> Result<ArrayRef> {
    let rows = executor.num_rows()?;
    if rows == 0 {
        return Ok(new_empty_array(&DataType::Boolean));
    }
    let mut col = ConjunctionBuffer::new::<AND>(rows);
    for argument in arguments {
        let value = argument.evaluate(executor)?;
        update::<AND>(&mut col, &value)?;
    }
    Ok(Arc::new(col.finish()))
}

/// Mutable workspace for one result, converted to an Arrow array only at the end.
/// Validity is allocated lazily and reused thereafter.
struct ConjunctionBuffer {
    values: MutableBuffer,
    validity: Option<MutableBuffer>,
    len: usize,
}

impl ConjunctionBuffer {
    fn new<const AND: bool>(len: usize) -> Self {
        let mut values = MutableBuffer::from_len_zeroed(len.div_ceil(64) * 8);
        if AND {
            values.as_slice_mut().fill(0xff);
        }
        Self {
            values,
            validity: None,
            len,
        }
    }

    fn finish(self) -> BooleanArray {
        let nulls = self
            .validity
            .map(|v| NullBuffer::new(BooleanBuffer::new(v.into(), 0, self.len)))
            .filter(|v| v.null_count() != 0);
        BooleanArray::new(BooleanBuffer::new(self.values.into(), 0, self.len), nulls)
    }
}

/// The concrete evaluation fixes AND/OR; representation dispatch stays inside this kernel.
fn update<const AND: bool>(col: &mut ConjunctionBuffer, input: &ArrayRef) -> Result<()> {
    if input.len() != col.len {
        return Err(Error::Execution("Boolean input lengths differ".into()));
    }
    let input = input
        .as_boolean_opt()
        .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
    update_bitmaps::<AND>(col, input.values(), input.nulls().map(NullBuffer::inner));
    Ok(())
}

fn update_bitmaps<const AND: bool>(
    col: &mut ConjunctionBuffer,
    right: &BooleanBuffer,
    right_validity: Option<&BooleanBuffer>,
) {
    // Compute validity from the original values, before replacing the values.
    // A value bit under NULL is arbitrary and cannot make the result known.
    match (&mut col.validity, right_validity) {
        (None, None) => {}
        (Some(left_valid), None) => {
            apply_bitwise_binary_op(
                left_valid.as_slice_mut(),
                0,
                right.values(),
                right.offset(),
                col.len,
                |valid, value| valid | if AND { !value } else { value },
            );
        }
        (None, Some(right_valid)) => {
            let validity = BooleanBuffer::from_bitwise_binary_op(
                col.values.as_slice(),
                0,
                right_valid.values(),
                right_valid.offset(),
                col.len,
                |value, valid| valid | if AND { !value } else { value },
            );
            let mut validity = validity.into_inner().into_mutable().unwrap();
            // Match values' whole-word storage for subsequent fused updates.
            validity.resize(col.values.len(), 0);
            col.validity = Some(validity);
        }
        (Some(left_valid), Some(right_valid)) => {
            // Arrow has no four-input, two-output in-place bitmap operation.
            // Use its slice-aware word iterators and merge both outputs in one
            // pass, retaining the two result allocations and no temporary bitmap.
            let right_values = right.bit_chunks().iter_padded();
            let right_valid = right_valid.bit_chunks().iter_padded();
            for (((a, av), b), bv) in col
                .values
                .typed_data_mut::<u64>()
                .iter_mut()
                .zip(left_valid.typed_data_mut::<u64>())
                .zip(right_values)
                .zip(right_valid)
            {
                let a_old = u64::from_le(*a);
                let av_old = u64::from_le(*av);
                *av = ((av_old & bv)
                    | (av_old & if AND { !a_old } else { a_old })
                    | (bv & if AND { !b } else { b }))
                .to_le();
                *a = (if AND { a_old & b } else { a_old | b }).to_le();
            }
            return;
        }
    }
    apply_bitwise_binary_op(
        col.values.as_slice_mut(),
        0,
        right.values(),
        right.offset(),
        col.len,
        |left, right| if AND { left & right } else { left | right },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Error,
        expr::scalar::{ConstantExpression, ReferenceExpression},
    };
    use arrow::array::Int64Array;

    #[test]
    fn streaming_updates_report_errors_in_argument_order_without_short_circuiting() {
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
        let executor = ScalarExpressionExecutor::new(&columns, 1);
        let missing =
            ReferenceExpression::new(99, ExpressionResultType::new(DataType::Boolean, false));
        for conjunction in [Conjunction::And, Conjunction::Or] {
            // An absorbing value must not skip a later argument's evaluation error.
            let absorbing = matches!(conjunction, Conjunction::Or);
            let expression = ConjunctionExpression::new(
                conjunction,
                vec![
                    ConstantExpression::boolean(Some(absorbing)).into_ref(),
                    missing.clone().into_ref(),
                ],
                false,
            )
            .to_evaluation()
            .unwrap();
            assert!(
                matches!(expression.evaluate(&executor), Err(Error::Execution(message))
                if message.contains("column index 99"))
            );
            // Process the earlier malformed result before evaluating the next argument.
            let expression = ConjunctionExpression::new(
                conjunction,
                vec![
                    ReferenceExpression::new(
                        0,
                        ExpressionResultType::new(DataType::Boolean, false),
                    )
                    .into_ref(),
                    missing.clone().into_ref(),
                ],
                false,
            )
            .to_evaluation()
            .unwrap();
            assert!(
                matches!(expression.evaluate(&executor), Err(Error::Execution(message))
                if message == "expected Boolean expression")
            );
            let empty = ScalarExpressionExecutor::new(&[], 0);
            assert!(expression.evaluate(&empty).unwrap().is_empty());
        }
    }
    fn sliced(offset: usize, len: usize, seed: usize, nullable: bool) -> ArrayRef {
        let size = offset + len + 1;
        let array: ArrayRef = Arc::new(BooleanArray::new(
            BooleanBuffer::from((0..size).map(|i| (i + seed) % 3 != 0).collect::<Vec<_>>()),
            nullable.then(|| {
                NullBuffer::from((0..size).map(|i| (i + seed) % 4 != 0).collect::<Vec<_>>())
            }),
        ));
        array.slice(offset, len)
    }

    fn check<const AND: bool>(inputs: Vec<ArrayRef>, len: usize) {
        let before = inputs.iter().map(|a| a.to_data()).collect::<Vec<_>>();
        let mut col = ConjunctionBuffer::new::<AND>(len);
        let values_ptr = col.values.as_ptr();
        let mut validity_ptr = None;
        let mut expected = BooleanArray::from(vec![AND; len]);
        for input in &inputs {
            let flat = input;
            expected = if AND {
                arrow::compute::and_kleene(&expected, flat.as_boolean()).unwrap()
            } else {
                arrow::compute::or_kleene(&expected, flat.as_boolean()).unwrap()
            };
            update::<AND>(&mut col, input).unwrap();
            assert_eq!(col.values.as_ptr(), values_ptr);
            if let Some(validity) = &col.validity {
                if let Some(ptr) = validity_ptr {
                    assert_eq!(validity.as_ptr(), ptr);
                } else {
                    validity_ptr = Some(validity.as_ptr());
                }
            }
            // Inspect without sharing the mutable buffers, so ownership stays unique.
            for i in 0..len {
                let valid = col
                    .validity
                    .as_ref()
                    .is_none_or(|v| arrow::util::bit_util::get_bit(v.as_slice(), i));
                let value = valid.then(|| arrow::util::bit_util::get_bit(col.values.as_slice(), i));
                let expected_value = expected.is_valid(i).then(|| expected.value(i));
                assert_eq!(value, expected_value);
            }
        }
        let result = col.finish();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(result.values().inner().as_ptr(), values_ptr);
        if let Some(nulls) = result.nulls() {
            assert_eq!(Some(nulls.buffer().as_ptr()), validity_ptr);
        }
        assert_eq!(
            inputs.iter().map(|a| a.to_data()).collect::<Vec<_>>(),
            before
        );
    }

    #[test]
    fn nullable_sliced_arrays_match_arrow_without_reallocating() {
        for len in [0, 1, 7, 8, 63, 64, 65, 127, 129, 257] {
            for offset in [0, 1, 7, 63, 65] {
                let inputs = vec![
                    sliced(offset, len, 0, true),
                    sliced(offset + 1, len, 1, false),
                    sliced(offset + 2, len, 2, true),
                    Arc::new(BooleanArray::from(vec![None; len])),
                    sliced(offset + 3, len, 3, false),
                ];
                check::<true>(inputs.clone(), len);
                check::<false>(inputs, len);
            }
        }
    }

    #[test]
    fn truth_table_ignores_values_under_null_and_accepts_all_valid_bitmaps() {
        let states = [Some(false), Some(true), None];
        for len in [9_usize, 65, 129] {
            for offset in [0, 1, 7, 8, 63, 64, 65] {
                for null_bits in 0..4 {
                    let input = |left: bool| -> ArrayRef {
                        let size = offset + len;
                        let rows = (0..size)
                            .map(|i| {
                                let row = i.saturating_sub(offset);
                                states[if left { row / 3 % 3 } else { row % 3 }]
                            })
                            .collect::<Vec<_>>();
                        let values = BooleanBuffer::from(
                            rows.iter()
                                .map(|v| v.unwrap_or(null_bits & if left { 1 } else { 2 } != 0))
                                .collect::<Vec<_>>(),
                        );
                        let nulls =
                            NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
                        Arc::new(BooleanArray::new(values, Some(nulls)).slice(offset, len))
                    };
                    // Explicit all-valid validity is semantically identical to no bitmap.
                    let all_valid: ArrayRef = Arc::new(BooleanArray::new(
                        BooleanBuffer::new_set(len),
                        Some(NullBuffer::new_valid(len)),
                    ));
                    let inputs = vec![
                        input(true),
                        input(false),
                        all_valid,
                        Arc::new(BooleanArray::from(vec![false; len])),
                    ];
                    check::<true>(inputs.clone(), len);
                    check::<false>(inputs, len);
                }
            }
        }
    }

    #[test]
    fn nonnullable_inputs_reuse_values_and_do_not_allocate_validity() {
        for and in [false, true] {
            let mut col = if and {
                ConjunctionBuffer::new::<true>(129)
            } else {
                ConjunctionBuffer::new::<false>(129)
            };
            let ptr = col.values.as_ptr();
            for input in [
                sliced(7, 129, 0, false),
                Arc::new(BooleanArray::from(vec![true; 129])),
                Arc::new(BooleanArray::from(vec![false; 129])),
                sliced(63, 129, 2, false),
            ] {
                if and {
                    update::<true>(&mut col, &input).unwrap();
                } else {
                    update::<false>(&mut col, &input).unwrap();
                }
                assert_eq!(col.values.as_ptr(), ptr);
                assert!(col.validity.is_none());
            }
            assert!(col.finish().nulls().is_none());
        }
    }

    #[test]
    fn invalid_inputs_fail_before_modifying_the_result() {
        let bad_inputs: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![true; 2])),
            Arc::new(arrow::array::Int64Array::from(vec![1; 3])),
        ];
        for input in bad_inputs {
            let mut col = ConjunctionBuffer::new::<true>(3);
            let ptr = col.values.as_ptr();
            let before = col.values.as_slice().to_vec();
            assert!(update::<true>(&mut col, &input).is_err());
            assert_eq!(col.values.as_ptr(), ptr);
            assert_eq!(col.values.as_slice(), before);
            assert!(col.validity.is_none());
        }
    }
}
