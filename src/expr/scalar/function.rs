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

use super::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use super::kernels::EvalFn;
use super::{ColumnValue, ScalarExprRef};
use super::{ExpressionResultType, kernels};
use crate::error::Result;
use arrow::{array::new_empty_array, datatypes::DataType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionKind {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Negate,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

/// A unary or binary built-in call, such as an arithmetic operator, a
/// comparison, or a NULL test.
#[derive(Clone, Debug)]
pub struct FunctionExpression {
    function: FunctionKind,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}

impl FunctionExpression {
    pub fn unary(
        function: FunctionKind,
        argument: ScalarExprRef,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            function,
            arguments: vec![argument],
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
        }
    }

    pub fn binary(
        function: FunctionKind,
        left: ScalarExprRef,
        right: ScalarExprRef,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            function,
            arguments: vec![left, right],
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
        }
    }

    pub fn function(&self) -> FunctionKind {
        self.function
    }

    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }

    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
    }

    pub fn to_evaluation(&self) -> Result<ScalarExpressionEvaluation> {
        match &self.arguments[..] {
            [argument] => Ok(UnaryFunctionExpressionEvaluation::try_new(self, argument)?.into()),
            [left, right] => {
                Ok(BinaryFunctionExpressionEvaluation::try_new(self, left, right)?.into())
            }
            _ => unreachable!("function expressions are built as unary or binary"),
        }
    }
}

#[derive(Debug)]
pub struct UnaryFunctionExpressionEvaluation {
    data_type: DataType,
    argument: Box<ScalarExpressionEvaluation>,
    eval_fn: EvalFn,
}
impl UnaryFunctionExpressionEvaluation {
    pub(super) fn try_new(
        expression: &FunctionExpression,
        argument: &ScalarExprRef,
    ) -> Result<Self> {
        let argument_evaluation = argument.to_evaluation()?;
        // Kernels are keyed by the declared operand type; the result type only
        // describes the output of e.g. a comparison.
        let eval_fn = kernels::bind_unary(expression.function, &argument.result_type().data_type)?;
        Ok(Self {
            data_type: expression.result_type.data_type.clone(),
            argument: Box::new(argument_evaluation),
            eval_fn,
        })
    }
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ColumnValue> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let argument = self.argument.evaluate(executor)?;
        self.eval(executor, &[argument])
    }
    fn eval(
        &self,
        executor: &ScalarExpressionExecutor,
        input: &[ColumnValue],
    ) -> Result<ColumnValue> {
        Ok(if executor.num_rows()? == 0 {
            ColumnValue::Array(new_empty_array(&self.data_type))
        } else {
            (self.eval_fn)(input)?
        })
    }
}

#[derive(Debug)]
pub struct BinaryFunctionExpressionEvaluation {
    data_type: DataType,
    left: Box<ScalarExpressionEvaluation>,
    right: Box<ScalarExpressionEvaluation>,
    eval_fn: EvalFn,
}

impl BinaryFunctionExpressionEvaluation {
    pub(super) fn try_new(
        expression: &FunctionExpression,
        left: &ScalarExprRef,
        right: &ScalarExprRef,
    ) -> Result<Self> {
        let left_evaluation = left.to_evaluation()?;
        let right_evaluation = right.to_evaluation()?;
        // Kernels are keyed by the declared operand type, which for arithmetic
        // is also the result type but for comparisons is not.
        let eval_fn = kernels::bind_binary(expression.function, &left.result_type().data_type)?;
        Ok(Self {
            data_type: expression.result_type.data_type.clone(),
            left: Box::new(left_evaluation),
            right: Box::new(right_evaluation),
            eval_fn,
        })
    }
    pub fn evaluate(&self, executor: &ScalarExpressionExecutor) -> Result<ColumnValue> {
        if executor.num_rows()? == 0 {
            return self.eval(executor, &[]);
        }
        let left = self.left.evaluate(executor)?;
        let right = self.right.evaluate(executor)?;
        self.eval(executor, &[left, right])
    }
    fn eval(
        &self,
        executor: &ScalarExpressionExecutor,
        input: &[ColumnValue],
    ) -> Result<ColumnValue> {
        Ok(if executor.num_rows()? == 0 {
            ColumnValue::Array(new_empty_array(&self.data_type))
        } else {
            (self.eval_fn)(input)?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::scalar::{
        CaseExpression, CoalesceExpression, ConstantExpression, ReferenceExpression,
    };
    use arrow::{
        array::{
            Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    fn ints(value: &ColumnValue) -> Vec<i64> {
        let len = match value {
            ColumnValue::Array(a) => a.len(),
            ColumnValue::Scalar(_) => 1,
        };
        let array = value.clone().into_array(len).unwrap();
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }
    #[test]
    fn eval_consumes_supplied_arrays_without_executing_children() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 0, 4]))],
        )
        .unwrap();
        let input = batch.slice(0, 3);
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        // This child cannot read its column. Supplied-input eval must use the
        // two argument arrays instead of recursively touching the bound child.
        let missing =
            ReferenceExpression::new(99, ExpressionResultType::new(DataType::Int64, false))
                .into_ref();
        let ScalarExpressionEvaluation::BinaryFunction(evaluation) = FunctionExpression::binary(
            FunctionKind::Add,
            missing,
            ConstantExpression::int64(Some(1000)).into_ref(),
            DataType::Int64,
            false,
        )
        .to_evaluation()
        .unwrap() else {
            unreachable!()
        };
        let left: ArrayRef = Arc::new(Int64Array::from(vec![30, 10, 30]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 1]));
        let input = [ColumnValue::Array(left), ColumnValue::Array(right)];
        let output = evaluation.eval(&executor, &input).unwrap();

        assert_eq!(ints(&output), vec![32, 12, 31]);
        assert!(evaluation.eval(&executor, &input[..1]).is_err());
        assert_eq!(ints(&output), vec![32, 12, 31]);
        // Zero rows skip arguments and kernels, even with invalid children.
        let empty = ScalarExpressionExecutor::new(&[], 0);
        assert!(
            evaluation
                .eval(&empty, &[])
                .unwrap()
                .into_array(0)
                .unwrap()
                .is_empty()
        );
        assert!(evaluation.evaluate(&executor).is_err());
    }

    fn compare_scalar_with_broadcast(
        array: ArrayRef,
        scalar: ArrayRef,
        functions: &[FunctionKind],
    ) {
        let data_type = array.data_type().clone();
        let executor = ScalarExpressionExecutor::new(std::slice::from_ref(&array), array.len());
        let broadcast = arrow::compute::take(
            scalar.as_ref(),
            &arrow::array::UInt64Array::from(vec![0; array.len()]),
            None,
        )
        .unwrap();
        for &function in functions {
            let output_type = match function {
                FunctionKind::Add
                | FunctionKind::Subtract
                | FunctionKind::Multiply
                | FunctionKind::Divide
                | FunctionKind::Remainder => data_type.clone(),
                _ => DataType::Boolean,
            };
            for left_scalar in [false, true] {
                let reference =
                    ReferenceExpression::new(0, ExpressionResultType::new(data_type.clone(), true))
                        .into_ref();
                let constant = ConstantExpression::try_new(scalar.clone())
                    .unwrap()
                    .into_ref();
                let (left, right) = if left_scalar {
                    (constant, reference)
                } else {
                    (reference, constant)
                };
                let evaluation =
                    FunctionExpression::binary(function, left, right, output_type.clone(), true)
                        .to_evaluation()
                        .unwrap();
                let baseline = kernels::bind_binary(function, &data_type).unwrap();
                let expected = if left_scalar {
                    baseline(&[
                        ColumnValue::Array(broadcast.clone()),
                        ColumnValue::Array(array.clone()),
                    ])
                } else {
                    baseline(&[
                        ColumnValue::Array(array.clone()),
                        ColumnValue::Array(broadcast.clone()),
                    ])
                };
                let actual = evaluation.evaluate(&executor);
                let context = format!(
                    "{data_type:?} {function:?} left_scalar={left_scalar} scalar={scalar:?}"
                );
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(
                            actual.into_array(array.len()).unwrap().to_data(),
                            expected.into_array(array.len()).unwrap().to_data(),
                            "{context}"
                        )
                    }
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string(), "{context}")
                    }
                    (actual, expected) => {
                        panic!("{context}: actual={actual:?}, expected={expected:?}")
                    }
                }
            }
        }
    }

    const COMPARISONS: &[FunctionKind] = &[
        FunctionKind::Equal,
        FunctionKind::NotEqual,
        FunctionKind::LessThan,
        FunctionKind::LessThanOrEqual,
        FunctionKind::GreaterThan,
        FunctionKind::GreaterThanOrEqual,
        FunctionKind::IsDistinctFrom,
        FunctionKind::IsNotDistinctFrom,
    ];

    #[test]
    fn scalar_numeric_kernels_match_broadcast_for_all_types_and_operand_orders() {
        let mut functions = vec![
            FunctionKind::Add,
            FunctionKind::Subtract,
            FunctionKind::Multiply,
            FunctionKind::Divide,
            FunctionKind::Remainder,
        ];
        functions.extend_from_slice(COMPARISONS);
        macro_rules! check {
            ($array:ty, $native:ty) => {{
                let values: ArrayRef = Arc::new(<$array>::from(vec![
                    Some(2 as $native),
                    None,
                    Some(0 as $native),
                    Some(1 as $native),
                    Some(<$native>::MIN),
                    Some(<$native>::MAX),
                ]));
                for scalar_index in 0..values.len() {
                    for input in [
                        values.clone(),
                        values.slice(1, values.len() - 1),
                        values.slice(0, 0),
                    ] {
                        compare_scalar_with_broadcast(
                            input,
                            values.slice(scalar_index, 1),
                            &functions,
                        );
                    }
                }
            }};
        }
        check!(Int8Array, i8);
        check!(Int16Array, i16);
        check!(Int32Array, i32);
        check!(Int64Array, i64);
        check!(UInt8Array, u8);
        check!(UInt16Array, u16);
        check!(UInt32Array, u32);
        check!(UInt64Array, u64);
        check!(Float32Array, f32);
        check!(Float64Array, f64);
    }

    #[test]
    fn scalar_float_special_values_and_arrow_comparison_fallback_match_broadcast() {
        macro_rules! check_float {
            ($array:ty, $native:ty) => {{
                let values: ArrayRef = Arc::new(<$array>::from(vec![
                    Some(-0.0),
                    Some(0.0),
                    None,
                    Some(<$native>::NAN),
                    Some(<$native>::INFINITY),
                    Some(<$native>::NEG_INFINITY),
                    Some(-1.0),
                ]));
                let mut functions = COMPARISONS.to_vec();
                functions.extend([
                    FunctionKind::Add,
                    FunctionKind::Subtract,
                    FunctionKind::Multiply,
                    FunctionKind::Divide,
                    FunctionKind::Remainder,
                ]);
                for i in 0..values.len() {
                    compare_scalar_with_broadcast(values.clone(), values.slice(i, 1), &functions);
                }
            }};
        }
        check_float!(Float32Array, f32);
        check_float!(Float64Array, f64);
        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("z")]));
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        for values in [strings, booleans] {
            for i in 0..values.len() {
                compare_scalar_with_broadcast(values.clone(), values.slice(i, 1), COMPARISONS);
            }
        }
    }

    #[test]
    fn scalar_integer_failures_ignore_null_rows_and_preserve_operand_order() {
        // Explicitly exercise the MIN / -1 overflow and signed remainder rule.
        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(i64::MIN), Some(2)]));
        let minus_one: ArrayRef = Arc::new(Int64Array::from(vec![-1]));
        compare_scalar_with_broadcast(
            values,
            minus_one,
            &[
                FunctionKind::Divide,
                FunctionKind::Remainder,
                FunctionKind::Subtract,
            ],
        );
        // A zero divisor is harmless when every input row is NULL.
        let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let zero: ArrayRef = Arc::new(Int64Array::from(vec![0]));
        compare_scalar_with_broadcast(
            nulls,
            zero,
            &[FunctionKind::Divide, FunctionKind::Remainder],
        );
    }

    #[test]
    fn constant_pair_execution_stays_lazy_and_repeated_outputs_remain_owned() {
        let add = |a, b| {
            FunctionExpression::binary(
                FunctionKind::Add,
                ConstantExpression::int64(Some(a)).into_ref(),
                ConstantExpression::int64(Some(b)).into_ref(),
                DataType::Int64,
                false,
            )
            .into_ref()
        };
        let dangerous = add(i64::MAX, 1);
        let evaluation = dangerous.to_evaluation().unwrap();
        let empty = ScalarExpressionExecutor::new(&[], 0);
        assert!(
            evaluation
                .evaluate(&empty)
                .unwrap()
                .into_array(0)
                .unwrap()
                .is_empty()
        );
        let input = ScalarExpressionExecutor::new(&[], 4);
        assert!(evaluation.evaluate(&input).is_err());

        let case = CaseExpression::new(
            vec![(
                ConstantExpression::boolean(Some(false)).into_ref(),
                dangerous.clone(),
            )],
            add(2, 3),
            DataType::Int64,
            false,
        )
        .to_evaluation()
        .unwrap();
        let coalesce = CoalesceExpression::new(vec![add(3, 4), dangerous], DataType::Int64, false)
            .to_evaluation()
            .unwrap();
        let first = case.evaluate(&input).unwrap();
        assert_eq!(ints(&first), vec![5; 4]);
        assert_eq!(ints(&coalesce.evaluate(&input).unwrap()), vec![7; 4]);
        assert_eq!(
            ints(
                &case
                    .evaluate(&ScalarExpressionExecutor::new(&[], 2))
                    .unwrap()
            ),
            vec![5; 2]
        );
        assert_eq!(ints(&first), vec![5; 4]);
    }
}
