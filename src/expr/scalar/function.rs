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

use super::{ExpressionResultType, ScalarExprRef};
use crate::arrow::datatypes::DataType;
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
#[derive(Clone, Debug)]
pub struct FunctionExpression {
    function: FunctionKind,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
}
impl FunctionExpression {
    pub(crate) fn build_suboperator(
        &self,
        builder: &mut crate::suboperator::SuboperatorScopeBuilder,
    ) -> crate::error::Result<crate::suboperator::VariableId> {
        let args = self
            .arguments()
            .iter()
            .map(|a| a.build_suboperator(builder))
            .collect::<crate::error::Result<Vec<_>>>()?;
        match args.as_slice() {
            [a] => builder.unary(self.function(), *a),
            [a, b] => builder.binary(self.function(), *a, *b),
            _ => Err(crate::error::Error::InvalidPlan(
                "invalid function arity".into(),
            )),
        }
    }

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
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::ArrayRef;
    use crate::arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use crate::expr::scalar::{
        CaseExpression, CoalesceExpression, ConstantExpression, ReferenceExpression,
    };
    use crate::suboperator::test_support::*;
    use std::sync::Arc;

    fn ints(array: &ArrayRef) -> Vec<i64> {
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }
    fn compare_scalar_with_broadcast(
        array: ArrayRef,
        scalar: ArrayRef,
        functions: &[FunctionKind],
    ) {
        let data_type = array.data_type().clone();
        let executor = TestInput::new(std::slice::from_ref(&array), array.len());
        let broadcast = crate::arrow::compute::take(
            scalar.as_ref(),
            &crate::arrow::array::UInt64Array::from(vec![0; array.len()]),
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
                        .prepare_test()
                        .unwrap();
                let baseline =
                    |left: &ArrayRef, right: &ArrayRef| -> crate::error::Result<ArrayRef> {
                        use ::arrow::compute::kernels::{cmp, numeric};
                        use FunctionKind::*;
                        Ok(match function {
                            Add => numeric::add(left, right)?,
                            Subtract => numeric::sub(left, right)?,
                            Multiply => numeric::mul(left, right)?,
                            Divide => numeric::div(left, right)?,
                            Remainder => numeric::rem(left, right)?,
                            Equal => Arc::new(cmp::eq(left, right)?),
                            NotEqual => Arc::new(cmp::neq(left, right)?),
                            LessThan => Arc::new(cmp::lt(left, right)?),
                            LessThanOrEqual => Arc::new(cmp::lt_eq(left, right)?),
                            GreaterThan => Arc::new(cmp::gt(left, right)?),
                            GreaterThanOrEqual => Arc::new(cmp::gt_eq(left, right)?),
                            IsDistinctFrom => Arc::new(cmp::distinct(left, right)?),
                            IsNotDistinctFrom => Arc::new(cmp::not_distinct(left, right)?),
                            _ => unreachable!(),
                        })
                    };
                let expected = if left_scalar {
                    baseline(&broadcast, &array)
                } else {
                    baseline(&array, &broadcast)
                };
                let actual = evaluation.evaluate(&executor);
                let context = format!(
                    "{data_type:?} {function:?} left_scalar={left_scalar} scalar={scalar:?}"
                );
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual.to_data(), expected.to_data(), "{context}")
                    }
                    (Err(actual), Err(expected)) => {
                        assert!(
                            matches!(actual, crate::error::Error::Arrow(_)),
                            "{context}: {actual:?}; oracle: {expected:?}"
                        )
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
        let evaluation = dangerous.prepare_test().unwrap();
        let empty = TestInput::new(&[], 0);
        assert!(evaluation.evaluate(&empty).unwrap().is_empty());
        let input = TestInput::new(&[], 4);
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
        .prepare_test()
        .unwrap();
        let coalesce = CoalesceExpression::new(vec![add(3, 4), dangerous], DataType::Int64, false)
            .prepare_test()
            .unwrap();
        let first = case.evaluate(&input).unwrap();
        assert_eq!(ints(&first), vec![5; 4]);
        assert_eq!(ints(&coalesce.evaluate(&input).unwrap()), vec![7; 4]);
        assert_eq!(
            ints(&case.evaluate(&TestInput::new(&[], 2)).unwrap()),
            vec![5; 2]
        );
        assert_eq!(ints(&first), vec![5; 4]);
    }
}
