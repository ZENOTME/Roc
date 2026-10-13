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

use super::{ScalarExprRef, build_input};
use crate::arrow::datatypes::DataType;
use crate::column::ColumnId;
use crate::error::{Error, Result};
use crate::kernel::{
    ColumnVariableBinding, KernelBuilder, KernelError, PendingErrors, ScalarFunction, VariableId,
    scalar,
};

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

/// A built-in function. Binary operands must have the same type; arithmetic
/// returns that type and every other function returns Boolean.
#[derive(Clone, Debug)]
pub struct FunctionExpression {
    function: FunctionKind,
    arguments: Vec<ScalarExprRef>,
    output: ColumnId,
}

impl FunctionExpression {
    pub fn unary(function: FunctionKind, argument: ScalarExprRef, output: ColumnId) -> Self {
        Self {
            function,
            arguments: vec![argument],
            output,
        }
    }

    pub fn binary(
        function: FunctionKind,
        left: ScalarExprRef,
        right: ScalarExprRef,
        output: ColumnId,
    ) -> Self {
        Self {
            function,
            arguments: vec![left, right],
            output,
        }
    }

    pub fn function(&self) -> FunctionKind {
        self.function
    }

    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }

    pub fn output(&self) -> ColumnId {
        self.output
    }

    pub(super) fn lower(
        &self,
        builder: &mut KernelBuilder<'_>,
    ) -> Result<(ColumnVariableBinding, PendingErrors)> {
        use FunctionKind::*;
        let mut errors = vec![];
        let mut inputs = vec![];
        let mut types = vec![];
        for argument in &self.arguments {
            let (binding, e) = build_input(builder, argument)?;
            errors.extend(e);
            inputs.push(binding);
            types.push(builder.column_type(argument.output())?.data_type.clone());
        }
        let f = self.function;
        let signature = || Error::InvalidPlan(format!("unsupported signature: {f:?}{types:?}"));
        let t = &types[0];
        let unary = matches!(f, Negate | IsNull | IsNotNull);
        if unary != (inputs.len() == 1) || (!unary && inputs.len() != 2) {
            return Err(Error::InvalidPlan("invalid function arity".into()));
        }
        if !unary && types[0] != types[1] {
            return Err(Error::InvalidPlan(
                "operand types differ; insert an explicit cast".into(),
            ));
        }
        let binding = match f {
            IsNull | IsNotNull => {
                if !scalar::supported(t) {
                    return Err(signature());
                }
                let value = match inputs[0].validity() {
                    None => builder.boolean(f == IsNotNull)?,
                    Some(v) if f == IsNull => builder.not(v)?,
                    Some(v) => v,
                };
                ColumnVariableBinding::Boolean {
                    value,
                    validity: None,
                }
            }
            Negate | Add | Subtract | Multiply | Divide | Remainder => {
                let signed = !matches!(
                    t,
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                );
                if !scalar::numeric(t) || (f == Negate && !signed) {
                    return Err(signature());
                }
                let validity = builder.strict_validity(&inputs)?;
                let values = inputs
                    .iter()
                    .map(|b| b.value().unwrap())
                    .collect::<Vec<_>>();
                let function = match f {
                    Negate => ScalarFunction::Negate,
                    Add => ScalarFunction::Add,
                    Subtract => ScalarFunction::Subtract,
                    Multiply => ScalarFunction::Multiply,
                    Divide => ScalarFunction::Divide,
                    _ => ScalarFunction::Remainder,
                };
                let out = builder.call(function, values)?;
                let flags: &[KernelError] = match (f, out.len()) {
                    (_, 1) => &[],
                    (Divide, _) => &[KernelError::DivideByZero, KernelError::ArithmeticOverflow],
                    (Remainder, _) => &[KernelError::DivideByZero],
                    _ => &[KernelError::ArithmeticOverflow],
                };
                for (flag, error) in out[1..].iter().zip(flags) {
                    errors.push(builder.pending(*flag, validity, *error, vec![])?);
                }
                let ty = builder.variable_type(out[0])?;
                ColumnVariableBinding::fixed(ty, out[0], validity)?
            }
            Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
                if !scalar::supported(t) {
                    return Err(signature());
                }
                let value = compare(builder, f, &inputs[0], &inputs[1])?;
                let validity = if *t == DataType::Null {
                    Some(builder.boolean(false)?)
                } else {
                    builder.strict_validity(&inputs)?
                };
                ColumnVariableBinding::Boolean { value, validity }
            }
            IsDistinctFrom | IsNotDistinctFrom => {
                if !scalar::supported(t) {
                    return Err(signature());
                }
                let (a, b) = (&inputs[0], &inputs[1]);
                let ne = compare(builder, NotEqual, a, b)?;
                let distinct = match (a.validity(), b.validity()) {
                    (None, None) => ne,
                    (av, bv) => {
                        let av = builder.materialize_validity(av)?;
                        let bv = builder.materialize_validity(bv)?;
                        let one_null = builder.call(ScalarFunction::NotEqual, vec![av, bv])?[0];
                        let both = builder.and(av, bv)?;
                        let differ = builder.and(both, ne)?;
                        builder.or(one_null, differ)?
                    }
                };
                let value = if f == IsDistinctFrom {
                    distinct
                } else {
                    builder.not(distinct)?
                };
                ColumnVariableBinding::Boolean {
                    value,
                    validity: None,
                }
            }
        };
        Ok((binding, errors))
    }
}

/// The comparison of the values only; NULL operands are handled by callers.
/// NULL-typed operands compare as false.
fn compare(
    builder: &mut KernelBuilder<'_>,
    f: FunctionKind,
    a: &ColumnVariableBinding,
    b: &ColumnVariableBinding,
) -> Result<VariableId> {
    if a.is_null_type() {
        return builder.boolean(false);
    }
    let function = match f {
        FunctionKind::Equal => ScalarFunction::Equal,
        FunctionKind::NotEqual => ScalarFunction::NotEqual,
        FunctionKind::LessThan => ScalarFunction::LessThan,
        FunctionKind::LessThanOrEqual => ScalarFunction::LessThanOrEqual,
        FunctionKind::GreaterThan => ScalarFunction::GreaterThan,
        _ => ScalarFunction::GreaterThanOrEqual,
    };
    let mut inputs = a.values();
    inputs.extend(b.values());
    Ok(builder.call(function, inputs)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use crate::expr::scalar::test_support::*;
    use crate::expr::scalar::{
        CaseExpression, CoalesceExpression, ConstantExpression, ReferenceExpression,
    };
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
                let mut t = TestPlan::new(&[(data_type.clone(), true)]);
                let reference = ReferenceExpression::new(t.input(0)).into_ref();
                let constant =
                    ConstantExpression::try_new(scalar.clone(), t.column(data_type.clone(), true))
                        .unwrap()
                        .into_ref();
                let (left, right) = if left_scalar {
                    (constant, reference)
                } else {
                    (reference, constant)
                };
                let output = t.column(output_type.clone(), true);
                let expr = FunctionExpression::binary(function, left, right, output).into_ref();
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
                let actual = t.evaluate(&expr, std::slice::from_ref(&array), array.len());
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
    fn failures_in_unselected_branches_are_not_raised() {
        let mut t = TestPlan::new(&[]);
        let add = |t: &mut TestPlan, a, b| {
            let (l, r, o) = (
                t.column(DataType::Int64, false),
                t.column(DataType::Int64, false),
                t.column(DataType::Int64, false),
            );
            FunctionExpression::binary(
                FunctionKind::Add,
                ConstantExpression::int64(Some(a), l).into_ref(),
                ConstantExpression::int64(Some(b), r).into_ref(),
                o,
            )
            .into_ref()
        };
        let dangerous = add(&mut t, i64::MAX, 1);
        assert!(t.evaluate(&dangerous, &[], 0).unwrap().is_empty());
        assert!(t.evaluate(&dangerous, &[], 4).is_err());

        let five = add(&mut t, 2, 3);
        let no = t.column(DataType::Boolean, false);
        let case = CaseExpression::new(
            vec![(
                ConstantExpression::boolean(Some(false), no).into_ref(),
                dangerous.clone(),
            )],
            five,
            t.column(DataType::Int64, false),
        )
        .into_ref();
        let seven = add(&mut t, 3, 4);
        let coalesce =
            CoalesceExpression::new(vec![seven, dangerous], t.column(DataType::Int64, false))
                .into_ref();
        assert_eq!(ints(&t.evaluate(&case, &[], 4).unwrap()), vec![5; 4]);
        assert_eq!(ints(&t.evaluate(&coalesce, &[], 4).unwrap()), vec![7; 4]);
        assert_eq!(ints(&t.evaluate(&case, &[], 2).unwrap()), vec![5; 2]);
    }
}
