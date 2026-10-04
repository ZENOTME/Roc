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

use crate::{
    error::{Error, Result},
    expr::agg::AggregateFunction,
};
use arrow::{
    array::{
        Array, ArrayRef, ArrowNativeTypeOp, BinaryArray, BinaryBuilder, Float64Array, Int64Array,
        ListArray, ListBuilder, PrimitiveArray, UInt64Array, new_empty_array, new_null_array,
    },
    buffer::NullBuffer,
    compute::{cast, kernels::interleave::interleave},
    datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, Int64Type, UInt64Type},
    row::{RowConverter, SortField},
};
use std::{borrow::Cow, collections::HashSet, sync::Arc};

fn input_type(inputs: &[DataType]) -> Result<DataType> {
    inputs
        .first()
        .cloned()
        .ok_or_else(|| Error::InvalidPlan("aggregate requires an argument".into()))
}

type UpdateFn = fn(&mut AccumulatorState, &[ArrayRef], &[usize]) -> Result<()>;

/// Worker-local enum state paired with its update function during construction.
pub(super) struct Accumulator {
    state: AccumulatorState,
    update_fn: UpdateFn,
}

enum AccumulatorState {
    Count(Vec<i64>),
    Distinct {
        groups: Vec<HashSet<Vec<u8>>>,
        converter: RowConverter,
    },
    Sum {
        groups: SumGroups,
        data_type: DataType,
    },
    Avg(Vec<(u64, f64)>),
    Covar(Vec<Covariance>),
    Extremum {
        groups: Vec<Option<(Vec<u8>, ArrayRef)>>,
        converter: RowConverter,
        data_type: DataType,
    },
}
/// Each group holds a native value and an independent validity flag so an
/// untouched group remains distinguishable from a zero sum. Construction pairs
/// this enum with its typed update function.
pub(super) enum SumGroups {
    Signed(TypedSum<Int64Type>),
    Unsigned(TypedSum<UInt64Type>),
    Float(TypedSum<Float64Type>),
}

pub(super) struct TypedSum<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
    valid: Vec<bool>,
}

impl<T: ArrowPrimitiveType> TypedSum<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn new() -> Self {
        Self {
            values: Vec::new(),
            valid: Vec::new(),
        }
    }

    fn resize(&mut self, count: usize) {
        self.values.resize(count, T::Native::ZERO);
        self.valid.resize(count, false);
    }

    #[inline]
    fn add(&mut self, id: usize, value: T::Native) -> Result<()> {
        // Preserve the first value (including floating-point signed zero).
        self.values[id] = if self.valid[id] {
            self.values[id].add_checked(value).map_err(|_| overflow())?
        } else {
            value
        };
        self.valid[id] = true;
        Ok(())
    }

    fn update(&mut self, array: &PrimitiveArray<T>, ids: &[usize]) -> Result<()> {
        let values = array.values();
        if array.null_count() == 0 {
            for (&id, &value) in ids.iter().zip(values.iter()) {
                self.add(id, value)?;
            }
        } else {
            // Iterating set bits respects sliced bitmap offsets and never reads
            // arbitrary payloads beneath nulls.
            for index in array.nulls().unwrap().valid_indices() {
                self.add(ids[index], values[index])?;
            }
        }
        Ok(())
    }

    fn state(&self) -> ArrayRef {
        Arc::new(PrimitiveArray::<T>::new(
            self.values.clone().into(),
            Some(NullBuffer::from(self.valid.clone())),
        ))
    }
}

impl SumGroups {
    fn bind(data_type: &DataType) -> Result<(Self, UpdateFn)> {
        Ok(match data_type {
            DataType::Int64 => (Self::Signed(TypedSum::new()), |state, values, ids| {
                let (groups, data_type) = state.as_sum_mut();
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.as_i64_mut().update(as_i64(&values), ids)
            }),
            DataType::UInt64 => (Self::Unsigned(TypedSum::new()), |state, values, ids| {
                let (groups, data_type) = state.as_sum_mut();
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.as_u64_mut().update(as_u64(&values), ids)
            }),
            DataType::Float64 => (Self::Float(TypedSum::new()), |state, values, ids| {
                let (groups, data_type) = state.as_sum_mut();
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.as_f64_mut().update(as_f64(&values), ids)
            }),
            _ => {
                return Err(Error::InvalidPlan(format!(
                    "unsupported sum result type: {data_type}"
                )));
            }
        })
    }

    fn as_i64_mut(&mut self) -> &mut TypedSum<Int64Type> {
        let Self::Signed(groups) = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        groups
    }

    fn as_u64_mut(&mut self) -> &mut TypedSum<UInt64Type> {
        let Self::Unsigned(groups) = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        groups
    }

    fn as_f64_mut(&mut self) -> &mut TypedSum<Float64Type> {
        let Self::Float(groups) = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        groups
    }

    fn resize(&mut self, count: usize) {
        match self {
            Self::Signed(groups) => groups.resize(count),
            Self::Unsigned(groups) => groups.resize(count),
            Self::Float(groups) => groups.resize(count),
        }
    }

    fn state(&self) -> ArrayRef {
        match self {
            Self::Signed(groups) => groups.state(),
            Self::Unsigned(groups) => groups.state(),
            Self::Float(groups) => groups.state(),
        }
    }
}
fn overflow() -> Error {
    Error::Execution("aggregate arithmetic overflow".into())
}
#[derive(Clone, Copy, Default)]
pub(super) struct Covariance {
    count: u64,
    mean_x: f64,
    mean_y: f64,
    co_moment: f64,
}
impl Covariance {
    fn update(&mut self, x: f64, y: f64) -> Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(overflow)?;
        let dx = x - self.mean_x;
        self.mean_x += dx / self.count as f64;
        self.mean_y += (y - self.mean_y) / self.count as f64;
        self.co_moment += dx * (y - self.mean_y);
        Ok(())
    }
    fn merge(&mut self, other: Self) -> Result<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = other;
            return Ok(());
        }
        let total = self.count.checked_add(other.count).ok_or_else(overflow)?;
        let dx = other.mean_x - self.mean_x;
        let dy = other.mean_y - self.mean_y;
        self.co_moment +=
            other.co_moment + dx * dy * (self.count as f64 * (other.count as f64 / total as f64));
        self.mean_x += dx * (other.count as f64 / total as f64);
        self.mean_y += dy * (other.count as f64 / total as f64);
        self.count = total;
        Ok(())
    }
}
impl Accumulator {
    pub fn new(
        function: AggregateFunction,
        distinct: bool,
        inputs: &[DataType],
        output: &DataType,
    ) -> Result<Self> {
        use AggregateFunction::*;
        let (state, update_fn): (AccumulatorState, UpdateFn) = match function {
            Count if distinct => {
                let input = input_type(inputs)?;
                (
                    AccumulatorState::Distinct {
                        groups: vec![],
                        converter: RowConverter::new(vec![SortField::new(input)])?,
                    },
                    update_distinct,
                )
            }
            Count => (AccumulatorState::Count(vec![]), update_count),
            Sum => {
                let (groups, update_fn) = SumGroups::bind(output)?;
                (
                    AccumulatorState::Sum {
                        groups,
                        data_type: output.clone(),
                    },
                    update_fn,
                )
            }
            Avg => (AccumulatorState::Avg(vec![]), update_avg),
            CovarPop => (AccumulatorState::Covar(vec![]), update_covar),
            Min | Max => {
                let input = input_type(inputs)?;
                (
                    AccumulatorState::Extremum {
                        groups: vec![],
                        converter: RowConverter::new(vec![SortField::new(input)])?,
                        data_type: output.clone(),
                    },
                    if function == Min {
                        update_extremum::<true>
                    } else {
                        update_extremum::<false>
                    },
                )
            }
        };
        Ok(Self { state, update_fn })
    }
    pub fn resize(&mut self, count: usize) {
        match &mut self.state {
            AccumulatorState::Count(groups) => groups.resize(count, 0),
            AccumulatorState::Distinct { groups, .. } => groups.resize_with(count, HashSet::new),
            AccumulatorState::Sum { groups, .. } => groups.resize(count),
            AccumulatorState::Avg(groups) => groups.resize(count, (0, 0.0)),
            AccumulatorState::Covar(groups) => groups.resize(count, Covariance::default()),
            AccumulatorState::Extremum { groups, .. } => groups.resize(count, None),
        }
    }
    pub fn update(&mut self, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
        (self.update_fn)(&mut self.state, values, ids)
    }
    pub fn state_types(&self) -> Vec<DataType> {
        match &self.state {
            AccumulatorState::Count(_) => vec![DataType::Int64],
            AccumulatorState::Distinct { .. } => vec![DataType::List(Arc::new(Field::new(
                "item",
                DataType::Binary,
                true,
            )))],
            AccumulatorState::Sum { data_type, .. }
            | AccumulatorState::Extremum { data_type, .. } => {
                vec![data_type.clone()]
            }
            AccumulatorState::Avg(_) => vec![DataType::UInt64, DataType::Float64],
            AccumulatorState::Covar(_) => vec![
                DataType::UInt64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
        }
    }
    pub fn state(&self) -> Result<Vec<ArrayRef>> {
        Ok(match &self.state {
            AccumulatorState::Count(groups) => vec![Arc::new(Int64Array::from(groups.clone()))],
            AccumulatorState::Distinct { groups, .. } => {
                let mut builder = ListBuilder::new(BinaryBuilder::new());
                for group in groups {
                    for key in group {
                        builder.values().append_value(key);
                    }
                    builder.append(true);
                }
                vec![Arc::new(builder.finish())]
            }
            AccumulatorState::Sum { groups, .. } => vec![groups.state()],
            AccumulatorState::Avg(groups) => vec![
                Arc::new(UInt64Array::from_iter_values(groups.iter().map(|g| g.0))),
                Arc::new(Float64Array::from_iter_values(groups.iter().map(|g| g.1))),
            ],
            AccumulatorState::Covar(groups) => vec![
                Arc::new(UInt64Array::from_iter_values(
                    groups.iter().map(|g| g.count),
                )),
                Arc::new(Float64Array::from_iter_values(
                    groups.iter().map(|g| g.mean_x),
                )),
                Arc::new(Float64Array::from_iter_values(
                    groups.iter().map(|g| g.mean_y),
                )),
                Arc::new(Float64Array::from_iter_values(
                    groups.iter().map(|g| g.co_moment),
                )),
            ],
            AccumulatorState::Extremum {
                groups, data_type, ..
            } => {
                if groups.is_empty() {
                    vec![new_empty_array(data_type)]
                } else {
                    let mut arrays = vec![new_null_array(data_type, 1)];
                    let mut indices = vec![];
                    for group in groups {
                        if let Some((_, value)) = group {
                            indices.push((arrays.len(), 0));
                            arrays.push(value.clone());
                        } else {
                            indices.push((0, 0));
                        }
                    }
                    vec![interleave(
                        &arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
                        &indices,
                    )?]
                }
            }
        })
    }
    pub fn merge(&mut self, states: &[ArrayRef], ids: &[usize]) -> Result<()> {
        match &mut self.state {
            AccumulatorState::Count(groups) => {
                let values = as_i64(&states[0]);
                for (i, &id) in ids.iter().enumerate() {
                    groups[id] = groups[id]
                        .checked_add(values.value(i))
                        .ok_or_else(overflow)?;
                }
            }
            AccumulatorState::Distinct { groups, .. } => {
                let lists = states[0].as_any().downcast_ref::<ListArray>().unwrap();
                for (i, &id) in ids.iter().enumerate() {
                    let values = lists.value(i);
                    let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
                    for key in values.iter().flatten() {
                        groups[id].insert(key.to_vec());
                    }
                }
            }
            AccumulatorState::Avg(groups) => {
                let (counts, sums) = (as_u64(&states[0]), as_f64(&states[1]));
                for (i, &id) in ids.iter().enumerate() {
                    groups[id].0 = groups[id]
                        .0
                        .checked_add(counts.value(i))
                        .ok_or_else(overflow)?;
                    groups[id].1 += sums.value(i);
                }
            }
            AccumulatorState::Covar(groups) => {
                let count = as_u64(&states[0]);
                let (x, y, c) = (as_f64(&states[1]), as_f64(&states[2]), as_f64(&states[3]));
                for (i, &id) in ids.iter().enumerate() {
                    groups[id].merge(Covariance {
                        count: count.value(i),
                        mean_x: x.value(i),
                        mean_y: y.value(i),
                        co_moment: c.value(i),
                    })?;
                }
            }
            AccumulatorState::Sum { .. } | AccumulatorState::Extremum { .. } => {
                self.update(states, ids)?
            }
        }
        Ok(())
    }
    pub fn evaluate(&self) -> Result<ArrayRef> {
        Ok(match &self.state {
            AccumulatorState::Distinct { groups, .. } => Arc::new(Int64Array::from(
                groups
                    .iter()
                    .map(|s| i64::try_from(s.len()).map_err(|_| overflow()))
                    .collect::<Result<Vec<_>>>()?,
            )),
            AccumulatorState::Avg(groups) => Arc::new(Float64Array::from(
                groups
                    .iter()
                    .map(|(count, sum)| (*count != 0).then(|| *sum / *count as f64))
                    .collect::<Vec<_>>(),
            )),
            AccumulatorState::Covar(groups) => Arc::new(Float64Array::from(
                groups
                    .iter()
                    .map(|g| (g.count != 0).then(|| g.co_moment / g.count as f64))
                    .collect::<Vec<_>>(),
            )),
            _ => self.state()?.remove(0),
        })
    }
}
/// Identical types can borrow the original array. Calling Arrow's cast in
/// that case reconstructs an array wrapper and clones its backing buffers.
fn cast_argument<'a>(argument: &'a ArrayRef, data_type: &DataType) -> Result<Cow<'a, ArrayRef>> {
    if argument.data_type() == data_type {
        Ok(Cow::Borrowed(argument))
    } else {
        Ok(Cow::Owned(cast(argument.as_ref(), data_type)?))
    }
}

impl AccumulatorState {
    fn as_count_mut(&mut self) -> &mut Vec<i64> {
        let Self::Count(groups) = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        groups
    }

    fn as_avg_mut(&mut self) -> &mut Vec<(u64, f64)> {
        let Self::Avg(groups) = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        groups
    }

    fn as_covar_mut(&mut self) -> &mut Vec<Covariance> {
        let Self::Covar(groups) = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        groups
    }

    fn as_distinct_mut(&mut self) -> (&mut Vec<HashSet<Vec<u8>>>, &mut RowConverter) {
        let Self::Distinct { groups, converter } = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        (groups, converter)
    }

    fn as_sum_mut(&mut self) -> (&mut SumGroups, &DataType) {
        let Self::Sum { groups, data_type } = self else {
            unreachable!("aggregate state and update function are bound together")
        };
        (groups, data_type)
    }

    fn as_extremum_mut(&mut self) -> (&mut Vec<Option<(Vec<u8>, ArrayRef)>>, &mut RowConverter) {
        let Self::Extremum {
            groups, converter, ..
        } = self
        else {
            unreachable!("aggregate state and update function are bound together")
        };
        (groups, converter)
    }
}

fn update_count(state: &mut AccumulatorState, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
    let groups = state.as_count_mut();
    let nulls = values.first().and_then(|v| v.logical_nulls());
    if let Some(nulls) = nulls.filter(|nulls| nulls.null_count() != 0) {
        for index in nulls.valid_indices() {
            let id = ids[index];
            groups[id] = groups[id].checked_add(1).ok_or_else(overflow)?;
        }
    } else {
        for &id in ids {
            groups[id] = groups[id].checked_add(1).ok_or_else(overflow)?;
        }
    }

    Ok(())
}

fn update_distinct(state: &mut AccumulatorState, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
    let (groups, converter) = state.as_distinct_mut();
    let rows = converter.convert_columns(values)?;
    let nulls = values[0].logical_nulls();
    for (i, &id) in ids.iter().enumerate() {
        if nulls.as_ref().is_none_or(|n| n.is_valid(i)) {
            groups[id].insert(rows.row(i).as_ref().to_vec());
        }
    }

    Ok(())
}

fn update_avg(state: &mut AccumulatorState, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
    let groups = state.as_avg_mut();
    let argument = values
        .first()
        .ok_or_else(|| Error::Execution("avg requires one argument".into()))?;
    let values = cast_argument(argument, &DataType::Float64)?;
    let values = as_f64(&values);
    for (i, &id) in ids.iter().enumerate() {
        if values.is_null(i) {
            continue;
        }
        groups[id].0 = groups[id].0.checked_add(1).ok_or_else(overflow)?;
        groups[id].1 += values.value(i);
    }

    Ok(())
}

fn update_covar(state: &mut AccumulatorState, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
    let groups = state.as_covar_mut();
    let (x, y) = values
        .split_first()
        .and_then(|(x, rest)| rest.first().map(|y| (x, y)))
        .ok_or_else(|| Error::Execution("covariance requires two arguments".into()))?;
    let x = cast_argument(x, &DataType::Float64)?;
    let y = cast_argument(y, &DataType::Float64)?;
    let (x, y) = (as_f64(&x), as_f64(&y));
    for (i, &id) in ids.iter().enumerate() {
        if x.is_valid(i) && y.is_valid(i) {
            groups[id].update(x.value(i), y.value(i))?;
        }
    }

    Ok(())
}

fn update_extremum<const MINIMUM: bool>(
    state: &mut AccumulatorState,
    values: &[ArrayRef],
    ids: &[usize],
) -> Result<()> {
    let (groups, converter) = state.as_extremum_mut();
    let rows = converter.convert_columns(values)?;
    let nulls = values[0].logical_nulls();
    for (i, &id) in ids.iter().enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
            continue;
        }
        let row = rows.row(i);
        let replace = groups[id].as_ref().is_none_or(|(key, _)| {
            if MINIMUM {
                row.as_ref() < key.as_slice()
            } else {
                row.as_ref() > key.as_slice()
            }
        });
        if replace {
            // Retain the chosen row; Arrow may share backing buffers.
            let value =
                arrow::compute::take(values[0].as_ref(), &UInt64Array::from(vec![i as u64]), None)?;
            groups[id] = Some((row.as_ref().to_vec(), value));
        }
    }

    Ok(())
}

fn as_i64(a: &ArrayRef) -> &Int64Array {
    a.as_any().downcast_ref().unwrap()
}
fn as_u64(a: &ArrayRef) -> &UInt64Array {
    a.as_any().downcast_ref().unwrap()
}
fn as_f64(a: &ArrayRef) -> &Float64Array {
    a.as_any().downcast_ref().unwrap()
}

#[cfg(test)]
mod cast_argument_tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn same_type_borrows_and_numeric_coercion_still_casts() {
        let source = Arc::new(Int64Array::from(vec![Some(4), None, Some(-2)])) as ArrayRef;
        let borrowed = cast_argument(&source, &DataType::Int64).unwrap();
        assert!(matches!(&borrowed, Cow::Borrowed(_)));
        assert!(Arc::ptr_eq(&source, borrowed.as_ref()));
        let narrow = Arc::new(Int32Array::from(vec![Some(4), None, Some(-2)])) as ArrayRef;
        let converted = cast_argument(&narrow, &DataType::Int64).unwrap();
        assert!(matches!(&converted, Cow::Owned(_)));
        assert_eq!(converted.to_data(), source.to_data());
    }
}
