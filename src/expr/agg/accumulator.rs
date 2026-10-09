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

fn input_type(input: Option<&DataType>) -> Result<DataType> {
    input
        .cloned()
        .ok_or_else(|| Error::invalid_plan("aggregate requires an argument".into()))
}

type UpdateFn = fn(&mut AccumulatorState, Option<&ArrayRef>, &[usize]) -> Result<()>;

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
            self.values[id]
                .add_checked(value)
                .map_err(|source| overflow().with_source(source))?
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

    /// All rows belong to group zero. Keep the running value in a local variable
    /// and visit contiguous valid slices, preserving sequential checked addition.
    fn update_global(
        &mut self,
        array: &PrimitiveArray<T>,
        add_value: impl Fn(T::Native, T::Native) -> Option<T::Native>,
    ) -> Result<()> {
        debug_assert_eq!(self.values.len(), 1);
        if array.len() == array.null_count() {
            return Ok(());
        }
        let values = array.values();
        let mut total = self.values[0];
        let mut start = 0;
        if !self.valid[0] {
            let first = array
                .nulls()
                .map_or(0, |nulls| nulls.valid_indices().next().unwrap());
            total = values[first];
            self.valid[0] = true;
            start = first + 1;
        }
        let result = (|| {
            let mut add = |slice: &[T::Native]| -> Result<()> {
                for &value in slice {
                    total = add_value(total, value).ok_or_else(overflow)?;
                }
                Ok(())
            };
            if array.null_count() == 0 {
                add(&values[start..])?;
            } else {
                for (lo, hi) in array.nulls().unwrap().valid_slices() {
                    if hi > start {
                        add(&values[lo.max(start)..hi])?;
                    }
                }
            }
            Ok(())
        })();
        // An error leaves exactly the successfully accumulated prefix, like add().
        self.values[0] = total;
        result
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
            DataType::Int64 => (Self::Signed(TypedSum::new()), |state, value, ids| {
                let (groups, data_type) = state.as_sum_mut();
                let argument =
                    value.ok_or_else(|| Error::internal("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.as_i64_mut().update(as_i64(&values), ids)
            }),
            DataType::UInt64 => (Self::Unsigned(TypedSum::new()), |state, value, ids| {
                let (groups, data_type) = state.as_sum_mut();
                let argument =
                    value.ok_or_else(|| Error::internal("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.as_u64_mut().update(as_u64(&values), ids)
            }),
            DataType::Float64 => (Self::Float(TypedSum::new()), |state, value, ids| {
                let (groups, data_type) = state.as_sum_mut();
                let argument =
                    value.ok_or_else(|| Error::internal("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.as_f64_mut().update(as_f64(&values), ids)
            }),
            _ => {
                return Err(Error::unsupported(format!(
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
#[track_caller]
fn overflow() -> Error {
    Error::new(
        crate::error::ErrorKind::ArithmeticOverflow,
        "aggregate arithmetic overflow",
    )
}
impl Accumulator {
    pub fn new(
        function: AggregateFunction,
        distinct: bool,
        input: Option<&DataType>,
        output: &DataType,
    ) -> Result<Self> {
        use AggregateFunction::*;
        if function != Count || distinct {
            input_type(input)?;
        }
        let (state, update_fn): (AccumulatorState, UpdateFn) = match function {
            Count if distinct => {
                let input = input_type(input)?;
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
            Min | Max => {
                let input = input_type(input)?;
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
            AccumulatorState::Extremum { groups, .. } => groups.resize(count, None),
        }
    }
    pub fn update(&mut self, value: Option<&ArrayRef>, ids: &[usize]) -> Result<()> {
        (self.update_fn)(&mut self.state, value, ids)
    }
    /// The operator binds this only when all input rows belong to group zero.
    pub fn bind_global_count(&mut self) {
        if matches!(self.state, AccumulatorState::Count(_)) {
            self.update_fn = update_global_count;
        }
    }
    pub fn bind_global_sum(&mut self) {
        if let AccumulatorState::Sum { groups, .. } = &self.state {
            self.update_fn = match groups {
                SumGroups::Signed(_) => update_global_sum_i64,
                SumGroups::Unsigned(_) => update_global_sum_u64,
                SumGroups::Float(_) => update_global_sum_f64,
            };
        }
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
            AccumulatorState::Sum { .. } | AccumulatorState::Extremum { .. } => {
                self.update(states.first(), ids)?
            }
        }
        Ok(())
    }
    pub fn evaluate(&self) -> Result<ArrayRef> {
        Ok(match &self.state {
            AccumulatorState::Distinct { groups, .. } => Arc::new(Int64Array::from(
                groups
                    .iter()
                    .map(|s| {
                        i64::try_from(s.len()).map_err(|source| overflow().with_source(source))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            AccumulatorState::Avg(groups) => Arc::new(Float64Array::from(
                groups
                    .iter()
                    .map(|(count, sum)| (*count != 0).then(|| *sum / *count as f64))
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

fn update_count(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    let groups = state.as_count_mut();
    let nulls = value.and_then(|v| v.logical_nulls());
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

fn update_global_count(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    let groups = state.as_count_mut();
    debug_assert_eq!(groups.len(), 1);
    debug_assert!(ids.iter().all(|&id| id == 0));
    let null_count = value
        .and_then(|v| v.logical_nulls())
        .map_or(0, |nulls| nulls.null_count());
    let delta =
        i64::try_from(ids.len() - null_count).map_err(|source| overflow().with_source(source))?;
    let total = &mut groups[0];
    let updated = total.checked_add(delta).ok_or_else(|| {
        // The row loop reaches MAX before failing on the next increment.
        *total = i64::MAX;
        overflow()
    })?;
    *total = updated;
    Ok(())
}

/// Integer arithmetic used to prove that every valid addition prefix fits.
/// Only the two integer SUM state types implement this private trait.
trait BatchInteger: ArrowNativeTypeOp {
    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn magnitude_bits(self) -> u64;
    fn magnitude_bound(bits: u64) -> u128;
    fn remaining_range(self) -> u128;
}

impl BatchInteger for i64 {
    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        i64::checked_add(self, rhs)
    }
    #[inline]
    fn magnitude_bits(self) -> u64 {
        (self ^ (self >> 63)) as u64
    }
    #[inline]
    fn magnitude_bound(bits: u64) -> u128 {
        u128::from(bits) + 1
    }
    #[inline]
    fn remaining_range(self) -> u128 {
        (i64::MAX as i128 - self as i128).min(self as i128 - i64::MIN as i128) as u128
    }
}

impl BatchInteger for u64 {
    #[inline]
    fn checked_add(self, rhs: Self) -> Option<Self> {
        u64::checked_add(self, rhs)
    }
    #[inline]
    fn magnitude_bits(self) -> u64 {
        self
    }
    #[inline]
    fn magnitude_bound(bits: u64) -> u128 {
        u128::from(bits)
    }
    #[inline]
    fn remaining_range(self) -> u128 {
        u128::from(u64::MAX - self)
    }
}

/// A running integer total with batch addition and prefix overflow checks.
/// The fast scan combines the wrapping subtotal and its safety bound so it
/// can vectorize; an inconclusive proof falls back to checked additions.
struct NumberBatchAdd<N> {
    total: N,
}

impl<N: BatchInteger> NumberBatchAdd<N> {
    #[inline]
    fn new(initial: N) -> Self {
        Self { total: initial }
    }

    #[inline]
    fn total(&self) -> N {
        self.total
    }

    #[inline]
    fn add<T: ArrowPrimitiveType<Native = N>>(&mut self, array: &PrimitiveArray<T>) -> Result<()> {
        let count = array.len() - array.null_count();
        if count == 0 {
            return Ok(());
        }
        let (bits, raw_sum) = array
            .values()
            .iter()
            .fold((0u64, N::ZERO), |(bits, sum), &value| {
                (bits | value.magnitude_bits(), sum.add_wrapping(value))
            });
        if N::magnitude_bound(bits) * count as u128 <= self.total.remaining_range() {
            // Cancel arbitrary NULL payloads modulo 2^64. The bound proves
            // that every valid prefix fits even if raw subtotals wrapped.
            let ignored = array
                .nulls()
                .filter(|n| n.null_count() != 0)
                .map_or(N::ZERO, |nulls| {
                    (!nulls.inner())
                        .set_indices()
                        .fold(N::ZERO, |sum, i| sum.add_wrapping(array.value(i)))
                });
            self.total = self
                .total
                .checked_add(raw_sum.sub_wrapping(ignored))
                .ok_or_else(overflow)?;
            return Ok(());
        }
        // An inconclusive bound is not overflow. Check the valid values in
        // input order, retaining the successful prefix if an addition fails.
        let mut add_slice = |slice: &[N]| -> Result<()> {
            for &value in slice {
                self.total = self.total.checked_add(value).ok_or_else(overflow)?;
            }
            Ok(())
        };
        if array.null_count() == 0 {
            add_slice(array.values())?;
        } else {
            for (lo, hi) in array.nulls().unwrap().valid_slices() {
                add_slice(&array.values()[lo..hi])?;
            }
        }
        Ok(())
    }
}

impl<T: ArrowPrimitiveType> TypedSum<T> {
    #[inline]
    fn update_global_integer(&mut self, array: &PrimitiveArray<T>) -> Result<()>
    where
        T::Native: BatchInteger,
    {
        if array.len() == array.null_count() {
            return Ok(());
        }
        let mut sum = NumberBatchAdd::new(self.values[0]);
        let result = sum.add(array);
        // Preserve the successfully accumulated prefix on overflow too.
        self.values[0] = sum.total();
        self.valid[0] = true;
        result
    }
}

fn update_global_sum_i64(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    debug_assert!(ids.iter().all(|&id| id == 0));
    let (groups, data_type) = state.as_sum_mut();
    let argument = value.ok_or_else(|| Error::internal("sum requires one argument".into()))?;
    debug_assert_eq!(argument.len(), ids.len());
    let values = cast_argument(argument, data_type)?;
    groups.as_i64_mut().update_global_integer(as_i64(&values))
}

fn update_global_sum_u64(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    debug_assert!(ids.iter().all(|&id| id == 0));
    let (groups, data_type) = state.as_sum_mut();
    let argument = value.ok_or_else(|| Error::internal("sum requires one argument".into()))?;
    debug_assert_eq!(argument.len(), ids.len());
    let values = cast_argument(argument, data_type)?;
    groups.as_u64_mut().update_global_integer(as_u64(&values))
}

fn update_global_sum_f64(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    debug_assert!(ids.iter().all(|&id| id == 0));
    let (groups, data_type) = state.as_sum_mut();
    let argument = value.ok_or_else(|| Error::internal("sum requires one argument".into()))?;
    debug_assert_eq!(argument.len(), ids.len());
    let values = cast_argument(argument, data_type)?;
    groups
        .as_f64_mut()
        .update_global(as_f64(&values), |a, b| Some(a + b))
}

fn update_distinct(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    let (groups, converter) = state.as_distinct_mut();
    let value = value.ok_or_else(|| Error::internal("aggregate requires an argument".into()))?;
    let rows = converter.convert_columns(std::slice::from_ref(value))?;
    let nulls = value.logical_nulls();
    for (i, &id) in ids.iter().enumerate() {
        if nulls.as_ref().is_none_or(|n| n.is_valid(i)) {
            groups[id].insert(rows.row(i).as_ref().to_vec());
        }
    }

    Ok(())
}

fn update_avg(state: &mut AccumulatorState, value: Option<&ArrayRef>, ids: &[usize]) -> Result<()> {
    let groups = state.as_avg_mut();
    let argument = value.ok_or_else(|| Error::internal("avg requires one argument".into()))?;
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

fn update_extremum<const MINIMUM: bool>(
    state: &mut AccumulatorState,
    value: Option<&ArrayRef>,
    ids: &[usize],
) -> Result<()> {
    let (groups, converter) = state.as_extremum_mut();
    let value = value.ok_or_else(|| Error::internal("aggregate requires an argument".into()))?;
    let rows = converter.convert_columns(std::slice::from_ref(value))?;
    let nulls = value.logical_nulls();
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
                arrow::compute::take(value.as_ref(), &UInt64Array::from(vec![i as u64]), None)?;
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
mod global_count_tests {
    use super::*;
    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::Int8Type;

    #[test]
    fn global_count_matches_row_updates_for_slices_and_dictionary_nulls() {
        let sliced: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(3),
            None,
            Some(5),
            Some(3),
            None,
        ]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), Some(1), None, Some(0)]),
                Arc::new(Int64Array::from(vec![Some(7), None])),
            )
            .unwrap(),
        );
        for value in [sliced.slice(1, 4), dictionary] {
            let mut rows = Accumulator::new(
                AggregateFunction::Count,
                false,
                Some(value.data_type()),
                &DataType::Int64,
            )
            .unwrap();
            let mut batch = Accumulator::new(
                AggregateFunction::Count,
                false,
                Some(value.data_type()),
                &DataType::Int64,
            )
            .unwrap();
            rows.resize(1);
            batch.resize(1);
            batch.bind_global_count();
            let ids = vec![0; value.len()];
            for _ in 0..2 {
                rows.update(Some(&value), &ids).unwrap();
                batch.update(Some(&value), &ids).unwrap();
            }
            assert_eq!(
                rows.evaluate().unwrap().to_data(),
                batch.evaluate().unwrap().to_data()
            );
        }
    }

    #[test]
    fn count_star_empty_input_and_distinct_keep_their_semantics() {
        let mut count =
            Accumulator::new(AggregateFunction::Count, false, None, &DataType::Int64).unwrap();
        count.resize(1);
        count.bind_global_count();
        count.update(None, &[]).unwrap();
        assert_eq!(as_i64(&count.evaluate().unwrap()).value(0), 0);
        count.update(None, &[0, 0, 0]).unwrap();
        assert_eq!(as_i64(&count.evaluate().unwrap()).value(0), 3);

        let mut distinct = Accumulator::new(
            AggregateFunction::Count,
            true,
            Some(&DataType::Int64),
            &DataType::Int64,
        )
        .unwrap();
        distinct.resize(1);
        distinct.bind_global_count();
        let value: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(7)]));
        distinct.update(Some(&value), &[0, 0, 0]).unwrap();
        assert_eq!(as_i64(&distinct.evaluate().unwrap()).value(0), 1);
    }

    #[test]
    fn overflow_preserves_the_same_successful_prefix_as_row_updates() {
        for global in [false, true] {
            let mut count =
                Accumulator::new(AggregateFunction::Count, false, None, &DataType::Int64).unwrap();
            count.resize(1);
            count.state.as_count_mut()[0] = i64::MAX - 1;
            if global {
                count.bind_global_count();
            }
            assert!(matches!(
                count.update(None, &[0, 0, 0]),
                Err(ref error) if error.kind() == crate::error::ErrorKind::ArithmeticOverflow
            ));
            assert_eq!(as_i64(&count.evaluate().unwrap()).value(0), i64::MAX);
        }
    }
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

#[cfg(test)]
mod global_sum_tests {
    use super::*;
    use arrow::array::{Float32Array, Int32Array, UInt32Array};

    #[test]
    fn number_batch_add_starts_with_initial_and_accumulates_multiple_batches() {
        let mut sum = NumberBatchAdd::new(5i64);
        sum.add(&Int64Array::from(vec![Some(10), None, Some(-3)]))
            .unwrap();
        assert_eq!(sum.total(), 12);
        sum.add(&Int64Array::from(vec![None, Some(8)])).unwrap();
        assert_eq!(sum.total(), 20);
        sum.add(&Int64Array::from(vec![None, None])).unwrap();
        assert_eq!(sum.total(), 20);
        // A failed batch retains its successful prefix, and the helper can
        // subsequently continue from that exact total.
        let mut sum = NumberBatchAdd::new(i64::MAX - 1);
        assert!(sum.add(&Int64Array::from(vec![1, 1, -1])).is_err());
        assert_eq!(sum.total(), i64::MAX);
        sum.add(&Int64Array::from(vec![-1])).unwrap();
        assert_eq!(sum.total(), i64::MAX - 1);
    }

    fn pair(input: &DataType, output: &DataType) -> (Accumulator, Accumulator) {
        let mut row = Accumulator::new(AggregateFunction::Sum, false, Some(input), output).unwrap();
        let mut global =
            Accumulator::new(AggregateFunction::Sum, false, Some(input), output).unwrap();
        row.resize(1);
        global.resize(1);
        global.bind_global_sum();
        (row, global)
    }

    fn compare(row: &Accumulator, global: &Accumulator) {
        let a = row.evaluate().unwrap();
        let b = global.evaluate().unwrap();
        assert_eq!(a.null_count(), b.null_count());
        if a.data_type() == &DataType::Float64 {
            assert_eq!(
                as_f64(&a)
                    .values()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
                as_f64(&b)
                    .values()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>()
            );
        } else {
            assert_eq!(a.to_data(), b.to_data());
        }
    }

    #[test]
    fn global_sum_matches_row_order_for_sliced_null_patterns_and_empty_batches() {
        for step in [1, 2, 7, 17, 151] {
            let nulls = NullBuffer::from((0..151).map(|i| i % step != 0).collect::<Vec<_>>());
            for array in [
                Arc::new(Int64Array::new(
                    (0..151).map(|i| i as i64 - 70).collect::<Vec<_>>().into(),
                    Some(nulls.clone()),
                )) as ArrayRef,
                Arc::new(UInt64Array::new(
                    (0..151).map(|i| i as u64).collect::<Vec<_>>().into(),
                    Some(nulls.clone()),
                )),
                Arc::new(Float64Array::new(
                    (0..151)
                        .map(|i| (i as f64 - 70.0) / 3.0)
                        .collect::<Vec<_>>()
                        .into(),
                    Some(nulls.clone()),
                )),
            ] {
                let (mut row, mut global) = pair(array.data_type(), array.data_type());
                for value in [array.slice(3, 0), array.slice(3, 143), array.slice(7, 100)] {
                    let ids = vec![0; value.len()];
                    row.update(Some(&value), &ids).unwrap();
                    global.update(Some(&value), &ids).unwrap();
                    compare(&row, &global);
                }
            }
        }
    }

    #[test]
    fn global_sum_checks_each_prefix_and_preserves_state_on_error() {
        for batches in [
            vec![
                Arc::new(Int64Array::from(vec![i64::MAX - 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1), Some(-1)])),
            ],
            vec![
                Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef,
                Arc::new(Int64Array::from(vec![None, Some(-1), Some(1)])),
            ],
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX - 1])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(1), None, Some(1)])),
            ],
        ] {
            let (mut row, mut global) = pair(batches[0].data_type(), batches[0].data_type());
            for (index, value) in batches.iter().enumerate() {
                let ids = vec![0; value.len()];
                assert_eq!(row.update(Some(value), &ids).is_err(), index == 1);
                assert_eq!(global.update(Some(value), &ids).is_err(), index == 1);
                compare(&row, &global);
            }
        }
    }

    #[test]
    fn bounded_sum_cancels_arbitrary_null_payloads_even_when_raw_sum_wraps() {
        for unsigned in [false, true] {
            let nulls = NullBuffer::from((0..97).map(|i| i == 37).collect::<Vec<_>>());
            let array: ArrayRef = if unsigned {
                let mut values = vec![u64::MAX; 97];
                values[37] = 7;
                Arc::new(UInt64Array::new(values.into(), Some(nulls)))
            } else {
                let mut values = vec![i64::MAX / 2; 97];
                values[37] = 7;
                Arc::new(Int64Array::new(values.into(), Some(nulls)))
            };
            // One valid small value makes the bound pass; arbitrary NULL payloads
            // wrap the raw subtotal repeatedly and must cancel modulo 2^64.
            let array = array.slice(3, 89);
            let (mut row, mut global) = pair(array.data_type(), array.data_type());
            for _ in 0..2 {
                let ids = vec![0; array.len()];
                row.update(Some(&array), &ids).unwrap();
                global.update(Some(&array), &ids).unwrap();
                compare(&row, &global);
            }
        }
    }

    #[test]
    fn bounded_sum_matches_checked_updates_for_deterministic_extreme_inputs() {
        let mut seed = 0x5ee_d123_89ab_cdefu64;
        for case in 0..128 {
            let mut signed = vec![];
            let mut unsigned = vec![];
            let mut valid = vec![];
            for i in 0..79 {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                signed.push(if case % 2 == 0 {
                    (seed % 200001) as i64 - 100000
                } else {
                    match seed % 5 {
                        0 => i64::MIN,
                        1 => i64::MAX,
                        2 => -1,
                        3 => 1,
                        _ => seed as i64,
                    }
                });
                unsigned.push(if case % 2 == 0 {
                    seed % 200001
                } else {
                    match seed % 3 {
                        0 => u64::MAX,
                        1 => 1,
                        _ => seed,
                    }
                });
                valid.push(i % 7 != 0 && seed % 4 != 0);
            }
            let nulls = NullBuffer::from(valid);
            for value in [
                Arc::new(Int64Array::new(signed.into(), Some(nulls.clone()))) as ArrayRef,
                Arc::new(UInt64Array::new(unsigned.into(), Some(nulls.clone()))),
            ] {
                let value = value.slice(case % 7, 65);
                let (mut row, mut global) = pair(value.data_type(), value.data_type());
                for _ in 0..3 {
                    let ids = vec![0; value.len()];
                    let a = row.update(Some(&value), &ids);
                    let b = global.update(Some(&value), &ids);
                    assert_eq!(a.is_err(), b.is_err());
                    compare(&row, &global);
                    if a.is_err() {
                        break;
                    }
                }
            }
        }
    }

    #[test]
    fn global_float_sum_preserves_first_signed_zero_null_payloads_and_addition_order() {
        let (mut row, mut global) = pair(&DataType::Float64, &DataType::Float64);
        for value in [
            Arc::new(Float64Array::new(
                vec![f64::NAN, -0.0, f64::INFINITY].into(),
                Some(NullBuffer::from(vec![false, true, false])),
            )) as ArrayRef,
            Arc::new(Float64Array::from(vec![1e16, -1e16, 1.0])),
            Arc::new(Float64Array::from(vec![f64::NAN])),
        ] {
            let ids = vec![0; value.len()];
            row.update(Some(&value), &ids).unwrap();
            global.update(Some(&value), &ids).unwrap();
            compare(&row, &global);
        }
    }

    #[test]
    fn global_sum_keeps_numeric_casts_and_partial_merges() {
        for (value, output) in [
            (
                Arc::new(Int32Array::from(vec![Some(3), None, Some(-3)])) as ArrayRef,
                DataType::Int64,
            ),
            (
                Arc::new(UInt32Array::from(vec![Some(3), None, Some(4)])) as ArrayRef,
                DataType::UInt64,
            ),
            (
                Arc::new(Float32Array::from(vec![Some(-0.0), None, Some(4.0)])) as ArrayRef,
                DataType::Float64,
            ),
        ] {
            let (mut row, mut global) = pair(value.data_type(), &output);
            let ids = vec![0; value.len()];
            row.update(Some(&value), &ids).unwrap();
            global.update(Some(&value), &ids).unwrap();
            compare(&row, &global);
            let partial = row.state().unwrap();
            row.merge(&partial, &[0]).unwrap();
            global.merge(&partial, &[0]).unwrap();
            compare(&row, &global);
        }
    }
}
