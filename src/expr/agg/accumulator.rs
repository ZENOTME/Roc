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
    compute::{cast, kernels::interleave::interleave, sum},
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

pub(crate) enum Accumulator {
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
        minimum: bool,
    },
}
/// Dispatch once per batch; each group holds a native value and an independent
/// validity flag so an untouched group remains distinguishable from a zero sum.
pub(crate) enum SumGroups {
    Signed(TypedSum<Int64Type, false>),
    Unsigned(TypedSum<UInt64Type, false>),
    Float(TypedSum<Float64Type, true>),
    // Keep construction permissive: unsupported SUM result types have always
    // been reported when executing or materializing the aggregate.
    Unsupported(DataType),
}

pub(crate) struct TypedSum<T: ArrowPrimitiveType, const PRESERVE_FIRST: bool> {
    values: Vec<T::Native>,
    valid: Vec<bool>,
}

impl<T: ArrowPrimitiveType, const PRESERVE_FIRST: bool> TypedSum<T, PRESERVE_FIRST>
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
    fn add(&mut self, id: usize, value: T::Native) {
        // Integers can always add into the initial zero. Floating-point groups
        // retain their first value, including its signed-zero representation.
        self.values[id] = if PRESERVE_FIRST && !self.valid[id] {
            value
        } else {
            self.values[id].add_wrapping(value)
        };
        self.valid[id] = true;
    }

    fn update(&mut self, array: &PrimitiveArray<T>, ids: &[usize]) {
        let values = array.values();
        if array.null_count() == 0 {
            for (&id, &value) in ids.iter().zip(values.iter()) {
                self.add(id, value);
            }
        } else if array.null_count() > array.len() / 2 {
            // Sparse inputs are cheaper to visit by their set-bit positions.
            for index in array.nulls().unwrap().valid_indices() {
                self.add(ids[index], values[index]);
            }
        } else {
            // For dense inputs, traverse values and IDs sequentially, reusing
            // each 64-bit validity word. Arrow accounts for sliced bit offsets.
            let nulls = array.nulls().unwrap();
            let bits = nulls.inner().bit_chunks();
            let ids_chunks = ids.chunks_exact(64);
            let values_chunks = values.chunks_exact(64);
            let ids_remainder = ids_chunks.remainder();
            let values_remainder = values_chunks.remainder();
            for ((ids, values), mut mask) in ids_chunks.zip(values_chunks).zip(bits.iter()) {
                for (&id, value) in ids.iter().zip(values) {
                    if mask & 1 != 0 {
                        // Dereference only after validity; null payloads do not
                        // participate in arithmetic (notably floating NaNs).
                        self.add(id, *value);
                    }
                    mask >>= 1;
                }
            }
            let mut mask = bits.remainder_bits();
            for (&id, value) in ids_remainder.iter().zip(values_remainder) {
                if mask & 1 != 0 {
                    self.add(id, *value);
                }
                mask >>= 1;
            }
        }
    }

    fn state(&self) -> ArrayRef {
        Arc::new(PrimitiveArray::<T>::new(
            self.values.clone().into(),
            Some(NullBuffer::from(self.valid.clone())),
        ))
    }
}

impl SumGroups {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int64 => Self::Signed(TypedSum::new()),
            DataType::UInt64 => Self::Unsigned(TypedSum::new()),
            DataType::Float64 => Self::Float(TypedSum::new()),
            _ => Self::Unsupported(data_type.clone()),
        }
    }

    fn resize(&mut self, count: usize) {
        match self {
            Self::Signed(groups) => groups.resize(count),
            Self::Unsigned(groups) => groups.resize(count),
            Self::Float(groups) => groups.resize(count),
            Self::Unsupported(_) => {}
        }
    }

    fn update(&mut self, values: &ArrayRef, ids: &[usize]) -> Result<()> {
        match self {
            Self::Signed(groups) => groups.update(as_i64(values), ids),
            Self::Unsigned(groups) => groups.update(as_u64(values), ids),
            Self::Float(groups) => groups.update(as_f64(values), ids),
            Self::Unsupported(data_type) => {
                if values.null_count() != values.len() {
                    return Err(unsupported_sum_type(data_type));
                }
            }
        }
        Ok(())
    }

    fn update_single(&mut self, values: &ArrayRef) -> Result<()> {
        match self {
            Self::Signed(groups) => {
                if let Some(value) = sum(as_i64(values)) {
                    groups.add(0, value);
                }
            }
            Self::Unsigned(groups) => {
                if let Some(value) = sum(as_u64(values)) {
                    groups.add(0, value);
                }
            }
            Self::Float(groups) => {
                // Preserve the grouped path's first value (including -0.0)
                // and accumulation order across both rows and batches.
                for value in as_f64(values).iter().flatten() {
                    groups.add(0, value);
                }
            }
            Self::Unsupported(data_type) => return Err(unsupported_sum_type(data_type)),
        }
        Ok(())
    }

    fn state(&self) -> Result<ArrayRef> {
        Ok(match self {
            Self::Signed(groups) => groups.state(),
            Self::Unsigned(groups) => groups.state(),
            Self::Float(groups) => groups.state(),
            Self::Unsupported(data_type) => return Err(unsupported_sum_type(data_type)),
        })
    }
}
fn overflow() -> Error {
    Error::Execution("aggregate arithmetic overflow".into())
}
#[derive(Clone, Copy, Default)]
pub(crate) struct Covariance {
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
        Ok(match function {
            Count if distinct => {
                let input = input_type(inputs)?;
                Self::Distinct {
                    groups: vec![],
                    converter: RowConverter::new(vec![SortField::new(input)])?,
                }
            }
            Count => Self::Count(vec![]),
            Sum => Self::Sum {
                groups: SumGroups::new(output),
                data_type: output.clone(),
            },
            Avg => Self::Avg(vec![]),
            CovarPop => Self::Covar(vec![]),
            Min | Max => {
                let input = input_type(inputs)?;
                Self::Extremum {
                    groups: vec![],
                    converter: RowConverter::new(vec![SortField::new(input)])?,
                    data_type: output.clone(),
                    minimum: function == Min,
                }
            }
        })
    }
    pub fn resize(&mut self, count: usize) {
        match self {
            Self::Count(groups) => groups.resize(count, 0),
            Self::Distinct { groups, .. } => groups.resize_with(count, HashSet::new),
            Self::Sum { groups, .. } => groups.resize(count),
            Self::Avg(groups) => groups.resize(count, (0, 0.0)),
            Self::Covar(groups) => groups.resize(count, Covariance::default()),
            Self::Extremum { groups, .. } => groups.resize(count, None),
        }
    }
    pub fn update(&mut self, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
        match self {
            Self::Count(groups) => {
                let nulls = values.first().and_then(|v| v.logical_nulls());
                if let Some(nulls) = nulls.filter(|nulls| nulls.null_count() != 0) {
                    if nulls.null_count() > nulls.len() / 2 {
                        for index in nulls.valid_indices() {
                            let id = ids[index];
                            groups[id] = groups[id].checked_add(1).ok_or_else(overflow)?;
                        }
                    } else {
                        let bits = nulls.inner().bit_chunks();
                        let chunks = ids.chunks_exact(64);
                        let remainder = chunks.remainder();
                        for (ids, mut mask) in chunks.zip(bits.iter()) {
                            for &id in ids {
                                if mask & 1 != 0 {
                                    groups[id] = groups[id].checked_add(1).ok_or_else(overflow)?;
                                }
                                mask >>= 1;
                            }
                        }
                        let mut mask = bits.remainder_bits();
                        for &id in remainder {
                            if mask & 1 != 0 {
                                groups[id] = groups[id].checked_add(1).ok_or_else(overflow)?;
                            }
                            mask >>= 1;
                        }
                    }
                } else {
                    for &id in ids {
                        groups[id] = groups[id].checked_add(1).ok_or_else(overflow)?;
                    }
                }
            }
            Self::Distinct { groups, converter } => {
                let rows = converter.convert_columns(values)?;
                let nulls = values[0].logical_nulls();
                for (i, &id) in ids.iter().enumerate() {
                    if nulls.as_ref().is_none_or(|n| n.is_valid(i)) {
                        groups[id].insert(rows.row(i).as_ref().to_vec());
                    }
                }
            }
            Self::Sum { groups, data_type } => {
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                groups.update(&values, ids)?;
            }
            Self::Avg(groups) => {
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
            }
            Self::Covar(groups) => {
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
            }
            Self::Extremum {
                groups,
                converter,
                minimum,
                ..
            } => {
                let rows = converter.convert_columns(values)?;
                let nulls = values[0].logical_nulls();
                for (i, &id) in ids.iter().enumerate() {
                    if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                        continue;
                    }
                    let row = rows.row(i);
                    let replace = groups[id].as_ref().is_none_or(|(key, _)| {
                        if *minimum {
                            row.as_ref() < key.as_slice()
                        } else {
                            row.as_ref() > key.as_slice()
                        }
                    });
                    if replace {
                        // Retain the chosen row; Arrow may share backing buffers.
                        let value = arrow::compute::take(
                            values[0].as_ref(),
                            &UInt64Array::from(vec![i as u64]),
                            None,
                        )?;
                        groups[id] = Some((row.as_ref().to_vec(), value));
                    }
                }
            }
        }
        Ok(())
    }
    /// Reduce a whole batch into the sole global group without materializing IDs.
    pub fn update_single(&mut self, values: &[ArrayRef], rows: usize) -> Result<()> {
        match self {
            Self::Count(groups) => {
                let null_count = values
                    .first()
                    .and_then(|value| value.logical_nulls())
                    .map_or(0, |nulls| nulls.null_count());
                let count = i64::try_from(rows - null_count).map_err(|_| overflow())?;
                groups[0] = groups[0].checked_add(count).ok_or_else(overflow)?;
            }
            Self::Sum { groups, data_type } => {
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("sum requires one argument".into()))?;
                let values = cast_argument(argument, data_type)?;
                // Integer batch reductions retain wrapping overflow; floating
                // updates retain the grouped path's order and signed zeros.
                groups.update_single(&values)?;
            }
            Self::Avg(groups) => {
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("avg requires one argument".into()))?;
                let values = cast_argument(argument, &DataType::Float64)?;
                let values = as_f64(&values);
                // AVG's floating sum must accumulate into the existing state
                // in input order, just like grouped updates. A batch subtotal
                // would reassociate additions and change cancellation results.
                for value in values.iter().flatten() {
                    groups[0].0 = groups[0].0.checked_add(1).ok_or_else(overflow)?;
                    groups[0].1 += value;
                }
            }
            _ => self.update(values, &vec![0; rows])?,
        }
        Ok(())
    }
    pub fn state_types(&self) -> Vec<DataType> {
        match self {
            Self::Count(_) => vec![DataType::Int64],
            Self::Distinct { .. } => vec![DataType::List(Arc::new(Field::new(
                "item",
                DataType::Binary,
                true,
            )))],
            Self::Sum { data_type, .. } | Self::Extremum { data_type, .. } => {
                vec![data_type.clone()]
            }
            Self::Avg(_) => vec![DataType::UInt64, DataType::Float64],
            Self::Covar(_) => vec![
                DataType::UInt64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
        }
    }
    pub fn state(&self) -> Result<Vec<ArrayRef>> {
        Ok(match self {
            Self::Count(groups) => vec![Arc::new(Int64Array::from(groups.clone()))],
            Self::Distinct { groups, .. } => {
                let mut builder = ListBuilder::new(BinaryBuilder::new());
                for group in groups {
                    for key in group {
                        builder.values().append_value(key);
                    }
                    builder.append(true);
                }
                vec![Arc::new(builder.finish())]
            }
            Self::Sum { groups, .. } => vec![groups.state()?],
            // Both state columns are null for groups without contributing values,
            // matching grouped AVG states without confusing an empty sum with zero.
            Self::Avg(groups) => vec![
                Arc::new(UInt64Array::from_iter(
                    groups
                        .iter()
                        .map(|(count, _)| (*count != 0).then_some(*count)),
                )),
                Arc::new(Float64Array::from_iter(
                    groups
                        .iter()
                        .map(|(count, sum)| (*count != 0).then_some(*sum)),
                )),
            ],
            Self::Covar(groups) => vec![
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
            Self::Extremum {
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
        match self {
            Self::Count(groups) => {
                let values = as_i64(&states[0]);
                for (i, &id) in ids.iter().enumerate() {
                    groups[id] = groups[id]
                        .checked_add(values.value(i))
                        .ok_or_else(overflow)?;
                }
            }
            Self::Distinct { groups, .. } => {
                let lists = states[0].as_any().downcast_ref::<ListArray>().unwrap();
                for (i, &id) in ids.iter().enumerate() {
                    let values = lists.value(i);
                    let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
                    for key in values.iter().flatten() {
                        groups[id].insert(key.to_vec());
                    }
                }
            }
            Self::Avg(groups) => {
                let (counts, sums) = (as_u64(&states[0]), as_f64(&states[1]));
                for (i, &id) in ids.iter().enumerate() {
                    if counts.is_valid(i) {
                        groups[id].0 = groups[id]
                            .0
                            .checked_add(counts.value(i))
                            .ok_or_else(overflow)?;
                    }
                    if sums.is_valid(i) {
                        groups[id].1 += sums.value(i);
                    }
                }
            }
            Self::Covar(groups) => {
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
            Self::Sum { .. } | Self::Extremum { .. } => self.update(states, ids)?,
        }
        Ok(())
    }
    pub fn evaluate(&self) -> Result<ArrayRef> {
        Ok(match self {
            Self::Distinct { groups, .. } => Arc::new(Int64Array::from(
                groups
                    .iter()
                    .map(|s| i64::try_from(s.len()).map_err(|_| overflow()))
                    .collect::<Result<Vec<_>>>()?,
            )),
            Self::Avg(groups) => Arc::new(Float64Array::from(
                groups
                    .iter()
                    .map(|(count, sum)| (*count != 0).then(|| *sum / *count as f64))
                    .collect::<Vec<_>>(),
            )),
            Self::Covar(groups) => Arc::new(Float64Array::from(
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

fn as_i64(a: &ArrayRef) -> &Int64Array {
    a.as_any().downcast_ref().unwrap()
}
fn as_u64(a: &ArrayRef) -> &UInt64Array {
    a.as_any().downcast_ref().unwrap()
}
fn as_f64(a: &ArrayRef) -> &Float64Array {
    a.as_any().downcast_ref().unwrap()
}
fn unsupported_sum_type(data_type: &DataType) -> Error {
    Error::Execution(format!("unsupported sum result type: {data_type}"))
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
