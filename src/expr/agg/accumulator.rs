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
        Array, ArrayRef, BinaryArray, BinaryBuilder, Float64Array, Int64Array, ListArray,
        ListBuilder, UInt64Array, new_empty_array, new_null_array,
    },
    compute::{cast, kernels::interleave::interleave},
    datatypes::{DataType, Field},
    row::{RowConverter, SortField},
};
use std::{collections::HashSet, sync::Arc};

fn input_type(inputs: &[DataType]) -> Result<DataType> {
    inputs
        .first()
        .cloned()
        .ok_or_else(|| Error::InvalidPlan("aggregate requires an argument".into()))
}

pub(super) enum Accumulator {
    Count(Vec<i64>),
    Distinct {
        groups: Vec<HashSet<Vec<u8>>>,
        converter: RowConverter,
    },
    Sum {
        groups: Vec<Option<Number>>,
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
#[derive(Clone, Copy)]
pub(super) enum Number {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}
impl Number {
    fn add(self, other: Self) -> Result<Self> {
        match (self, other) {
            (Self::Signed(a), Self::Signed(b)) => {
                Ok(Self::Signed(a.checked_add(b).ok_or_else(overflow)?))
            }
            (Self::Unsigned(a), Self::Unsigned(b)) => {
                Ok(Self::Unsigned(a.checked_add(b).ok_or_else(overflow)?))
            }
            (Self::Float(a), Self::Float(b)) => Ok(Self::Float(a + b)),
            _ => Err(Error::Execution(
                "incompatible numeric aggregate states".into(),
            )),
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
                groups: vec![],
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
            Self::Sum { groups, .. } => groups.resize(count, None),
            Self::Avg(groups) => groups.resize(count, (0, 0.0)),
            Self::Covar(groups) => groups.resize(count, Covariance::default()),
            Self::Extremum { groups, .. } => groups.resize(count, None),
        }
    }
    pub fn update(&mut self, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
        match self {
            Self::Count(groups) => {
                let nulls = values.first().and_then(|v| v.logical_nulls());
                for (i, &id) in ids.iter().enumerate() {
                    if nulls.as_ref().is_none_or(|n| n.is_valid(i)) {
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
                let values = cast(argument.as_ref(), data_type)?;
                for (i, &id) in ids.iter().enumerate() {
                    if values.is_null(i) {
                        continue;
                    }
                    let number = match data_type {
                        DataType::Int64 => Number::Signed(as_i64(&values).value(i)),
                        DataType::UInt64 => Number::Unsigned(as_u64(&values).value(i)),
                        DataType::Float64 => Number::Float(as_f64(&values).value(i)),
                        _ => return Err(unsupported_sum_type(data_type)),
                    };
                    groups[id] = Some(match groups[id] {
                        Some(old) => old.add(number)?,
                        None => number,
                    });
                }
            }
            Self::Avg(groups) => {
                let argument = values
                    .first()
                    .ok_or_else(|| Error::Execution("avg requires one argument".into()))?;
                let values = cast(argument.as_ref(), &DataType::Float64)?;
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
                let x = cast(x.as_ref(), &DataType::Float64)?;
                let y = cast(y.as_ref(), &DataType::Float64)?;
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
            Self::Sum { groups, data_type } => vec![number_array(groups, data_type)?],
            Self::Avg(groups) => vec![
                Arc::new(UInt64Array::from_iter_values(groups.iter().map(|g| g.0))),
                Arc::new(Float64Array::from_iter_values(groups.iter().map(|g| g.1))),
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
                    groups[id].0 = groups[id]
                        .0
                        .checked_add(counts.value(i))
                        .ok_or_else(overflow)?;
                    groups[id].1 += sums.value(i);
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

fn number_array(groups: &[Option<Number>], data_type: &DataType) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Int64 => Arc::new(Int64Array::from(
            groups
                .iter()
                .map(|n| match n {
                    Some(Number::Signed(v)) => Some(*v),
                    None => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            groups
                .iter()
                .map(|n| match n {
                    Some(Number::Unsigned(v)) => Some(*v),
                    None => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            groups
                .iter()
                .map(|n| match n {
                    Some(Number::Float(v)) => Some(*v),
                    None => None,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        _ => return Err(unsupported_sum_type(data_type)),
    })
}
