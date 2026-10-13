//! Aggregate state helpers called from generated row code. Update dispatch is
//! bound once when the state is constructed; helpers never receive a batch.
use crate::{
    arrow::{array::*, datatypes::*},
    error::{Error, Result},
    expr::agg::AggregateFunction,
    kernel::scalar::{self, Cell, OwnedCell},
};
use std::{collections::HashSet, sync::Arc};
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

#[derive(Default)]
#[repr(C)]
pub(crate) struct Group {
    pub count: i64,
    pub sum: Cell,
    avg: (u64, f64),
    covar: Covariance,
    distinct: HashSet<Vec<u8>>,
    extremum: Option<(Vec<u8>, OwnedCell)>,
}
#[derive(Default)]
#[repr(C)]
pub(crate) struct GroupView {
    pub data: *mut Group,
    pub len: usize,
}
// The pointer is only used while its owning accumulator is exclusively borrowed.
unsafe impl Send for GroupView {}
#[repr(C)]
pub(crate) struct Accumulator {
    pub generated: GroupView,
    groups: Vec<Group>,
    function: AggregateFunction,
    distinct: bool,
    #[cfg(test)]
    float_inputs: Vec<fn(Cell) -> f64>,
    key: scalar::KeyEncoder,
    own: scalar::OwnValue,
    output: DataType,
    #[cfg(test)]
    update_row: fn(&mut Self, usize, &[Cell]) -> Result<()>,
}
impl Accumulator {
    pub fn new(
        function: AggregateFunction,
        distinct: bool,
        inputs: &[DataType],
        output: &DataType,
    ) -> Result<Self> {
        use AggregateFunction::*;
        if distinct && (function != Count || inputs.len() != 1) {
            return Err(Error::InvalidPlan("DISTINCT requires COUNT(expr)".into()));
        }
        let arity = match function {
            Count => inputs.len() <= 1,
            CovarPop => inputs.len() == 2,
            _ => inputs.len() == 1,
        };
        if !arity || inputs.iter().any(|t| !scalar::supported(t)) {
            return Err(Error::InvalidPlan("unsupported aggregate arguments".into()));
        }
        let valid_output = match function {
            Count => output == &DataType::Int64,
            Avg | CovarPop => output == &DataType::Float64,
            Min | Max => inputs.first() == Some(output),
            Sum => true,
        };
        if !valid_output {
            return Err(Error::InvalidPlan(
                "aggregate output type does not match its kernel".into(),
            ));
        }
        if function == Sum
            && !matches!(
                output,
                DataType::Int64 | DataType::UInt64 | DataType::Float64
            )
        {
            return Err(Error::InvalidPlan(format!(
                "unsupported SUM output type: {output}"
            )));
        }
        #[cfg(test)]
        let update_row = match function {
            Count if distinct => Self::distinct_row,
            Count => Self::count_row,
            Sum => match output {
                DataType::Float64 => Self::sum_float_row,
                DataType::Int64 | DataType::UInt64 => {
                    if matches!(inputs.first(), Some(DataType::Float32 | DataType::Float64)) {
                        Self::sum_float_integer_row
                    } else {
                        Self::sum_integer_row
                    }
                }
                _ => Self::unsupported_sum_row,
            },
            Avg => Self::avg_row,
            CovarPop => Self::covar_row,
            Min => Self::extremum_row::<true>,
            Max => Self::extremum_row::<false>,
        };
        #[cfg(test)]
        let float_inputs = if matches!(function, Sum | Avg | CovarPop) {
            inputs
                .iter()
                .map(scalar::bind_float)
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        let key = scalar::bind_key(inputs.first().unwrap_or(&DataType::Null))?;
        let own = scalar::bind_owner(inputs.first().unwrap_or(&DataType::Null));
        Ok(Self {
            generated: GroupView::default(),
            groups: vec![],
            function,
            distinct,
            #[cfg(test)]
            float_inputs,
            key,
            own,
            output: output.clone(),
            #[cfg(test)]
            update_row,
        })
    }
    pub fn resize(&mut self, count: usize) {
        if count > self.groups.len() {
            self.groups.resize_with(count, Group::default);
        }
        self.generated = GroupView {
            data: self.groups.as_mut_ptr(),
            len: self.groups.len(),
        };
    }
    /// Group slots, for updates applied to a whole chunk at once.
    pub(crate) fn groups_mut(&mut self) -> &mut [Group] {
        &mut self.groups
    }
    #[cfg(test)]
    pub(crate) fn update_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        self.resize(id + 1);
        (self.update_row)(self, id, values)
    }
    fn count_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        if values.first().is_none_or(|v| v.valid != 0) {
            self.groups[id].count = self.groups[id].count.checked_add(1).ok_or_else(overflow)?;
        }
        Ok(())
    }
    fn distinct_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        if values[0].valid != 0 {
            let mut key = vec![];
            (self.key)(values[0], &mut key);
            self.groups[id].distinct.insert(key);
        }
        Ok(())
    }
    fn sum_integer_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        let v = values[0];
        if v.valid != 0 {
            let sum = &mut self.groups[id].sum;
            sum.bits = sum.bits.wrapping_add(v.bits);
            sum.valid = 1;
        }
        Ok(())
    }
    #[cfg(test)]
    fn sum_float_integer_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        let v = values[0];
        let v = Cell {
            bits: (self.float_inputs[0])(v) as i64 as u64,
            ..v
        };
        self.sum_integer_row(id, &[v])
    }
    #[cfg(test)]
    fn sum_float_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        let v = values[0];
        if v.valid != 0 {
            let v = (self.float_inputs[0])(v);
            let sum = &mut self.groups[id].sum;
            sum.bits = if sum.valid == 0 {
                v
            } else {
                f64::from_bits(sum.bits) + v
            }
            .to_bits();
            sum.valid = 1;
        }
        Ok(())
    }
    #[cfg(test)]
    fn unsupported_sum_row(&mut self, _id: usize, _values: &[Cell]) -> Result<()> {
        Err(Error::Execution(format!(
            "unsupported sum result type: {}",
            self.output
        )))
    }
    #[cfg(test)]
    fn avg_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        if values[0].valid != 0 {
            let g = &mut self.groups[id].avg;
            g.0 = g.0.checked_add(1).ok_or_else(overflow)?;
            g.1 += (self.float_inputs[0])(values[0]);
        }
        Ok(())
    }
    #[cfg(test)]
    fn covar_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        if values.iter().all(|v| v.valid != 0) {
            self.groups[id].covar.update(
                (self.float_inputs[0])(values[0]),
                (self.float_inputs[1])(values[1]),
            )?;
        }
        Ok(())
    }
    fn extremum_row<const MIN: bool>(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        if values[0].valid == 0 {
            return Ok(());
        }
        let mut key = vec![];
        (self.key)(values[0], &mut key);
        let g = &mut self.groups[id].extremum;
        if g.as_ref()
            .is_none_or(|(old, _)| if MIN { key < *old } else { key > *old })
        {
            *g = Some((key, (self.own)(values[0])));
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn update(&mut self, values: &[ArrayRef], ids: &[usize]) -> Result<()> {
        let columns = values
            .iter()
            .map(scalar::decode)
            .collect::<Result<Vec<_>>>()?;
        for (row, &id) in ids.iter().enumerate() {
            let args = columns.iter().map(|c| c[row]).collect::<Vec<_>>();
            self.update_row(id, &args)?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn update_single(&mut self, values: &[ArrayRef], rows: usize) -> Result<()> {
        self.resize(1);
        self.update(values, &vec![0; rows])
    }
    pub fn state_types(&self) -> Vec<DataType> {
        use AggregateFunction::*;
        match self.function {
            Count if self.distinct => vec![DataType::List(Arc::new(Field::new(
                "item",
                DataType::Binary,
                true,
            )))],
            Count => vec![DataType::Int64],
            Sum | Min | Max => vec![self.output.clone()],
            Avg => vec![DataType::UInt64, DataType::Float64],
            CovarPop => vec![
                DataType::UInt64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
        }
    }
    pub fn state(&self) -> Result<Vec<ArrayRef>> {
        use AggregateFunction::*;
        Ok(match self.function {
            Count if self.distinct => {
                let mut b = ListBuilder::new(BinaryBuilder::new());
                for g in &self.groups {
                    for k in &g.distinct {
                        b.values().append_value(k);
                    }
                    b.append(true);
                }
                vec![Arc::new(b.finish())]
            }
            Count => vec![Arc::new(Int64Array::from_iter_values(
                self.groups.iter().map(|g| g.count),
            ))],
            Sum => {
                if !matches!(
                    self.output,
                    DataType::Int64 | DataType::UInt64 | DataType::Float64
                ) {
                    return Err(Error::Execution(format!(
                        "unsupported sum result type: {}",
                        self.output
                    )));
                }
                vec![scalar::encode(
                    &self.output,
                    &self.groups.iter().map(|g| g.sum).collect::<Vec<_>>(),
                )?]
            }
            Min | Max => vec![scalar::encode(
                &self.output,
                &self
                    .groups
                    .iter()
                    .map(|g| {
                        g.extremum
                            .as_ref()
                            .map_or(Cell::default(), |(_, v)| v.get())
                    })
                    .collect::<Vec<_>>(),
            )?],
            Avg => vec![
                Arc::new(UInt64Array::from_iter(
                    self.groups
                        .iter()
                        .map(|g| (g.avg.0 != 0).then_some(g.avg.0)),
                )),
                Arc::new(Float64Array::from_iter(
                    self.groups
                        .iter()
                        .map(|g| (g.avg.0 != 0).then_some(g.avg.1)),
                )),
            ],
            CovarPop => vec![
                Arc::new(UInt64Array::from_iter_values(
                    self.groups.iter().map(|g| g.covar.count),
                )),
                Arc::new(Float64Array::from_iter_values(
                    self.groups.iter().map(|g| g.covar.mean_x),
                )),
                Arc::new(Float64Array::from_iter_values(
                    self.groups.iter().map(|g| g.covar.mean_y),
                )),
                Arc::new(Float64Array::from_iter_values(
                    self.groups.iter().map(|g| g.covar.co_moment),
                )),
            ],
        })
    }
    pub fn merge(&mut self, states: &[ArrayRef], ids: &[usize]) -> Result<()> {
        use AggregateFunction::*;
        if self.function == Count && self.distinct {
            let lists = states[0].as_any().downcast_ref::<ListArray>().unwrap();
            for (i, &id) in ids.iter().enumerate() {
                let a = lists.value(i);
                let a = a.as_any().downcast_ref::<BinaryArray>().unwrap();
                for k in a.iter().flatten() {
                    self.groups[id].distinct.insert(k.to_vec());
                }
            }
            return Ok(());
        }
        let cols = states
            .iter()
            .map(scalar::decode)
            .collect::<Result<Vec<_>>>()?;
        for (i, &id) in ids.iter().enumerate() {
            let g = &mut self.groups[id];
            match self.function {
                Count => {
                    g.count = g
                        .count
                        .checked_add(cols[0][i].bits as i64)
                        .ok_or_else(overflow)?
                }
                Avg => {
                    if cols[0][i].valid != 0 {
                        g.avg.0 = g.avg.0.checked_add(cols[0][i].bits).ok_or_else(overflow)?;
                    }
                    if cols[1][i].valid != 0 {
                        g.avg.1 += f64::from_bits(cols[1][i].bits);
                    }
                }
                CovarPop => g.covar.merge(Covariance {
                    count: cols[0][i].bits,
                    mean_x: f64::from_bits(cols[1][i].bits),
                    mean_y: f64::from_bits(cols[2][i].bits),
                    co_moment: f64::from_bits(cols[3][i].bits),
                })?,
                Sum => {
                    let v = cols[0][i];
                    if v.valid != 0 {
                        if self.output == DataType::Float64 {
                            let v = f64::from_bits(v.bits);
                            g.sum.bits = if g.sum.valid == 0 {
                                v
                            } else {
                                f64::from_bits(g.sum.bits) + v
                            }
                            .to_bits();
                        } else {
                            g.sum.bits = g.sum.bits.wrapping_add(v.bits);
                        }
                        g.sum.valid = 1;
                    }
                }
                Min => self.extremum_row::<true>(id, &[cols[0][i]])?,
                Max => self.extremum_row::<false>(id, &[cols[0][i]])?,
            }
        }
        Ok(())
    }
    pub fn evaluate(&self) -> Result<ArrayRef> {
        use AggregateFunction::*;
        Ok(match self.function {
            Count if self.distinct => Arc::new(Int64Array::from(
                self.groups
                    .iter()
                    .map(|g| i64::try_from(g.distinct.len()).map_err(|_| overflow()))
                    .collect::<Result<Vec<_>>>()?,
            )),
            Avg => Arc::new(Float64Array::from_iter(
                self.groups
                    .iter()
                    .map(|g| (g.avg.0 != 0).then(|| g.avg.1 / g.avg.0 as f64)),
            )),
            CovarPop => Arc::new(Float64Array::from_iter(self.groups.iter().map(|g| {
                (g.covar.count != 0).then(|| g.covar.co_moment / g.covar.count as f64)
            }))),
            _ => self.state()?.remove(0),
        })
    }
}

/// Monomorphized scalar entry points. Production generated code binds one of
/// these directly; the state contains no row-level operation dispatcher.
pub(crate) trait RowKernel {
    fn update(a: &mut Accumulator, id: usize, values: &[Cell]) -> Result<()>;
}
macro_rules! row_kernel {
    ($name:ident,$method:ident) => {
        pub(crate) struct $name;
        impl RowKernel for $name {
            fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
                a.$method(id, v)
            }
        }
    };
}
row_kernel!(CountValues, count_row);
row_kernel!(CountDistinct, distinct_row);
row_kernel!(SumInteger, sum_integer_row);
pub(crate) struct CountRows;
impl RowKernel for CountRows {
    fn update(a: &mut Accumulator, id: usize, _: &[Cell]) -> Result<()> {
        a.groups[id].count = a.groups[id].count.checked_add(1).ok_or_else(overflow)?;
        Ok(())
    }
}
pub(crate) struct SumFloat;
impl RowKernel for SumFloat {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        if v[0].valid != 0 {
            let value = f64::from_bits(v[0].bits);
            let sum = &mut a.groups[id].sum;
            sum.bits = if sum.valid == 0 {
                value
            } else {
                f64::from_bits(sum.bits) + value
            }
            .to_bits();
            sum.valid = 1;
        }
        Ok(())
    }
}
pub(crate) struct Average;
impl RowKernel for Average {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        if v[0].valid != 0 {
            let g = &mut a.groups[id].avg;
            g.0 = g.0.checked_add(1).ok_or_else(overflow)?;
            g.1 += f64::from_bits(v[0].bits);
        }
        Ok(())
    }
}
pub(crate) struct CovarianceUpdate;
impl RowKernel for CovarianceUpdate {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        if v[0].valid != 0 && v[1].valid != 0 {
            a.groups[id]
                .covar
                .update(f64::from_bits(v[0].bits), f64::from_bits(v[1].bits))?;
        }
        Ok(())
    }
}
pub(crate) struct Minimum;
impl RowKernel for Minimum {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        a.extremum_row::<true>(id, v)
    }
}
pub(crate) struct Maximum;
impl RowKernel for Maximum {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        a.extremum_row::<false>(id, v)
    }
}
