//! Aggregate state helpers called from generated row code. Update dispatch is
//! bound once when the state is constructed; helpers never receive a batch.
use crate::{
    arrow::{array::*, datatypes::*},
    error::{Error, ErrorKind, Result},
    expr::agg::AggregateFunction,
    kernel::scalar::{self, Cell, OwnedCell},
};
use std::{collections::HashSet, sync::Arc};
#[track_caller]
fn overflow() -> Error {
    Error::new(
        ErrorKind::ArithmeticOverflow,
        "aggregate arithmetic overflow",
    )
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
    /// The scale of a decimal input.
    input_scale: i8,
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
        if distinct && function != Count {
            return Err(Error::unsupported(
                "DISTINCT is supported only for COUNT".into(),
            ));
        }
        if distinct && inputs.len() != 1 {
            return Err(Error::invalid_plan("DISTINCT requires COUNT(expr)".into()));
        }
        let arity = match function {
            Count => inputs.len() <= 1,
            CovarPop => inputs.len() == 2,
            _ => inputs.len() == 1,
        };
        if !arity {
            return Err(Error::invalid_plan(format!(
                "{function:?} does not take {} arguments",
                inputs.len()
            )));
        }
        if let Some(t) = inputs.iter().find(|t| !scalar::supported(t)) {
            return Err(Error::unsupported(format!(
                "unsupported aggregate argument type: {t}"
            )));
        }
        let decimal_average = matches!(
            (inputs.first(), output),
            (Some(DataType::Decimal128(..)), DataType::Decimal128(..))
        );
        let valid_output = match function {
            Count => output == &DataType::Int64,
            Avg => output == &DataType::Float64 || decimal_average,
            CovarPop | StddevSamp => output == &DataType::Float64,
            Min | Max => inputs.first() == Some(output),
            Sum => true,
        };
        if !valid_output {
            return Err(Error::invalid_plan(
                "aggregate output type does not match its kernel".into(),
            ));
        }
        if function == Sum
            && !matches!(
                output,
                DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Decimal128(..)
            )
        {
            return Err(Error::unsupported(format!(
                "unsupported sum result type: {output}"
            )));
        }
        let input_scale = match inputs.first() {
            Some(DataType::Decimal128(_, s)) => *s,
            _ => 0,
        };
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
                DataType::Decimal128(..) => Self::sum_wide_row,
                _ => Self::unsupported_sum_row,
            },
            Avg if decimal_average => Self::avg_wide_row,
            Avg => Self::avg_row,
            CovarPop => Self::covar_row,
            StddevSamp => Self::variance_row,
            Min => Self::extremum_row::<true>,
            Max => Self::extremum_row::<false>,
        };
        #[cfg(test)]
        let float_inputs = if matches!(function, Sum | Avg | CovarPop | StddevSamp) {
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
            input_scale,
            output: output.clone(),
            #[cfg(test)]
            update_row,
        })
    }
    fn decimal_average(&self) -> bool {
        self.function == AggregateFunction::Avg && matches!(self.output, DataType::Decimal128(..))
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
        Err(Error::internal(format!(
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
    fn sum_wide_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        let v = values[0];
        if v.valid != 0 {
            let sum = &mut self.groups[id].sum;
            *sum = Cell::wide(sum.value128().wrapping_add(v.value128()));
        }
        Ok(())
    }
    fn avg_wide_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        let v = values[0];
        if v.valid != 0 {
            let g = &mut self.groups[id];
            g.count = g.count.checked_add(1).ok_or_else(overflow)?;
            g.sum = Cell::wide(g.sum.value128().wrapping_add(v.value128()));
        }
        Ok(())
    }
    fn variance_row(&mut self, id: usize, values: &[Cell]) -> Result<()> {
        if values[0].valid != 0 {
            let x = f64::from_bits(values[0].bits);
            self.groups[id].covar.update(x, x)?;
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
            Avg if self.decimal_average() => {
                vec![DataType::Int64, DataType::Decimal128(38, self.input_scale)]
            }
            Avg => vec![DataType::UInt64, DataType::Float64],
            CovarPop | StddevSamp => vec![
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
                    DataType::Int64
                        | DataType::UInt64
                        | DataType::Float64
                        | DataType::Decimal128(..)
                ) {
                    return Err(Error::internal(format!(
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
            Avg if self.decimal_average() => vec![
                Arc::new(Int64Array::from_iter_values(
                    self.groups.iter().map(|g| g.count),
                )),
                Arc::new(
                    Decimal128Array::from_iter_values(self.groups.iter().map(|g| g.sum.value128()))
                        .with_data_type(DataType::Decimal128(38, self.input_scale)),
                ),
            ],
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
            CovarPop | StddevSamp => vec![
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
        let decimal_average = self.decimal_average();
        for (i, &id) in ids.iter().enumerate() {
            let g = &mut self.groups[id];
            match self.function {
                Count => {
                    g.count = g
                        .count
                        .checked_add(cols[0][i].bits as i64)
                        .ok_or_else(overflow)?
                }
                Avg if decimal_average => {
                    g.count = g
                        .count
                        .checked_add(cols[0][i].bits as i64)
                        .ok_or_else(overflow)?;
                    g.sum = Cell::wide(g.sum.value128().wrapping_add(cols[1][i].value128()));
                }
                Avg => {
                    if cols[0][i].valid != 0 {
                        g.avg.0 = g.avg.0.checked_add(cols[0][i].bits).ok_or_else(overflow)?;
                    }
                    if cols[1][i].valid != 0 {
                        g.avg.1 += f64::from_bits(cols[1][i].bits);
                    }
                }
                CovarPop | StddevSamp => g.covar.merge(Covariance {
                    count: cols[0][i].bits,
                    mean_x: f64::from_bits(cols[1][i].bits),
                    mean_y: f64::from_bits(cols[2][i].bits),
                    co_moment: f64::from_bits(cols[3][i].bits),
                })?,
                Sum => {
                    let v = cols[0][i];
                    if v.valid != 0 {
                        if matches!(self.output, DataType::Decimal128(..)) {
                            g.sum = Cell::wide(g.sum.value128().wrapping_add(v.value128()));
                        } else if self.output == DataType::Float64 {
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
            Avg if self.decimal_average() => {
                let DataType::Decimal128(precision, scale) = self.output else {
                    unreachable!("checked when constructed");
                };
                // sum * 10^(result scale - input scale) / count, truncated.
                let shift = 10i128.pow((scale - self.input_scale).max(0) as u32);
                let bound = 10i128.pow(precision as u32);
                let values = self
                    .groups
                    .iter()
                    .map(|g| {
                        if g.count == 0 {
                            return Ok(None);
                        }
                        let v = g.sum.value128().checked_mul(shift).ok_or_else(overflow)?
                            / g.count as i128;
                        if v.unsigned_abs() >= bound as u128 {
                            return Err(overflow());
                        }
                        Ok(Some(v))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(Decimal128Array::from(values).with_data_type(self.output.clone()))
            }
            StddevSamp => Arc::new(Float64Array::from_iter(self.groups.iter().map(|g| {
                (g.covar.count > 1).then(|| (g.covar.co_moment / (g.covar.count - 1) as f64).sqrt())
            }))),
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
pub(crate) struct SumWide;
impl RowKernel for SumWide {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        a.sum_wide_row(id, v)
    }
}
pub(crate) struct AverageWide;
impl RowKernel for AverageWide {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        a.avg_wide_row(id, v)
    }
}
pub(crate) struct VarianceUpdate;
impl RowKernel for VarianceUpdate {
    fn update(a: &mut Accumulator, id: usize, v: &[Cell]) -> Result<()> {
        a.variance_row(id, v)
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

#[cfg(test)]
mod tests {
    use super::Accumulator;
    use crate::arrow::{
        array::{Array, ArrayRef, AsArray, BooleanArray},
        compute::filter_record_batch,
        datatypes::DataType,
        record_batch::RecordBatch,
    };
    use crate::column::{ColumnId, ColumnType};
    use crate::expr::scalar::{CastMode, test_support::TestPlan};
    use crate::kernel::{KernelBuilder, LocalState, Program};
    use crate::{
        error::{Error, Result},
        expr::agg::AggregateExpression,
    };
    use asyncband::shutdown::{Shutdown, ShutdownGuard};
    use std::sync::Arc;

    /// Drives one accumulator with caller-provided group IDs. FILTER and the
    /// coerced arguments are evaluated by programs over the plan's input columns;
    /// arguments only see rows kept by FILTER.
    pub struct AccumulatorTestHarness {
        expression: Arc<AggregateExpression>,
        filter: Option<(Program, LocalState)>,
        arguments: (Program, LocalState),
        accumulator: Accumulator,
        _shutdown: Shutdown,
        guard: ShutdownGuard,
    }

    impl AccumulatorTestHarness {
        pub fn try_new(plan: &TestPlan, expression: Arc<AggregateExpression>) -> Result<Self> {
            let mut columns = plan.columns.clone();
            let accumulator = expression.accumulator(&columns)?;
            let coerced = expression
                .input_types(&columns)?
                .into_iter()
                .map(|t| columns.allocate(ColumnType::new(t, true)))
                .collect::<Vec<ColumnId>>();
            let filter = expression
                .filter()
                .map(|filter| {
                    let mut builder = KernelBuilder::new(&columns);
                    builder.bind_inputs(&plan.inputs)?;
                    let errors = filter.build_kernels(&mut builder)?;
                    builder.raise(errors)?;
                    let program = Program::compile(builder.finish(Some(&[filter.output()]))?)?;
                    let local = program.new_local()?;
                    Ok::<_, Error>((program, local))
                })
                .transpose()?;
            let mut builder = KernelBuilder::new(&columns);
            builder.bind_inputs(&plan.inputs)?;
            for (argument, &column) in expression.arguments().iter().zip(&coerced) {
                let errors = argument.build_kernels(&mut builder)?;
                builder.raise(errors)?;
                let binding = builder.column_binding(argument.output())?;
                let from = columns.column_type(argument.output())?.data_type.clone();
                let to = columns.column_type(column)?.data_type.clone();
                let (binding, errors) =
                    builder.cast_binding(&binding, &from, &to, CastMode::Try)?;
                builder.bind_column(column, binding, errors)?;
            }
            let program = Program::compile(builder.finish(Some(&coerced))?)?;
            let local = program.new_local()?;
            let (shutdown, guard) = asyncband::shutdown::new();
            Ok(Self {
                expression,
                filter,
                arguments: (program, local),
                accumulator,
                _shutdown: shutdown,
                guard,
            })
        }
        pub fn state_types(&self) -> Vec<DataType> {
            self.accumulator.state_types()
        }
        pub fn resize(&mut self, count: usize) {
            self.accumulator.resize(count);
        }
        pub fn update(&mut self, input: &RecordBatch, ids: &[usize], groups: usize) -> Result<()> {
            if ids.len() != input.num_rows() || ids.iter().any(|&id| id >= groups) {
                return Err(Error::invalid_input(
                    "aggregate group IDs do not match input".into(),
                ));
            }
            self.resize(groups);
            let (input, selected) = self.select(input)?;
            let ids = match selected {
                Some(rows) => rows.into_iter().map(|i| ids[i]).collect(),
                None => ids.to_vec(),
            };
            if ids.is_empty() {
                return Ok(());
            }
            let values = self.evaluate_arguments(&input)?;
            self.accumulator.update(&values, &ids)
        }
        /// Updates an ungrouped aggregate, retaining one state even for empty input.
        pub fn update_single(&mut self, input: &RecordBatch) -> Result<()> {
            self.resize(1);
            let (input, _) = self.select(input)?;
            if input.num_rows() == 0 {
                return Ok(());
            }
            let values = self.evaluate_arguments(&input)?;
            self.accumulator.update_single(&values, input.num_rows())
        }
        /// Applies FILTER, returning the kept rows and their original positions.
        fn select(&mut self, input: &RecordBatch) -> Result<(RecordBatch, Option<Vec<usize>>)> {
            let Some((program, local)) = &mut self.filter else {
                return Ok((input.clone(), None));
            };
            let output = program.run(input, local, &self.guard)?.unwrap();
            let mask = output.column(0).as_boolean();
            let rows = (0..mask.len())
                .filter(|&i| mask.is_valid(i) && mask.value(i))
                .collect::<Vec<_>>();
            let mut keep = vec![false; input.num_rows()];
            for &row in &rows {
                keep[row] = true;
            }
            let input =
                filter_record_batch(input, &BooleanArray::from(keep)).map_err(|source| {
                    Error::internal("FILTER mask failed".into()).with_source(source)
                })?;
            Ok((input, Some(rows)))
        }
        fn evaluate_arguments(&mut self, input: &RecordBatch) -> Result<Vec<ArrayRef>> {
            let (program, local) = &mut self.arguments;
            Ok(program
                .run(input, local, &self.guard)?
                .unwrap()
                .columns()
                .to_vec())
        }

        pub fn merge(&mut self, state: &[ArrayRef], ids: &[usize], groups: usize) -> Result<()> {
            self.resize(groups);
            let types = self.state_types();
            if ids.iter().any(|&id| id >= groups)
                || state.len() != types.len()
                || state
                    .iter()
                    .zip(types)
                    .any(|(a, t)| a.len() != ids.len() || a.data_type() != &t)
            {
                return Err(Error::invalid_input(format!(
                    "invalid partial state for {:?}",
                    self.expression.function()
                )));
            }
            self.accumulator.merge(state, ids)
        }
        pub fn state(&self) -> Result<Vec<ArrayRef>> {
            self.accumulator.state()
        }
        pub fn evaluate(&self) -> Result<ArrayRef> {
            self.accumulator.evaluate()
        }
    }

    mod harness {
        use super::*;
        use crate::arrow::array::{Float64Array, Int64Array};
        use crate::expr::agg::AggregateFunction;
        use crate::expr::scalar::{
            ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
        };

        fn input(plan: &TestPlan, columns: Vec<ArrayRef>) -> RecordBatch {
            RecordBatch::try_new(plan.columns.schema(&plan.inputs).unwrap(), columns).unwrap()
        }

        #[test]
        fn filtered_arguments_and_constants_follow_original_group_ids() {
            let mut plan = TestPlan::new(&[(DataType::Int64, true), (DataType::Boolean, false)]);
            let input = input(
                &plan,
                vec![
                    Arc::new(Int64Array::from(vec![
                        Some(40),
                        Some(10),
                        Some(40),
                        None,
                        Some(20),
                    ])),
                    Arc::new(BooleanArray::from(vec![true, true, true, true, false])),
                ],
            );
            let ids = [1, 0, 1, 0, 1];
            let three = plan.column(DataType::Int64, false);
            for (argument, expected) in [
                (
                    ReferenceExpression::new(plan.input(0)).into_ref(),
                    vec![Some(10), Some(80)],
                ),
                (
                    ConstantExpression::int64(Some(3), three).into_ref(),
                    vec![Some(6), Some(6)],
                ),
            ] {
                let output = plan.column(DataType::Int64, true);
                let expression = Arc::new(
                    AggregateExpression::new(AggregateFunction::Sum, vec![argument], output)
                        .with_filter(ReferenceExpression::new(plan.input(1)).into_ref()),
                );
                let mut executor = AccumulatorTestHarness::try_new(&plan, expression).unwrap();
                executor.update(&input, &ids, 2).unwrap();
                let result = executor.evaluate().unwrap();
                assert_eq!(
                    result
                        .as_primitive::<crate::arrow::datatypes::Int64Type>()
                        .iter()
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            let output = plan.column(DataType::Int64, false);
            let expression = Arc::new(AggregateExpression::new(
                AggregateFunction::Count,
                vec![],
                output,
            ));
            let mut executor = AccumulatorTestHarness::try_new(&plan, expression).unwrap();
            executor.update(&input, &ids, 2).unwrap();
            assert_eq!(
                executor
                    .evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(2), Some(3)]
            );
            assert!(
                executor
                    .merge(&executor.state().unwrap(), &[0, 2], 2)
                    .is_err()
            );
        }

        #[test]
        fn rejected_rows_skip_fallible_arguments() {
            let mut plan = TestPlan::new(&[(DataType::Int64, false)]);
            let input = input(&plan, vec![Arc::new(Int64Array::from(vec![0, 0]))]);
            let eight = plan.column(DataType::Int64, false);
            let quotient = plan.column(DataType::Int64, false);
            let never = plan.column(DataType::Boolean, false);
            let output = plan.column(DataType::Int64, true);
            let expression = AggregateExpression::new(
                AggregateFunction::Sum,
                vec![
                    FunctionExpression::binary(
                        FunctionKind::Divide,
                        ConstantExpression::int64(Some(8), eight).into_ref(),
                        ReferenceExpression::new(plan.input(0)).into_ref(),
                        quotient,
                    )
                    .into_ref(),
                ],
                output,
            );
            let mut unfiltered =
                AccumulatorTestHarness::try_new(&plan, Arc::new(expression.clone())).unwrap();
            assert!(unfiltered.update_single(&input).is_err());
            let expression =
                expression.with_filter(ConstantExpression::boolean(Some(false), never).into_ref());
            let mut filtered =
                AccumulatorTestHarness::try_new(&plan, Arc::new(expression)).unwrap();
            filtered.update(&input, &[0, 0], 1).unwrap();
            filtered.update_single(&input).unwrap();
            assert!(filtered.evaluate().unwrap().is_null(0));
        }

        #[test]
        fn mixed_bound_and_computed_arguments_use_filtered_input() {
            let mut plan = TestPlan::new(&[(DataType::Int64, false), (DataType::Boolean, false)]);
            let input = input(
                &plan,
                vec![
                    Arc::new(Int64Array::from(vec![0, 2, 4])),
                    Arc::new(BooleanArray::from(vec![false, true, true])),
                ],
            );
            let reference = ReferenceExpression::new(plan.input(0)).into_ref();
            let eight = plan.column(DataType::Int64, false);
            let quotient = plan.column(DataType::Int64, false);
            let output = plan.column(DataType::Float64, true);
            let divided = FunctionExpression::binary(
                FunctionKind::Divide,
                ConstantExpression::int64(Some(8), eight).into_ref(),
                reference.clone(),
                quotient,
            )
            .into_ref();
            let expression = Arc::new(
                AggregateExpression::new(
                    AggregateFunction::CovarPop,
                    vec![reference, divided],
                    output,
                )
                .with_filter(ReferenceExpression::new(plan.input(1)).into_ref()),
            );
            let mut grouped = AccumulatorTestHarness::try_new(&plan, expression.clone()).unwrap();
            let mut single = AccumulatorTestHarness::try_new(&plan, expression).unwrap();
            grouped.update(&input, &[0, 0, 0], 1).unwrap();
            single.update_single(&input).unwrap();
            for executor in [grouped, single] {
                assert_eq!(
                    executor
                        .evaluate()
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    -1.0
                );
            }
        }
    }

    mod optimized {
        use super::AccumulatorTestHarness;
        use std::sync::Arc;

        use crate::arrow::{
            array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, UInt64Array},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use crate::expr::{
            agg::{AggregateExpression, AggregateFunction},
            scalar::{
                ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression,
                test_support::TestPlan,
            },
        };

        fn batch(array: ArrayRef) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    array.data_type().clone(),
                    true,
                )])),
                vec![array],
            )
            .unwrap()
        }

        /// `function(value)` over one nullable input column.
        fn expression(
            function: AggregateFunction,
            input: DataType,
            output: DataType,
        ) -> (TestPlan, AggregateExpression) {
            let mut plan = TestPlan::new(&[(input, true)]);
            let output = plan.column(output, true);
            let expression = AggregateExpression::new(
                function,
                vec![ReferenceExpression::new(plan.input(0)).into_ref()],
                output,
            );
            (plan, expression)
        }

        fn executor((plan, expression): (TestPlan, AggregateExpression)) -> AccumulatorTestHarness {
            AccumulatorTestHarness::try_new(&plan, Arc::new(expression)).unwrap()
        }

        #[test]
        fn global_reduction_matches_grouped_path_across_batches_and_nulls() {
            let batches = [
                batch(Arc::new(Int64Array::from(vec![
                    None,
                    Some(4),
                    Some(-4),
                    None,
                ]))),
                batch(Arc::new(Int64Array::from(vec![Some(9), Some(9), None]))),
                batch(Arc::new(Int64Array::from(vec![None, None]))),
                batch(Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))),
            ];
            for (function, output) in [
                (AggregateFunction::Count, DataType::Int64),
                (AggregateFunction::Sum, DataType::Int64),
                (AggregateFunction::Avg, DataType::Float64),
                (AggregateFunction::Min, DataType::Int64),
                (AggregateFunction::Max, DataType::Int64),
            ] {
                let expr = expression(function, DataType::Int64, output);
                let mut single = executor(expr.clone());
                let mut grouped = executor(expr);
                for batch in &batches {
                    single.update_single(batch).unwrap();
                    grouped
                        .update(batch, &vec![0; batch.num_rows()], 1)
                        .unwrap();
                    assert_eq!(
                        single.evaluate().unwrap().to_data(),
                        grouped.evaluate().unwrap().to_data()
                    );
                    for (left, right) in
                        single.state().unwrap().iter().zip(grouped.state().unwrap())
                    {
                        assert_eq!(left.to_data(), right.to_data());
                    }
                }
            }
            let (plan, count) =
                expression(AggregateFunction::Count, DataType::Int64, DataType::Int64);
            let mut distinct = executor((plan, count.with_distinct()));
            for batch in &batches {
                distinct.update_single(batch).unwrap();
            }
            assert_eq!(
                distinct
                    .evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                3
            );
        }

        #[test]
        fn global_integer_sums_wrap_within_and_between_batches() {
            for array in [
                Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(1)])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(1)])),
            ] {
                let kind = array.data_type().clone();
                let mut single = executor(expression(AggregateFunction::Sum, kind.clone(), kind));
                single.update_single(&batch(array.clone())).unwrap();
                single.update_single(&batch(array)).unwrap();
                let output = single.evaluate().unwrap();
                match output.data_type() {
                    DataType::Int64 => assert_eq!(
                        output
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                        0
                    ),
                    DataType::UInt64 => assert_eq!(
                        output
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(0),
                        0
                    ),
                    _ => unreachable!(),
                }
            }
        }

        #[test]
        fn global_empty_states_and_checked_counts_are_preserved() {
            let nulls = batch(Arc::new(Int64Array::from(vec![None, None])));
            let mut sum = executor(expression(
                AggregateFunction::Sum,
                DataType::Int64,
                DataType::Int64,
            ));
            sum.update_single(&nulls).unwrap();
            assert!(sum.evaluate().unwrap().is_null(0));
            let mut avg = executor(expression(
                AggregateFunction::Avg,
                DataType::Int64,
                DataType::Float64,
            ));
            avg.update_single(&nulls).unwrap();
            assert!(avg.evaluate().unwrap().is_null(0));
            assert!(avg.state().unwrap().iter().all(|state| state.is_null(0)));
            let mut count = executor(expression(
                AggregateFunction::Count,
                DataType::Int64,
                DataType::Int64,
            ));
            count
                .merge(&[Arc::new(Int64Array::from(vec![i64::MAX]))], &[0], 1)
                .unwrap();
            count.update_single(&nulls).unwrap();
            assert!(
                count
                    .update_single(&batch(Arc::new(Int64Array::from(vec![1]))))
                    .is_err()
            );
            let mut plan = TestPlan::new(&[(DataType::Int64, true)]);
            let output = plan.column(DataType::Int64, false);
            let mut count_star = executor((
                plan,
                AggregateExpression::new(AggregateFunction::Count, vec![], output),
            ));
            count_star.update_single(&nulls).unwrap();
            assert_eq!(
                count_star
                    .evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                2
            );
            let mut overflowing_avg = executor(expression(
                AggregateFunction::Avg,
                DataType::Int64,
                DataType::Float64,
            ));
            overflowing_avg
                .merge(
                    &[
                        Arc::new(UInt64Array::from(vec![u64::MAX])),
                        Arc::new(Float64Array::from(vec![0.0])),
                    ],
                    &[0],
                    1,
                )
                .unwrap();
            overflowing_avg.update_single(&nulls).unwrap();
            assert!(
                overflowing_avg
                    .update_single(&batch(Arc::new(Int64Array::from(vec![1]))))
                    .is_err()
            );
        }

        #[test]
        fn global_filter_excludes_errors_before_arguments_are_evaluated() {
            let input = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("divisor", DataType::Int64, false),
                    Field::new("keep", DataType::Boolean, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![0, 2, 0, 4])),
                    Arc::new(BooleanArray::from(vec![
                        Some(false),
                        Some(true),
                        None,
                        Some(true),
                    ])),
                ],
            )
            .unwrap();
            let mut plan = TestPlan::new(&[(DataType::Int64, false), (DataType::Boolean, true)]);
            let eight = plan.column(DataType::Int64, false);
            let quotient = plan.column(DataType::Int64, false);
            let output = plan.column(DataType::Int64, true);
            let argument = FunctionExpression::binary(
                FunctionKind::Divide,
                ConstantExpression::int64(Some(8), eight).into_ref(),
                ReferenceExpression::new(plan.input(0)).into_ref(),
                quotient,
            )
            .into_ref();
            let filter = ReferenceExpression::new(plan.input(1)).into_ref();
            let mut sum = executor((
                plan,
                AggregateExpression::new(AggregateFunction::Sum, vec![argument], output)
                    .with_filter(filter),
            ));
            sum.update_single(&input).unwrap();
            assert_eq!(
                sum.evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                6
            );
        }

        #[test]
        fn grouped_sums_preserve_typed_state_and_sliced_null_bitmaps() {
            for array in [
                Arc::new(Int64Array::from(vec![
                    Some(999),
                    Some(i64::MAX),
                    None,
                    Some(1),
                    Some(7),
                    Some(-7),
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![
                    Some(999),
                    Some(u64::MAX),
                    None,
                    Some(1),
                    Some(7),
                    Some(1),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(999.0),
                    Some(4.0),
                    None,
                    Some(-4.0),
                    Some(7.0),
                    Some(1.0),
                ])),
            ] {
                let kind = array.data_type().clone();
                let mut sums = executor(expression(AggregateFunction::Sum, kind.clone(), kind));
                sums.update(&batch(array.slice(1, 5)), &[0, 1, 0, 2, 2], 4)
                    .unwrap();
                let state = sums.state().unwrap();
                assert!(state[0].is_null(1));
                assert!(state[0].is_null(3));
                let output = sums.evaluate().unwrap();
                match output.data_type() {
                    DataType::Int64 => assert_eq!(
                        output.as_any().downcast_ref::<Int64Array>().unwrap(),
                        &Int64Array::from(vec![Some(i64::MIN), None, Some(0), None])
                    ),
                    DataType::UInt64 => assert_eq!(
                        output.as_any().downcast_ref::<UInt64Array>().unwrap(),
                        &UInt64Array::from(vec![Some(0), None, Some(8), None])
                    ),
                    DataType::Float64 => assert_eq!(
                        output.as_any().downcast_ref::<Float64Array>().unwrap(),
                        &Float64Array::from(vec![Some(0.0), None, Some(8.0), None])
                    ),
                    _ => unreachable!(),
                }
                let mut merged = executor(expression(
                    AggregateFunction::Sum,
                    state[0].data_type().clone(),
                    state[0].data_type().clone(),
                ));
                merged.merge(&state, &[3, 2, 1, 0], 4).unwrap();
                assert!(merged.evaluate().unwrap().is_null(0));
                assert!(merged.evaluate().unwrap().is_null(2));
            }
        }

        #[test]
        fn public_grouped_update_still_rejects_invalid_ids() {
            let mut sum = executor(expression(
                AggregateFunction::Sum,
                DataType::Int64,
                DataType::Int64,
            ));
            let input = batch(Arc::new(Int64Array::from(vec![1, 2])));
            assert!(sum.update(&input, &[0], 1).is_err());
            assert!(sum.update(&input, &[0, 1], 1).is_err());
            assert!(sum.update(&input, &[0, usize::MAX], 1).is_err());
        }

        #[test]
        fn grouped_count_and_sum_cross_bitmap_words_without_reading_null_payloads() {
            use crate::arrow::buffer::NullBuffer;

            let values = (0..151).map(|index| index as i64 - 70).collect::<Vec<_>>();
            let valid = (0..151).map(|index| index % 7 != 0).collect::<Vec<_>>();
            let array = Arc::new(Int64Array::new(
                values.clone().into(),
                Some(NullBuffer::from(valid.clone())),
            )) as ArrayRef;
            let input = batch(array.slice(3, 143));
            let ids = (0..143).map(|index| index % 5).collect::<Vec<_>>();
            let mut expected_sums = [0i64; 5];
            let mut expected_counts = [0i64; 5];
            for (index, &id) in ids.iter().enumerate() {
                if valid[index + 3] {
                    expected_sums[id] = expected_sums[id].wrapping_add(values[index + 3]);
                    expected_counts[id] += 1;
                }
            }
            for (function, expected) in [
                (AggregateFunction::Sum, expected_sums),
                (AggregateFunction::Count, expected_counts),
            ] {
                let mut aggregate =
                    executor(expression(function, DataType::Int64, DataType::Int64));
                aggregate.update(&input, &ids, 5).unwrap();
                assert_eq!(
                    aggregate
                        .evaluate()
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &Int64Array::from(expected.to_vec())
                );
            }
        }

        fn assert_float_aggregate_state_eq(
            single: &AccumulatorTestHarness,
            grouped: &AccumulatorTestHarness,
        ) {
            let bit_values = |values: ArrayRef| {
                values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(f64::to_bits))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                bit_values(single.evaluate().unwrap()),
                bit_values(grouped.evaluate().unwrap())
            );
            for (single, grouped) in single
                .state()
                .unwrap()
                .into_iter()
                .zip(grouped.state().unwrap())
            {
                if single.data_type() == &DataType::Float64 {
                    assert_eq!(bit_values(single), bit_values(grouped));
                } else {
                    assert_eq!(single.to_data(), grouped.to_data());
                }
            }
        }

        #[test]
        fn global_float_sum_keeps_first_negative_zero_and_ignores_null_payloads() {
            use crate::arrow::buffer::NullBuffer;

            let expr = expression(AggregateFunction::Sum, DataType::Float64, DataType::Float64);
            let mut single = executor(expr.clone());
            let mut grouped = executor(expr);
            let nulls = batch(Arc::new(Float64Array::from(vec![None, None])));
            single.update_single(&nulls).unwrap();
            grouped.update(&nulls, &[0, 0], 1).unwrap();
            assert!(single.evaluate().unwrap().is_null(0));
            assert_float_aggregate_state_eq(&single, &grouped);

            // NULL payloads contain NaNs and may not participate in the reduction.
            let array: ArrayRef = Arc::new(Float64Array::new(
                vec![123.0, f64::NAN, -0.0, f64::NAN].into(),
                Some(NullBuffer::from(vec![true, false, true, false])),
            ));
            let input = batch(array.slice(1, 3));
            single.update_single(&input).unwrap();
            grouped.update(&input, &[0, 0, 0], 1).unwrap();
            assert_float_aggregate_state_eq(&single, &grouped);
            assert_eq!(
                single
                    .evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                (-0.0f64).to_bits()
            );

            for input in [nulls, input.slice(0, 0)] {
                single.update_single(&input).unwrap();
                grouped
                    .update(&input, &vec![0; input.num_rows()], 1)
                    .unwrap();
                assert_float_aggregate_state_eq(&single, &grouped);
            }
        }

        #[test]
        fn global_float_sum_and_avg_keep_grouped_order_across_batches_and_merge() {
            for function in [AggregateFunction::Sum, AggregateFunction::Avg] {
                for chunks in [
                    vec![vec![Some(1e16), Some(1.0), Some(-1e16)]],
                    vec![vec![Some(1e16)], vec![Some(1.0), None, Some(-1e16)]],
                    vec![vec![Some(-0.0)], vec![None, Some(-0.0)]],
                    vec![vec![
                        Some(1e16),
                        Some(1.0),
                        Some(-1e16),
                        Some(2.0),
                        Some(1e16),
                        None,
                        Some(1.0),
                        Some(-1e16),
                        Some(4.0),
                    ]],
                ] {
                    let expr = expression(function, DataType::Float64, DataType::Float64);
                    let mut single = executor(expr.clone());
                    let mut grouped = executor(expr.clone());
                    for chunk in chunks {
                        let input = batch(Arc::new(Float64Array::from(chunk)));
                        single.update_single(&input).unwrap();
                        grouped
                            .update(&input, &vec![0; input.num_rows()], 1)
                            .unwrap();
                        assert_float_aggregate_state_eq(&single, &grouped);
                    }
                    let mut merged_single = executor(expr.clone());
                    let mut merged_grouped = executor(expr);
                    merged_single
                        .merge(&single.state().unwrap(), &[0], 1)
                        .unwrap();
                    merged_grouped
                        .merge(&grouped.state().unwrap(), &[0], 1)
                        .unwrap();
                    let tail = batch(Arc::new(Float64Array::from(vec![
                        Some(1.0),
                        None,
                        Some(-1e16),
                    ])));
                    merged_single.update_single(&tail).unwrap();
                    merged_grouped.update(&tail, &[0, 0, 0], 1).unwrap();
                    assert_float_aggregate_state_eq(&merged_single, &merged_grouped);
                }
            }
        }
    }

    mod compatibility {
        use super::AccumulatorTestHarness;
        use std::sync::Arc;

        use crate::arrow::{
            array::{Array, ArrayRef, Float64Array, UInt64Array},
            buffer::NullBuffer,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use crate::expr::{
            agg::{AggregateExpression, AggregateFunction},
            scalar::{ReferenceExpression, test_support::TestPlan},
        };

        fn avg_executor() -> AccumulatorTestHarness {
            let mut plan = TestPlan::new(&[(DataType::Float64, true)]);
            let output = plan.column(DataType::Float64, true);
            let expression = AggregateExpression::new(
                AggregateFunction::Avg,
                vec![ReferenceExpression::new(plan.input(0)).into_ref()],
                output,
            );
            AccumulatorTestHarness::try_new(&plan, Arc::new(expression)).unwrap()
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
            executor: &AccumulatorTestHarness,
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
    }

    mod hotloop {
        use super::AccumulatorTestHarness;
        use crate::arrow::{
            array::{Array, ArrayRef, Float64Array, Int64Array},
            buffer::NullBuffer,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use crate::expr::{
            agg::{AggregateExpression, AggregateFunction},
            scalar::{ReferenceExpression, test_support::TestPlan},
        };
        use std::sync::Arc;

        fn execute(
            function: AggregateFunction,
            values: ArrayRef,
            ids: &[usize],
            groups: usize,
        ) -> ArrayRef {
            let data_type = values.data_type().clone();
            let output_type = if function == AggregateFunction::Count {
                DataType::Int64
            } else {
                data_type.clone()
            };
            let input = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    data_type.clone(),
                    true,
                )])),
                vec![values],
            )
            .unwrap();
            let mut plan = TestPlan::new(&[(data_type, true)]);
            let output = plan.column(output_type, true);
            let expression = AggregateExpression::new(
                function,
                vec![ReferenceExpression::new(plan.input(0)).into_ref()],
                output,
            );
            let mut executor =
                AccumulatorTestHarness::try_new(&plan, Arc::new(expression)).unwrap();
            executor.update(&input, ids, groups).unwrap();
            executor.evaluate().unwrap()
        }

        #[test]
        fn dense_and_sparse_null_masks_match_reference_at_word_and_slice_boundaries() {
            for offset in 0..9 {
                for len in [0, 1, 2, 63, 64, 65, 127, 128, 129] {
                    for pattern in 0..4 {
                        let valid = (0..offset + len + 3)
                            .map(|index| match pattern {
                                0 => true,
                                1 => index % 17 != 0,
                                2 => index % 17 == 0,
                                _ => false,
                            })
                            .collect::<Vec<_>>();
                        let payload = (0..valid.len())
                            .map(|index| {
                                if valid[index] {
                                    index as i64 - 73
                                } else {
                                    i64::MAX
                                }
                            })
                            .collect::<Vec<_>>();
                        let array = Arc::new(Int64Array::new(
                            payload.clone().into(),
                            Some(NullBuffer::from(valid.clone())),
                        )) as ArrayRef;
                        let array = array.slice(offset, len);
                        let ids = (0..len).map(|index| index % 7).collect::<Vec<_>>();
                        let mut sums = [None::<i64>; 7];
                        let mut counts = [0i64; 7];
                        for (index, &id) in ids.iter().enumerate() {
                            if valid[offset + index] {
                                sums[id] = Some(
                                    sums[id].unwrap_or(0).wrapping_add(payload[offset + index]),
                                );
                                counts[id] += 1;
                            }
                        }
                        for (function, expected) in [
                            (AggregateFunction::Sum, Int64Array::from(sums.to_vec())),
                            (AggregateFunction::Count, Int64Array::from(counts.to_vec())),
                        ] {
                            let result = execute(function, array.clone(), &ids, 7);
                            assert_eq!(
                                result.as_any().downcast_ref::<Int64Array>().unwrap(),
                                &expected,
                                "offset={offset} len={len} pattern={pattern} aggregate={function:?}"
                            );
                        }
                    }
                }
            }
        }

        #[test]
        fn dense_float_masks_ignore_nan_payloads_and_preserve_first_signed_zero() {
            let len = 131;
            let valid = (0..len + 4)
                .map(|index| index % 19 != 0)
                .collect::<Vec<_>>();
            let payload = (0..valid.len())
                .map(|index| if valid[index] { -0.0 } else { f64::NAN })
                .collect::<Vec<_>>();
            let array = Arc::new(Float64Array::new(
                payload.into(),
                Some(NullBuffer::from(valid.clone())),
            )) as ArrayRef;
            let result = execute(
                AggregateFunction::Sum,
                array.slice(3, len),
                &(0..len).collect::<Vec<_>>(),
                len,
            );
            let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
            for index in 0..len {
                if valid[index + 3] {
                    assert!(result.is_valid(index));
                    assert_eq!(result.value(index).to_bits(), (-0.0f64).to_bits());
                } else {
                    assert!(result.is_null(index));
                }
            }
        }
    }
}
