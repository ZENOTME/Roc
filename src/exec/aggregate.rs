use super::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkStatus, SourceExec, SourceExecutor};
use crate::{
    CancellationToken, Error, Result, expr,
    operator::{AggregateFunction, AggregateLayout},
};
use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array},
    compute::cast,
    datatypes::DataType,
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

enum Accumulator {
    Count { count: i64 },
    // Widen sums before combining workers, so partitioning and merge order cannot
    // create an Int64 overflow when the final SUM or AVG is representable.
    Int { value: Option<i128>, count: i64 },
    Float { value: Option<f64>, count: i64 },
}
impl Accumulator {
    fn count(&self) -> i64 {
        match self {
            Self::Count { count } | Self::Int { count, .. } | Self::Float { count, .. } => *count,
        }
    }
    fn add_count(&mut self, count: i64) -> Result<()> {
        let current = match self {
            Self::Count { count } | Self::Int { count, .. } | Self::Float { count, .. } => count,
        };
        *current = add(*current, count)?;
        Ok(())
    }
    fn int(&self) -> Option<i128> {
        match self {
            Self::Int { value, .. } => *value,
            _ => None,
        }
    }
    fn float(&self) -> Option<f64> {
        match self {
            Self::Float { value, .. } => *value,
            _ => None,
        }
    }
}

/// Worker-local aggregation; only Arrow partial batches cross worker boundaries.
struct AggregateState {
    bound: Arc<AggregateLayout>,
    converter: RowConverter,
    index: HashMap<Vec<u8>, usize>,
    keys: Vec<Vec<u8>>,
    states: Vec<Vec<Accumulator>>,
}
fn add(a: i64, b: i64) -> Result<i64> {
    a.checked_add(b)
        .ok_or_else(|| Error::Execution("Int64 aggregate overflow".into()))
}
impl AggregateState {
    fn new(bound: AggregateLayout) -> Result<Self> {
        let converter = RowConverter::new(
            bound
                .group_schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        let mut this = Self {
            bound: Arc::new(bound),
            converter,
            index: HashMap::new(),
            keys: Vec::new(),
            states: Vec::new(),
        };
        // SQL global aggregation produces one row even if no rows reach this instance.
        if this.bound.operator.groups.is_empty() {
            this.states.push(this.empty_states());
        }
        Ok(this)
    }
    fn empty_states(&self) -> Vec<Accumulator> {
        let mut states = Vec::with_capacity(self.bound.operator.aggregates.len());
        for (agg, ty) in self
            .bound
            .operator
            .aggregates
            .iter()
            .zip(&self.bound.value_types)
        {
            states.push(match (agg.function, ty) {
                (AggregateFunction::Count, _) => Accumulator::Count { count: 0 },
                (_, DataType::Int64) => Accumulator::Int {
                    value: None,
                    count: 0,
                },
                (_, DataType::Float64) => Accumulator::Float {
                    value: None,
                    count: 0,
                },
                _ => unreachable!("aggregate types were checked at bind time"),
            });
        }
        states
    }
    fn group_ids(&mut self, batch: &RecordBatch, partial: bool) -> Result<Vec<usize>> {
        let mut ids = Vec::with_capacity(batch.num_rows());
        if self.bound.operator.groups.is_empty() {
            ids.resize(batch.num_rows(), 0);
            return Ok(ids);
        }
        let cols = if partial {
            batch.columns()[..self.bound.operator.groups.len()].to_vec()
        } else {
            self.bound
                .operator
                .groups
                .iter()
                .map(|g| g.expr.evaluate(batch))
                .collect::<Result<Vec<_>>>()?
        };
        let rows = self.converter.convert_columns(&cols)?;
        for row in rows.iter() {
            let id = match self.index.get(row.as_ref()) {
                Some(id) => *id,
                None => {
                    let id = self.states.len();
                    let key = row.as_ref().to_vec();
                    self.index.insert(key.clone(), id);
                    let states = self.empty_states();
                    self.keys.push(key);
                    self.states.push(states);
                    id
                }
            };
            ids.push(id);
        }
        Ok(ids)
    }
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        self.apply(batch, false)
    }
    pub(crate) fn merge(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.bound.partial_schema {
            return Err(Error::Execution("partial aggregate schema mismatch".into()));
        }
        self.apply(batch, true)
    }
    fn apply(&mut self, batch: &RecordBatch, partial: bool) -> Result<()> {
        let ids = self.group_ids(batch, partial)?;
        for (i, agg) in self.bound.operator.aggregates.iter().enumerate() {
            let base = self.bound.operator.groups.len() + i * 2;
            let array = if partial {
                Some(batch.column(base).clone())
            } else {
                agg.expr.as_ref().map(|e| e.evaluate(batch)).transpose()?
            };
            let counts = if partial {
                Some(
                    batch
                        .column(base + 1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                )
            } else {
                None
            };
            let numeric = if agg.function == AggregateFunction::Count {
                None
            } else if partial {
                array.clone()
            } else {
                Some(cast(
                    array.as_ref().unwrap().as_ref(),
                    &self.bound.value_types[i],
                )?)
            };
            let ints = numeric
                .as_ref()
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>());
            let floats = numeric
                .as_ref()
                .and_then(|a| a.as_any().downcast_ref::<Float64Array>());
            let wide_ints = numeric
                .as_ref()
                .and_then(|a| a.as_any().downcast_ref::<Decimal128Array>());
            // COUNT accepts encoded and nested inputs, whose logical nulls can
            // live in dictionary/run/union values rather than a top-level mask.
            let validity = array.as_ref().and_then(|array| array.logical_nulls());
            for (row, group) in ids.iter().copied().enumerate() {
                let state = &mut self.states[group][i];
                let valid = validity.as_ref().is_none_or(|mask| mask.is_valid(row));
                let count = counts.map_or(i64::from(valid), |a| a.value(row));
                state.add_count(count)?;
                if agg.function == AggregateFunction::Count || !valid {
                    continue;
                }
                if let Some(v) = ints
                    .map(|a| i128::from(a.value(row)))
                    .or_else(|| wide_ints.map(|a| a.value(row)))
                {
                    let Accumulator::Int { value, .. } = state else {
                        return Err(Error::Execution("integer aggregate state mismatch".into()));
                    };
                    *value = Some(match (*value, agg.function) {
                        (None, _) => v,
                        (Some(x), AggregateFunction::Min) => x.min(v),
                        (Some(x), AggregateFunction::Max) => x.max(v),
                        (Some(x), _) => x
                            .checked_add(v)
                            .ok_or_else(|| Error::Execution("Int128 aggregate overflow".into()))?,
                    });
                } else if let Some(a) = floats {
                    let v = a.value(row);
                    let Accumulator::Float { value, .. } = state else {
                        return Err(Error::Execution("float aggregate state mismatch".into()));
                    };
                    *value = Some(match (*value, agg.function) {
                        (None, _) => v,
                        (Some(x), AggregateFunction::Min) => {
                            if x.total_cmp(&v).is_le() {
                                x
                            } else {
                                v
                            }
                        }
                        (Some(x), AggregateFunction::Max) => {
                            if x.total_cmp(&v).is_ge() {
                                x
                            } else {
                                v
                            }
                        }
                        (Some(x), _) => x + v,
                    });
                }
            }
        }
        Ok(())
    }
    pub(crate) fn finish(&self, partial: bool) -> Result<RecordBatch> {
        let mut columns = if self.bound.operator.groups.is_empty() {
            vec![]
        } else {
            let parser = self.converter.parser();
            self.converter
                .convert_rows(self.keys.iter().map(|key| parser.parse(key)))?
        };
        for (i, agg) in self.bound.operator.aggregates.iter().enumerate() {
            let value: ArrayRef = if !partial && agg.function == AggregateFunction::Count {
                Arc::new(Int64Array::from_iter_values(
                    self.states.iter().map(|s| s[i].count()),
                ))
            } else if !partial && agg.function == AggregateFunction::Avg {
                Arc::new(Float64Array::from_iter(self.states.iter().map(|s| {
                    let v = &s[i];
                    if v.count() == 0 {
                        None
                    } else {
                        v.float()
                            .or(v.int().map(|x| x as f64))
                            .map(|x| x / v.count() as f64)
                    }
                })))
            } else if partial
                && self
                    .bound
                    .partial_schema
                    .field(self.bound.operator.groups.len() + i * 2)
                    .data_type()
                    == &DataType::Decimal128(38, 0)
            {
                Arc::new(
                    Decimal128Array::from_iter(self.states.iter().map(|s| s[i].int()))
                        .with_precision_and_scale(38, 0)?,
                )
            } else if self.bound.value_types[i] == DataType::Int64 {
                Arc::new(Int64Array::from_iter(
                    self.states
                        .iter()
                        .map(|s| {
                            s[i].int()
                                .map(i64::try_from)
                                .transpose()
                                .map_err(|_| Error::Execution("Int64 aggregate overflow".into()))
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ))
            } else {
                Arc::new(Float64Array::from_iter(
                    self.states.iter().map(|s| s[i].float()),
                ))
            };
            columns.push(value);
            if partial {
                columns.push(Arc::new(Int64Array::from_iter_values(
                    self.states.iter().map(|s| s[i].count()),
                )));
            }
        }
        expr::make_batch(
            if partial {
                self.bound.partial_schema.clone()
            } else {
                self.bound.output_schema.clone()
            },
            columns,
            self.states.len(),
        )
    }
}

#[derive(Debug, Default)]
struct AggregateSharedState {
    finalized: OnceLock<RecordBatch>,
}

#[derive(Clone, Debug)]
pub struct AggregateSinkExec {
    operator: AggregateLayout,
    shared: Arc<AggregateSharedState>,
}

#[derive(Clone, Debug)]
pub struct AggregateSourceExec {
    shared: Arc<AggregateSharedState>,
}

pub fn aggregate_execs(operator: AggregateLayout) -> (AggregateSinkExec, AggregateSourceExec) {
    let shared = Arc::new(AggregateSharedState::default());
    (
        AggregateSinkExec {
            operator,
            shared: shared.clone(),
        },
        AggregateSourceExec { shared },
    )
}

struct AggregateSinkGlobalContext {
    partials: Mutex<Vec<RecordBatch>>,
    batch_rows: usize,
}

struct AggregateExecutor {
    global: Arc<AggregateSinkGlobalContext>,
    local: AggregateState,
}

impl SinkExec for AggregateSinkExec {
    fn init_global_context(
        &self,
        batch_rows: usize,
        _cancel: &crate::CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(AggregateSinkGlobalContext {
            partials: Mutex::new(Vec::new()),
            batch_rows,
        }))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
        let global = global
            .downcast::<AggregateSinkGlobalContext>()
            .map_err(|_| {
                Error::Execution("aggregate sink received an invalid global context".into())
            })?;
        Ok(Box::new(AggregateExecutor {
            global,
            local: AggregateState::new(self.operator.clone())?,
        }))
    }

    fn finalize<'a>(
        &'a self,
        global: GlobalExecContextRef,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let global = global
                .downcast::<AggregateSinkGlobalContext>()
                .map_err(|_| {
                    Error::Execution("aggregate sink received an invalid global context".into())
                })?;
            let partials = std::mem::take(&mut *global.partials.lock().unwrap());
            let mut state = AggregateState::new(self.operator.clone())?;
            for partial in partials {
                for offset in (0..partial.num_rows()).step_by(global.batch_rows) {
                    if ctx.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    state.merge(
                        &partial.slice(offset, global.batch_rows.min(partial.num_rows() - offset)),
                    )?;
                    crate::pipeline::yield_now().await;
                }
            }
            let batch = state.finish(false)?;
            self.shared
                .finalized
                .set(batch)
                .map_err(|_| Error::Execution("aggregate sink finalized more than once".into()))
        })
    }
}
impl SinkExecutor for AggregateExecutor {
    fn sink<'a>(
        &'a mut self,
        _ctx: &'a CancellationToken,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkStatus>> {
        Box::pin(async move {
            self.local.update(input)?;
            Ok(SinkStatus::NeedMoreInput)
        })
    }

    fn combine<'a>(self: Box<Self>, _ctx: &'a CancellationToken) -> BoxFuture<'a, Result<()>> {
        let Self { global, local } = *self;
        Box::pin(async move {
            let partial = local.finish(true)?;
            global.partials.lock().unwrap().push(partial);
            Ok(())
        })
    }
}

struct AggregateSourceGlobalContext {
    batch: RecordBatch,
    next_row: AtomicUsize,
    batch_rows: usize,
}

impl SourceExec for AggregateSourceExec {
    fn init_global_context(
        &self,
        batch_rows: usize,
        _cancel: &crate::CancellationToken,
    ) -> Result<GlobalExecContextRef> {
        let batch = self.shared.finalized.get().cloned().ok_or_else(|| {
            Error::Execution("aggregate source initialized before sink finalization".into())
        })?;
        Ok(Arc::new(AggregateSourceGlobalContext {
            batch,
            next_row: AtomicUsize::new(0),
            batch_rows,
        }))
    }
    fn new_executor(&self, global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
        let global = global
            .downcast::<AggregateSourceGlobalContext>()
            .map_err(|_| {
                Error::Execution("aggregate source received an invalid global context".into())
            })?;
        Ok(Box::new(AggregateSourceExecutor { global }))
    }
}

struct AggregateSourceExecutor {
    global: Arc<AggregateSourceGlobalContext>,
}
impl SourceExecutor for AggregateSourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        ctx: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if ctx.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let offset = self
                .global
                .next_row
                .fetch_add(self.global.batch_rows, Ordering::Relaxed);
            if offset >= self.global.batch.num_rows() {
                return Ok(None);
            }
            Ok(Some(
                self.global.batch.slice(
                    offset,
                    self.global
                        .batch_rows
                        .min(self.global.batch.num_rows() - offset),
                ),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::{Expr, NamedExpr},
        operator::{AggregateExpr, AggregateOperator},
    };
    use arrow::{
        array::StringArray,
        datatypes::{Field, Schema},
    };

    fn layout(groups: Vec<NamedExpr>) -> AggregateLayout {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]));
        AggregateOperator {
            groups,
            aggregates: vec![
                AggregateExpr {
                    name: "rows".into(),
                    function: AggregateFunction::Count,
                    expr: None,
                },
                AggregateExpr {
                    name: "non_null".into(),
                    function: AggregateFunction::Count,
                    expr: Some(Expr::column("value")),
                },
                AggregateExpr {
                    name: "total".into(),
                    function: AggregateFunction::Sum,
                    expr: Some(Expr::column("value")),
                },
                AggregateExpr {
                    name: "average".into(),
                    function: AggregateFunction::Avg,
                    expr: Some(Expr::column("value")),
                },
            ],
            input_schema: schema.clone(),
        }
        .layout(&schema)
        .unwrap()
    }

    #[test]
    fn merges_worker_partials_with_nulls_and_groups() {
        let layout = layout(vec![Expr::column("group").alias("group")]);
        let batch = RecordBatch::try_new(
            layout.operator.input_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])),
            ],
        )
        .unwrap();
        let mut first = AggregateState::new(layout.clone()).unwrap();
        let mut second = AggregateState::new(layout.clone()).unwrap();
        first.update(&batch.slice(0, 2)).unwrap();
        second.update(&batch.slice(2, 2)).unwrap();
        let mut merged = AggregateState::new(layout).unwrap();
        merged.merge(&first.finish(true).unwrap()).unwrap();
        merged.merge(&second.finish(true).unwrap()).unwrap();
        let result = merged.finish(false).unwrap();
        let rows = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let non_null = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let totals = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let averages = result
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(
            (
                rows.value(0),
                non_null.value(0),
                totals.value(0),
                averages.value(0)
            ),
            (2, 1, 1, 1.0)
        );
        assert_eq!(
            (
                rows.value(1),
                non_null.value(1),
                totals.value(1),
                averages.value(1)
            ),
            (2, 2, 7, 3.5)
        );
    }

    #[test]
    fn empty_global_aggregate_produces_one_row() {
        let result = AggregateState::new(layout(vec![]))
            .unwrap()
            .finish(false)
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert!(result.column(2).is_null(0));
        assert!(result.column(3).is_null(0));
    }
}
