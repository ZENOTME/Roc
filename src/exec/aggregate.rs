use super::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult, SourceExec, SourceExecutor};
use crate::{Cancel, Error, Result, operator::AggregateOperator};
use arrow::{
    array::{ArrayRef, new_empty_array},
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{RowConverter, SortField},
};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::GroupsAccumulatorAdapter;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

// Bound merge work between cancellation checks and share output morsels among
// workers. The pipeline applies its own configured batch size to each morsel.
const AGGREGATE_MORSEL_ROWS: usize = 2048;

/// Worker-local grouping; DataFusion owns aggregate state and evaluation.
struct AggregateState {
    operator: Arc<AggregateOperator>,
    converter: RowConverter,
    index: HashMap<Vec<u8>, usize>,
    keys: Vec<Vec<u8>>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
}

/// Each aggregate keeps its own DataFusion state arrays; there is no flattened
/// partial schema or column-offset mapping between aggregates.
struct PartialAggregate {
    groups: RecordBatch,
    states: Vec<Vec<ArrayRef>>,
}

impl PartialAggregate {
    fn num_rows(&self) -> usize {
        self.groups.num_rows()
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            groups: self.groups.slice(offset, len),
            states: self
                .states
                .iter()
                .map(|state| state.iter().map(|array| array.slice(offset, len)).collect())
                .collect(),
        }
    }
}

impl AggregateState {
    fn new(operator: Arc<AggregateOperator>) -> Result<Self> {
        let converter = RowConverter::new(
            operator
                .groups()
                .output_schema()
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        let grouped = !operator.groups().output_schema().fields().is_empty();
        let mut accumulators = Vec::with_capacity(operator.aggregates().len());
        for aggregate in operator.aggregates() {
            let mut accumulator: Box<dyn GroupsAccumulator> =
                if grouped && aggregate.groups_accumulator_supported() {
                    aggregate.create_groups_accumulator()?
                } else {
                    let aggregate = aggregate.clone();
                    Box::new(GroupsAccumulatorAdapter::new(move || {
                        aggregate.create_accumulator()
                    }))
                };
            if !grouped {
                // Allocate the global adapter's single group without merging
                // any state rows, preserving scalar empty-input semantics.
                let empty = aggregate
                    .state_fields()?
                    .iter()
                    .map(|field| new_empty_array(field.data_type()))
                    .collect::<Vec<_>>();
                accumulator.merge_batch(&empty, &[], 1)?;
            }
            accumulators.push(accumulator);
        }
        Ok(Self {
            operator,
            converter,
            index: HashMap::new(),
            keys: vec![],
            accumulators,
        })
    }

    fn group_count(&self) -> usize {
        if self.operator.groups().output_schema().fields().is_empty() {
            1
        } else {
            self.keys.len()
        }
    }

    fn group_ids(&mut self, columns: &[ArrayRef], rows: usize) -> Result<Vec<usize>> {
        if columns.is_empty() {
            return Ok(vec![0; rows]);
        }
        let rows = self.converter.convert_columns(columns)?;
        let mut ids = Vec::with_capacity(rows.num_rows());
        for row in rows.iter() {
            let id = match self.index.get(row.as_ref()) {
                Some(id) => *id,
                None => {
                    let id = self.keys.len();
                    let key = row.as_ref().to_vec();
                    self.index.insert(key.clone(), id);
                    self.keys.push(key);
                    id
                }
            };
            ids.push(id);
        }
        Ok(ids)
    }

    fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let groups = self.operator.groups().project_batch(batch)?;
        let ids = self.group_ids(groups.columns(), batch.num_rows())?;
        let count = self.group_count();
        for (accumulator, aggregate) in self.accumulators.iter_mut().zip(self.operator.aggregates())
        {
            let values = aggregate
                .expressions()
                .iter()
                .map(|expr| expr.evaluate(batch)?.into_array(batch.num_rows()))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            accumulator.update_batch(&values, &ids, None, count)?;
        }
        Ok(())
    }

    fn merge(&mut self, partial: &PartialAggregate) -> Result<()> {
        if partial.num_rows() == 0 {
            return Ok(());
        }
        let ids = self.group_ids(partial.groups.columns(), partial.num_rows())?;
        let count = self.group_count();
        for (accumulator, state) in self.accumulators.iter_mut().zip(&partial.states) {
            accumulator.merge_batch(state, &ids, count)?;
        }
        Ok(())
    }

    fn group_columns(&self) -> Result<Vec<ArrayRef>> {
        if self.operator.groups().output_schema().fields().is_empty() {
            return Ok(vec![]);
        }
        let parser = self.converter.parser();
        Ok(self
            .converter
            .convert_rows(self.keys.iter().map(|key| parser.parse(key)))?)
    }

    fn finish_partial(mut self) -> Result<PartialAggregate> {
        let count = self.group_count();
        let groups = RecordBatch::try_new_with_options(
            self.operator.groups().output_schema().clone(),
            self.group_columns()?,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?;
        let states = self
            .accumulators
            .iter_mut()
            .zip(self.operator.aggregates())
            .map(|(accumulator, aggregate)| {
                if count == 0 {
                    // The adapter cannot infer state array types without groups.
                    Ok(aggregate
                        .state_fields()?
                        .iter()
                        .map(|field| new_empty_array(field.data_type()))
                        .collect())
                } else {
                    accumulator.state(EmitTo::All)
                }
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        Ok(PartialAggregate { groups, states })
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let count = self.group_count();
        let schema = self.operator.output_schema();
        if count == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }
        let mut columns = self.group_columns()?;
        for accumulator in &mut self.accumulators {
            columns.push(accumulator.evaluate(EmitTo::All)?);
        }
        Ok(RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?)
    }
}

#[derive(Debug, Default)]
struct AggregateSharedState {
    finalized: OnceLock<RecordBatch>,
}

#[derive(Clone, Debug)]
pub struct AggregateSinkExec {
    operator: Arc<AggregateOperator>,
    shared: Arc<AggregateSharedState>,
}

#[derive(Clone, Debug)]
pub struct AggregateSourceExec {
    shared: Arc<AggregateSharedState>,
}

impl AggregateOperator {
    /// Creates sink and source roles sharing the same aggregation result.
    pub fn into_execs(self) -> (AggregateSinkExec, AggregateSourceExec) {
        let shared = Arc::new(AggregateSharedState::default());
        (
            AggregateSinkExec {
                operator: Arc::new(self),
                shared: shared.clone(),
            },
            AggregateSourceExec { shared },
        )
    }
}

struct AggregateSinkGlobalContext {
    partials: Mutex<Vec<PartialAggregate>>,
}

struct AggregateExecutor {
    global: Arc<AggregateSinkGlobalContext>,
    local: AggregateState,
}

impl SinkExec for AggregateSinkExec {
    fn init_global_context(&self, _cancel: &Cancel) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(AggregateSinkGlobalContext {
            partials: Mutex::new(Vec::new()),
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
        cancel: &'a Cancel,
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
                for offset in (0..partial.num_rows()).step_by(AGGREGATE_MORSEL_ROWS) {
                    if cancel.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    state.merge(&partial.slice(
                        offset,
                        AGGREGATE_MORSEL_ROWS.min(partial.num_rows() - offset),
                    ))?;
                    crate::pipeline::yield_now().await;
                }
            }
            let batch = state.finish()?;
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
        _cancel: &'a Cancel,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            self.local.update(input)?;
            Ok(SinkResult::NeedMoreInput)
        })
    }

    fn combine(self: Box<Self>, _cancel: &Cancel) -> BoxFuture<'_, Result<()>> {
        let Self { global, local } = *self;
        Box::pin(async move {
            let partial = local.finish_partial()?;
            global.partials.lock().unwrap().push(partial);
            Ok(())
        })
    }
}

struct AggregateSourceGlobalContext {
    batch: RecordBatch,
    next_row: AtomicUsize,
}

impl SourceExec for AggregateSourceExec {
    fn init_global_context(&self, _cancel: &Cancel) -> Result<GlobalExecContextRef> {
        let batch = self.shared.finalized.get().cloned().ok_or_else(|| {
            Error::Execution("aggregate source initialized before sink finalization".into())
        })?;
        Ok(Arc::new(AggregateSourceGlobalContext {
            batch,
            next_row: AtomicUsize::new(0),
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

    fn finalize<'a>(
        &'a self,
        _global: GlobalExecContextRef,
        _cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct AggregateSourceExecutor {
    global: Arc<AggregateSourceGlobalContext>,
}
impl SourceExecutor for AggregateSourceExecutor {
    fn next_batch<'a>(
        &'a mut self,
        cancel: &'a Cancel,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let offset = self
                .global
                .next_row
                .fetch_add(AGGREGATE_MORSEL_ROWS, Ordering::Relaxed);
            if offset >= self.global.batch.num_rows() {
                return Ok(None);
            }
            Ok(Some(self.global.batch.slice(
                offset,
                AGGREGATE_MORSEL_ROWS.min(self.global.batch.num_rows() - offset),
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion_common::ScalarValue;
    use datafusion_functions_aggregate::{
        average::avg_udaf, count::count_udaf, covariance::covar_pop_udaf,
        first_last::first_value_udaf, sum::sum_udaf,
    };
    use datafusion_physical_expr::{
        PhysicalExprRef, PhysicalSortExpr,
        aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
        expressions::{Column, Literal},
        projection::{ProjectionExprs, Projector},
    };

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn value() -> PhysicalExprRef {
        Arc::new(Column::new("value", 1))
    }

    fn groups(grouped: bool) -> Projector {
        ProjectionExprs::from_indices(if grouped { &[0] } else { &[] }, &schema())
            .make_projector(&schema())
            .unwrap()
    }

    fn aggregates() -> Vec<Arc<AggregateFunctionExpr>> {
        vec![
            AggregateExprBuilder::new(sum_udaf(), vec![value()]).alias("sum"),
            AggregateExprBuilder::new(avg_udaf(), vec![value()]).alias("avg"),
            AggregateExprBuilder::new(count_udaf(), vec![value()]).alias("count"),
            AggregateExprBuilder::new(count_udaf(), vec![value()])
                .distinct()
                .alias("distinct"),
            // Multiple arguments and four state columns, through the adapter.
            AggregateExprBuilder::new(covar_pop_udaf(), vec![value(), value()]).alias("covar"),
            AggregateExprBuilder::new(
                count_udaf(),
                vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
            )
            .alias("rows"),
        ]
        .into_iter()
        .map(|builder| Arc::new(builder.schema(schema()).build().unwrap()))
        .collect()
    }

    fn batch(keys: Vec<Option<&str>>, values: Vec<Option<f64>>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn merges_native_and_adapter_states_across_workers() {
        let aggregates = aggregates();
        assert!(aggregates[0].groups_accumulator_supported());
        assert!(!aggregates[4].groups_accumulator_supported());
        let operator = Arc::new(AggregateOperator::try_new(groups(true), aggregates).unwrap());
        let batches = [
            batch(
                vec![Some("a"), None, Some("a"), Some("b")],
                vec![Some(1.), Some(2.), Some(3.), None],
            ),
            batch(
                vec![Some("b"), Some("a"), None],
                vec![Some(8.), Some(3.), Some(4.)],
            ),
        ];
        let mut merged = AggregateState::new(operator.clone()).unwrap();
        for batch in batches {
            let mut worker = AggregateState::new(operator.clone()).unwrap();
            // Exercise repeated updates and one-row partial merge chunks.
            for row in 0..batch.num_rows() {
                worker.update(&batch.slice(row, 1)).unwrap();
            }
            let partial = worker.finish_partial().unwrap();
            assert_eq!(partial.groups.schema(), *operator.groups().output_schema());
            for row in 0..partial.num_rows() {
                merged.merge(&partial.slice(row, 1)).unwrap();
            }
        }
        let result = merged.finish().unwrap();
        assert_eq!(result.schema(), operator.output_schema());
        assert_eq!(result.num_rows(), 3);
        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..3 {
            let (sum, avg, count, distinct, covar, rows) = if keys.is_null(row) {
                (6., 3., 2, 2, 1., 2)
            } else if keys.value(row) == "a" {
                (7., 7. / 3., 3, 2, 8. / 9., 3)
            } else {
                assert_eq!(keys.value(row), "b");
                (8., 8., 1, 1, 0., 2)
            };
            for (column, expected) in [(1, sum), (2, avg), (5, covar)] {
                let array = result
                    .column(column)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert!(array.is_valid(row));
                assert!((array.value(row) - expected).abs() < 1e-10);
            }
            for (column, expected) in [(3, count), (4, distinct), (6, rows)] {
                let array = result
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(array.value(row), expected);
            }
        }
    }

    #[test]
    fn empty_global_input_has_one_row_including_adapter_results() {
        let operator = Arc::new(AggregateOperator::try_new(groups(false), aggregates()).unwrap());
        let mut merged = AggregateState::new(operator.clone()).unwrap();
        for update_empty in [false, true] {
            let mut worker = AggregateState::new(operator.clone()).unwrap();
            if update_empty {
                worker.update(&RecordBatch::new_empty(schema())).unwrap();
            }
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        for result in [
            merged.finish().unwrap(),
            AggregateState::new(operator).unwrap().finish().unwrap(),
        ] {
            assert_eq!(result.num_rows(), 1);
            for col in [0, 1, 4] {
                assert!(
                    result.column(col).is_null(0),
                    "column {col}: {:?}",
                    result.column(col)
                );
            }
            for col in [2, 3, 5] {
                assert_eq!(
                    ScalarValue::try_from_array(result.column(col), 0).unwrap(),
                    ScalarValue::Int64(Some(0))
                );
            }
        }
    }

    #[test]
    fn all_null_group_stays_null_across_empty_batches_and_merges() {
        let operator = Arc::new(AggregateOperator::try_new(groups(true), aggregates()).unwrap());
        let mut worker = AggregateState::new(operator.clone()).unwrap();
        worker
            .update(&batch(vec![Some("a"); 2], vec![None; 2]))
            .unwrap();
        worker.update(&RecordBatch::new_empty(schema())).unwrap();
        let mut merged = AggregateState::new(operator.clone()).unwrap();
        merged.merge(&worker.finish_partial().unwrap()).unwrap();
        merged
            .merge(
                &AggregateState::new(operator.clone())
                    .unwrap()
                    .finish_partial()
                    .unwrap(),
            )
            .unwrap();
        let result = merged.finish().unwrap();
        assert_eq!(result.num_rows(), 1);
        for col in [1, 2, 5] {
            assert!(result.column(col).is_null(0));
        }
        for (col, expected) in [(3, 0), (4, 0), (6, 2)] {
            assert_eq!(
                ScalarValue::try_from_array(result.column(col), 0).unwrap(),
                ScalarValue::Int64(Some(expected))
            );
        }
    }

    #[test]
    fn global_input_merges_distinct_and_multi_column_states() {
        let operator = Arc::new(AggregateOperator::try_new(groups(false), aggregates()).unwrap());
        let mut merged = AggregateState::new(operator.clone()).unwrap();
        for values in [vec![Some(2.), None], vec![Some(2.), Some(4.)]] {
            let mut worker = AggregateState::new(operator.clone()).unwrap();
            worker
                .update(&batch(vec![None; values.len()], values))
                .unwrap();
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        let result = merged.finish().unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            ScalarValue::try_from_array(result.column(0), 0).unwrap(),
            ScalarValue::Float64(Some(8.))
        );
        assert_eq!(
            ScalarValue::try_from_array(result.column(3), 0).unwrap(),
            ScalarValue::Int64(Some(2))
        );
        assert_eq!(
            ScalarValue::try_from_array(result.column(5), 0).unwrap(),
            ScalarValue::Int64(Some(4))
        );
    }

    #[test]
    fn empty_grouped_input_and_grouping_without_aggregates() {
        for aggregates in [aggregates(), vec![]] {
            let operator = Arc::new(AggregateOperator::try_new(groups(true), aggregates).unwrap());
            let mut state = AggregateState::new(operator.clone()).unwrap();
            state.update(&RecordBatch::new_empty(schema())).unwrap();
            let partial = state.finish_partial().unwrap();
            assert_eq!(partial.num_rows(), 0);
            assert_eq!(partial.groups.schema(), *operator.groups().output_schema());
            for (arrays, aggregate) in partial.states.iter().zip(operator.aggregates()) {
                let fields = aggregate.state_fields().unwrap();
                assert_eq!(arrays.len(), fields.len());
                for (array, field) in arrays.iter().zip(fields) {
                    assert_eq!(array.len(), 0);
                    assert_eq!(array.data_type(), field.data_type());
                }
            }
            let mut merged = AggregateState::new(operator.clone()).unwrap();
            merged.merge(&partial).unwrap();
            let result = merged.finish().unwrap();
            assert_eq!(result.num_rows(), 0);
            assert_eq!(result.schema(), operator.output_schema());
        }

        let operator = Arc::new(AggregateOperator::try_new(groups(true), vec![]).unwrap());
        let mut worker = AggregateState::new(operator.clone()).unwrap();
        worker
            .update(&batch(vec![Some("a"), None, Some("a")], vec![None; 3]))
            .unwrap();
        let mut merged = AggregateState::new(operator).unwrap();
        merged.merge(&worker.finish_partial().unwrap()).unwrap();
        let result = merged.finish().unwrap();
        assert_eq!((result.num_rows(), result.num_columns()), (2, 1));
        assert_eq!(result.column(0).null_count(), 1);
    }

    #[test]
    fn rejects_empty_specification_and_ordered_aggregates() {
        assert!(AggregateOperator::try_new(groups(false), vec![]).is_err());
        let ordered = AggregateExprBuilder::new(first_value_udaf(), vec![value()])
            .schema(schema())
            .alias("first")
            .order_by(vec![PhysicalSortExpr {
                expr: value(),
                options: Default::default(),
            }])
            .build()
            .unwrap();
        assert!(matches!(
            AggregateOperator::try_new(groups(true), vec![Arc::new(ordered)]),
            Err(Error::Plan(_))
        ));
    }
}
