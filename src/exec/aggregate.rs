use super::ProjectionExecutor;
use super::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult, SourceExec, SourceExecutor};
use crate::expr::agg::executor::AggregateExpressionExecutor;
use crate::{
    error::{Error, Result},
    operator::AggregateOperator,
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::{
    array::ArrayRef,
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{RowConverter, SortField},
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

// Bound merge work between cancellation checks and share output morsels among
// workers. The pipeline applies its own configured batch size to each morsel.
const AGGREGATE_MORSEL_ROWS: usize = 2048;

/// Worker-local grouping, expression evaluation, and aggregate states.
struct AggregateState {
    converter: RowConverter,
    index: HashMap<Vec<u8>, usize>,
    keys: Vec<Vec<u8>>,
    groups: ProjectionExecutor,
    output_schema: SchemaRef,
    accumulators: Vec<AggregateExpressionExecutor>,
}

/// Each aggregate keeps its own state arrays; there is no flattened
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
        let groups = ProjectionExecutor::try_new(operator.groups().clone())?;
        let converter = RowConverter::new(
            groups
                .output_schema()
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        let grouped = !groups.output_schema().fields().is_empty();
        let mut fields = groups.output_schema().fields().to_vec();
        let mut accumulators = vec![];
        for aggregate in operator.aggregates() {
            let mut executor = AggregateExpressionExecutor::try_new(
                aggregate.clone(),
                operator.groups().input_schema().clone(),
            )?;
            let result = executor.result();
            fields.push(Arc::new(Field::new(
                aggregate.output_name(),
                result.data_type.clone(),
                result.nullable,
            )));
            if !grouped {
                executor.resize(1);
            }
            accumulators.push(executor);
        }
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            groups.output_schema().metadata().clone(),
        ));
        Ok(Self {
            converter,
            index: HashMap::new(),
            keys: vec![],
            groups,
            output_schema,
            accumulators,
        })
    }

    fn group_count(&self) -> usize {
        if self.groups.output_schema().fields().is_empty() {
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
        let groups = self.groups.project_batch(batch)?;
        let ids = self.group_ids(groups.columns(), batch.num_rows())?;
        let count = self.group_count();
        for accumulator in &mut self.accumulators {
            accumulator.update(
                &crate::expr::scalar::executor::ExpressionInput::new(
                    batch.columns(),
                    batch.num_rows(),
                ),
                &ids,
                count,
            )?;
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
            accumulator.merge(state, &ids, count)?;
        }
        Ok(())
    }

    fn group_columns(&self) -> Result<Vec<ArrayRef>> {
        if self.groups.output_schema().fields().is_empty() {
            return Ok(vec![]);
        }
        let parser = self.converter.parser();
        Ok(self
            .converter
            .convert_rows(self.keys.iter().map(|key| parser.parse(key)))?)
    }

    fn finish_partial(self) -> Result<PartialAggregate> {
        let count = self.group_count();
        let groups = RecordBatch::try_new_with_options(
            self.groups.output_schema().clone(),
            self.group_columns()?,
            &RecordBatchOptions::new().with_row_count(Some(count)),
        )?;
        let states = self
            .accumulators
            .iter()
            .map(|a| a.state())
            .collect::<Result<Vec<_>>>()?;
        Ok(PartialAggregate { groups, states })
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let count = self.group_count();
        let schema = self.output_schema.clone();
        if count == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }
        let mut columns = self.group_columns()?;
        for accumulator in &mut self.accumulators {
            columns.push(accumulator.evaluate()?);
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
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
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
        shutdown_guard: &'a ShutdownGuard,
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
                    if shutdown_guard.is_shutdown_requested() {
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
        _shutdown_guard: &'a ShutdownGuard,
        input: &'a RecordBatch,
    ) -> BoxFuture<'a, Result<SinkResult>> {
        Box::pin(async move {
            self.local.update(input)?;
            Ok(SinkResult::NeedMoreInput)
        })
    }

    fn combine(self: Box<Self>, _shutdown_guard: &ShutdownGuard) -> BoxFuture<'_, Result<()>> {
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
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
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
        _shutdown_guard: &'a ShutdownGuard,
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
        shutdown_guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if shutdown_guard.is_shutdown_requested() {
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
    use crate::{
        expr::scalar::ScalarExprRef,
        expr::{
            agg::{AggregateExpression, AggregateFunction},
            scalar::ReferenceExpression,
        },
        operator::Projection,
    };
    use arrow::{
        array::{Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn value() -> ScalarExprRef {
        ReferenceExpression::new(1).into_ref()
    }
    fn groups(grouped: bool) -> Projection {
        Projection::from_indices(schema(), if grouped { &[0] } else { &[] }).unwrap()
    }
    fn aggregates() -> Vec<Arc<AggregateExpression>> {
        use AggregateFunction::*;
        vec![
            Arc::new(AggregateExpression::new(Sum, vec![value()])),
            Arc::new(AggregateExpression::new(Avg, vec![value()]).with_alias("avg")),
            Arc::new(AggregateExpression::new(Count, vec![value()]).with_alias("count")),
            Arc::new(
                AggregateExpression::new(Count, vec![value()])
                    .with_distinct()
                    .with_alias("distinct"),
            ),
            Arc::new(
                AggregateExpression::new(CovarPop, vec![value(), value()]).with_alias("covar"),
            ),
            Arc::new(AggregateExpression::new(Count, vec![]).with_alias("rows")),
        ]
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
    fn merges_distinct_and_multi_column_states_across_workers() {
        let aggregates = aggregates();
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
            assert_eq!(
                partial.groups.schema(),
                operator.groups().output_schema().unwrap()
            );
            for row in 0..partial.num_rows() {
                merged.merge(&partial.slice(row, 1)).unwrap();
            }
        }
        let result = merged.finish().unwrap();
        assert_eq!(result.schema(), operator.output_schema().unwrap());
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
    fn empty_global_input_has_one_row() {
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
                    result
                        .column(col)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    0
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
                result
                    .column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected
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
            result
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            8.
        );
        assert_eq!(
            result
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            result
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4
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
            assert_eq!(
                partial.groups.schema(),
                operator.groups().output_schema().unwrap()
            );
            for (arrays, aggregate) in partial.states.iter().zip(operator.aggregates()) {
                let fields = AggregateExpressionExecutor::try_new(aggregate.clone(), schema())
                    .unwrap()
                    .state_types();
                assert_eq!(arrays.len(), fields.len());
                for (array, field) in arrays.iter().zip(fields) {
                    assert_eq!(array.len(), 0);
                    assert_eq!(array.data_type(), &field);
                }
            }
            let mut merged = AggregateState::new(operator.clone()).unwrap();
            merged.merge(&partial).unwrap();
            let result = merged.finish().unwrap();
            assert_eq!(result.num_rows(), 0);
            assert_eq!(result.schema(), operator.output_schema().unwrap());
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
    fn rejects_empty_specification() {
        assert!(AggregateOperator::try_new(groups(false), vec![]).is_err());
    }

    #[test]
    fn aggregate_filter_runs_before_arguments_and_preserves_empty_groups() {
        use crate::expr::scalar::{ConstantExpression, FunctionExpression, ScalarFunction};
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let int = |v| ConstantExpression::int64(Some(v)).into_ref();
        let predicate =
            FunctionExpression::new(ScalarFunction::NotEqual, vec![value(), int(0)]).into_ref();
        let division =
            FunctionExpression::new(ScalarFunction::Divide, vec![int(100), value()]).into_ref();
        let aggregates = vec![
            Arc::new(
                AggregateExpression::new(AggregateFunction::Sum, vec![division])
                    .with_filter(predicate.clone())
                    .with_alias("sum"),
            ),
            Arc::new(
                AggregateExpression::new(AggregateFunction::Count, vec![value()])
                    .with_distinct()
                    .with_filter(predicate)
                    .with_alias("distinct"),
            ),
        ];
        let operator = Arc::new(
            AggregateOperator::try_new(
                Projection::from_indices(input_schema.clone(), &[0]).unwrap(),
                aggregates,
            )
            .unwrap(),
        );
        let mut merged = AggregateState::new(operator.clone()).unwrap();
        for _ in 0..2 {
            let input = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "a"])),
                    Arc::new(Int64Array::from(vec![2, 0, 4])),
                ],
            )
            .unwrap();
            let mut worker = AggregateState::new(operator.clone()).unwrap();
            worker.update(&input).unwrap();
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        let result = merged.finish().unwrap();
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(150), None]
        );
        assert_eq!(
            result
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2, 0]
        );
    }

    #[test]
    fn extrema_merge_values_and_keep_string_null_semantics() {
        let input_schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let expr = ReferenceExpression::new(0).into_ref();
        let aggregates = [AggregateFunction::Min, AggregateFunction::Max]
            .into_iter()
            .map(|f| {
                Arc::new(
                    AggregateExpression::new(f, vec![expr.clone()]).with_alias(format!("{f:?}")),
                )
            })
            .collect();
        let operator = Arc::new(
            AggregateOperator::try_new(Projection::new(input_schema.clone(), vec![]), aggregates)
                .unwrap(),
        );
        let mut merged = AggregateState::new(operator.clone()).unwrap();
        for values in [vec![Some("z"), None], vec![None, Some("a")], vec![None]] {
            let input = RecordBatch::try_new(
                input_schema.clone(),
                vec![Arc::new(StringArray::from(values))],
            )
            .unwrap();
            let mut worker = AggregateState::new(operator.clone()).unwrap();
            worker.update(&input).unwrap();
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        let result = merged.finish().unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "z"
        );
        let empty = AggregateState::new(operator).unwrap().finish().unwrap();
        assert!(empty.column(0).is_null(0));
        assert!(empty.column(1).is_null(0));
    }

    #[test]
    fn unsigned_sum_stays_exact_and_signed_sum_checks_overflow() {
        use arrow::array::UInt64Array;
        for (data_type, array) in [
            (
                DataType::UInt64,
                Arc::new(UInt64Array::from(vec![
                    Some(9_007_199_254_740_993),
                    None,
                    Some(2),
                ])) as ArrayRef,
            ),
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(1)])) as ArrayRef,
            ),
        ] {
            let input_schema =
                Arc::new(Schema::new(vec![Field::new("v", data_type.clone(), true)]));
            let operator = Arc::new(
                AggregateOperator::try_new(
                    Projection::new(input_schema.clone(), vec![]),
                    vec![Arc::new(
                        AggregateExpression::new(
                            AggregateFunction::Sum,
                            vec![ReferenceExpression::new(0).into_ref()],
                        )
                        .with_alias("sum"),
                    )],
                )
                .unwrap(),
            );
            let mut state = AggregateState::new(operator.clone()).unwrap();
            let input = RecordBatch::try_new(input_schema, vec![array]).unwrap();
            if data_type == DataType::Int64 {
                assert!(state.update(&input).is_err());
            } else {
                state.update(&input).unwrap();
                let mut merged = AggregateState::new(operator).unwrap();
                merged.merge(&state.finish_partial().unwrap()).unwrap();
                assert_eq!(
                    merged
                        .finish()
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0),
                    9_007_199_254_740_995
                );
            }
        }
    }

    #[test]
    fn filtered_count_star_handles_zero_column_batches() {
        use crate::expr::scalar::ConstantExpression;
        let input_schema = Arc::new(Schema::empty());
        let expressions = [true, false]
            .into_iter()
            .map(|v| {
                Arc::new(
                    AggregateExpression::new(AggregateFunction::Count, vec![])
                        .with_filter(ConstantExpression::boolean(Some(v)).into_ref())
                        .with_alias(format!("count_{v}")),
                )
            })
            .collect();
        let operator = Arc::new(
            AggregateOperator::try_new(Projection::new(input_schema.clone(), vec![]), expressions)
                .unwrap(),
        );
        let mut state = AggregateState::new(operator).unwrap();
        let batch = RecordBatch::try_new_with_options(
            input_schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        state.update(&batch).unwrap();
        let result = state.finish().unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }

    #[test]
    fn invalid_aggregate_signatures_are_plan_errors() {
        use crate::expr::scalar::ConstantExpression;
        for expr in [
            AggregateExpression::new(AggregateFunction::Sum, vec![]),
            AggregateExpression::new(AggregateFunction::Sum, vec![value()]).with_distinct(),
            AggregateExpression::new(AggregateFunction::Count, vec![]).with_distinct(),
            AggregateExpression::new(AggregateFunction::Count, vec![value(), value()]),
            AggregateExpression::new(AggregateFunction::CovarPop, vec![value()]),
            AggregateExpression::new(
                AggregateFunction::Sum,
                vec![ConstantExpression::string(Some("x")).into_ref()],
            ),
            AggregateExpression::new(AggregateFunction::Count, vec![]).with_filter(value()),
        ] {
            assert!(matches!(
                AggregateOperator::try_new(
                    groups(false),
                    vec![Arc::new(expr.with_alias("invalid"))]
                ),
                Err(Error::InvalidPlan(_))
            ));
        }
    }
}
