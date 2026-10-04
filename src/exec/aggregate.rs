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

use super::ProjectionExecutor;
use super::{GlobalExecContextRef, SinkExec, SinkExecutor, SinkResult, SourceExec, SourceExecutor};
use crate::expr::agg::executor::AggregateExpressionExecutor;
use crate::{
    error::{Error, Result},
    operator::AggregateOperator,
};
use arrow::datatypes::SchemaRef;
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
    fn new(operator: &AggregateOperator) -> Result<Self> {
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
        let output_schema = operator.output_schema();
        let mut accumulators = vec![];
        for aggregate in operator.aggregates() {
            let mut executor = AggregateExpressionExecutor::try_new(aggregate.clone())?;
            if !grouped {
                executor.resize(1);
            }
            accumulators.push(executor);
        }
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
        if self.groups.output_schema().fields().is_empty() {
            for accumulator in &mut self.accumulators {
                accumulator.update_single(batch)?;
            }
            return Ok(());
        }
        let groups = self.groups.project_batch(batch)?;
        let ids = self.group_ids(groups.columns(), batch.num_rows())?;
        let count = self.group_count();
        for accumulator in &mut self.accumulators {
            accumulator.update(batch, &ids, count)?;
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

    fn finish(self) -> Result<RecordBatch> {
        let count = self.group_count();
        let schema = self.output_schema.clone();
        if count == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }
        let mut columns = self.group_columns()?;
        for accumulator in &self.accumulators {
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
            local: AggregateState::new(&self.operator)?,
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
            let mut state = AggregateState::new(&self.operator)?;
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
        expr::ExpressionResultType,
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

    /// Column 1 (`value`) of `schema`.
    fn value() -> ScalarExprRef {
        ReferenceExpression::new(1, ExpressionResultType::new(DataType::Float64, true)).into_ref()
    }
    fn groups(grouped: bool) -> Projection {
        Projection::from_indices(schema(), if grouped { &[0] } else { &[] }).unwrap()
    }
    /// Result types and nullability are host-supplied: SUM over Float64 is
    /// Float64, AVG and COVAR_POP are Float64, and only COUNT is non-nullable.
    fn aggregates() -> Vec<Arc<AggregateExpression>> {
        use AggregateFunction::*;
        vec![
            Arc::new(AggregateExpression::new(
                Sum,
                vec![value()],
                DataType::Float64,
                true,
            )),
            Arc::new(
                AggregateExpression::new(Avg, vec![value()], DataType::Float64, true)
                    .with_alias("avg"),
            ),
            Arc::new(
                AggregateExpression::new(Count, vec![value()], DataType::Int64, false)
                    .with_alias("count"),
            ),
            Arc::new(
                AggregateExpression::new(Count, vec![value()], DataType::Int64, false)
                    .with_distinct()
                    .with_alias("distinct"),
            ),
            Arc::new(
                AggregateExpression::new(CovarPop, vec![value(), value()], DataType::Float64, true)
                    .with_alias("covar"),
            ),
            Arc::new(
                AggregateExpression::new(Count, vec![], DataType::Int64, false).with_alias("rows"),
            ),
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
        let mut merged = AggregateState::new(&operator).unwrap();
        for batch in batches {
            let mut worker = AggregateState::new(&operator).unwrap();
            // Exercise repeated updates and one-row partial merge chunks.
            for row in 0..batch.num_rows() {
                worker.update(&batch.slice(row, 1)).unwrap();
            }
            let partial = worker.finish_partial().unwrap();
            assert_eq!(partial.groups.schema(), operator.groups().output_schema());
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
    fn empty_global_input_has_one_row() {
        let operator = Arc::new(AggregateOperator::try_new(groups(false), aggregates()).unwrap());
        let mut merged = AggregateState::new(&operator).unwrap();
        for update_empty in [false, true] {
            let mut worker = AggregateState::new(&operator).unwrap();
            if update_empty {
                worker.update(&RecordBatch::new_empty(schema())).unwrap();
            }
            merged.merge(&worker.finish_partial().unwrap()).unwrap();
        }
        for result in [
            merged.finish().unwrap(),
            AggregateState::new(&operator).unwrap().finish().unwrap(),
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
        let mut worker = AggregateState::new(&operator).unwrap();
        worker
            .update(&batch(vec![Some("a"); 2], vec![None; 2]))
            .unwrap();
        worker.update(&RecordBatch::new_empty(schema())).unwrap();
        let mut merged = AggregateState::new(&operator).unwrap();
        merged.merge(&worker.finish_partial().unwrap()).unwrap();
        merged
            .merge(
                &AggregateState::new(&operator)
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
        let mut merged = AggregateState::new(&operator).unwrap();
        for values in [vec![Some(2.), None], vec![Some(2.), Some(4.)]] {
            let mut worker = AggregateState::new(&operator).unwrap();
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
            let mut state = AggregateState::new(&operator).unwrap();
            state.update(&RecordBatch::new_empty(schema())).unwrap();
            let partial = state.finish_partial().unwrap();
            assert_eq!(partial.num_rows(), 0);
            assert_eq!(partial.groups.schema(), operator.groups().output_schema());
            for (arrays, aggregate) in partial.states.iter().zip(operator.aggregates()) {
                let fields = AggregateExpressionExecutor::try_new(aggregate.clone())
                    .unwrap()
                    .state_types();
                assert_eq!(arrays.len(), fields.len());
                for (array, field) in arrays.iter().zip(fields) {
                    assert_eq!(array.len(), 0);
                    assert_eq!(array.data_type(), &field);
                }
            }
            let mut merged = AggregateState::new(&operator).unwrap();
            merged.merge(&partial).unwrap();
            let result = merged.finish().unwrap();
            assert_eq!(result.num_rows(), 0);
            assert_eq!(result.schema(), operator.output_schema());
        }

        let operator = Arc::new(AggregateOperator::try_new(groups(true), vec![]).unwrap());
        let mut worker = AggregateState::new(&operator).unwrap();
        worker
            .update(&batch(vec![Some("a"), None, Some("a")], vec![None; 3]))
            .unwrap();
        let mut merged = AggregateState::new(&operator).unwrap();
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
        use crate::expr::scalar::{ConstantExpression, FunctionExpression, FunctionKind};
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        // Column 1 of this input is Int64, unlike the Float64 column of `schema`.
        let value = || {
            ReferenceExpression::new(1, ExpressionResultType::new(DataType::Int64, false))
                .into_ref()
        };
        let int = |v| ConstantExpression::int64(Some(v)).into_ref();
        // A comparison declares its Boolean result; arithmetic declares the
        // numeric type its kernel is bound against.
        let predicate = FunctionExpression::binary(
            FunctionKind::NotEqual,
            value(),
            int(0),
            DataType::Boolean,
            false,
        )
        .into_ref();
        let division = FunctionExpression::binary(
            FunctionKind::Divide,
            int(100),
            value(),
            DataType::Int64,
            true,
        )
        .into_ref();
        let aggregates = vec![
            Arc::new(
                AggregateExpression::new(
                    AggregateFunction::Sum,
                    vec![division],
                    DataType::Int64,
                    true,
                )
                .with_filter(predicate.clone())
                .with_alias("sum"),
            ),
            Arc::new(
                AggregateExpression::new(
                    AggregateFunction::Count,
                    vec![value()],
                    DataType::Int64,
                    false,
                )
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
        let mut merged = AggregateState::new(&operator).unwrap();
        for _ in 0..2 {
            let input = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "a"])),
                    Arc::new(Int64Array::from(vec![2, 0, 4])),
                ],
            )
            .unwrap();
            let mut worker = AggregateState::new(&operator).unwrap();
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
        let expr =
            ReferenceExpression::new(0, ExpressionResultType::new(DataType::Utf8, true)).into_ref();
        let aggregates = [AggregateFunction::Min, AggregateFunction::Max]
            .into_iter()
            .map(|f| {
                Arc::new(
                    AggregateExpression::new(f, vec![expr.clone()], DataType::Utf8, true)
                        .with_alias(format!("{f:?}")),
                )
            })
            .collect();
        let operator =
            Arc::new(AggregateOperator::try_new(Projection::new(vec![]), aggregates).unwrap());
        let mut merged = AggregateState::new(&operator).unwrap();
        for values in [vec![Some("z"), None], vec![None, Some("a")], vec![None]] {
            let input = RecordBatch::try_new(
                input_schema.clone(),
                vec![Arc::new(StringArray::from(values))],
            )
            .unwrap();
            let mut worker = AggregateState::new(&operator).unwrap();
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
        let empty = AggregateState::new(&operator).unwrap().finish().unwrap();
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
                    Projection::new(vec![]),
                    vec![Arc::new(
                        AggregateExpression::new(
                            AggregateFunction::Sum,
                            vec![
                                ReferenceExpression::new(
                                    0,
                                    ExpressionResultType::new(data_type.clone(), true),
                                )
                                .into_ref(),
                            ],
                            data_type.clone(),
                            true,
                        )
                        .with_alias("sum"),
                    )],
                )
                .unwrap(),
            );
            let mut state = AggregateState::new(&operator).unwrap();
            let input = RecordBatch::try_new(input_schema, vec![array]).unwrap();
            if data_type == DataType::Int64 {
                assert!(state.update(&input).is_err());
            } else {
                state.update(&input).unwrap();
                let mut merged = AggregateState::new(&operator).unwrap();
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
                    AggregateExpression::new(
                        AggregateFunction::Count,
                        vec![],
                        DataType::Int64,
                        false,
                    )
                    .with_filter(ConstantExpression::boolean(Some(v)).into_ref())
                    .with_alias(format!("count_{v}")),
                )
            })
            .collect();
        let operator =
            Arc::new(AggregateOperator::try_new(Projection::new(vec![]), expressions).unwrap());
        let mut state = AggregateState::new(&operator).unwrap();
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
    fn aggregate_signatures_without_usable_kernels_are_plan_errors() {
        // DISTINCT and MIN/MAX derive their state key type from an argument, so
        // a missing argument has no kernel to build. The operator itself only
        // assembles descriptions; executor construction is the first point that
        // builds an accumulator.
        for expr in [
            AggregateExpression::new(AggregateFunction::Count, vec![], DataType::Int64, false)
                .with_distinct(),
            AggregateExpression::new(AggregateFunction::Min, vec![], DataType::Utf8, true),
            AggregateExpression::new(AggregateFunction::Max, vec![], DataType::Utf8, true),
            // Only COUNT keeps a DISTINCT set; the others would silently drop it.
            AggregateExpression::new(
                AggregateFunction::Sum,
                vec![value()],
                DataType::Float64,
                true,
            )
            .with_distinct(),
        ] {
            let operator = AggregateOperator::try_new(
                groups(false),
                vec![Arc::new(expr.with_alias("invalid"))],
            )
            .expect("operator construction does not validate aggregates");
            assert!(matches!(
                AggregateState::new(&operator),
                Err(Error::InvalidPlan(_))
            ));
        }
    }

    #[test]
    fn aggregate_executor_defers_other_signature_checks_to_runtime() {
        // Arity and argument types are host responsibilities now. COUNT simply
        // ignores arguments beyond the first; SUM, AVG, and COVAR_POP build
        // argument-independent state and reject a missing argument once values
        // arrive, because their arity is not checked while constructing.
        let count_two = AggregateExpression::new(
            AggregateFunction::Count,
            vec![value(), value()],
            DataType::Int64,
            false,
        );
        let operator = AggregateOperator::try_new(
            groups(false),
            vec![Arc::new(count_two.with_alias("count_two"))],
        )
        .unwrap();
        let mut state = AggregateState::new(&operator).unwrap();
        state
            .update(&batch(vec![Some("a")], vec![None]))
            .expect("extra COUNT arguments are accepted");
        assert_eq!(
            state
                .finish()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );

        for expr in [
            AggregateExpression::new(AggregateFunction::Sum, vec![], DataType::Float64, true),
            AggregateExpression::new(AggregateFunction::Avg, vec![], DataType::Float64, true),
            AggregateExpression::new(
                AggregateFunction::CovarPop,
                vec![value()],
                DataType::Float64,
                true,
            ),
        ] {
            let operator = AggregateOperator::try_new(
                groups(false),
                vec![Arc::new(expr.with_alias("missing_argument"))],
            )
            .unwrap();
            let mut state = AggregateState::new(&operator).unwrap();
            assert!(
                state
                    .update(&batch(vec![Some("a")], vec![Some(1.)]))
                    .is_err()
            );
        }

        use crate::expr::scalar::ConstantExpression;
        // SUM does not validate that its argument is numeric: Arrow's safe cast
        // turns an unparseable text value into NULL, and the row is skipped.
        let operator = Arc::new(
            AggregateOperator::try_new(
                groups(false),
                vec![Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Sum,
                        vec![ConstantExpression::string(Some("x")).into_ref()],
                        DataType::Int64,
                        true,
                    )
                    .with_alias("sum"),
                )],
            )
            .unwrap(),
        );
        let mut state = AggregateState::new(&operator).unwrap();
        state
            .update(&batch(vec![Some("a")], vec![Some(1.)]))
            .expect("an unparseable value is skipped, not rejected");
        assert!(state.finish().unwrap().column(0).is_null(0));

        // A filter must evaluate to Boolean before rows can be selected.
        let operator = Arc::new(
            AggregateOperator::try_new(
                groups(false),
                vec![Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Count,
                        vec![],
                        DataType::Int64,
                        false,
                    )
                    .with_filter(value())
                    .with_alias("rows"),
                )],
            )
            .unwrap(),
        );
        let mut state = AggregateState::new(&operator).unwrap();
        assert!(
            state
                .update(&batch(vec![Some("a")], vec![Some(1.)]))
                .is_err()
        );
    }

    #[test]
    fn unsupported_sum_result_type_is_an_execution_error() {
        // The host declares the SUM result type; an unusable one must not panic.
        let operator = Arc::new(
            AggregateOperator::try_new(
                groups(false),
                vec![Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Sum,
                        vec![value()],
                        DataType::Utf8,
                        true,
                    )
                    .with_alias("sum"),
                )],
            )
            .unwrap(),
        );
        let mut state = AggregateState::new(&operator).unwrap();
        assert!(matches!(
            state.update(&batch(vec![Some("a")], vec![Some(1.)])),
            Err(Error::Execution(_))
        ));
    }
}
