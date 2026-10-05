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

use super::{GlobalExecContextRef, ProcessExec, ProcessExecutor, ProcessResult};
use crate::expr::scalar::executor::{ScalarExpressionEvaluation, ScalarExpressionExecutor};
use crate::{
    error::{Error, Result},
    expr::scalar::ScalarExprRef,
};
use arrow::{array::AsArray, compute::filter_record_batch, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FilterExec {
    predicate: ScalarExprRef,
}
impl FilterExec {
    pub fn new(predicate: ScalarExprRef) -> Self {
        Self { predicate }
    }
}
impl ProcessExec for FilterExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn ProcessExecutor>> {
        Ok(Box::new(FilterExecutor {
            predicate: self.predicate.to_evaluation()?,
        }))
    }
}

/// Built once from the description, then retained across batches.
struct FilterExecutor {
    predicate: ScalarExpressionEvaluation,
}
impl ProcessExecutor for FilterExecutor {
    fn execute(&mut self, input: &RecordBatch) -> Result<ProcessResult> {
        let executor = ScalarExpressionExecutor::new(input.columns(), input.num_rows());
        let predicate = self.predicate.evaluate(&executor)?;
        let predicate = predicate.into_array(input.num_rows())?;
        let mask = predicate
            .as_boolean_opt()
            .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
        // Arrow applies validity as part of the filter: only valid TRUE rows
        // pass. Keep that bitmap instead of expanding it to row indices and
        // rebuilding the same bitmap before filtering every column.
        let output = filter_record_batch(input, mask)?;
        Ok(ProcessResult::NeedMoreInput(output))
    }
    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{
        ExpressionResultType,
        scalar::{ConstantExpression, ReferenceExpression},
    };
    use arrow::{
        array::{BooleanArray, Int64Array},
        buffer::{BooleanBuffer, NullBuffer},
        datatypes::{DataType, Field, Schema},
    };

    fn filter(input: &RecordBatch, predicate: ScalarExprRef) -> Result<RecordBatch> {
        let mut filter = FilterExecutor {
            predicate: predicate.to_evaluation()?,
        };
        let ProcessResult::NeedMoreInput(output) = filter.execute(input)? else {
            unreachable!()
        };
        Ok(output)
    }

    fn batch(mask: BooleanArray) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new("predicate", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..mask.len() as i64)),
                Arc::new(mask),
            ],
        )
        .unwrap()
    }

    #[test]
    fn predicate_bitmap_ignores_true_bits_under_nulls_and_respects_slices() {
        // The invalid row deliberately has its value bit set to TRUE.
        let input = batch(BooleanArray::new(
            BooleanBuffer::from(vec![false, true, true, false, true, false]),
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        ))
        .slice(1, 4);
        let output = filter(
            &input,
            ReferenceExpression::new(1, ExpressionResultType::new(DataType::Boolean, true))
                .into_ref(),
        )
        .unwrap();
        assert_eq!(
            output
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values()
                .as_ref(),
            &[1, 4]
        );
        assert_eq!(output.schema(), input.schema());
    }

    #[test]
    fn constant_predicates_and_empty_inputs_preserve_filter_semantics() {
        let input = batch(BooleanArray::from(vec![true, false, true]));
        for (predicate, rows) in [(Some(true), 3), (Some(false), 0), (None, 0)] {
            let expr = ConstantExpression::boolean(predicate).into_ref();
            assert_eq!(filter(&input, expr.clone()).unwrap().num_rows(), rows);
            assert_eq!(filter(&input.slice(1, 0), expr).unwrap().num_rows(), 0);
        }
    }

    #[test]
    fn non_boolean_predicate_is_an_execution_error() {
        let input = batch(BooleanArray::from(vec![true]));
        assert!(matches!(
            filter(&input, ConstantExpression::int64(Some(1)).into_ref()),
            Err(Error::Execution(_))
        ));
    }
}
