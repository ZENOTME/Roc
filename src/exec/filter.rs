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

use super::{GlobalExecContextRef, ProcessExec};
use crate::{error::Result, expr::scalar::ScalarExprRef};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FilterExec {
    predicate: ScalarExprRef,
    output_projection: Option<Vec<usize>>,
}
impl FilterExec {
    pub fn new(predicate: ScalarExprRef) -> Self {
        Self {
            predicate,
            output_projection: None,
        }
    }
    pub fn with_output_projection(mut self, indices: Vec<usize>) -> Self {
        self.output_projection = Some(indices);
        self
    }
}
impl ProcessExec for FilterExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn emit_program(
        &self,
        _global: GlobalExecContextRef,
        builder: &mut crate::program::ProgramBuilder,
        input: crate::program::ValueId,
    ) -> Result<crate::program::ValueId> {
        builder.emit_filter(input, &self.predicate, self.output_projection.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{
        ExpressionResultType,
        scalar::{ConstantExpression, ReferenceExpression},
    };
    use crate::{error::Error, exec::ProcessResult};
    use arrow::array::AsArray;
    use arrow::{
        array::{BooleanArray, Int64Array},
        buffer::{BooleanBuffer, NullBuffer},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    fn filter(input: &RecordBatch, predicate: ScalarExprRef) -> Result<RecordBatch> {
        let mut filter = FilterExec {
            predicate: predicate,
            output_projection: None,
        }
        .new_executor(Arc::new(()))
        .unwrap();
        let ProcessResult::NeedMoreInput(output) = filter.execute(&input.clone().into())? else {
            unreachable!()
        };
        output.into_record_batch()
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
    #[test]
    fn projection_filters_selected_columns_after_reading_full_predicate_input() {
        let input = batch(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ]));
        for (indices, columns) in [(vec![0], 1), (vec![], 0), (vec![0, 0], 2)] {
            let mut filter = FilterExec {
                predicate: ReferenceExpression::new(
                    1,
                    ExpressionResultType::new(DataType::Boolean, true),
                )
                .into_ref(),
                output_projection: Some(indices),
            }
            .new_executor(Arc::new(()))
            .unwrap();
            let ProcessResult::NeedMoreInput(output) =
                filter.execute(&input.clone().into()).unwrap()
            else {
                unreachable!()
            };
            assert_eq!(output.num_columns(), columns);
            assert_eq!(output.num_rows(), 2);
            for array in output.materialize().unwrap().columns() {
                assert_eq!(
                    array
                        .as_primitive::<arrow::datatypes::Int64Type>()
                        .values()
                        .as_ref(),
                    &[0, 3]
                );
            }
        }
        for indices in [vec![0], vec![], vec![0, 0]] {
            for (value, rows) in [(Some(true), 4), (Some(false), 0), (None, 0)] {
                let mut filter = FilterExec {
                    predicate: ConstantExpression::boolean(value).into_ref(),
                    output_projection: Some(indices.clone()),
                }
                .new_executor(Arc::new(()))
                .unwrap();
                let ProcessResult::NeedMoreInput(output) =
                    filter.execute(&input.clone().into()).unwrap()
                else {
                    unreachable!()
                };
                let expected = input.project(&indices).unwrap().slice(0, rows);
                assert_eq!(output.schema(), expected.schema());
                assert_eq!(output.num_columns(), indices.len());
                assert_eq!(output.num_rows(), rows);
                assert_eq!(output.into_record_batch().unwrap(), expected);
            }
        }
        let mut invalid = FilterExec {
            predicate: ConstantExpression::boolean(Some(true)).into_ref(),
            output_projection: Some(vec![2]),
        }
        .new_executor(Arc::new(()))
        .unwrap();
        assert!(invalid.execute(&input.into()).is_err());
    }
}
