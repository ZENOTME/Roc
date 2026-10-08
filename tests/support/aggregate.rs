#![allow(dead_code)]
use roc::{error::{Error,Result},expr::{agg::{AggregateAccumulator,AggregateExpression,AggregateFunction},predicate::select_true},program::{ProcessProgram,ProgramBuilder,Value}};
use arrow::{array::{ArrayRef,BooleanArray},compute::filter_record_batch,datatypes::DataType,record_batch::RecordBatch};
use std::sync::Arc;
fn bind(e:&roc::expr::scalar::ScalarExpression)->Result<ProcessProgram>{let mut b=ProgramBuilder::default();let input=b.value();let value=e.emit(&mut b,input)?;b.build_value(input,value)}
pub struct AggregateInput {argument:Option<ProcessProgram>,filter:Option<ProcessProgram>}
impl AggregateInput {
    pub fn new(e:&AggregateExpression)->Result<Self>{Ok(Self{argument:e.argument().map(|e|bind(e)).transpose()?,filter:e.filter().map(|e|bind(e)).transpose()?})}
    pub fn apply(&mut self,state:&mut AggregateAccumulator,input:&RecordBatch,ids:&[usize],groups:usize)->Result<()>{
        if ids.len()!=input.num_rows()||ids.iter().any(|&id|id>=groups){return Err(Error::Execution("aggregate group IDs do not match input".into()));}
        state.resize(groups);
        let selected=self.filter.as_mut().map(|p|select_true(p.run_value(&Value::input(input.columns(),input.num_rows()))?.into_array(input.num_rows())?)).transpose()?;
        let selected_ids=selected.as_ref().map(|rows|rows.iter().map(|&i|ids[i]).collect::<Vec<_>>());let ids=selected_ids.as_deref().unwrap_or(ids);
        if ids.is_empty(){return Ok(());}
        let selected_input=selected.as_ref().map(|rows|{let mut bits=vec![false;input.num_rows()];for &i in rows{bits[i]=true;}filter_record_batch(input,&BooleanArray::from(bits))}).transpose()?;
        let input=selected_input.as_ref().unwrap_or(input);
        let value=self.argument.as_mut().map(|p|p.run_value(&Value::input(input.columns(),input.num_rows()))?.into_array(input.num_rows())).transpose()?;
        state.update(value.as_ref(),ids,groups)
    }
}
pub struct AggregateHarness {state:AggregateAccumulator,input:AggregateInput}
impl AggregateHarness {
    pub fn try_new(e:Arc<AggregateExpression>)->Result<Self>{Ok(Self{input:AggregateInput::new(&e)?,state:AggregateAccumulator::try_new(e)?})}
    pub fn update(&mut self,input:&RecordBatch,ids:&[usize],groups:usize)->Result<()>{self.input.apply(&mut self.state,input,ids,groups)}
    pub fn resize(&mut self,count:usize){self.state.resize(count)}
    pub fn bind_global_count(&mut self){self.state.bind_global_count()}
    pub fn bind_global_sum(&mut self){self.state.bind_global_sum()}
    pub fn state_types(&self)->Vec<DataType>{self.state.state_types()}
    pub fn state(&self)->Result<Vec<ArrayRef>>{self.state.state()}
    pub fn evaluate(&self)->Result<ArrayRef>{self.state.evaluate()}
    pub fn merge(&mut self,state:&[ArrayRef],ids:&[usize],groups:usize)->Result<()>{self.state.merge(state,ids,groups)}
}
#[cfg(test)]
mod tests {
    use super::*;
    use roc::expr::ExpressionResultType;
    use roc::expr::scalar::{ConstantExpression, ReferenceExpression};
    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
    };

    #[test]
    fn filtered_arguments_and_constants_follow_original_group_ids() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(40),
                Some(10),
                Some(40),
                None,
                Some(20),
            ])),
            Arc::new(BooleanArray::from(vec![true, true, true, true, false])),
        ];
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, true),
                Field::new("keep", DataType::Boolean, false),
            ])),
            columns,
        )
        .unwrap();
        let ids = [1, 0, 1, 0, 1];
        for (argument, expected) in [
            (
                Some(
                    ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true))
                        .into_ref(),
                ),
                vec![Some(10), Some(80)],
            ),
            (
                Some(ConstantExpression::int64(Some(3)).into_ref()),
                vec![Some(6), Some(6)],
            ),
        ] {
            let expression = Arc::new(
                AggregateExpression::new(AggregateFunction::Sum, argument, DataType::Int64, true)
                    .with_filter(
                        ReferenceExpression::new(
                            1,
                            ExpressionResultType::new(DataType::Boolean, false),
                        )
                        .into_ref(),
                    ),
            );
            let mut executor = AggregateHarness::try_new(expression).unwrap();
            executor.update(&input, &ids, 2).unwrap();
            let result = executor.evaluate().unwrap();
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let expression = Arc::new(AggregateExpression::new(
            AggregateFunction::Count,
            None,
            DataType::Int64,
            false,
        ));
        let mut executor = AggregateHarness::try_new(expression).unwrap();
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
    fn references_check_indices_after_filtering() {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        // References must reject out-of-range indices, including usize::MAX,
        // without panicking.
        for index in [1, usize::MAX] {
            let expression = AggregateExpression::new(
                AggregateFunction::Sum,
                Some(
                    ReferenceExpression::new(
                        index,
                        ExpressionResultType::new(DataType::Int64, false),
                    )
                    .into_ref(),
                ),
                DataType::Int64,
                true,
            );
            let mut grouped =
                AggregateHarness::try_new(Arc::new(expression.clone())).unwrap();
            for result in [grouped.update(&input, &[0, 0], 1)] {
                assert!(
                    matches!(result, Err(Error::Execution(message)) if message == format!("column index {index} out of bounds"))
                );
            }
            // When FILTER rejects every row, argument evaluation is skipped entirely,
            // including its bounds checks, as on the generic expression path.
            let expression =
                expression.with_filter(ConstantExpression::boolean(Some(false)).into_ref());
            let mut filtered = AggregateHarness::try_new(Arc::new(expression)).unwrap();
            filtered.update(&input, &[0, 0], 1).unwrap();
            assert!(filtered.evaluate().unwrap().is_null(0));
        }
    }

    #[test]
    fn computed_argument_uses_filtered_input() {
        use roc::expr::scalar::{FunctionExpression, FunctionKind};

        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new("keep", DataType::Boolean, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![0, 2, 4])),
                Arc::new(BooleanArray::from(vec![false, true, true])),
            ],
        )
        .unwrap();
        let reference =
            ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, false))
                .into_ref();
        let divided = FunctionExpression::binary(
            FunctionKind::Divide,
            ConstantExpression::int64(Some(8)).into_ref(),
            reference.clone(),
            DataType::Int64,
            false,
        )
        .into_ref();
        let expression = Arc::new(
            AggregateExpression::new(AggregateFunction::Sum, Some(divided), DataType::Int64, true)
                .with_filter(
                    ReferenceExpression::new(
                        1,
                        ExpressionResultType::new(DataType::Boolean, false),
                    )
                    .into_ref(),
                ),
        );
        let mut grouped = AggregateHarness::try_new(expression.clone()).unwrap();
        grouped.update(&input, &[0, 0, 0], 1).unwrap();
        for executor in [grouped] {
            assert_eq!(
                executor
                    .evaluate()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                6
            );
        }
    }
}
