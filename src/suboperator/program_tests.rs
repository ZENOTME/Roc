use super::*;
use crate::arrow::array::{AsArray, BooleanArray};
use crate::arrow::record_batch::RecordBatch;
use crate::operator::Projection;

#[cfg(test)]
mod lowering {
    use super::*;
    use crate::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    use crate::{
        expr::{ExpressionResultType, agg::*, scalar::*},
        operator::{AggregateOperator, ProjectionExpression},
    };
    fn reference(index: usize) -> ScalarExprRef {
        ReferenceExpression::new(index, ExpressionResultType::new(DataType::Int64, true)).into_ref()
    }
    fn add(input: ScalarExprRef, n: i64) -> ScalarExprRef {
        FunctionExpression::binary(
            FunctionKind::Add,
            input,
            ConstantExpression::int64(Some(n)).into_ref(),
            DataType::Int64,
            true,
        )
        .into_ref()
    }
    fn project(expr: ScalarExprRef) -> Projection {
        Projection::new(vec![ProjectionExpression::new(expr, "x")])
    }
    fn batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }
    fn modes() -> Vec<ExecutionMode> {
        vec![ExecutionMode::Compiled]
    }
    fn execute(
        p: &ProgramContext,
        local: &mut ProgramExecutionContext,
        input: RecordBatch,
    ) -> Result<RecordBatch> {
        p.execute_record_batch(&input, local)
    }
    #[test]
    fn projections_share_ids_and_fuse_across_physical_boundaries() {
        for mode in modes() {
            let mut b = SuboperatorScopeBuilder::new();
            for _ in 0..8 {
                project(add(reference(0), 1))
                    .build_suboperator(&mut b)
                    .unwrap();
            }
            let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
            assert_eq!(
                p.programs()
                    .iter()
                    .filter(|p| p.kind() == "source_column")
                    .count(),
                1
            );
            let mut local = p.new_execution_context();
            let out = execute(&p, &mut local, batch(vec![Some(1), None, Some(-8)])).unwrap();
            assert_eq!(
                out.column(0).as_ref(),
                &Int64Array::from(vec![Some(9), None, Some(0)])
            );
            if mode == ExecutionMode::Compiled {
                assert!(p.explain().contains("cranelift_loop"));
            }
        }
    }
    #[test]
    fn shared_expression_rebinds_after_projection_and_selection() {
        for mode in modes() {
            let expression = add(reference(0), 1);
            let mut b = SuboperatorScopeBuilder::new();
            // Both columns reuse the same node in one view. Reusing that node
            // after a projection must instead read the new column 0.
            Projection::new(vec![
                ProjectionExpression::new(expression.clone(), "x"),
                ProjectionExpression::new(expression.clone(), "copy"),
            ])
            .build_suboperator(&mut b)
            .unwrap();
            project(expression.clone())
                .build_suboperator(&mut b)
                .unwrap();
            let predicate = FunctionExpression::binary(
                FunctionKind::LessThan,
                reference(0),
                ConstantExpression::int64(Some(5)).into_ref(),
                DataType::Boolean,
                true,
            )
            .into_ref();
            crate::operator::FilterOperator::new(predicate)
                .build_suboperator(&mut b)
                .unwrap();
            project(expression).build_suboperator(&mut b).unwrap();
            let context = b.finish(vec![]).unwrap();
            assert_eq!(
                context
                    .suboperators()
                    .iter()
                    .filter(|op| matches!(
                        op,
                        Suboperator::Call {
                            function: FunctionKind::Add,
                            ..
                        }
                    ))
                    .count(),
                3
            );
            let p = context.compile(mode).unwrap();
            let out = execute(
                &p,
                &mut p.new_execution_context(),
                batch(vec![Some(1), Some(2), Some(3), None]),
            )
            .unwrap();
            assert_eq!(out.column(0).as_ref(), &Int64Array::from(vec![4, 5]));
        }
    }
    #[test]
    fn discarded_projection_still_reports_errors_but_filter_skips_later_work() {
        for mode in modes() {
            let mut b = SuboperatorScopeBuilder::new();
            project(add(reference(0), 1))
                .build_suboperator(&mut b)
                .unwrap();
            Projection::new(vec![]).build_suboperator(&mut b).unwrap();
            let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
            let mut local = p.new_execution_context();
            assert!(execute(&p, &mut local, batch(vec![Some(i64::MAX)])).is_err());
            let empty_columns = execute(&p, &mut local, batch(vec![Some(1), None])).unwrap();
            assert_eq!(
                (empty_columns.num_columns(), empty_columns.num_rows()),
                (0, 2)
            );

            let mut b = SuboperatorScopeBuilder::new();
            let predicate = FunctionExpression::binary(
                FunctionKind::LessThan,
                reference(0),
                ConstantExpression::int64(Some(10)).into_ref(),
                DataType::Boolean,
                true,
            )
            .into_ref();
            crate::operator::FilterOperator::new(predicate)
                .build_suboperator(&mut b)
                .unwrap();
            project(add(reference(0), 1))
                .build_suboperator(&mut b)
                .unwrap();
            let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
            assert_eq!(
                p.programs()
                    .iter()
                    .filter(|p| p.kind() == "projection")
                    .count(),
                1
            );
            let mut local = p.new_execution_context();
            let out = execute(&p, &mut local, batch(vec![Some(i64::MAX), None, Some(2)])).unwrap();
            assert_eq!(out.column(0).as_ref(), &Int64Array::from(vec![3]));
        }
    }
    #[test]
    fn aggregate_kernels_use_ordinary_slots_and_instances_do_not_share_updates() {
        for mode in modes() {
            let mut b = SuboperatorScopeBuilder::new();
            project(add(reference(0), 1))
                .build_suboperator(&mut b)
                .unwrap();
            let agg = AggregateOperator::try_new(
                Projection::new(vec![]),
                vec![Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Sum,
                        vec![add(reference(0), 2)],
                        DataType::Int64,
                        true,
                    )
                    .with_alias("sum"),
                )],
            )
            .unwrap();
            let states = agg.build_suboperator(&mut b).unwrap();
            let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
            assert_eq!(states.len(), 2);
            let schedule = p.explain();
            assert!(schedule.contains("group_lookup"));
            assert!(schedule.contains("aggregate_update"));
            let mut a = p.new_execution_context();
            let mut z = p.new_execution_context();
            execute(&p, &mut a, batch(vec![Some(1), None, Some(2)])).unwrap();
            execute(&p, &mut a, batch(vec![Some(3)])).unwrap();
            execute(&p, &mut z, batch(vec![Some(100)])).unwrap();
            let (_, a) = crate::suboperator::aggregation::partial(&mut a, &states).unwrap();
            let (_, z) = crate::suboperator::aggregation::partial(&mut z, &states).unwrap();
            // SUM's first partial column is the sum; its other column tracks validity/count.
            assert_eq!(a[0][0].as_ref(), &Int64Array::from(vec![15]));
            assert_eq!(z[0][0].as_ref(), &Int64Array::from(vec![103]));
            let mut empty = p.new_execution_context();
            p.initialize(&mut empty).unwrap();
            let (groups, partial) =
                crate::suboperator::aggregation::partial(&mut empty, &states).unwrap();
            assert_eq!(groups.num_rows(), 1);
            assert_eq!(partial.len(), 1);
        }
    }
    #[test]
    fn filtered_aggregate_updates_only_surviving_rows_across_batches() {
        for mode in modes() {
            let mut b = SuboperatorScopeBuilder::new();
            let x = reference(0).build_suboperator(&mut b).unwrap();
            let zero = b.constant(Arc::new(Int64Array::from(vec![0]))).unwrap();
            let positive = b.binary(FunctionKind::GreaterThan, x, zero).unwrap();
            b.filter(positive).unwrap();
            let ten = b.constant(Arc::new(Int64Array::from(vec![10]))).unwrap();
            let small = b.binary(FunctionKind::LessThan, x, ten).unwrap();
            b.filter(small).unwrap();
            let aggregate = AggregateOperator::try_new(
                Projection::new(vec![]),
                vec![Arc::new(
                    AggregateExpression::new(
                        AggregateFunction::Sum,
                        vec![reference(0)],
                        DataType::Int64,
                        true,
                    )
                    .with_alias("sum"),
                )],
            )
            .unwrap();
            let states = aggregate.build_suboperator(&mut b).unwrap();
            let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
            assert!(!p.explain().contains("materialize"));
            let mut local = p.new_execution_context();
            for values in [
                vec![Some(0), None, Some(2), Some(i64::MAX)],
                vec![],
                vec![Some(3), Some(-1)],
            ] {
                execute(&p, &mut local, batch(values)).unwrap();
            }
            let (_, result) =
                crate::suboperator::aggregation::partial(&mut local, &states).unwrap();
            assert_eq!(result[0][0].as_ref(), &Int64Array::from(vec![5]));
        }
    }

    #[test]
    fn external_states_validate_types_at_binding() {
        let mut b = SuboperatorScopeBuilder::new();
        let state = b.state(StateBuilder::external::<Vec<i64>>());
        let context = b
            .finish(vec![])
            .unwrap()
            .compile(ExecutionMode::Compiled)
            .unwrap();
        let mut local = ProgramExecutionContext::default();
        assert!(local.bind_state(&context, state, State::new(3u32)).is_err());
        local
            .bind_state(&context, state, State::new(vec![3i64]))
            .unwrap();
        assert_eq!(
            local
                .take_state(state)
                .unwrap()
                .into_inner::<Vec<i64>>()
                .unwrap(),
            vec![3]
        );
    }
}
#[cfg(test)]
mod filter_tests {
    use super::*;
    use crate::arrow::{
        array::{BooleanArray, Int64Array},
        buffer::{BooleanBuffer, NullBuffer},
        datatypes::{DataType, Field, Schema},
    };
    use crate::expr::{
        ExpressionResultType,
        scalar::{ConstantExpression, ReferenceExpression},
    };

    fn filter(input: &RecordBatch, predicate: ScalarExprRef) -> Result<RecordBatch> {
        let mut builder = SuboperatorScopeBuilder::new();
        crate::operator::FilterOperator::new(predicate).build_suboperator(&mut builder)?;
        let program = builder.finish(vec![])?.compile(ExecutionMode::Compiled)?;
        let mut local = program.new_execution_context();
        program.execute_record_batch(input, &mut local)
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
                .as_primitive::<crate::arrow::datatypes::Int64Type>()
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
    fn non_boolean_predicate_is_a_plan_error() {
        let input = batch(BooleanArray::from(vec![true]));
        assert!(matches!(
            filter(&input, ConstantExpression::int64(Some(1)).into_ref()),
            Err(Error::InvalidPlan(_))
        ));
    }
}

#[test]
fn reject_boundaries_and_data_edges_that_escape_their_scope() {
    let mut b = SuboperatorScopeBuilder::new();
    let predicate = b.input(
        0,
        crate::expr::ExpressionResultType::new(DataType::Boolean, false),
    );
    b.filter(predicate).unwrap();
    let second = b.not(predicate).unwrap();
    b.filter(second).unwrap();
    let scope = b.finish(vec![second]).unwrap();
    let mut backwards = scope.clone();
    backwards.boundaries.swap(0, 1);
    assert!(matches!(
        backwards.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
    let mut forward_control = scope.clone();
    forward_control.boundaries[0].operator = ScopeSuboperator::Filter {
        predicate: second,
        body_inputs: vec![],
        body_outputs: vec![],
    };
    assert!(matches!(
        forward_control.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
    let mut invalid_end = scope.clone();
    invalid_end.boundaries[0].end = usize::MAX;
    assert!(matches!(
        invalid_end.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
    let mut cycle = scope;
    if let Suboperator::Not { input, .. } = &mut cycle.suboperators[1] {
        *input = second;
    }
    assert!(matches!(
        cycle.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
}

#[test]
fn effect_tokens_order_shared_consumers_without_repeating_updates() {
    use super::super::aggregation::{AccumulatorState, GroupState};
    use crate::arrow::{array::Int64Array, datatypes::Schema};
    use crate::{
        exec::aggregate::group_index::GroupIndex,
        expr::{
            ExpressionResultType,
            agg::{AggregateFunction, accumulator::Accumulator},
        },
    };
    let modes = vec![ExecutionMode::Compiled];
    for mode in modes {
        let mut b = SuboperatorScopeBuilder::new();
        let groups = b.state(StateBuilder::new(|| {
            let schema = Arc::new(Schema::empty());
            Ok(GroupState {
                index: GroupIndex::new(&schema)?,
                schema,
            })
        }));
        let sum = b.state(StateBuilder::new(|| {
            let mut accumulator = Accumulator::new(
                AggregateFunction::Sum,
                false,
                &[DataType::Int64],
                &DataType::Int64,
            )?;
            accumulator.resize(1);
            Ok(AccumulatorState { accumulator })
        }));
        let ids = b.group_lookup(groups, vec![]).unwrap();
        let x = b.input(0, ExpressionResultType::new(DataType::Int64, false));
        let done = b
            .aggregate_update([groups, sum], vec![ids, x], AggregateKernel::SumI64)
            .unwrap();
        let a = b
            .with_dependencies(&[done], |b| b.group_lookup(groups, vec![]))
            .unwrap();
        let c = b
            .with_dependencies(&[done], |b| b.group_lookup(groups, vec![]))
            .unwrap();
        // A dependency used in a local branch must not execute again in that branch.
        let mask = b
            .constant(Arc::new(BooleanArray::from(vec![true])))
            .unwrap();
        let zero = b
            .constant(Arc::new(crate::arrow::array::UInt64Array::from(vec![0])))
            .unwrap();
        let branch = b.case(vec![(mask, a)], zero).unwrap();
        let p = b.finish(vec![branch, c]).unwrap().compile(mode).unwrap();
        let kinds = p.programs().iter().map(Program::kind).collect::<Vec<_>>();
        assert_eq!(
            kinds
                .iter()
                .filter(|&&kind| kind == "aggregate_update")
                .count(),
            1
        );
        let update = kinds
            .iter()
            .position(|&kind| kind == "aggregate_update")
            .unwrap();
        assert_eq!(
            kinds[..update]
                .iter()
                .filter(|&&kind| kind == "group_lookup")
                .count(),
            1
        );
        assert_eq!(
            kinds[update + 1..]
                .iter()
                .filter(|&&kind| kind == "group_lookup")
                .count(),
            2
        );
        let mut local = p.new_execution_context();
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        p.execute(&[input], 3, &mut local).unwrap();
        let (_, values) = super::super::aggregation::partial(&mut local, &[groups, sum]).unwrap();
        assert_eq!(values[0][0].as_ref(), &Int64Array::from(vec![6]));
    }
}

#[test]
fn scope_output_and_token_visibility_is_checked_before_codegen() {
    use super::super::context::{DependencyToken, Producer};
    let mut b = SuboperatorScopeBuilder::new();
    let predicate = b.input(
        0,
        crate::expr::ExpressionResultType::new(DataType::Boolean, false),
    );
    let output = b.filter_with_outputs(predicate, vec![predicate]).unwrap()[0];
    let token = b.dependency_of(output).unwrap();
    let next = b.with_dependencies(&[token], |b| b.not(output)).unwrap();
    let scope = b.finish(vec![next]).unwrap();
    let mut invalid = scope.clone();
    let ScopeSuboperator::Filter { body_inputs, .. } = &mut invalid.boundaries[0].operator;
    body_inputs[0] = output;
    assert!(matches!(
        invalid.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
    let mut invalid = scope.clone();
    invalid.dependencies.insert(Producer::Node(0), vec![token]);
    assert!(matches!(
        invalid.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
    let mut invalid = scope;
    invalid.dependencies.insert(
        Producer::Node(1),
        vec![DependencyToken {
            scope: token.scope,
            producer: Producer::Node(1),
        }],
    );
    assert!(matches!(
        invalid.compile(ExecutionMode::Compiled),
        Err(Error::InvalidPlan(_))
    ));
}
