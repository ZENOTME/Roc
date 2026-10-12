use arrow::{
    array::{ArrayRef, BooleanArray, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use roc::{
    expr::ExpressionResultType,
    suboperator::{
        ExecutionMode, FunctionKind, ScopeSuboperator, Suboperator, SuboperatorScopeBuilder,
    },
};
use std::sync::Arc;

fn modes() -> Vec<ExecutionMode> {
    vec![ExecutionMode::Compiled]
}
fn int() -> ExpressionResultType {
    ExpressionResultType::new(DataType::Int64, true)
}
fn boolean() -> ExpressionResultType {
    ExpressionResultType::new(DataType::Boolean, true)
}
fn literal(b: &mut SuboperatorScopeBuilder, value: i64) -> roc::suboperator::VariableId {
    b.constant(Arc::new(Int64Array::from(vec![value]))).unwrap()
}

#[test]
fn nested_scopes_keep_ancestor_values_and_skip_fallible_descendants() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let x = b.input(0, int());
        let zero = literal(&mut b, 0);
        let positive = b.binary(FunctionKind::GreaterThan, x, zero).unwrap();
        b.filter(positive).unwrap();
        // No ordinary nodes between these two scope breakers.
        b.filter(positive).unwrap();
        let limit = literal(&mut b, 10);
        let small = b.binary(FunctionKind::LessThan, x, limit).unwrap();
        b.filter(small).unwrap();
        let quotient = b.binary(FunctionKind::Divide, limit, x).unwrap();
        let answer = b.binary(FunctionKind::Add, quotient, x).unwrap();
        let scope = b.finish(vec![answer, x]).unwrap();
        assert_eq!(scope.boundaries().len(), 3);
        assert_eq!(scope.boundaries()[0].end, scope.boundaries()[1].end);
        assert!(matches!(
            scope.boundaries()[0].operator,
            ScopeSuboperator::Filter { .. }
        ));
        assert!(
            !scope
                .suboperators()
                .iter()
                .any(|op| matches!(op, Suboperator::Materialize { .. }))
        );
        let program = scope.compile(mode).unwrap();
        let mut local = program.new_execution_context();
        for values in [
            vec![Some(0), Some(2), None, Some(i64::MAX), Some(-1), Some(5)],
            vec![],
            vec![Some(0)],
        ] {
            let input: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let output = program.execute(&[input], values.len(), &mut local).unwrap();
            let selected = values
                .into_iter()
                .flatten()
                .filter(|x| *x > 0 && *x < 10)
                .collect::<Vec<_>>();
            assert_eq!(
                output[0].as_ref(),
                &Int64Array::from(selected.iter().map(|x| 10 / x + x).collect::<Vec<_>>())
            );
            assert_eq!(output[1].as_ref(), &Int64Array::from(selected));
        }
    }
}

#[test]
fn filter_fuses_with_checked_arithmetic_and_recovers_after_failure() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let x = b.input(0, int());
        let mask = b.input(1, boolean());
        b.filter(mask).unwrap();
        let one = literal(&mut b, 1);
        let next = b.binary(FunctionKind::Add, x, one).unwrap();
        let program = b.finish(vec![next, x]).unwrap().compile(mode).unwrap();
        if mode == ExecutionMode::Compiled {
            assert_eq!(program.compiled_program_count(), 1, "{}", program.explain());
            assert!(program.explain().contains("filter scopes: 1"));
            assert!(!program.explain().contains("filter_scope inputs"));
        }
        let mut local = program.new_execution_context();
        let good: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![i64::MAX, 5, i64::MAX, -2])),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(true),
            ])),
        ];
        let bad: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![i64::MAX])),
            Arc::new(BooleanArray::from(vec![true])),
        ];
        assert!(program.execute(&bad, 1, &mut local).is_err());
        let output = program.execute(&good, 4, &mut local).unwrap();
        assert_eq!(output[0].as_ref(), &Int64Array::from(vec![6, -1]));
        assert_eq!(output[1].as_ref(), &Int64Array::from(vec![5, -2]));
    }
}

#[test]
fn conditional_regions_capture_ancestor_variables_after_projection() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let x = b.input(0, int());
        let one = literal(&mut b, 1);
        let projected = b.binary(FunctionKind::Add, x, one).unwrap();
        b.set_columns(
            vec![projected],
            Arc::new(Schema::new(vec![Field::new(
                "renamed",
                DataType::Int64,
                true,
            )])),
        )
        .unwrap();
        let limit = literal(&mut b, 10);
        let predicate = b.binary(FunctionKind::LessThan, projected, limit).unwrap();
        b.filter(predicate).unwrap();
        assert_eq!(b.column(0, int()).unwrap(), projected);
        let zero = literal(&mut b, 0);
        let divisor_ok = b.binary(FunctionKind::NotEqual, projected, zero).unwrap();
        let divided = b.binary(FunctionKind::Divide, limit, projected).unwrap();
        let case = b.case(vec![(divisor_ok, divided)], zero).unwrap();
        let null = b.constant(Arc::new(Int64Array::from(vec![None]))).unwrap();
        let coalesce = b.coalesce(vec![null, projected]).unwrap();
        let program = b
            .finish(vec![case, coalesce])
            .unwrap()
            .compile(mode)
            .unwrap();
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(-1),
            None,
            Some(20),
            Some(4),
        ]));
        let output = program
            .execute(&[input], 5, &mut program.new_execution_context())
            .unwrap();
        assert_eq!(output[0].as_ref(), &Int64Array::from(vec![10, 0, 2]));
        assert_eq!(output[1].as_ref(), &Int64Array::from(vec![1, 0, 5]));
    }
}

#[test]
fn filter_only_preserves_sliced_complex_columns_and_zero_column_row_counts() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let mask = b.input(0, boolean());
        b.filter(mask).unwrap();
        let program = b.finish(vec![]).unwrap().compile(mode).unwrap();
        assert!(!program.explain().contains("materialize"));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("mask", DataType::Boolean, true),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    None,
                    Some(true),
                    Some(false),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("outside"),
                    Some("a"),
                    Some("excluded"),
                    None,
                    Some("z"),
                ])),
            ],
        )
        .unwrap()
        .slice(1, 3);
        let result = program
            .execute_record_batch(&batch, &mut program.new_execution_context())
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(
            result.column(1).as_ref(),
            &StringArray::from(vec![Some("a"), None])
        );
        assert_eq!(result.schema(), batch.schema());
        for value in [Some(true), Some(false), None] {
            let mut b = SuboperatorScopeBuilder::new();
            let p = b
                .constant(Arc::new(BooleanArray::from(vec![value])))
                .unwrap();
            b.filter(p).unwrap();
            b.filter(p).unwrap();
            let program = b.finish(vec![]).unwrap().compile(mode).unwrap();
            let batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(7)),
            )
            .unwrap();
            let result = program
                .execute_record_batch(&batch, &mut program.new_execution_context())
                .unwrap();
            assert_eq!(result.num_rows(), if value == Some(true) { 7 } else { 0 });
            assert_eq!(result.num_columns(), 0);
        }
    }
}

#[test]
fn filter_keeps_effects_in_their_owner_scope_and_publishes_only_at_batch_sink() {
    use roc::suboperator::{ProgramControl, State, StateBuilder};
    fn collect(state: &mut State, batch: &RecordBatch) -> roc::error::Result<ProgramControl> {
        state.get_mut::<Vec<RecordBatch>>()?.push(batch.clone());
        Ok(ProgramControl::Continue)
    }
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let x = b.input(0, int());
        let one = literal(&mut b, 1);
        let next = b.binary(FunctionKind::Add, x, one).unwrap();
        b.set_columns(
            vec![next],
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        )
        .unwrap();
        let predicate = b
            .constant(Arc::new(BooleanArray::from(vec![false])))
            .unwrap();
        b.filter(predicate).unwrap();
        let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
        // Earlier projection is observable even though a later filter drops all rows.
        assert!(
            p.execute(
                &[Arc::new(Int64Array::from(vec![i64::MAX]))],
                1,
                &mut p.new_execution_context()
            )
            .is_err()
        );

        let mut b = SuboperatorScopeBuilder::new();
        let mask = b.input(0, boolean());
        b.filter(mask).unwrap();
        let state = b.state(StateBuilder::new(|| Ok(Vec::<RecordBatch>::new())));
        b.batch_update(state, collect).unwrap();
        let p = b.finish(vec![]).unwrap().compile(mode).unwrap();
        assert!(!p.explain().contains("materialize"));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "mask",
                DataType::Boolean,
                true,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true, false, true]))],
        )
        .unwrap();
        let mut local = p.new_execution_context();
        p.execute_record_batch(&batch, &mut local).unwrap();
        p.execute_record_batch(&batch.slice(1, 1), &mut local)
            .unwrap();
        let batches = local
            .take_state(state)
            .unwrap()
            .into_inner::<Vec<RecordBatch>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }
}

#[test]
fn scope_outputs_are_fresh_body_definitions_in_all_execution_modes() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let x = b.input(0, int());
        let mask = b.input(1, boolean());
        let body = b.filter_with_outputs(mask, vec![x, mask]).unwrap();
        assert_ne!(body[0], x);
        let entered = b.dependency_of(body[0]).unwrap();
        let one = literal(&mut b, 1);
        let sum = b
            .with_dependencies(&[entered], |b| b.binary(FunctionKind::Add, body[0], one))
            .unwrap();
        // A scope output can also feed the next scope at the same boundary index.
        let second = b.filter_with_outputs(body[1], vec![sum, body[0]]).unwrap();
        let scope = b.finish(vec![second[0], second[1], x]).unwrap();
        assert_eq!(scope.boundaries()[0].operator.outputs(), &body);
        let p = scope.compile(mode).unwrap();
        let mut local = p.new_execution_context();
        for masks in [
            vec![Some(false), Some(true), None, Some(true)],
            vec![Some(false); 4],
        ] {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![i64::MAX, 4, i64::MAX, 8])),
                Arc::new(BooleanArray::from(masks.clone())),
            ];
            let result = p.execute(&columns, 4, &mut local).unwrap();
            let expected = if masks[1] == Some(true) {
                vec![5, 9]
            } else {
                vec![]
            };
            assert_eq!(result[0].as_ref(), &Int64Array::from(expected));
            assert_eq!(result[1].as_ref(), result[2].as_ref());
        }
    }
}

#[test]
fn complex_scope_outputs_and_local_conditionals_capture_the_body() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let text = b.input(0, ExpressionResultType::new(DataType::Utf8, true));
        let mask = b.input(1, boolean());
        let body = b.filter_with_outputs(mask, vec![text, mask]).unwrap();
        let null = b
            .constant(Arc::new(StringArray::from(vec![None::<&str>])))
            .unwrap();
        let value = b.case(vec![(body[1], body[0])], null).unwrap();
        let output = b.coalesce(vec![value, null]).unwrap();
        let p = b.finish(vec![output]).unwrap().compile(mode).unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("ignored"),
                Some("kept"),
                None,
                Some("no"),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(true),
                None,
            ])),
        ];
        let result = p
            .execute(&columns, 4, &mut p.new_execution_context())
            .unwrap();
        assert_eq!(
            result[0].as_ref(),
            &StringArray::from(vec![Some("kept"), None])
        );
    }
}

#[test]
fn ordering_tokens_preserve_work_without_runtime_values_and_reject_foreign_tokens() {
    for mode in modes() {
        let mut b = SuboperatorScopeBuilder::new();
        let one = literal(&mut b, 1);
        let zero = literal(&mut b, 0);
        let unused = b.binary(FunctionKind::Divide, one, zero).unwrap();
        let ready = b.dependency_of(unused).unwrap();
        let false_value = b
            .constant(Arc::new(BooleanArray::from(vec![false])))
            .unwrap();
        b.with_dependencies(&[ready], |b| b.filter(false_value))
            .unwrap();
        let scope = b.finish(vec![]).unwrap();
        // Only the four data variables occupy slots. Token edges allocate none.
        assert_eq!(scope.variables().len(), 4);
        let p = scope.compile(mode).unwrap();
        assert!(p.execute(&[], 1, &mut p.new_execution_context()).is_err());

        let mut foreign = SuboperatorScopeBuilder::new();
        let value = literal(&mut foreign, 7);
        let token = foreign.dependency_of(value).unwrap();
        let mut b = SuboperatorScopeBuilder::new();
        let mut invoked = false;
        assert!(
            b.with_dependencies(&[token], |_| {
                invoked = true;
                Ok(())
            })
            .is_err()
        );
        assert!(!invoked);
    }
}

#[test]
fn batch_publication_cannot_be_used_as_a_row_scope() {
    use roc::suboperator::{ProgramControl, StateBuilder};
    let mut b = SuboperatorScopeBuilder::new();
    let state = b.state(StateBuilder::new(|| Ok(())));
    b.batch_update(state, |_, _| Ok(ProgramControl::Continue))
        .unwrap();
    let value = literal(&mut b, 1);
    assert!(matches!(
        b.finish(vec![value])
            .unwrap()
            .compile(ExecutionMode::Compiled),
        Err(roc::error::Error::InvalidPlan(_))
    ));
}
