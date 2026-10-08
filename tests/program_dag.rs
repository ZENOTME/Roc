use arrow::{
    array::{ArrayRef, BooleanArray, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    exec::Batch,
    expr::{ExpressionResultType, scalar::*},
    operator::{Projection, ProjectionExpression},
    program::{ProcessProgram, ProgramBuilder, Value},
};
use std::sync::Arc;
fn col(i: usize) -> ScalarExprRef {
    ReferenceExpression::new(i, ExpressionResultType::new(DataType::Int64, false)).into_ref()
}
fn int(v: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(v)).into_ref()
}
fn binary(kind: FunctionKind, a: ScalarExprRef, b: ScalarExprRef) -> ScalarExprRef {
    let ty = if matches!(kind, FunctionKind::LessThan) {
        DataType::Boolean
    } else {
        DataType::Int64
    };
    FunctionExpression::binary(kind, a, b, ty, false).into_ref()
}
fn expression() -> ScalarExprRef {
    binary(
        FunctionKind::Add,
        binary(FunctionKind::Multiply, col(0), int(3)),
        col(1),
    )
}
fn input(values: &[i64]) -> Batch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(vec![7; values.len()])),
        ],
    )
    .unwrap()
    .into()
}
fn plan(fused: bool) -> ProcessProgram {
    let mut b = ProgramBuilder::default();
    let input = b.value();
    let filtered = b
        .emit_filter(input, &binary(FunctionKind::LessThan, col(0), int(4)), None)
        .unwrap();
    let out = b
        .emit_project(
            filtered,
            &Projection::new(vec![ProjectionExpression::new(expression(), "v")]),
        )
        .unwrap();
    if fused {
        assert_eq!(b.fuse().unwrap().regions, 1);
    }
    b.build_batch(input, Some(out)).unwrap()
}
#[test]
fn expressions_emit_separate_value_instructions_and_share_references() {
    let mut b = ProgramBuilder::default();
    let domain = b.value();
    let expr = binary(
        FunctionKind::Add,
        binary(FunctionKind::Multiply, col(0), int(3)),
        col(0),
    );
    let out = expr.emit(&mut b, domain).unwrap();
    let mut p = b.build_value(domain, out).unwrap();
    assert_eq!(p.program_count(), 4); // reference, constant, multiply, add
    for values in [vec![1, 2, 3], vec![9], vec![0, 4]] {
        let got = p
            .run_value(&Value::Batch(input(&values)))
            .unwrap()
            .into_array(values.len())
            .unwrap();
        assert_eq!(
            got.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            values.iter().map(|v| v * 4).collect::<Vec<_>>()
        );
    }
}
#[cfg(feature = "jit")]
#[test]
fn fused_region_matches_direct_and_keeps_selected_input_fallback() {
    let mut direct = plan(false);
    let mut fused = plan(true);
    assert!(direct.program_count() > fused.program_count());
    for n in [1, 63, 64, 65, 2048] {
        let batch = input(&(0..n).map(|i| i as i64 % 9 - 4).collect::<Vec<_>>());
        assert_eq!(
            direct.run_batch(&batch).unwrap(),
            fused.run_batch(&batch).unwrap()
        );
        let selected = batch
            .filter(&BooleanArray::from(
                (0..n).map(|i| i % 3 != 0).collect::<Vec<_>>(),
            ))
            .unwrap();
        let a = direct.run_batch(&selected).unwrap();
        let b = fused.run_batch(&selected).unwrap();
        assert_eq!(a.num_rows(), b.num_rows());
        if a.num_rows() != 0 {
            assert_eq!(a, b);
        }
    }
}
#[cfg(feature = "jit")]
#[test]
fn arithmetic_fuses_without_filter_or_project_and_recovers_after_overflow() {
    let mut b = ProgramBuilder::default();
    let domain = b.value();
    let out = expression().emit(&mut b, domain).unwrap();
    assert_eq!(b.fuse().unwrap().regions, 1);
    let mut p = b.build_value(domain, out).unwrap();
    assert_eq!(p.program_count(), 1);
    assert!(p.run_value(&Value::Batch(input(&[i64::MAX]))).is_err());
    let output = p
        .run_value(&Value::Batch(input(&[2])))
        .unwrap()
        .into_array(1)
        .unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        13
    );
}
#[test]
fn separate_outputs_of_one_dag_stay_available_to_multiple_consumers() {
    let mut b = ProgramBuilder::default();
    let domain = b.value();
    let out = b
        .emit_project(
            domain,
            &Projection::new(vec![
                ProjectionExpression::new(col(0), "original"),
                ProjectionExpression::new(expression(), "computed"),
                ProjectionExpression::new(col(0), "again"),
            ]),
        )
        .unwrap();
    let mut p = b.build_batch(domain, Some(out)).unwrap();
    let batch = input(&[1, 2, 3]);
    let result = p.run_batch(&batch).unwrap();
    assert!(Arc::ptr_eq(result.column(0), batch.physical().column(0)));
    assert!(Arc::ptr_eq(result.column(0), result.column(2)));
}

#[test]
fn returned_value_can_precede_the_last_instruction(){
    let mut b=ProgramBuilder::default();let domain=b.value();
    let original=col(0).emit(&mut b,domain).unwrap();
    expression().emit(&mut b,domain).unwrap();
    let mut p=b.build_value(domain,original).unwrap();
    let value=p.run_value(&Value::Batch(input(&[5]))).unwrap().into_array(1).unwrap();
    assert_eq!(value.as_any().downcast_ref::<Int64Array>().unwrap().value(0),5);
}
#[cfg(feature="jit")]
#[test]
fn fused_selection_respects_offsets_and_skips_inactive_overflow(){
    let mut b=ProgramBuilder::default();let domain=b.value();let result=expression().emit(&mut b,domain).unwrap();
    assert_eq!(b.fuse().unwrap().regions,1);let mut p=b.build_value(domain,result).unwrap();
    let mut values=vec![i64::MAX;139];for i in (1..139).step_by(3){values[i]=i as i64;}
    let batch=input(&values).filter(&BooleanArray::from((0..139).map(|i|i%3==1).collect::<Vec<_>>())).unwrap().slice(2,41);
    let output=p.run_value(&Value::Batch(batch.clone())).unwrap().into_array(batch.num_rows()).unwrap();
    let expected=batch.materialize().unwrap().column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().iter().map(|v|v*3+7).collect::<Vec<_>>();
    assert_eq!(output.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(),expected);
    assert!(p.run_value(&Value::input(&[],0)).unwrap().into_array(0).unwrap().is_empty());
}
#[cfg(feature="jit")]
#[test]
fn retained_fanout_survives_fusion_and_empty_batch_schema_is_preserved(){
    let mut b=ProgramBuilder::default();let domain=b.value();let original=col(0).emit(&mut b,domain).unwrap();
    let output=expression().emit(&mut b,domain).unwrap();b.retain(original);
    assert_eq!(b.fuse().unwrap().regions,1);let mut p=b.build_value(domain,output).unwrap();
    p.context.set(domain,Value::Batch(input(&[2])));p.call().unwrap();
    let result=p.context.column(original,domain).unwrap().into_array(1).unwrap();
    assert_eq!(result.as_any().downcast_ref::<Int64Array>().unwrap().value(0),2);
    assert_eq!(plan(false).run_batch(&input(&[9])).unwrap(),plan(true).run_batch(&input(&[9])).unwrap());
}
#[cfg(feature="jit")]
#[test]
fn fusion_does_not_drop_disconnected_errors_or_compile_reference_only_projects(){
    let mut b=ProgramBuilder::default();let domain=b.value();
    binary(FunctionKind::Multiply,col(0),int(i64::MAX)).emit(&mut b,domain).unwrap();
    let output=b.emit_project(domain,&Projection::new(vec![ProjectionExpression::new(expression(),"v")])).unwrap();
    b.fuse().unwrap();let mut p=b.build_batch(domain,Some(output)).unwrap();
    assert!(p.run_batch(&input(&[2])).is_err());
    let mut b=ProgramBuilder::default();let domain=b.value();b.emit_project(domain,&Projection::new(vec![ProjectionExpression::new(col(0),"x")])).unwrap();
    assert_eq!(b.fuse().unwrap().regions,0);
}
