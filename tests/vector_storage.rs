//! Lifetime and binding checks across windows and reused execution instances.
use arrow::{
    array::{Array, Int64Array},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnType},
    expr::scalar::{ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression},
    kernel::{ExecutionMode, KernelBuilder, KernelLibrary, Program},
};
use std::sync::Arc;

#[test]
fn reused_state_keeps_outputs_and_sliced_nullable_inputs_across_windows() {
    for mode in [
        ExecutionMode::Compile,
        ExecutionMode::ZeroCompile(KernelLibrary::Generated),
        ExecutionMode::ZeroCompile(KernelLibrary::Native),
    ] {
        let mut columns = ColumnContext::new();
        let input = columns.allocate(ColumnType::new(DataType::Int64, true));
        let mut expr = ReferenceExpression::new(input).into_ref();
        let mut first = None;
        for i in 1..=16 {
            let constant = columns.allocate(ColumnType::new(DataType::Int64, false));
            let output = columns.allocate(ColumnType::new(DataType::Int64, true));
            expr = FunctionExpression::binary(
                FunctionKind::Add,
                expr,
                ConstantExpression::int64(Some(i), constant).into_ref(),
                output,
            )
            .into_ref();
            if i == 1 {
                first = Some(output);
            }
        }
        let mut builder = KernelBuilder::new(&columns);
        builder.bind_inputs(&[input]).unwrap();
        let errors = expr.build_kernels(&mut builder).unwrap();
        builder.raise(errors).unwrap();
        let program = Program::new(
            builder
                .finish(Some(&[first.unwrap(), expr.output(), input]))
                .unwrap(),
            mode,
        )
        .unwrap();
        let mut local = program.new_local().unwrap();
        let (_shutdown, guard) = asyncband::shutdown::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let mut retained = vec![];
        // Grow and shrink buffers, cross window boundaries, and follow failure
        // with another batch using the same LocalState.
        for (iteration, rows) in [8195, 1, 0, 1024, 1025, 37, 16391, 1023]
            .into_iter()
            .enumerate()
        {
            let offset = 137;
            let values = (0..rows + offset)
                .map(|i| {
                    if i % 17 == 0 {
                        i64::MAX
                    } else {
                        i as i64 - 700
                    }
                })
                .collect::<Vec<_>>();
            let valid =
                NullBuffer::from((0..rows + offset).map(|i| i % 17 != 0).collect::<Vec<_>>());
            let input = Int64Array::new(values.into(), Some(valid)).slice(offset, rows);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(input)]).unwrap();
            let output = program.run(&batch, &mut local, &guard).unwrap().unwrap();
            for (c, delta) in [1, 136, 0].into_iter().enumerate() {
                let array = output
                    .column(c)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for r in 0..rows {
                    let i = r + offset;
                    assert_eq!(
                        array.is_null(r),
                        i % 17 == 0,
                        "{mode:?}, batch {iteration}, row {r}"
                    );
                    if i % 17 != 0 {
                        assert_eq!(array.value(r), i as i64 - 700 + delta);
                    }
                }
            }
            let saved = output
                .columns()
                .iter()
                .map(|a| {
                    a.as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            retained.push((output, saved));
            if rows > 0 {
                let mut bad = vec![0; rows];
                bad[rows - 1] = i64::MAX;
                let bad =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(bad))])
                        .unwrap();
                assert!(program.run(&bad, &mut local, &guard).is_err());
            }
        }
        for (output, saved) in retained {
            for (array, expected) in output.columns().iter().zip(saved) {
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    expected
                );
            }
        }
    }
}
