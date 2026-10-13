//! Reproducible CPU microbenchmark; compilation is measured separately.
//! cargo run --release --features jit --example vector_program_bench
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnId, ColumnType},
    error::Result,
    expr::scalar::*,
    kernel::{KernelBuilder, Program},
};
use std::{hint::black_box, sync::Arc, time::Instant};

/// Sixteen additions over x; `barrier` inserts a fallible division.
fn expression(columns: &mut ColumnContext, x: ColumnId, barrier: bool) -> ScalarExprRef {
    let int = |columns: &mut ColumnContext, v| {
        let output = columns.allocate(ColumnType::new(DataType::Int64, false));
        ConstantExpression::int64(Some(v), output).into_ref()
    };
    let mut x = ReferenceExpression::new(x).into_ref();
    for i in 0..16 {
        let constant = int(columns, i + 1);
        let output = columns.allocate(ColumnType::new(DataType::Int64, true));
        x = FunctionExpression::binary(FunctionKind::Add, x, constant, output).into_ref();
        if barrier && i == 7 {
            let three = int(columns, 3);
            let output = columns.allocate(ColumnType::new(DataType::Int64, true));
            x = FunctionExpression::binary(FunctionKind::Divide, x, three, output).into_ref();
        }
    }
    x
}

fn program(barrier: bool) -> Result<Program> {
    let mut columns = ColumnContext::new();
    let x = columns.allocate(ColumnType::new(DataType::Int64, true));
    let expression = expression(&mut columns, x, barrier);
    let mut builder = KernelBuilder::new(&columns);
    builder.bind_inputs(&[x])?;
    let errors = expression.build_kernels(&mut builder)?;
    builder.raise(errors)?;
    Program::compile(builder.finish(Some(&[expression.output()]))?)
}

fn main() -> Result<()> {
    let rows = 1_048_576;
    let batch_rows = 8192;
    let samples = 21;
    println!(
        "rows={rows}, batch_rows={batch_rows}, samples={samples}, CPU workers=1; nullable Int64, 16 additions"
    );
    println!("case,prepare_ms,median_ms,min_ms,max_ms");
    let (_shutdown, guard) = asyncband::shutdown::new();
    for barrier in [false, true] {
        let column: ArrayRef = Arc::new(Int64Array::from_iter(
            (0..batch_rows).map(|i| if i % 7 == 0 { None } else { Some(i as i64) }),
        ));
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![column],
        )?;
        let start = Instant::now();
        let p = program(barrier)?;
        let prepare = start.elapsed().as_secs_f64() * 1000.0;
        let mut local = p.new_local()?;
        let expected = p.run(&input, &mut local, &guard)?;
        assert_eq!(program(barrier)?.run(&input, &mut local, &guard)?, expected);
        let mut times = vec![];
        // Three whole-workload warmup rounds precede measurements.
        for round in 0..samples + 3 {
            let start = Instant::now();
            for _ in 0..rows / batch_rows {
                black_box(p.run(black_box(&input), &mut local, &guard)?);
            }
            if round >= 3 {
                times.push(start.elapsed().as_secs_f64() * 1000.0);
            }
        }
        times.sort_by(f64::total_cmp);
        println!(
            "{},{prepare:.3},{:.3},{:.3},{:.3}",
            if barrier {
                "integer_division"
            } else {
                "arithmetic_chain"
            },
            times[times.len() / 2],
            times[0],
            times[times.len() - 1],
        );
    }
    Ok(())
}
