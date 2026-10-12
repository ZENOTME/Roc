//! Reproducible CPU microbenchmark; compilation is measured separately.
//! cargo run --release --features jit --example vector_program_bench
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use roc::{
    error::Result,
    expr::{ExpressionResultType, scalar::*},
    suboperator::*,
};
use std::{hint::black_box, sync::Arc, time::Instant};
fn expression(barrier: bool) -> ScalarExprRef {
    let mut x =
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true)).into_ref();
    for i in 0..16 {
        x = FunctionExpression::binary(
            FunctionKind::Add,
            x,
            ConstantExpression::int64(Some(i + 1)).into_ref(),
            DataType::Int64,
            true,
        )
        .into_ref();
        if barrier && i == 7 {
            x = FunctionExpression::binary(
                FunctionKind::Divide,
                x,
                ConstantExpression::int64(Some(3)).into_ref(),
                DataType::Int64,
                true,
            )
            .into_ref();
        }
    }
    x
}
fn main() -> Result<()> {
    let rows = 1_048_576;
    let batch_rows = 8192;
    let samples = 21;
    println!(
        "rows={rows}, batch_rows={batch_rows}, samples={samples}, CPU workers=1; nullable Int64, 16 additions"
    );
    println!("case,mode,prepare_ms,median_ms,min_ms,max_ms,compiled_programs");
    for barrier in [false, true] {
        let input: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter(
            (0..batch_rows).map(|i| if i % 7 == 0 { None } else { Some(i as i64) }),
        ))];
        let modes = {
            #[cfg(feature = "jit")]
            {
                vec![
                    ExecutionMode::Native,
                    ExecutionMode::Compiled,
                    ExecutionMode::Staged,
                ]
            }
            #[cfg(not(feature = "jit"))]
            {
                vec![ExecutionMode::Native]
            }
        };
        let reference = ProgramContext::from_expressions(
            &[expression(barrier)],
            roc::suboperator::ExecutionMode::Native,
        )?;
        let expected =
            reference.execute(&input, batch_rows, &mut reference.new_execution_context())?;
        let mut prepared = Vec::new();
        for mode in modes {
            let start = Instant::now();
            let p = ProgramContext::from_expressions(&[expression(barrier)], mode)?;
            let prepare_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut local = p.new_execution_context();
            p.initialize(&mut local)?;
            assert_eq!(p.execute(&input, batch_rows, &mut local)?, expected);
            prepared.push((mode, p, local, prepare_ms, vec![]));
        }
        // Rotate modes every round to avoid always measuring one mode at a
        // different thermal/turbo state. Three whole-workload rounds warm up.
        for round in 0..samples + 3 {
            for shift in 0..prepared.len() {
                let index = (round + shift) % prepared.len();
                let (_, p, local, _, times) = &mut prepared[index];
                let start = Instant::now();
                for _ in 0..rows / batch_rows {
                    black_box(p.execute(black_box(&input), batch_rows, local)?);
                }
                if round >= 3 {
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                }
            }
        }
        for (mode, p, _, prepare, mut times) in prepared {
            times.sort_by(f64::total_cmp);
            println!(
                "{},{mode:?},{prepare:.3},{:.3},{:.3},{:.3},{}",
                if barrier {
                    "native_division_boundary"
                } else {
                    "arithmetic_chain"
                },
                times[times.len() / 2],
                times[0],
                times[times.len() - 1],
                p.compiled_program_count()
            );
        }
    }
    Ok(())
}
