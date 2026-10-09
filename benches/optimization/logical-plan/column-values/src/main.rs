use arrow::{array::{ArrayRef, BooleanArray, Int64Array}, datatypes::DataType};
use std::{hint::black_box, sync::Arc, time::Instant};
macro_rules! engine {
    ($module:ident, $engine:ident, $finish:expr) => {
        mod $module {
            use super::*;
            use $engine::expr::{ExpressionResultType, scalar::*, scalar::executor::ScalarExpressionExecutor};
            pub fn prepare(input: ArrayRef, case: usize) -> Box<dyn FnMut() -> ArrayRef> {
                let rows = input.len();
                let executor = ScalarExpressionExecutor::new(&[input], rows);
                let column = || ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true)).into_ref();
                let int = |v| ConstantExpression::int64(Some(v)).into_ref();
                let binary = |op, left, right, dtype| FunctionExpression::binary(op, left, right, dtype, true).into_ref();
                let expr = match case {
                    0 => binary(FunctionKind::Add, column(), int(1), DataType::Int64),
                    1 => binary(FunctionKind::Add, column(), binary(FunctionKind::Add, int(1), int(2), DataType::Int64), DataType::Int64),
                    _ => ConjunctionExpression::new(Conjunction::And, vec![binary(FunctionKind::GreaterThan, column(), int(250), DataType::Boolean), binary(FunctionKind::LessThan, column(), int(750), DataType::Boolean)], true).into_ref(),
                };
                let evaluation = expr.to_evaluation().unwrap();
                Box::new(move || ($finish)(evaluation.evaluate(&executor).unwrap(), rows))
            }
        }
    }
}
engine!(before, roc_before, |value: ArrayRef, _rows| value);
engine!(after, roc_after, |value: roc_after::expr::scalar::ColumnValue, rows| value.into_array(rows).unwrap());
fn validate(left: ArrayRef, right: ArrayRef) {
    assert_eq!(left.data_type(), right.data_type());
    if left.data_type() == &DataType::Int64 {
        assert_eq!(left.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), right.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>());
    } else {
        assert_eq!(left.as_any().downcast_ref::<BooleanArray>().unwrap().iter().collect::<Vec<_>>(), right.as_any().downcast_ref::<BooleanArray>().unwrap().iter().collect::<Vec<_>>());
    }
}
fn timing(eval: &mut dyn FnMut() -> ArrayRef, iterations: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations { black_box(eval()); }
    start.elapsed().as_secs_f64() * 1e6 / iterations as f64
}
fn main() {
    println!("rows,nullable,case,sample,order,before_us,after_us");
    for rows in [8192, 65536] {
        for nullable in [false, true] {
            let input: ArrayRef = Arc::new(Int64Array::from_iter((0..rows).map(|i| if nullable && i % 4 == 0 { None } else { Some((i % 1000) as i64) })));
            for case in 0..3 {
                let mut before = before::prepare(input.clone(), case);
                let mut after = after::prepare(input.clone(), case);
                validate(before(), after());
                for _ in 0..100 { black_box(before()); black_box(after()); }
                let iterations = if rows == 8192 { 4000 } else { 500 };
                for sample in 0..9 {
                    let (b,a) = if sample % 2 == 0 { let b=timing(&mut before,iterations); let a=timing(&mut after,iterations); (b,a) } else { let a=timing(&mut after,iterations); let b=timing(&mut before,iterations); (b,a) };
                    println!("{rows},{nullable},{case},{sample},{},{b:.6},{a:.6}", if sample%2==0 {"before-first"} else {"after-first"});
                }
            }
        }
    }
}
