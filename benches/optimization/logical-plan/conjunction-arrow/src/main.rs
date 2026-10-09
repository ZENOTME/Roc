#![allow(dead_code)]
use arrow::{
    array::{ArrayRef, AsArray, BooleanArray},
    buffer::{BooleanBuffer, NullBuffer},
};
use std::{hint::black_box, sync::Arc, time::Instant};
mod error {
    #[derive(Debug)]
    pub enum Error {
        Execution(String),
    }
    pub type Result<T> = std::result::Result<T, Error>;
}
mod old;
mod reused;
fn inputs(len: usize, nullable: bool) -> Vec<ArrayRef> {
    (0..4)
        .map(|seed| {
            let offset = seed * 7;
            let size = len + offset;
            let v = BooleanBuffer::from((0..size).map(|i| (i + seed) % 3 != 0).collect::<Vec<_>>());
            let n = nullable.then(|| {
                NullBuffer::from((0..size).map(|i| (i + seed) % 4 != 0).collect::<Vec<_>>())
            });
            Arc::new(BooleanArray::new(v, n).slice(offset, len)) as ArrayRef
        })
        .collect()
}
fn original<const AND: bool>(inputs: &[ArrayRef], len: usize) -> BooleanArray {
    let mut c = old::ConjunctionBuffer::new::<AND>(len);
    for v in inputs {
        old::update::<AND>(&mut c, v).unwrap();
    }
    c.finish()
}
fn composed<const AND: bool>(inputs: &[ArrayRef], len: usize) -> BooleanArray {
    let mut c = reused::ConjunctionBuffer::new::<AND>(len);
    for v in inputs {
        reused::update::<AND>(&mut c, v).unwrap();
    }
    c.finish()
}
fn df_arrow<const AND: bool>(inputs: &[ArrayRef], len: usize) -> BooleanArray {
    let mut c = BooleanArray::new(
        if AND {
            BooleanBuffer::new_set(len)
        } else {
            BooleanBuffer::new_unset(len)
        },
        None,
    );
    for v in inputs {
        c = if AND {
            arrow::compute::and_kleene(&c, v.as_boolean()).unwrap()
        } else {
            arrow::compute::or_kleene(&c, v.as_boolean()).unwrap()
        };
    }
    c
}
fn case<const AND: bool>(len: usize, nullable: bool) {
    let input = inputs(len, nullable);
    let expected = df_arrow::<AND>(&input, len).iter().collect::<Vec<_>>();
    for f in [original::<AND>, composed::<AND>] {
        assert_eq!(f(&input, len).iter().collect::<Vec<_>>(), expected);
    }
    let iterations = if len == 8192 { 4000 } else { 500 };
    let kernels = [original::<AND>, composed::<AND>, df_arrow::<AND>];
    for sample in 0..9 {
        let order = if sample % 2 == 0 {
            [0, 1, 2]
        } else {
            [2, 1, 0]
        };
        for index in order {
            for _ in 0..20 {
                black_box(kernels[index](black_box(&input), len));
            }
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(kernels[index](black_box(&input), len));
            }
            let ns = start.elapsed().as_nanos() as f64 / iterations as f64;
            println!(
                "{len},{nullable},{},{},{sample},{iterations},{ns:.3}",
                if AND { "and" } else { "or" },
                ["fused_before", "arrow_composed", "df_arrow_kleene"][index]
            );
        }
    }
}
fn main() {
    println!("rows,nullable,op,implementation,sample,iterations,ns_per_batch");
    for len in [8192, 65536] {
        for nullable in [false, true] {
            case::<true>(len, nullable);
            case::<false>(len, nullable);
        }
    }
}
