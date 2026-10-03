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

use arrow::{
    array::{Array, ArrayRef, ArrowNativeTypeOp, Float64Array, Int64Array, PrimitiveArray},
    compute::{binary, kernels::numeric, try_binary},
    datatypes::{DataType, Float64Type, Int64Type},
    error::ArrowError,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, sync::Arc, time::Duration};

type Kernel = fn(&dyn Array, &dyn Array) -> Result<ArrayRef, ArrowError>;

// Both routes use one opaque function-pointer call per evaluation. This prevents
// the benchmark compiler from specializing calls using the known input arrays.
#[inline(never)]
fn dynamic_add(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    numeric::add(&left, &right)
}

fn bind_add(left: &DataType, right: &DataType) -> Result<Kernel, ArrowError> {
    match (left, right) {
        (DataType::Int64, DataType::Int64) => Ok(add_int64),
        (DataType::Float64, DataType::Float64) => Ok(add_float64),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "unsupported example signature: {left} + {right}"
        ))),
    }
}

#[inline(never)]
fn add_int64(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    // Safe downcasts still check Any's type ID. Binding removes the DataType,
    // operation, and scalar/array dispatch, not these checks.
    let left = left.as_any().downcast_ref::<Int64Array>().unwrap();
    let right = right.as_any().downcast_ref::<Int64Array>().unwrap();
    let output: PrimitiveArray<Int64Type> = try_binary(left, right, |l, r| l.add_checked(r))?;
    Ok(Arc::new(output))
}

#[inline(never)]
fn add_float64(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let left = left.as_any().downcast_ref::<Float64Array>().unwrap();
    let right = right.as_any().downcast_ref::<Float64Array>().unwrap();
    let output: PrimitiveArray<Float64Type> = binary(left, right, |l, r| l.add_wrapping(r))?;
    Ok(Arc::new(output))
}

fn input(data_type: &DataType, rows: usize, nulls: bool) -> (ArrayRef, ArrayRef) {
    let values = |offset: usize| {
        (0..rows).map(move |i| {
            // Each input has ~10% nulls at different positions (~20% in output).
            (!nulls || i % 10 != offset).then_some(((i * (offset + 1)) % 1000) as i64)
        })
    };
    match data_type {
        DataType::Int64 => (
            Arc::new(Int64Array::from_iter(values(0))),
            Arc::new(Int64Array::from_iter(values(3))),
        ),
        DataType::Float64 => (
            Arc::new(Float64Array::from_iter(
                values(0).map(|v| v.map(|v| v as f64)),
            )),
            Arc::new(Float64Array::from_iter(
                values(3).map(|v| v.map(|v| v as f64)),
            )),
        ),
        _ => unreachable!(),
    }
}

fn validate(left: &dyn Array, right: &dyn Array) -> Result<(), ArrowError> {
    let bound = bind_add(left.data_type(), right.data_type())?;
    assert_eq!(
        dynamic_add(left, right)?.to_data(),
        bound(left, right)?.to_data()
    );
    Ok(())
}

fn validate_edges() -> Result<(), ArrowError> {
    let left = Int64Array::from(vec![i64::MAX]);
    let right = Int64Array::from(vec![1]);
    for kernel in [dynamic_add as Kernel, add_int64 as Kernel] {
        assert!(matches!(
            kernel(&left, &right),
            Err(ArrowError::ArithmeticOverflow(_))
        ));
        assert!(kernel(&left, &Int64Array::from(vec![1, 2])).is_err());
    }
    // Overflow in a null slot must not cause an error.
    let left = Int64Array::new(vec![i64::MAX, 2].into(), Some(vec![false, true].into()));
    validate(&left, &Int64Array::from(vec![1, 3]))?;
    for data_type in [DataType::Int64, DataType::Float64] {
        let (left, right) = input(&data_type, 0, false);
        validate(left.as_ref(), right.as_ref())?;
    }
    Ok(())
}

// All timed calls allocate and drop their output. Binding, input creation, and
// correctness checks are excluded. Both routes keep safe Any downcasts.
fn bench_add(c: &mut Criterion) {
    validate_edges().unwrap();
    for data_type in [DataType::Int64, DataType::Float64] {
        for nulls in [false, true] {
            let mut group = c.benchmark_group(format!("add/{data_type}/nulls={nulls}"));
            for rows in [1, 16, 64, 256, 2048, 16384] {
                let (left, right) = input(&data_type, rows, nulls);
                validate(left.as_ref(), right.as_ref()).unwrap();
                group.throughput(Throughput::Elements(rows as u64));
                for (name, kernel) in [
                    ("dynamic", dynamic_add as Kernel),
                    (
                        "bound",
                        bind_add(left.data_type(), right.data_type()).unwrap(),
                    ),
                ] {
                    let kernel = black_box(kernel);
                    group.bench_with_input(BenchmarkId::new(name, rows), &rows, |b, _| {
                        b.iter(|| {
                            black_box(
                                kernel(black_box(left.as_ref()), black_box(right.as_ref()))
                                    .unwrap(),
                            )
                        });
                    });
                }
            }
            group.finish();
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(50)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1));
    targets = bench_add
}
criterion_main!(benches);
