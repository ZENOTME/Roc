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
    array::{
        Array, ArrayRef, ArrowNativeTypeOp, Int8Array, Int16Array, Int32Array, Int64Array,
        PrimitiveArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    compute::{binary, kernels::numeric, try_binary},
    datatypes::{
        DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
        UInt64Type,
    },
    error::ArrowError,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, sync::Arc, time::Duration};

type Kernel = unsafe fn(&dyn Array, &dyn Array) -> Result<ArrayRef, ArrowError>;

#[inline(never)]
fn dynamic_checked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    numeric::add(&left, &right)
}

#[inline(never)]
fn dynamic_wrapping(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    numeric::add_wrapping(&left, &right)
}

// Arrow exposes no numeric::add_unchecked. Use an equivalent integer DataType
// dispatcher for these two modes; const ALL chooses the loop at compile time.
// SAFETY: jointly valid sums must fit in i64 (or the selected integer type).
// With ALL=true, sums at NULL payload positions must fit as well.
#[inline(never)]
unsafe fn dynamic_unchecked<const ALL: bool>(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    macro_rules! execute {
        ($array:ty, $ty:ty) => {{
            let left = left.as_any().downcast_ref::<$array>().unwrap();
            let right = right.as_any().downcast_ref::<$array>().unwrap();
            let output: PrimitiveArray<$ty> = if ALL {
                binary(left, right, |l, r| unsafe { l.unchecked_add(r) })?
            } else {
                try_binary(left, right, |l, r| Ok(unsafe { l.unchecked_add(r) }))?
            };
            Ok(Arc::new(output))
        }};
    }
    match (left.data_type(), right.data_type()) {
        (DataType::Int8, DataType::Int8) => execute!(Int8Array, Int8Type),
        (DataType::Int16, DataType::Int16) => execute!(Int16Array, Int16Type),
        (DataType::Int32, DataType::Int32) => execute!(Int32Array, Int32Type),
        (DataType::Int64, DataType::Int64) => execute!(Int64Array, Int64Type),
        (DataType::UInt8, DataType::UInt8) => execute!(UInt8Array, UInt8Type),
        (DataType::UInt16, DataType::UInt16) => execute!(UInt16Array, UInt16Type),
        (DataType::UInt32, DataType::UInt32) => execute!(UInt32Array, UInt32Type),
        (DataType::UInt64, DataType::UInt64) => execute!(UInt64Array, UInt64Type),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "unsupported unchecked addition: {} + {}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn variants() -> [(&'static str, Kernel, Kernel); 4] {
    [
        ("checked", dynamic_checked, checked),
        ("unchecked", dynamic_unchecked::<false>, unchecked),
        ("unchecked_all", dynamic_unchecked::<true>, unchecked_all),
        ("wrapping", dynamic_wrapping, wrapping),
    ]
}

fn bind_int64(left: &DataType, right: &DataType, kernel: Kernel) -> Kernel {
    assert_eq!((left, right), (&DataType::Int64, &DataType::Int64));
    kernel
}

#[inline(never)]
fn checked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let left = left.as_any().downcast_ref::<Int64Array>().unwrap();
    let right = right.as_any().downcast_ref::<Int64Array>().unwrap();
    let output: PrimitiveArray<Int64Type> = try_binary(left, right, |l, r| l.add_checked(r))?;
    Ok(Arc::new(output))
}

// SAFETY: sums must fit in i64 at every jointly valid position.
#[inline(never)]
unsafe fn unchecked(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let left = left.as_any().downcast_ref::<Int64Array>().unwrap();
    let right = right.as_any().downcast_ref::<Int64Array>().unwrap();
    let output: PrimitiveArray<Int64Type> =
        try_binary(left, right, |l, r| Ok(unsafe { l.unchecked_add(r) }))?;
    Ok(Arc::new(output))
}

// SAFETY: sums must fit in i64 at ALL positions, including NULL payloads.
// binary evaluates all underlying values, unlike try_binary's valid-only loop.
#[inline(never)]
unsafe fn unchecked_all(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let left = left.as_any().downcast_ref::<Int64Array>().unwrap();
    let right = right.as_any().downcast_ref::<Int64Array>().unwrap();
    let output: PrimitiveArray<Int64Type> =
        binary(left, right, |l, r| unsafe { l.unchecked_add(r) })?;
    Ok(Arc::new(output))
}

#[inline(never)]
fn wrapping(left: &dyn Array, right: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let left = left.as_any().downcast_ref::<Int64Array>().unwrap();
    let right = right.as_any().downcast_ref::<Int64Array>().unwrap();
    let output: PrimitiveArray<Int64Type> = binary(left, right, |l, r| l.wrapping_add(r))?;
    Ok(Arc::new(output))
}

fn input(rows: usize, nulls: bool) -> (Int64Array, Int64Array) {
    let values = |offset: usize| {
        (0..rows).map(move |i| {
            // ~10% NULLs in each input, at different positions (~20% in output).
            (!nulls || i % 10 != offset).then_some(((i * (offset + 1)) % 1000) as i64)
        })
    };
    (
        Int64Array::from_iter(values(0)),
        Int64Array::from_iter(values(3)),
    )
}

fn validate(left: &Int64Array, right: &Int64Array) {
    // Prove the stronger precondition, including NULL payloads, before unsafe
    // calls. Validation is outside timing; timed inputs are immutable and reused.
    assert_eq!(left.len(), right.len());
    assert!(
        left.values()
            .iter()
            .zip(right.values())
            .all(|(&l, &r)| l.checked_add(r).is_some())
    );
    let expected = checked(left, right).unwrap();
    for (_, dynamic, typed) in variants() {
        for kernel in [
            dynamic,
            bind_int64(left.data_type(), right.data_type(), typed),
        ] {
            // SAFETY: every underlying pair was just validated.
            let actual = unsafe { kernel(left, right) }.unwrap();
            assert_eq!(actual.to_data(), expected.to_data());
        }
    }
}

fn bench_add(c: &mut Criterion) {
    // Also check empty and all-NULL inputs without timing them.
    for rows in [0, 1] {
        let (left, right) = input(rows, true);
        validate(&left, &right);
    }
    for nulls in [false, true] {
        let mut group = c.benchmark_group(format!("add_dispatch/Int64/nulls={nulls}"));
        for rows in [64, 2048, 16384] {
            let (left, right) = input(rows, nulls);
            validate(&left, &right);
            group.throughput(Throughput::Elements(rows as u64));
            for (name, dynamic, typed) in variants() {
                let bound = bind_int64(left.data_type(), right.data_type(), typed);
                for (mode, kernel) in [("dynamic", dynamic), ("static", bound)] {
                    // Both routes use one opaque function-pointer call and safe
                    // downcasts. Binding occurs outside timing; dispatch is timed.
                    let kernel = black_box(kernel);
                    group.bench_with_input(
                        BenchmarkId::new(format!("{name}/{mode}"), rows),
                        &rows,
                        |b, _| {
                            b.iter(|| {
                                // SAFETY: reuse the exact validated immutable buffers.
                                black_box(
                                    unsafe {
                                        kernel(
                                            black_box(&left as &dyn Array),
                                            black_box(&right as &dyn Array),
                                        )
                                    }
                                    .unwrap(),
                                )
                            });
                        },
                    );
                }
            }
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(100)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = bench_add
}
criterion_main!(benches);
