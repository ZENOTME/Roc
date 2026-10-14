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

//! Window execution against independent expectations: chunk lengths around
//! window and bitmap word boundaries, the first failing row across kernels and
//! windows, Filter masks and constant divisors.
use arrow::{
    array::*,
    compute::kernels::numeric,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use roc::{
    column::{ColumnContext, ColumnType},
    error::Result,
    expr::scalar::*,
    kernel::*,
};
use std::sync::Arc;

mod support;
use support::*;

const LENGTHS: [usize; 13] = [0, 1, 15, 16, 17, 63, 64, 65, 1023, 1024, 1025, 2049, 3000];

fn column(i: usize, data_type: DataType) -> ScalarExprRef {
    ReferenceExpression::new(input_column(i, data_type, true)).into_ref()
}

fn binary(f: FunctionKind, a: ScalarExprRef, b: ScalarExprRef, data_type: DataType) -> ScalarExprRef {
    FunctionExpression::binary(f, a, b, output_column(data_type, true)).into_ref()
}

fn int(v: i64) -> ScalarExprRef {
    ConstantExpression::int64(Some(v), output_column(DataType::Int64, false)).into_ref()
}

fn values(n: usize, offset: usize) -> ArrayRef {
    let all = (0..n + offset)
        .map(|i| (i % 7 != 3).then(|| (i as i64 * 7919) % 100_003 - 50_000))
        .collect::<Int64Array>();
    Arc::new(all.slice(offset, n))
}

#[test]
fn arithmetic_matches_arrow_around_window_boundaries() {
    // ((x + 7) * 3 - x) / 5
    let x = column(0, DataType::Int64);
    let e = binary(FunctionKind::Add, x.clone(), int(7), DataType::Int64);
    let e = binary(FunctionKind::Multiply, e, int(3), DataType::Int64);
    let e = binary(FunctionKind::Subtract, e, x, DataType::Int64);
    let e = binary(FunctionKind::Divide, e, int(5), DataType::Int64);
    let p = TestProgram::try_new(e).unwrap();
    for n in LENGTHS {
        for offset in [0, 3] {
            let input = values(n, offset);
            let out = p.evaluate(&TestInput::new(std::slice::from_ref(&input), n)).unwrap();
            let expected = numeric::add(&input, &Int64Array::new_scalar(7)).unwrap();
            let expected = numeric::mul(&expected, &Int64Array::new_scalar(3)).unwrap();
            let expected = numeric::sub(&expected, &input).unwrap();
            let expected = numeric::div(&expected, &Int64Array::new_scalar(5)).unwrap();
            assert_eq!(out.as_ref(), expected.as_ref(), "{n} rows from {offset}");
        }
    }
}

#[test]
fn the_first_failing_row_wins_across_kernels_and_windows() {
    // a / b + 1: b = 0 fails the division at row `zero`; a = MAX overflows the
    // addition at row `max`, a later kernel.
    let a = column(0, DataType::Int64);
    let b = column(1, DataType::Int64);
    let e = binary(FunctionKind::Divide, a, b, DataType::Int64);
    let e = binary(FunctionKind::Add, e, int(1), DataType::Int64);
    let p = TestProgram::try_new(e).unwrap();
    let rows = 3000;
    for (zero, max) in [(5, 2900), (2900, 5), (1023, 1024), (1024, 1023), (100, 200), (200, 100), (2999, 2998)] {
        let a: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|i| if i == max { i64::MAX } else { i as i64 }),
        ));
        let b: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|i| if i == zero { 0 } else { 1 }),
        ));
        let error = p.evaluate(&TestInput::new(&[a, b], rows)).unwrap_err().to_string();
        if zero < max {
            assert!(error.contains("Divide by zero"), "{zero} {max}: {error}");
        } else {
            assert!(error.contains("overflow"), "{zero} {max}: {error}");
        }
    }
}

#[test]
fn rare_errors_are_found_in_every_part_of_a_window() {
    let x = column(0, DataType::Int64);
    let p = TestProgram::try_new(binary(FunctionKind::Add, x, int(1), DataType::Int64)).unwrap();
    for n in [17, 64, 65, 1023, 1100, 2500] {
        for bad in [0, n / 2, n - 1] {
            let input: ArrayRef = Arc::new(Int64Array::from_iter(
                (0..n).map(|i| if i == bad { Some(i64::MAX) } else { Some(i as i64) }),
            ));
            let error = p.evaluate(&TestInput::new(std::slice::from_ref(&input), n)).unwrap_err();
            assert!(error.to_string().contains("overflow"), "{n} {bad}: {error}");
            // The same row is NULL: nothing fails.
            let nulled: ArrayRef = Arc::new(Int64Array::from_iter(
                (0..n).map(|i| if i == bad { None } else { Some(i as i64) }),
            ));
            let out = p.evaluate(&TestInput::new(std::slice::from_ref(&nulled), n)).unwrap();
            assert_eq!(out.null_count(), 1);
        }
    }
}

#[test]
fn constant_divisors_divide_exactly_for_every_integer_type() {
    macro_rules! check {
        ($array:ty, $t:ty, $values:expr, $divisors:expr) => {{
            let values: ArrayRef = Arc::new(<$array>::from_iter_values($values));
            let n = values.len();
            for d in $divisors {
                let divisor = <$array>::from(vec![d as $t]);
                for (kind, f) in [
                    (FunctionKind::Divide, numeric::div as fn(&dyn Datum, &dyn Datum) -> _),
                    (FunctionKind::Remainder, numeric::rem),
                ] {
                    let e = binary(
                        kind,
                        column(0, values.data_type().clone()),
                        constant(Arc::new(divisor.clone())).unwrap().into_ref(),
                        values.data_type().clone(),
                    );
                    let out = TestProgram::try_new(e)
                        .unwrap()
                        .evaluate(&TestInput::new(std::slice::from_ref(&values), n))
                        .unwrap();
                    let expected = f(&values, &Scalar::new(divisor.clone())).unwrap();
                    assert_eq!(out.as_ref(), expected.as_ref(), "{kind:?} by {d}");
                }
            }
        }};
    }
    let wide = |n: i64| (0..n).map(move |i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15u64 as i64) >> (i % 61));
    check!(Int8Array, i8, i8::MIN..=i8::MAX, [2i64, 3, 7, 10, 127, -3, -128]);
    check!(UInt8Array, u8, 0..=u8::MAX, [2i64, 3, 7, 10, 255]);
    check!(Int16Array, i16, (i16::MIN..=i16::MAX).step_by(7), [2i64, 3, 1000, i16::MAX as i64, -7]);
    check!(UInt16Array, u16, (0..=u16::MAX).step_by(5), [2i64, 3, 1000, u16::MAX as i64]);
    check!(Int32Array, i32, wide(5000).map(|v| v as i32), [2i64, 3, 7, 1_000_000_007, i32::MAX as i64, -9]);
    check!(UInt32Array, u32, wide(5000).map(|v| v as u32), [2i64, 3, 7, 1_000_000_007, u32::MAX as i64]);
    check!(Int64Array, i64, wide(5000).chain([i64::MIN, i64::MAX, 0, -1]), [2i64, 3, 7, 1_000_000_007, i64::MAX, -9]);
    check!(UInt64Array, u64, wide(5000).map(|v| v as u64).chain([u64::MAX]), [2i64, 3, 7, 1_000_000_007, -1]);
}

/// `SELECT x, x + 1, s WHERE x > 50`, in a kernel graph with a Filter domain.
fn filtered(mode: ExecutionMode) -> Result<Program> {
    let mut columns = ColumnContext::new();
    let x = columns.allocate(ColumnType::new(DataType::Int64, true));
    let s = columns.allocate(ColumnType::new(DataType::Utf8, true));
    let y = columns.allocate(ColumnType::new(DataType::Int64, true));
    let mut b = KernelBuilder::new(&columns);
    b.bind_inputs(&[x, s])?;
    let input = b.column_binding(x)?;
    let (value, validity) = (input.value().unwrap(), input.validity());
    let fifty = b.constant(ConstantValue::Int64(50))?;
    let above = b.call(ScalarFunction::GreaterThan, vec![value, fifty])?[0];
    let predicate = b.predicate(&ColumnVariableBinding::Boolean {
        value: above,
        validity,
    })?;
    let domain = b.filter(predicate)?;
    b.set_current_domain(domain)?;
    let one = b.constant(ConstantValue::Int64(1))?;
    let sum = b.call(ScalarFunction::Add, vec![value, one])?;
    let pending = b.pending(sum[1], validity, KernelError::ArithmeticOverflow, vec![])?;
    b.raise(vec![pending])?;
    b.bind_column(y, ColumnVariableBinding::Int64 { value: sum[0], validity }, vec![])?;
    Program::new(b.finish(Some(&[x, y, s]))?, mode)
}

#[test]
fn filter_masks_compact_values_validity_and_strings() {
    let programs = [ExecutionMode::Compile, ExecutionMode::ZeroCompile].map(|m| filtered(m).unwrap());
    let (_shutdown, guard) = asyncband::shutdown::new();
    for n in LENGTHS {
        let x = (0..n)
            .map(|i| (i % 11 != 4).then(|| (i as i64 * 37) % 101))
            .collect::<Int64Array>();
        let s = (0..n)
            .map(|i| (i % 5 != 0).then(|| format!("row {i}")))
            .collect::<StringArray>();
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(x.clone()), Arc::new(s.clone())]).unwrap();
        let kept = (0..n).filter(|&i| x.is_valid(i) && x.value(i) > 50).collect::<Vec<_>>();
        let expected_x = kept.iter().map(|&i| Some(x.value(i))).collect::<Int64Array>();
        let expected_y = kept.iter().map(|&i| Some(x.value(i) + 1)).collect::<Int64Array>();
        let expected_s = kept.iter().map(|&i| s.is_valid(i).then(|| s.value(i))).collect::<StringArray>();
        for program in &programs {
            let mut local = program.new_local().unwrap();
            let out = program.run(&batch, &mut local, &guard).unwrap().unwrap();
            assert_eq!(out.num_rows(), kept.len(), "{n} rows, {:?}", program.mode());
            assert_eq!(out.column(0).as_ref(), &expected_x as &dyn Array);
            assert_eq!(out.column(1).as_ref(), &expected_y as &dyn Array);
            assert_eq!(out.column(2).as_ref(), &expected_s as &dyn Array);
        }
    }
}

#[test]
fn only_compiled_programs_have_generated_code() {
    let x = column(0, DataType::Int64);
    let e = binary(FunctionKind::Add, x, int(1), DataType::Int64);
    let p = TestProgram::try_new(e).unwrap();
    assert!(p.program().generated_ir().contains("define i64 @roc_fused_0"));
    let mut columns = ColumnContext::new();
    let x = columns.allocate(ColumnType::new(DataType::Int64, false));
    let mut b = KernelBuilder::new(&columns);
    b.bind_inputs(&[x]).unwrap();
    let program = Program::new(b.finish(Some(&[x])).unwrap(), ExecutionMode::ZeroCompile).unwrap();
    assert_eq!(program.generated_ir(), "");
}

#[test]
fn every_chunk_of_a_schema_with_nullable_fields_is_checked_for_nulls() {
    let mut columns = ColumnContext::new();
    let x = columns.allocate(ColumnType::new(DataType::Int64, false));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let (_shutdown, guard) = asyncband::shutdown::new();
    for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
        let mut b = KernelBuilder::new(&columns);
        b.bind_inputs(&[x]).unwrap();
        let program = Program::new(b.finish(Some(&[x])).unwrap(), mode).unwrap();
        let mut local = program.new_local().unwrap();
        let clean = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let nulls = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![Some(1), None]))]).unwrap();
        assert_eq!(program.run(&clean, &mut local, &guard).unwrap().unwrap().num_rows(), 2);
        let error = program.run(&nulls, &mut local, &guard).unwrap_err();
        assert!(error.to_string().contains("non-nullable"), "{mode:?}: {error}");
    }
}

/// `CAST(CAST(CAST(s AS INT) * 2 AS BIGINT) AS VARCHAR)`: the fused Int64
/// stores must not overwrite Int32 rows it has yet to read, whatever slots
/// the kernels run one at a time share.
#[test]
fn fused_outputs_never_overwrite_rows_they_read() {
    let cast = |e: ScalarExprRef, t: DataType| CastExpression::new(e, CastMode::Strict, output_column(t, false)).into_ref();
    let s = ReferenceExpression::new(input_column(0, DataType::Utf8, false)).into_ref();
    let p = cast(s, DataType::Int32);
    let q = FunctionExpression::binary(FunctionKind::Add, p.clone(), p, output_column(DataType::Int32, false)).into_ref();
    let out = cast(cast(q, DataType::Int64), DataType::Utf8);
    let n = 200;
    let input: ArrayRef = Arc::new(StringArray::from_iter_values((0..n).map(|i| i.to_string())));
    let r = TestProgram::try_new(out).unwrap().evaluate(&TestInput::new(&[input], n)).unwrap();
    let expected = StringArray::from_iter_values((0..n).map(|i| (2 * i).to_string()));
    assert_eq!(r.as_ref(), &expected as &dyn Array);
}

/// `(parse(s) + 1 + y, format(parse(s) + 1 + y))`, also over a window that
/// may raise: fused stores, and the steps rerun after a possible raise, read
/// their inputs intact.
fn parse_add_format(mode: ExecutionMode, raise: bool, sum_column: bool) -> Result<Program> {
    let mut columns = ColumnContext::new();
    let s = columns.allocate(ColumnType::new(DataType::Utf8, false));
    let y = columns.allocate(ColumnType::new(DataType::Int64, true));
    let r = columns.allocate(ColumnType::new(DataType::Int64, true));
    let t = columns.allocate(ColumnType::new(DataType::Utf8, true));
    let mut b = KernelBuilder::new(&columns);
    b.bind_inputs(&[s, y])?;
    let ColumnVariableBinding::Bytes { ptr, len, .. } = b.column_binding(s)? else {
        unreachable!("strings bind bytes")
    };
    let yb = b.column_binding(y)?;
    let (yv, valid) = (yb.value().unwrap(), yb.validity());
    let a = b.call(ScalarFunction::ParseText { to: VariableType::Int64 }, vec![ptr, len])?[0];
    let one = b.constant(ConstantValue::Int64(1))?;
    let a1 = b.call(ScalarFunction::Add, vec![a, one])?[0];
    let sum = b.call(ScalarFunction::Add, vec![a1, yv])?;
    if raise {
        let pending = b.pending(sum[1], valid, KernelError::ArithmeticOverflow, vec![])?;
        b.raise(vec![pending])?;
    }
    let f = b.call(ScalarFunction::FormatText, vec![sum[0]])?;
    b.bind_column(r, ColumnVariableBinding::Int64 { value: sum[0], validity: valid }, vec![])?;
    b.bind_column(t, ColumnVariableBinding::Bytes { ptr: f[0], len: f[1], validity: valid }, vec![])?;
    let outputs: &[_] = if sum_column { &[t, r] } else { &[t] };
    Program::new(b.finish(Some(outputs))?, mode)
}

#[test]
fn rerun_steps_read_inputs_the_fused_function_left_intact() {
    let (_shutdown, guard) = asyncband::shutdown::new();
    let n = 100;
    // Row 50 is NULL over i64::MAX: fused code may see an overflow there.
    let s = StringArray::from_iter_values((0..n).map(|i| i.to_string()));
    let values = (0..n as i64).map(|i| if i == 50 { i64::MAX } else { i * 1000 }).collect::<Vec<_>>();
    let nulls = arrow::buffer::NullBuffer::from((0..n).map(|i| i != 50).collect::<Vec<_>>());
    let y = Int64Array::new(values.into(), Some(nulls));
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false), Field::new("y", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(s), Arc::new(y)]).unwrap();
    let valid = |i: usize| i != 50;
    let expected_r = (0..n).map(|i| valid(i).then(|| 1001 * i as i64 + 1)).collect::<Int64Array>();
    let expected_t = (0..n).map(|i| valid(i).then(|| (1001 * i + 1).to_string())).collect::<StringArray>();
    for (raise, sum_column) in [(false, false), (false, true), (true, false), (true, true)] {
        for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
            let p = parse_add_format(mode, raise, sum_column).unwrap();
            let mut local = p.new_local().unwrap();
            let out = p.run(&batch, &mut local, &guard).unwrap().unwrap();
            let case = format!("{mode:?} raise={raise} sum={sum_column}");
            assert_eq!(out.column(0).as_ref(), &expected_t as &dyn Array, "{case}");
            if sum_column {
                assert_eq!(out.column(1).as_ref(), &expected_r as &dyn Array, "{case}");
            }
        }
    }
}

#[test]
fn casts_to_the_same_type_pass_values_through() {
    let mut columns = ColumnContext::new();
    let y = columns.allocate(ColumnType::new(DataType::UInt64, false));
    let r = columns.allocate(ColumnType::new(DataType::UInt64, false));
    let (_shutdown, guard) = asyncband::shutdown::new();
    let input = UInt64Array::from_iter_values((0..100).map(|i| i * 0x0101_0101_0101));
    let schema = Schema::new(vec![Field::new("y", DataType::UInt64, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(input.clone())]).unwrap();
    for twice in [false, true] {
        for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
            let mut b = KernelBuilder::new(&columns);
            b.bind_inputs(&[y]).unwrap();
            let mut v = b.column_binding(y).unwrap().value().unwrap();
            for _ in 0..1 + twice as usize {
                v = b.call(ScalarFunction::Cast { to: VariableType::UInt64 }, vec![v]).unwrap()[0];
            }
            b.bind_column(r, ColumnVariableBinding::UInt64 { value: v, validity: None }, vec![]).unwrap();
            let p = Program::new(b.finish(Some(&[r])).unwrap(), mode).unwrap();
            let mut local = p.new_local().unwrap();
            let out = p.run(&batch, &mut local, &guard).unwrap().unwrap();
            assert_eq!(out.column(0).as_ref(), &input as &dyn Array, "{twice} {mode:?}");
        }
    }
}

/// `WHERE b` over a Boolean column whose last window has fewer than a
/// window bitmap of bytes left in its buffer.
#[test]
fn boolean_inputs_ending_in_a_short_window_filter_exactly() {
    let mut columns = ColumnContext::new();
    let b_col = columns.allocate(ColumnType::new(DataType::Boolean, false));
    let x = columns.allocate(ColumnType::new(DataType::Int64, false));
    let (_shutdown, guard) = asyncband::shutdown::new();
    for n in [1088usize, 1025, 2048 + 64] {
        // A buffer of exactly the bitmap's bytes, without Arrow's padding.
        let bits = (0..n).map(|i| i % 3 != 0).collect::<Vec<_>>();
        let words = (0..n.div_ceil(64))
            .map(|w| (0..64).filter(|i| w * 64 + i < n && bits[w * 64 + i]).fold(0u64, |a, i| a | 1 << i))
            .collect::<Vec<_>>();
        let buffer = arrow::buffer::Buffer::from_vec(words);
        let flags = BooleanArray::new(arrow::buffer::BooleanBuffer::new(buffer, 0, n), None);
        let xs = Int64Array::from_iter_values(0..n as i64);
        let schema = Schema::new(vec![Field::new("b", DataType::Boolean, false), Field::new("x", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(flags), Arc::new(xs)]).unwrap();
        for mode in [ExecutionMode::Compile, ExecutionMode::ZeroCompile] {
            let mut kb = KernelBuilder::new(&columns);
            kb.bind_inputs(&[b_col, x]).unwrap();
            let predicate = kb.column_binding(b_col).unwrap().value().unwrap();
            let domain = kb.filter(predicate).unwrap();
            kb.set_current_domain(domain).unwrap();
            let p = Program::new(kb.finish(Some(&[x])).unwrap(), mode).unwrap();
            let mut local = p.new_local().unwrap();
            let out = p.run(&batch, &mut local, &guard).unwrap().unwrap();
            let expected = (0..n as i64).filter(|i| i % 3 != 0).collect::<Int64Array>();
            assert_eq!(out.column(0).as_ref(), &expected as &dyn Array, "{n} {mode:?}");
        }
    }
}
