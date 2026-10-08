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
    array::*,
    buffer::{NullBuffer, OffsetBuffer},
    compute::{cast, take},
    datatypes::*,
};
use roc::expr::scalar::{ColumnValue, ConstantExpression, ScalarValue};
use std::sync::Arc;

fn decoded(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Dictionary(_, values) => decoded(&cast(array, values).unwrap()),
        DataType::RunEndEncoded(_, values) => decoded(&cast(array, values.data_type()).unwrap()),
        _ => array.clone(),
    }
}

// Arrow take supplies the expected values independently of scalar broadcasting.
fn check(input: ArrayRef) {
    let expected_type = input.data_type().clone();
    for index in 0..input.len() {
        let slice = input.slice(index, 1);
        let scalar = ScalarValue::try_from_array(&slice, 0).unwrap();
        assert_eq!(scalar.data_type(), expected_type);
        assert_eq!(scalar.is_null(), slice.logical_null_count() != 0);
        let source = decoded(&slice);
        for len in [0, 1, 7] {
            let output = scalar.to_array_of_size(len).unwrap();
            assert_eq!(output.len(), len);
            assert_eq!(output.data_type(), &expected_type);
            // Arrow take infers zero rows from a zero-width binary buffer.
            // Supply cardinality explicitly for this valid Arrow type.
            let expected: ArrayRef = if expected_type == DataType::FixedSizeBinary(0) {
                Arc::new(
                    FixedSizeBinaryArray::try_new_with_len(
                        0,
                        arrow::buffer::Buffer::from(Vec::<u8>::new()),
                        source.is_null(0).then(|| NullBuffer::new_null(len)),
                        len,
                    )
                    .unwrap(),
                )
            } else {
                take(&source, &UInt32Array::from(vec![0; len]), None).unwrap()
            };
            assert_eq!(
                decoded(&output).to_data(),
                expected.to_data(),
                "type {expected_type}, row {index}, len {len}"
            );
        }
    }
    assert!(ScalarValue::try_from_array(&input, input.len()).is_err());
}

#[test]
fn native_and_temporal_values_roundtrip_with_typed_nulls() {
    check(Arc::new(Int8Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(Int16Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(Int32Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(Int64Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(UInt8Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(UInt16Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(UInt32Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(UInt64Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(Float16Array::from(vec![
        Some(<Float16Type as ArrowPrimitiveType>::Native::from_f32(7.5)),
        None,
        Some(<Float16Type as ArrowPrimitiveType>::Native::from_f32(42.0)),
    ])));
    check(Arc::new(Float32Array::from(vec![
        Some(7.5),
        None,
        Some(42.0),
    ])));
    check(Arc::new(Float64Array::from(vec![
        Some(7.5),
        None,
        Some(42.0),
    ])));
    check(Arc::new(Date32Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(Date64Array::from(vec![Some(7), None, Some(42)])));
    check(Arc::new(Time32SecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(Time32MillisecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(Time64MicrosecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(Time64NanosecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(DurationSecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(DurationMillisecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(DurationMicrosecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(DurationNanosecondArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(IntervalYearMonthArray::from(vec![
        Some(7),
        None,
        Some(42),
    ])));
    check(Arc::new(IntervalDayTimeArray::from(vec![
        Some(IntervalDayTime::new(2, 7)),
        None,
        Some(IntervalDayTime::new(-3, 42)),
    ])));
    check(Arc::new(IntervalMonthDayNanoArray::from(vec![
        Some(IntervalMonthDayNano::new(1, 2, 7)),
        None,
        Some(IntervalMonthDayNano::new(-2, 3, 42)),
    ])));
    check(Arc::new(
        TimestampSecondArray::from(vec![Some(-7), None, Some(42)]).with_timezone("Asia/Shanghai"),
    ));
    check(Arc::new(
        TimestampMillisecondArray::from(vec![Some(-7), None, Some(42)])
            .with_timezone("Asia/Shanghai"),
    ));
    check(Arc::new(
        TimestampMicrosecondArray::from(vec![Some(-7), None, Some(42)])
            .with_timezone("Asia/Shanghai"),
    ));
    check(Arc::new(
        TimestampNanosecondArray::from(vec![Some(-7), None, Some(42)])
            .with_timezone("Asia/Shanghai"),
    ));
    check(Arc::new(BooleanArray::from(vec![
        Some(false),
        None,
        Some(true),
    ])));
    check(Arc::new(NullArray::new(3)));
    let input: ArrayRef = Arc::new(Date32Array::from(vec![42]));
    assert!(matches!(
        ScalarValue::try_from_array(&input, 0).unwrap(),
        ScalarValue::Date32(Some(42))
    ));
}

#[test]
fn decimal_precision_and_scale_roundtrip() {
    for scale in [-2, 3] {
        check(Arc::new(
            Decimal32Array::from(vec![Some(-123), None, Some(456)])
                .with_precision_and_scale(8, scale)
                .unwrap(),
        ));
    }
    for scale in [-2, 3] {
        check(Arc::new(
            Decimal64Array::from(vec![Some(-123), None, Some(456)])
                .with_precision_and_scale(8, scale)
                .unwrap(),
        ));
    }
    for scale in [-2, 3] {
        check(Arc::new(
            Decimal128Array::from(vec![Some(-123), None, Some(456)])
                .with_precision_and_scale(8, scale)
                .unwrap(),
        ));
    }
    for scale in [-2, 3] {
        check(Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(-123)),
                None,
                Some(i256::from_i128(456)),
            ])
            .with_precision_and_scale(8, scale)
            .unwrap(),
        ));
    }
    assert!(ScalarValue::Decimal32(Some(1), 0, 0).to_array().is_err());
}

#[test]
fn strings_and_binary_values_roundtrip() {
    check(Arc::new(StringArray::from(vec![
        Some("long string exceeding an inline view"),
        None,
        Some(""),
    ])));
    check(Arc::new(LargeStringArray::from(vec![
        Some("long string exceeding an inline view"),
        None,
        Some(""),
    ])));
    check(Arc::new(StringViewArray::from(vec![
        Some("long string exceeding an inline view"),
        None,
        Some(""),
    ])));
    check(Arc::new(BinaryArray::from(vec![
        Some(&b"long binary exceeding an inline view"[..]),
        None,
        Some(&b""[..]),
    ])));
    check(Arc::new(LargeBinaryArray::from(vec![
        Some(&b"long binary exceeding an inline view"[..]),
        None,
        Some(&b""[..]),
    ])));
    check(Arc::new(BinaryViewArray::from(vec![
        Some(&b"long binary exceeding an inline view"[..]),
        None,
        Some(&b""[..]),
    ])));
    check(Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(&b"ab"[..]), None, Some(&b"cd"[..])].into_iter(),
            2,
        )
        .unwrap(),
    ));
    check(Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size([Some(&b""[..]), None].into_iter(), 0)
            .unwrap(),
    ));
    assert!(
        ScalarValue::FixedSizeBinary(2, Some(vec![1]))
            .to_array()
            .is_err()
    );
}

#[test]
fn nested_arrays_preserve_fields_slices_and_parent_nulls() {
    let field = Arc::new(Field::new("element", DataType::Int64, true).with_metadata(
        std::collections::HashMap::from([("meaning".into(), "test".into())]),
    ));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(20), Some(30)]));
    let nulls = Some(NullBuffer::from(vec![true, false, true]));
    check(Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(vec![0, 2, 2, 4].into()),
        values.clone(),
        nulls.clone(),
    )));
    check(Arc::new(LargeListArray::new(
        field.clone(),
        OffsetBuffer::new(vec![0, 2, 2, 4].into()),
        values.clone(),
        nulls.clone(),
    )));
    check(Arc::new(ListViewArray::new(
        field.clone(),
        vec![2, 0, 0].into(),
        vec![2, 0, 2].into(),
        values.clone(),
        nulls.clone(),
    )));
    check(Arc::new(LargeListViewArray::new(
        field.clone(),
        vec![2, 0, 0].into(),
        vec![2, 0, 2].into(),
        values.clone(),
        nulls.clone(),
    )));
    check(Arc::new(FixedSizeListArray::new(
        field.clone(),
        1,
        values.slice(0, 3),
        nulls.clone(),
    )));
    let structure = StructArray::new(vec![field].into(), vec![values.slice(0, 3)], nulls.clone());
    check(Arc::new(structure));
    let entries = StructArray::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
        None,
    );
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    check(Arc::new(MapArray::new(
        entries_field,
        OffsetBuffer::new(vec![0, 1, 1, 2].into()),
        entries,
        nulls,
        true,
    )));
    // A typed nested enum variant must really contain a single row.
    let bad = ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
        [Some(vec![Some(1)]), Some(vec![Some(2)])],
    )));
    assert!(bad.to_array_of_size(0).is_err());
    assert!(bad.to_array_of_size(7).is_err());
}

#[test]
fn dictionary_keys_and_logical_nulls_roundtrip() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
    macro_rules! dictionary {
        ($ty:ty) => {
            check(Arc::new(
                DictionaryArray::<$ty>::try_new(
                    PrimitiveArray::<$ty>::from(vec![Some(2), None, Some(1), Some(0)]),
                    values.clone(),
                )
                .unwrap(),
            ));
        };
    }
    dictionary!(Int8Type);
    dictionary!(Int16Type);
    dictionary!(Int32Type);
    dictionary!(Int64Type);
    dictionary!(UInt8Type);
    dictionary!(UInt16Type);
    dictionary!(UInt32Type);
    dictionary!(UInt64Type);
    assert!(
        ScalarValue::Dictionary(
            Box::new(DataType::Float32),
            Box::new(ScalarValue::Int64(Some(1)))
        )
        .to_array()
        .is_err()
    );
}

#[test]
fn run_end_values_preserve_metadata_and_sliced_logical_indices() {
    macro_rules! run {
        ($ty:ty) => {{
            let ends = PrimitiveArray::<$ty>::from(vec![2, 4, 7]);
            let values = Int64Array::from(vec![Some(10), None, Some(30)]);
            let run = RunArray::<$ty>::try_new(&ends, &values).unwrap();
            let data_type = DataType::RunEndEncoded(
                Arc::new(Field::new("ends", <$ty>::DATA_TYPE, false)),
                Arc::new(Field::new("values", DataType::Int64, true).with_metadata(
                    std::collections::HashMap::from([("unit".into(), "test".into())]),
                )),
            );
            let data = run
                .to_data()
                .into_builder()
                .data_type(data_type)
                .build()
                .unwrap();
            check(make_array(data).slice(1, 5));
        }};
    }
    run!(Int16Type);
    run!(Int32Type);
    run!(Int64Type);
    let value = ScalarValue::RunEndEncoded(
        Arc::new(Field::new("ends", DataType::Int16, false)),
        Arc::new(Field::new("values", DataType::Int64, true)),
        Box::new(ScalarValue::Int64(Some(1))),
    );
    assert!(value.to_array_of_size(i16::MAX as usize + 1).is_err());
    let bad = ScalarValue::RunEndEncoded(
        Arc::new(Field::new("ends", DataType::Int32, false)),
        Arc::new(Field::new("values", DataType::Utf8, true)),
        Box::new(ScalarValue::Int64(Some(1))),
    );
    assert!(bad.to_array().is_err());
}

#[test]
fn sparse_and_dense_unions_preserve_active_child_and_nulls() {
    let fields = UnionFields::try_new(
        [3, 7],
        [
            Field::new("number", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let dense = UnionArray::try_new(
        fields.clone(),
        vec![7, 3, 7, 3].into(),
        Some(vec![0, 0, 1, 1].into()),
        vec![
            Arc::new(Int64Array::from(vec![Some(10), None])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .unwrap();
    check(Arc::new(dense.slice(1, 3)));
    let sparse = UnionArray::try_new(
        fields.clone(),
        vec![7, 3, 7, 3].into(),
        None,
        vec![
            Arc::new(Int64Array::from(vec![None, Some(10), None, None])),
            Arc::new(StringArray::from(vec![Some("a"), None, None, None])),
        ],
    )
    .unwrap();
    check(Arc::new(sparse.slice(1, 3)));
    let bad = ScalarValue::Union(
        Some((7, Box::new(ScalarValue::Int64(Some(1))))),
        fields,
        UnionMode::Sparse,
    );
    assert!(bad.to_array().is_err());
}

#[test]
fn typed_constants_stay_scalar_and_broadcast_at_array_consumers() {
    let value = ScalarValue::TimestampNanosecond(Some(42), Some(Arc::from("UTC")));
    let mut evaluation = ConstantExpression::new(value).program().unwrap();
    let result = evaluation.run_value(&Value::input(&[], 7)).unwrap();
    assert!(
        matches!(&result, ColumnValue::Scalar(ScalarValue::TimestampNanosecond(Some(42), timezone)) if timezone.as_deref() == Some("UTC"))
    );
    let output = result.into_array(7).unwrap();
    assert_eq!(
        output.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
    );
    assert_eq!(
        output
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .as_ref(),
        &[42; 7]
    );
}

#[path = "support/program.rs"]
mod program_support;
use program_support::*;
