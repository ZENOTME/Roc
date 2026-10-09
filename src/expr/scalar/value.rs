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

//! Scalar expression values; broadcasting happens only at array consumers.
use crate::error::{Error, Result};
use arrow::{array::*, compute::take, datatypes::*};
use std::sync::Arc;

/// Values use the same explicit representations as DataFusion scalars.
/// Nested variants retain a typed Arrow array containing exactly one logical row.
#[derive(Clone, Debug)]
pub enum ScalarValue {
    /// A typed NULL, retained for the existing constant constructor API.
    Null(DataType),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float16(Option<<Float16Type as ArrowPrimitiveType>::Native>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Date32(Option<i32>),
    Date64(Option<i64>),
    Time32Second(Option<i32>),
    Time32Millisecond(Option<i32>),
    Time64Microsecond(Option<i64>),
    Time64Nanosecond(Option<i64>),
    DurationSecond(Option<i64>),
    DurationMillisecond(Option<i64>),
    DurationMicrosecond(Option<i64>),
    DurationNanosecond(Option<i64>),
    IntervalYearMonth(Option<i32>),
    IntervalDayTime(Option<IntervalDayTime>),
    IntervalMonthDayNano(Option<IntervalMonthDayNano>),
    Boolean(Option<bool>),
    Decimal32(Option<i32>, u8, i8),
    Decimal64(Option<i64>, u8, i8),
    Decimal128(Option<i128>, u8, i8),
    Decimal256(Option<i256>, u8, i8),
    Utf8(Option<String>),
    LargeUtf8(Option<String>),
    Utf8View(Option<String>),
    Binary(Option<Vec<u8>>),
    LargeBinary(Option<Vec<u8>>),
    BinaryView(Option<Vec<u8>>),
    FixedSizeBinary(i32, Option<Vec<u8>>),
    FixedSizeList(Arc<FixedSizeListArray>),
    List(Arc<ListArray>),
    LargeList(Arc<LargeListArray>),
    ListView(Arc<ListViewArray>),
    LargeListView(Arc<LargeListViewArray>),
    Struct(Arc<StructArray>),
    Map(Arc<MapArray>),
    TimestampSecond(Option<i64>, Option<Arc<str>>),
    TimestampMillisecond(Option<i64>, Option<Arc<str>>),
    TimestampMicrosecond(Option<i64>, Option<Arc<str>>),
    TimestampNanosecond(Option<i64>, Option<Arc<str>>),
    Union(Option<(i8, Box<ScalarValue>)>, UnionFields, UnionMode),
    Dictionary(Box<DataType>, Box<ScalarValue>),
    RunEndEncoded(FieldRef, FieldRef, Box<ScalarValue>),
}

impl ScalarValue {
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self> {
        if index >= array.len() {
            return Err(Error::invalid_input("scalar index out of bounds".into()));
        }
        macro_rules! primitive {
            ($ty:ty, $variant:ident $(, $metadata:expr)*) => {{
                let value = array.as_primitive::<$ty>();
                Self::$variant(value.is_valid(index).then(|| value.value(index)) $(, $metadata)*)
            }};
        }
        macro_rules! owned {
            ($array:ty, $variant:ident) => {{
                let value = array.as_any().downcast_ref::<$array>().unwrap();
                Self::$variant(value.is_valid(index).then(|| value.value(index).to_owned()))
            }};
        }
        macro_rules! nested {
            ($array:ty, $variant:ident) => {
                Self::$variant(Arc::new(
                    array
                        .as_any()
                        .downcast_ref::<$array>()
                        .unwrap()
                        .slice(index, 1),
                ))
            };
        }
        Ok(match array.data_type() {
            DataType::Null => Self::Null(DataType::Null),
            DataType::Int8 => primitive!(Int8Type, Int8),
            DataType::Int16 => primitive!(Int16Type, Int16),
            DataType::Int32 => primitive!(Int32Type, Int32),
            DataType::Int64 => primitive!(Int64Type, Int64),
            DataType::UInt8 => primitive!(UInt8Type, UInt8),
            DataType::UInt16 => primitive!(UInt16Type, UInt16),
            DataType::UInt32 => primitive!(UInt32Type, UInt32),
            DataType::UInt64 => primitive!(UInt64Type, UInt64),
            DataType::Float16 => primitive!(Float16Type, Float16),
            DataType::Float32 => primitive!(Float32Type, Float32),
            DataType::Float64 => primitive!(Float64Type, Float64),
            DataType::Date32 => primitive!(Date32Type, Date32),
            DataType::Date64 => primitive!(Date64Type, Date64),
            DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType, Time32Second),
            DataType::Time32(TimeUnit::Millisecond) => {
                primitive!(Time32MillisecondType, Time32Millisecond)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive!(Time64MicrosecondType, Time64Microsecond)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                primitive!(Time64NanosecondType, Time64Nanosecond)
            }
            DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType, DurationSecond),
            DataType::Duration(TimeUnit::Millisecond) => {
                primitive!(DurationMillisecondType, DurationMillisecond)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                primitive!(DurationMicrosecondType, DurationMicrosecond)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                primitive!(DurationNanosecondType, DurationNanosecond)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                primitive!(IntervalYearMonthType, IntervalYearMonth)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                primitive!(IntervalDayTimeType, IntervalDayTime)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                primitive!(IntervalMonthDayNanoType, IntervalMonthDayNano)
            }
            DataType::Decimal32(precision, scale) => {
                primitive!(Decimal32Type, Decimal32, *precision, *scale)
            }
            DataType::Decimal64(precision, scale) => {
                primitive!(Decimal64Type, Decimal64, *precision, *scale)
            }
            DataType::Decimal128(precision, scale) => {
                primitive!(Decimal128Type, Decimal128, *precision, *scale)
            }
            DataType::Decimal256(precision, scale) => {
                primitive!(Decimal256Type, Decimal256, *precision, *scale)
            }
            DataType::Timestamp(TimeUnit::Second, timezone) => {
                primitive!(TimestampSecondType, TimestampSecond, timezone.clone())
            }
            DataType::Timestamp(TimeUnit::Millisecond, timezone) => primitive!(
                TimestampMillisecondType,
                TimestampMillisecond,
                timezone.clone()
            ),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => primitive!(
                TimestampMicrosecondType,
                TimestampMicrosecond,
                timezone.clone()
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone) => primitive!(
                TimestampNanosecondType,
                TimestampNanosecond,
                timezone.clone()
            ),
            DataType::Boolean => {
                let value = array.as_boolean();
                Self::Boolean(value.is_valid(index).then(|| value.value(index)))
            }
            DataType::Utf8 => owned!(StringArray, Utf8),
            DataType::LargeUtf8 => owned!(LargeStringArray, LargeUtf8),
            DataType::Utf8View => owned!(StringViewArray, Utf8View),
            DataType::Binary => owned!(BinaryArray, Binary),
            DataType::LargeBinary => owned!(LargeBinaryArray, LargeBinary),
            DataType::BinaryView => owned!(BinaryViewArray, BinaryView),
            DataType::FixedSizeBinary(width) => {
                let value = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                Self::FixedSizeBinary(
                    *width,
                    value.is_valid(index).then(|| value.value(index).to_vec()),
                )
            }
            DataType::FixedSizeList(..) => nested!(FixedSizeListArray, FixedSizeList),
            DataType::List(..) => nested!(ListArray, List),
            DataType::LargeList(..) => nested!(LargeListArray, LargeList),
            DataType::ListView(..) => nested!(ListViewArray, ListView),
            DataType::LargeListView(..) => nested!(LargeListViewArray, LargeListView),
            DataType::Struct(..) => nested!(StructArray, Struct),
            DataType::Map(..) => nested!(MapArray, Map),
            DataType::Dictionary(key_type, _) => {
                macro_rules! dictionary {
                    ($ty:ty) => {{
                        let array = array.as_dictionary::<$ty>();
                        match array.key(index) {
                            Some(key) => Self::try_from_array(array.values(), key)?,
                            None => Self::Null(array.values().data_type().clone()),
                        }
                    }};
                }
                let value = match key_type.as_ref() {
                    DataType::Int8 => dictionary!(Int8Type),
                    DataType::Int16 => dictionary!(Int16Type),
                    DataType::Int32 => dictionary!(Int32Type),
                    DataType::Int64 => dictionary!(Int64Type),
                    DataType::UInt8 => dictionary!(UInt8Type),
                    DataType::UInt16 => dictionary!(UInt16Type),
                    DataType::UInt32 => dictionary!(UInt32Type),
                    DataType::UInt64 => dictionary!(UInt64Type),
                    _ => return Err(Error::invalid_input("invalid dictionary key type".into())),
                };
                Self::Dictionary(key_type.clone(), Box::new(value))
            }
            DataType::RunEndEncoded(run_ends, values) => {
                macro_rules! run {
                    ($ty:ty) => {{
                        let array = array.as_any().downcast_ref::<RunArray<$ty>>().unwrap();
                        Self::try_from_array(array.values(), array.get_physical_index(index))?
                    }};
                }
                let value = match run_ends.data_type() {
                    DataType::Int16 => run!(Int16Type),
                    DataType::Int32 => run!(Int32Type),
                    DataType::Int64 => run!(Int64Type),
                    _ => return Err(Error::invalid_input("invalid run-end type".into())),
                };
                Self::RunEndEncoded(run_ends.clone(), values.clone(), Box::new(value))
            }
            DataType::Union(fields, mode) => {
                let array = array.as_any().downcast_ref::<UnionArray>().unwrap();
                let id = array.type_id(index);
                let value = Self::try_from_array(array.child(id), array.value_offset(index))?;
                Self::Union(Some((id, Box::new(value))), fields.clone(), *mode)
            }
            other => {
                return Err(Error::unsupported(format!(
                    "unsupported scalar type: {other}"
                )));
            }
        })
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null(data_type) => data_type.clone(),
            Self::Int8(..) => DataType::Int8,
            Self::Int16(..) => DataType::Int16,
            Self::Int32(..) => DataType::Int32,
            Self::Int64(..) => DataType::Int64,
            Self::UInt8(..) => DataType::UInt8,
            Self::UInt16(..) => DataType::UInt16,
            Self::UInt32(..) => DataType::UInt32,
            Self::UInt64(..) => DataType::UInt64,
            Self::Float16(..) => DataType::Float16,
            Self::Float32(..) => DataType::Float32,
            Self::Float64(..) => DataType::Float64,
            Self::Date32(..) => DataType::Date32,
            Self::Date64(..) => DataType::Date64,
            Self::Time32Second(..) => DataType::Time32(TimeUnit::Second),
            Self::Time32Millisecond(..) => DataType::Time32(TimeUnit::Millisecond),
            Self::Time64Microsecond(..) => DataType::Time64(TimeUnit::Microsecond),
            Self::Time64Nanosecond(..) => DataType::Time64(TimeUnit::Nanosecond),
            Self::DurationSecond(..) => DataType::Duration(TimeUnit::Second),
            Self::DurationMillisecond(..) => DataType::Duration(TimeUnit::Millisecond),
            Self::DurationMicrosecond(..) => DataType::Duration(TimeUnit::Microsecond),
            Self::DurationNanosecond(..) => DataType::Duration(TimeUnit::Nanosecond),
            Self::IntervalYearMonth(..) => DataType::Interval(IntervalUnit::YearMonth),
            Self::IntervalDayTime(..) => DataType::Interval(IntervalUnit::DayTime),
            Self::IntervalMonthDayNano(..) => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::Decimal32(_, precision, scale) => DataType::Decimal32(*precision, *scale),
            Self::Decimal64(_, precision, scale) => DataType::Decimal64(*precision, *scale),
            Self::Decimal128(_, precision, scale) => DataType::Decimal128(*precision, *scale),
            Self::Decimal256(_, precision, scale) => DataType::Decimal256(*precision, *scale),
            Self::TimestampSecond(_, timezone) => {
                DataType::Timestamp(TimeUnit::Second, timezone.clone())
            }
            Self::TimestampMillisecond(_, timezone) => {
                DataType::Timestamp(TimeUnit::Millisecond, timezone.clone())
            }
            Self::TimestampMicrosecond(_, timezone) => {
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
            }
            Self::TimestampNanosecond(_, timezone) => {
                DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone())
            }
            Self::Boolean(..) => DataType::Boolean,
            Self::Utf8(..) => DataType::Utf8,
            Self::LargeUtf8(..) => DataType::LargeUtf8,
            Self::Utf8View(..) => DataType::Utf8View,
            Self::Binary(..) => DataType::Binary,
            Self::LargeBinary(..) => DataType::LargeBinary,
            Self::BinaryView(..) => DataType::BinaryView,
            Self::FixedSizeBinary(width, _) => DataType::FixedSizeBinary(*width),
            Self::FixedSizeList(value) => value.data_type().clone(),
            Self::List(value) => value.data_type().clone(),
            Self::LargeList(value) => value.data_type().clone(),
            Self::ListView(value) => value.data_type().clone(),
            Self::LargeListView(value) => value.data_type().clone(),
            Self::Struct(value) => value.data_type().clone(),
            Self::Map(value) => value.data_type().clone(),
            Self::Union(_, fields, mode) => DataType::Union(fields.clone(), *mode),
            Self::Dictionary(key, value) => {
                DataType::Dictionary(key.clone(), Box::new(value.data_type()))
            }
            Self::RunEndEncoded(run_ends, values, _) => {
                DataType::RunEndEncoded(run_ends.clone(), values.clone())
            }
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            Self::Null(..) => true,
            Self::Int8(value) => value.is_none(),
            Self::Int16(value) => value.is_none(),
            Self::Int32(value) => value.is_none(),
            Self::Int64(value) => value.is_none(),
            Self::UInt8(value) => value.is_none(),
            Self::UInt16(value) => value.is_none(),
            Self::UInt32(value) => value.is_none(),
            Self::UInt64(value) => value.is_none(),
            Self::Float16(value) => value.is_none(),
            Self::Float32(value) => value.is_none(),
            Self::Float64(value) => value.is_none(),
            Self::Date32(value) => value.is_none(),
            Self::Date64(value) => value.is_none(),
            Self::Time32Second(value) => value.is_none(),
            Self::Time32Millisecond(value) => value.is_none(),
            Self::Time64Microsecond(value) => value.is_none(),
            Self::Time64Nanosecond(value) => value.is_none(),
            Self::DurationSecond(value) => value.is_none(),
            Self::DurationMillisecond(value) => value.is_none(),
            Self::DurationMicrosecond(value) => value.is_none(),
            Self::DurationNanosecond(value) => value.is_none(),
            Self::IntervalYearMonth(value) => value.is_none(),
            Self::IntervalDayTime(value) => value.is_none(),
            Self::IntervalMonthDayNano(value) => value.is_none(),
            Self::Boolean(value) => value.is_none(),
            Self::Utf8(value) => value.is_none(),
            Self::LargeUtf8(value) => value.is_none(),
            Self::Utf8View(value) => value.is_none(),
            Self::Binary(value) => value.is_none(),
            Self::LargeBinary(value) => value.is_none(),
            Self::BinaryView(value) => value.is_none(),
            Self::Decimal32(value, ..) => value.is_none(),
            Self::Decimal64(value, ..) => value.is_none(),
            Self::Decimal128(value, ..) => value.is_none(),
            Self::Decimal256(value, ..) => value.is_none(),
            Self::TimestampSecond(value, _) => value.is_none(),
            Self::TimestampMillisecond(value, _) => value.is_none(),
            Self::TimestampMicrosecond(value, _) => value.is_none(),
            Self::TimestampNanosecond(value, _) => value.is_none(),
            Self::FixedSizeBinary(_, value) => value.is_none(),
            Self::FixedSizeList(value) => value.logical_null_count() != 0,
            Self::List(value) => value.logical_null_count() != 0,
            Self::LargeList(value) => value.logical_null_count() != 0,
            Self::ListView(value) => value.logical_null_count() != 0,
            Self::LargeListView(value) => value.logical_null_count() != 0,
            Self::Struct(value) => value.logical_null_count() != 0,
            Self::Map(value) => value.logical_null_count() != 0,
            Self::Union(value, ..) => value.as_ref().is_none_or(|(_, value)| value.is_null()),
            Self::Dictionary(_, value) | Self::RunEndEncoded(_, _, value) => value.is_null(),
        }
    }

    pub fn as_boolean(&self) -> Result<Option<bool>> {
        match self {
            Self::Boolean(value) => Ok(*value),
            Self::Null(DataType::Boolean) => Ok(None),
            _ => Err(Error::invalid_input("expected Boolean expression".into())),
        }
    }

    pub fn to_array(&self) -> Result<ArrayRef> {
        self.to_array_of_size(1)
    }

    pub fn to_array_of_size(&self, len: usize) -> Result<ArrayRef> {
        macro_rules! primitive {
            ($array:ty, $value:expr) => {
                match $value {
                    Some(value) => <$array>::from_value(*value, len),
                    None => <$array>::new_null(len),
                }
            };
        }
        Ok(match self {
            Self::Null(data_type) => new_null_array(data_type, len),
            Self::Int8(value) => Arc::new(primitive!(Int8Array, value)),
            Self::Int16(value) => Arc::new(primitive!(Int16Array, value)),
            Self::Int32(value) => Arc::new(primitive!(Int32Array, value)),
            Self::Int64(value) => Arc::new(primitive!(Int64Array, value)),
            Self::UInt8(value) => Arc::new(primitive!(UInt8Array, value)),
            Self::UInt16(value) => Arc::new(primitive!(UInt16Array, value)),
            Self::UInt32(value) => Arc::new(primitive!(UInt32Array, value)),
            Self::UInt64(value) => Arc::new(primitive!(UInt64Array, value)),
            Self::Float16(value) => Arc::new(primitive!(Float16Array, value)),
            Self::Float32(value) => Arc::new(primitive!(Float32Array, value)),
            Self::Float64(value) => Arc::new(primitive!(Float64Array, value)),
            Self::Date32(value) => Arc::new(primitive!(Date32Array, value)),
            Self::Date64(value) => Arc::new(primitive!(Date64Array, value)),
            Self::Time32Second(value) => Arc::new(primitive!(Time32SecondArray, value)),
            Self::Time32Millisecond(value) => Arc::new(primitive!(Time32MillisecondArray, value)),
            Self::Time64Microsecond(value) => Arc::new(primitive!(Time64MicrosecondArray, value)),
            Self::Time64Nanosecond(value) => Arc::new(primitive!(Time64NanosecondArray, value)),
            Self::DurationSecond(value) => Arc::new(primitive!(DurationSecondArray, value)),
            Self::DurationMillisecond(value) => {
                Arc::new(primitive!(DurationMillisecondArray, value))
            }
            Self::DurationMicrosecond(value) => {
                Arc::new(primitive!(DurationMicrosecondArray, value))
            }
            Self::DurationNanosecond(value) => Arc::new(primitive!(DurationNanosecondArray, value)),
            Self::IntervalYearMonth(value) => Arc::new(primitive!(IntervalYearMonthArray, value)),
            Self::IntervalDayTime(value) => Arc::new(primitive!(IntervalDayTimeArray, value)),
            Self::IntervalMonthDayNano(value) => {
                Arc::new(primitive!(IntervalMonthDayNanoArray, value))
            }
            Self::Decimal32(value, precision, scale) => Arc::new(
                primitive!(Decimal32Array, value)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|source| invalid_decimal_metadata(source))?,
            ),
            Self::Decimal64(value, precision, scale) => Arc::new(
                primitive!(Decimal64Array, value)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|source| invalid_decimal_metadata(source))?,
            ),
            Self::Decimal128(value, precision, scale) => Arc::new(
                primitive!(Decimal128Array, value)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|source| invalid_decimal_metadata(source))?,
            ),
            Self::Decimal256(value, precision, scale) => Arc::new(
                primitive!(Decimal256Array, value)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|source| invalid_decimal_metadata(source))?,
            ),
            Self::TimestampSecond(value, timezone) => Arc::new(
                primitive!(TimestampSecondArray, value).with_timezone_opt(timezone.clone()),
            ),
            Self::TimestampMillisecond(value, timezone) => Arc::new(
                primitive!(TimestampMillisecondArray, value).with_timezone_opt(timezone.clone()),
            ),
            Self::TimestampMicrosecond(value, timezone) => Arc::new(
                primitive!(TimestampMicrosecondArray, value).with_timezone_opt(timezone.clone()),
            ),
            Self::TimestampNanosecond(value, timezone) => Arc::new(
                primitive!(TimestampNanosecondArray, value).with_timezone_opt(timezone.clone()),
            ),
            Self::Boolean(value) => {
                Arc::new(BooleanArray::from_iter(std::iter::repeat_n(*value, len)))
            }
            Self::Utf8(value) => Arc::new(StringArray::from_iter(std::iter::repeat_n(
                value.as_deref(),
                len,
            ))),
            Self::LargeUtf8(value) => Arc::new(LargeStringArray::from_iter(std::iter::repeat_n(
                value.as_deref(),
                len,
            ))),
            Self::Utf8View(value) => Arc::new(StringViewArray::from_iter(std::iter::repeat_n(
                value.as_deref(),
                len,
            ))),
            Self::Binary(value) => Arc::new(BinaryArray::from_iter(std::iter::repeat_n(
                value.as_deref(),
                len,
            ))),
            Self::LargeBinary(value) => Arc::new(LargeBinaryArray::from_iter(std::iter::repeat_n(
                value.as_deref(),
                len,
            ))),
            Self::BinaryView(value) => Arc::new(BinaryViewArray::from_iter(std::iter::repeat_n(
                value.as_deref(),
                len,
            ))),
            Self::FixedSizeBinary(width, value) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    std::iter::repeat_n(value.as_deref(), len),
                    *width,
                )
                .map_err(|source| {
                    Error::invalid_input("fixed-size binary value does not match its width".into())
                        .with_source(source)
                })?,
            ),
            Self::FixedSizeList(value) => repeat_nested(value.clone(), len)?,
            Self::List(value) => repeat_nested(value.clone(), len)?,
            Self::LargeList(value) => repeat_nested(value.clone(), len)?,
            Self::ListView(value) => repeat_nested(value.clone(), len)?,
            Self::LargeListView(value) => repeat_nested(value.clone(), len)?,
            Self::Struct(value) => repeat_nested(value.clone(), len)?,
            Self::Map(value) => repeat_nested(value.clone(), len)?,
            Self::Union(value, fields, mode) => match value {
                None => new_null_array(&self.data_type(), len),
                Some((id, value)) => {
                    if !fields.iter().any(|(field_id, field)| {
                        field_id == *id && field.data_type() == &value.data_type()
                    }) {
                        return Err(Error::invalid_input(
                            "union scalar does not match its field".into(),
                        ));
                    }
                    let children = fields
                        .iter()
                        .map(|(field_id, field)| {
                            if field_id == *id {
                                value.to_array_of_size(len)
                            } else {
                                Ok(new_null_array(
                                    field.data_type(),
                                    if *mode == UnionMode::Sparse { len } else { 0 },
                                ))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let offsets = if *mode == UnionMode::Dense {
                        let len_i32 = i32::try_from(len)
                            .map_err(|_| Error::invalid_input("union length exceeds i32".into()))?;
                        Some(arrow::buffer::ScalarBuffer::from_iter(0..len_i32))
                    } else {
                        None
                    };
                    Arc::new(
                        UnionArray::try_new(
                            fields.clone(),
                            std::iter::repeat_n(*id, len).collect(),
                            offsets,
                            children,
                        )
                        .map_err(|source| {
                            Error::invalid_input("invalid union scalar".into()).with_source(source)
                        })?,
                    )
                }
            },
            Self::Dictionary(key, value) => {
                macro_rules! dictionary {
                    ($ty:ty) => {{
                        let keys = if value.is_null() {
                            PrimitiveArray::<$ty>::new_null(len)
                        } else {
                            PrimitiveArray::<$ty>::from_value(0, len)
                        };
                        Arc::new(
                            DictionaryArray::<$ty>::try_new(keys, value.to_array()?).map_err(
                                |source| {
                                    Error::internal(
                                        "failed to build dictionary scalar with key zero".into(),
                                    )
                                    .with_source(source)
                                },
                            )?,
                        ) as ArrayRef
                    }};
                }
                match key.as_ref() {
                    DataType::Int8 => dictionary!(Int8Type),
                    DataType::Int16 => dictionary!(Int16Type),
                    DataType::Int32 => dictionary!(Int32Type),
                    DataType::Int64 => dictionary!(Int64Type),
                    DataType::UInt8 => dictionary!(UInt8Type),
                    DataType::UInt16 => dictionary!(UInt16Type),
                    DataType::UInt32 => dictionary!(UInt32Type),
                    DataType::UInt64 => dictionary!(UInt64Type),
                    _ => return Err(Error::invalid_input("invalid dictionary key type".into())),
                }
            }
            Self::RunEndEncoded(run_ends, values, value) => {
                if values.data_type() != &value.data_type() {
                    return Err(Error::invalid_input(
                        "run scalar does not match its value field".into(),
                    ));
                }
                macro_rules! run {
                    ($ty:ty, $native:ty) => {{
                        let end = <$native>::try_from(len).map_err(|_| {
                            Error::invalid_input("scalar length exceeds run-end type".into())
                        })?;
                        let ends = PrimitiveArray::<$ty>::from(vec![end]);
                        let data = ArrayData::builder(self.data_type())
                            .len(len)
                            .add_child_data(ends.to_data())
                            .add_child_data(value.to_array()?.to_data())
                            .build()
                            .map_err(|source| {
                                Error::invalid_input("invalid run-end encoded scalar".into())
                                    .with_source(source)
                            })?;
                        Arc::new(RunArray::<$ty>::from(data)) as ArrayRef
                    }};
                }
                if len == 0 {
                    new_empty_array(&self.data_type())
                } else {
                    match run_ends.data_type() {
                        DataType::Int16 => run!(Int16Type, i16),
                        DataType::Int32 => run!(Int32Type, i32),
                        DataType::Int64 => run!(Int64Type, i64),
                        _ => return Err(Error::invalid_input("invalid run-end type".into())),
                    }
                }
            }
        })
    }
}

#[cold]
#[track_caller]
fn invalid_decimal_metadata(source: arrow::error::ArrowError) -> Error {
    Error::invalid_input("invalid decimal precision or scale".into()).with_source(source)
}

/// Broadcast one typed nested value using Arrow kernels.
fn repeat_nested(value: ArrayRef, len: usize) -> Result<ArrayRef> {
    if value.len() != 1 {
        return Err(Error::invalid_input(
            "nested scalar must contain exactly one value".into(),
        ));
    }
    Ok(if len == 0 {
        value.slice(0, 0)
    } else if len == 1 {
        value
    } else if value.logical_null_count() != 0 {
        new_null_array(value.data_type(), len)
    } else {
        take(value.as_ref(), &UInt64Array::from(vec![0; len]), None).map_err(|source| {
            Error::internal("failed to broadcast nested scalar".into()).with_source(source)
        })?
    })
}

/// A scalar applies to every input row; an array has one value per row.
#[derive(Clone, Debug)]
pub enum ColumnValue {
    Array(ArrayRef),
    Scalar(ScalarValue),
}

impl ColumnValue {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Array(value) => value.data_type().clone(),
            Self::Scalar(value) => value.data_type(),
        }
    }

    /// Broadcast only at a consumer that requires an ordinary Arrow array.
    /// Array inputs preserve their buffers and must match the caller's row count.
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        match self {
            Self::Array(value) if value.len() == num_rows => Ok(value),
            Self::Array(_) => Err(Error::invalid_input(
                "column value length differs from input".into(),
            )),
            Self::Scalar(value) => value.to_array_of_size(num_rows),
        }
    }
}

pub(super) trait ScalarPrimitiveType: ArrowPrimitiveType {
    fn scalar(value: &ScalarValue) -> Result<Option<Self::Native>>;
    fn value(value: Option<Self::Native>) -> ScalarValue;
}
macro_rules! scalar_primitive {
    ($ty:ty, $variant:ident) => {
        impl ScalarPrimitiveType for $ty {
            fn scalar(value: &ScalarValue) -> Result<Option<Self::Native>> {
                match value {
                    ScalarValue::$variant(value) => Ok(*value),
                    ScalarValue::Null(data_type) if *data_type == Self::DATA_TYPE => Ok(None),
                    _ => Err(Error::invalid_input(format!(
                        "expected {} scalar",
                        Self::DATA_TYPE
                    ))),
                }
            }
            fn value(value: Option<Self::Native>) -> ScalarValue {
                ScalarValue::$variant(value)
            }
        }
    };
}
scalar_primitive!(Int8Type, Int8);
scalar_primitive!(Int16Type, Int16);
scalar_primitive!(Int32Type, Int32);
scalar_primitive!(Int64Type, Int64);
scalar_primitive!(UInt8Type, UInt8);
scalar_primitive!(UInt16Type, UInt16);
scalar_primitive!(UInt32Type, UInt32);
scalar_primitive!(UInt64Type, UInt64);
scalar_primitive!(Float32Type, Float32);
scalar_primitive!(Float64Type, Float64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_boundaries_preserve_identity_and_validate_row_counts() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]).slice(1, 2));
        let output = ColumnValue::Array(input.clone()).into_array(2).unwrap();
        assert!(Arc::ptr_eq(&input, &output));
        assert!(ColumnValue::Array(input).into_array(3).is_err());
    }

    #[test]
    fn scalar_conversion_preserves_slices_nulls_and_arrow_type_metadata() {
        let date: ArrayRef = Arc::new(Date32Array::from(vec![Some(9), None, Some(42)]));
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123), None, Some(-456)])
                .with_precision_and_scale(20, 3)
                .unwrap(),
        );
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("before"), None, Some("after")]));
        let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        for input in [date, decimal, text, boolean] {
            for index in [1, 2] {
                let slice = input.slice(index, 1);
                let scalar = ScalarValue::try_from_array(&slice, 0).unwrap();
                assert_eq!(scalar.data_type(), input.data_type().clone());
                assert_eq!(scalar.is_null(), index == 1);
                for len in [0, 1, 17] {
                    let output = scalar.to_array_of_size(len).unwrap();
                    let expected =
                        take(slice.as_ref(), &UInt64Array::from(vec![0; len]), None).unwrap();
                    assert_eq!(output.to_data(), expected.to_data());
                }
            }
            assert!(ScalarValue::try_from_array(&input, input.len()).is_err());
        }
    }
}
