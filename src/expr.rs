//! Scalar expressions evaluated by execution operators.
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{
    self,
    kernels::{cmp, numeric},
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Column(String),
    Int64(Option<i64>),
    Float64(#[serde(with = "float_literal")] Option<f64>),
    Boolean(Option<bool>),
    Utf8(Option<String>),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    Not(Box<Expr>),
    Cast(Box<Expr>, DataType),
}

impl Expr {
    pub fn column_names(&self, names: &mut std::collections::HashSet<String>) {
        match self {
            Self::Column(name) => {
                names.insert(name.clone());
            }
            Self::Binary { left, right, .. } => {
                left.column_names(names);
                right.column_names(names);
            }
            Self::IsNull(expr) | Self::Not(expr) | Self::Cast(expr, _) => expr.column_names(names),
            Self::Int64(_) | Self::Float64(_) | Self::Boolean(_) | Self::Utf8(_) => {}
        }
    }
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }
    pub fn binary(self, op: BinaryOp, right: Self) -> Self {
        Self::Binary {
            left: Box::new(self),
            op,
            right: Box::new(right),
        }
    }
    pub fn alias(self, name: impl Into<String>) -> NamedExpr {
        NamedExpr {
            name: name.into(),
            expr: self,
        }
    }
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let n = batch.num_rows();
        Ok(match self {
            Self::Column(name) => batch.column(batch.schema().index_of(name)?).clone(),
            Self::Int64(x) => Arc::new(match x {
                Some(value) => Int64Array::from_value(*value, n),
                None => Int64Array::new_null(n),
            }),
            Self::Float64(x) => Arc::new(match x {
                Some(value) => Float64Array::from_value(*value, n),
                None => Float64Array::new_null(n),
            }),
            Self::Boolean(x) => Arc::new(match x {
                Some(true) => BooleanArray::new(BooleanBuffer::new_set(n), None),
                Some(false) => BooleanArray::new(BooleanBuffer::new_unset(n), None),
                None => BooleanArray::new_null(n),
            }),
            Self::Utf8(x) => Arc::new(match x {
                Some(value) => {
                    StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), n))
                }
                None => StringArray::new_null(n),
            }),
            Self::IsNull(x) => {
                let input = x.evaluate(batch)?;
                Arc::new(compute::is_null(input.as_ref())?)
            }
            Self::Not(x) => {
                let input = x.evaluate(batch)?;
                Arc::new(compute::not(boolean(&input)?)?)
            }
            Self::Cast(x, ty) => compute::cast_with_options(
                x.evaluate(batch)?.as_ref(),
                ty,
                &compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?,
            Self::Binary { left, op, right } => {
                let l = left.evaluate(batch)?;
                let r = right.evaluate(batch)?;
                match op {
                    BinaryOp::Add => numeric::add(&l, &r)?,
                    BinaryOp::Subtract => numeric::sub(&l, &r)?,
                    BinaryOp::Multiply => numeric::mul(&l, &r)?,
                    BinaryOp::Divide => numeric::div(&l, &r)?,
                    BinaryOp::Eq => Arc::new(cmp::eq(&l, &r)?),
                    BinaryOp::NotEq => Arc::new(cmp::neq(&l, &r)?),
                    BinaryOp::Lt => Arc::new(cmp::lt(&l, &r)?),
                    BinaryOp::LtEq => Arc::new(cmp::lt_eq(&l, &r)?),
                    BinaryOp::Gt => Arc::new(cmp::gt(&l, &r)?),
                    BinaryOp::GtEq => Arc::new(cmp::gt_eq(&l, &r)?),
                    BinaryOp::And => Arc::new(compute::and_kleene(boolean(&l)?, boolean(&r)?)?),
                    BinaryOp::Or => Arc::new(compute::or_kleene(boolean(&l)?, boolean(&r)?)?),
                }
            }
        })
    }
    /// Infer the field produced by an expression. Common scalar types are
    /// checked without running a kernel; less common Arrow types retain Arrow's
    /// own kernel validation on an empty batch.
    pub fn field(&self, schema: &SchemaRef) -> Result<Field> {
        let field = match self {
            Self::Column(name) => schema.field_with_name(name)?.clone(),
            Self::Int64(value) => Field::new("", DataType::Int64, value.is_none()),
            Self::Float64(value) => Field::new("", DataType::Float64, value.is_none()),
            Self::Boolean(value) => Field::new("", DataType::Boolean, value.is_none()),
            Self::Utf8(value) => Field::new("", DataType::Utf8, value.is_none()),
            Self::IsNull(input) => {
                input.field(schema)?;
                Field::new("", DataType::Boolean, false)
            }
            Self::Not(input) => {
                let input = input.field(schema)?;
                if input.data_type() != &DataType::Boolean {
                    return Err(Error::Plan("NOT requires a Boolean expression".into()));
                }
                Field::new("", DataType::Boolean, input.is_nullable())
            }
            Self::Cast(input, ty) => {
                let input = input.field(schema)?;
                if !compute::can_cast_types(input.data_type(), ty) {
                    return Err(Error::Plan(format!(
                        "cannot cast {} to {ty}",
                        input.data_type()
                    )));
                }
                Field::new("", ty.clone(), input.is_nullable())
            }
            Self::Binary { left, op, right } => {
                let left = left.field(schema)?;
                let right = right.field(schema)?;
                let ty = left.data_type();
                let basic = matches!(
                    ty,
                    DataType::Int32
                        | DataType::Int64
                        | DataType::Float64
                        | DataType::Boolean
                        | DataType::Utf8
                ) && ty == right.data_type();
                if basic {
                    let output = match op {
                        BinaryOp::Add
                        | BinaryOp::Subtract
                        | BinaryOp::Multiply
                        | BinaryOp::Divide
                            if matches!(
                                ty,
                                DataType::Int32 | DataType::Int64 | DataType::Float64
                            ) =>
                        {
                            ty.clone()
                        }
                        BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq => DataType::Boolean,
                        BinaryOp::And | BinaryOp::Or if ty == &DataType::Boolean => {
                            DataType::Boolean
                        }
                        _ => return Err(Error::Plan("unsupported binary expression types".into())),
                    };
                    Field::new("", output, left.is_nullable() || right.is_nullable())
                } else {
                    let output = self.evaluate(&RecordBatch::new_empty(schema.clone()))?;
                    Field::new("", output.data_type().clone(), true)
                }
            }
        };
        Ok(field)
    }
    pub fn data_type(&self, schema: &SchemaRef) -> Result<DataType> {
        Ok(self.field(schema)?.data_type().clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NamedExpr {
    pub name: String,
    pub expr: Expr,
}

pub fn boolean(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| Error::Plan("predicate must be Boolean".into()))
}
pub fn filter(batch: RecordBatch, expr: &Expr) -> Result<RecordBatch> {
    let mask = expr.evaluate(&batch)?;
    Ok(compute::filter_record_batch(&batch, boolean(&mask)?)?)
}
pub fn project_schema(input: &SchemaRef, expressions: &[NamedExpr]) -> Result<SchemaRef> {
    let fields = expressions
        .iter()
        .map(|e| Ok(e.expr.field(input)?.with_name(&e.name)))
        .collect::<Result<Vec<_>>>()?;
    let mut names = std::collections::HashSet::new();
    if fields.iter().any(|f| !names.insert(f.name())) {
        return Err(Error::Plan("duplicate projection name".into()));
    }
    Ok(Arc::new(Schema::new(fields)))
}
pub fn project(
    batch: RecordBatch,
    expressions: &[NamedExpr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let arrays = expressions
        .iter()
        .map(|e| e.expr.evaluate(&batch))
        .collect::<Result<_>>()?;
    make_batch(schema, arrays, batch.num_rows())
}
pub fn make_batch(schema: SchemaRef, columns: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

// JSON numbers cannot represent NaN/infinity. Preserve all literal bits across plan RPCs.
mod float_literal {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error> {
        value.map(f64::to_bits).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<f64>, D::Error> {
        Ok(Option::<u64>::deserialize(deserializer)?.map(f64::from_bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expression_fields_preserve_nullability_through_projection() {
        let input = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let fields = project_schema(
            &input,
            &[
                Expr::column("id").alias("id_copy"),
                Expr::column("value").alias("optional"),
                Expr::column("value")
                    .binary(BinaryOp::Add, Expr::Int64(Some(1)))
                    .alias("sum"),
                Expr::IsNull(Box::new(Expr::column("value"))).alias("missing"),
                Expr::Int64(None).alias("null_literal"),
            ],
        )
        .unwrap();
        assert_eq!(
            fields
                .fields()
                .iter()
                .map(|field| field.is_nullable())
                .collect::<Vec<_>>(),
            vec![false, true, true, false, true]
        );
    }
}
