pub mod agg;
pub mod predicate;
pub mod scalar;

use arrow::datatypes::DataType;

/// An expression's result type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionResultType {
    data_type: DataType,
    nullable: bool,
}

impl ExpressionResultType {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}
