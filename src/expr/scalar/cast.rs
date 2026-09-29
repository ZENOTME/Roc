use super::BoundScalarExprRef;
use arrow::datatypes::DataType;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastMode {
    Strict,
    Try,
}
#[derive(Clone, Debug)]
pub struct BoundCastExpression {
    input: BoundScalarExprRef,
    target: DataType,
    mode: CastMode,
}
impl BoundCastExpression {
    pub fn new(input: BoundScalarExprRef, target: DataType, mode: CastMode) -> Self {
        Self {
            input,
            target,
            mode,
        }
    }
    pub fn input(&self) -> &BoundScalarExprRef {
        &self.input
    }
    pub fn target(&self) -> &DataType {
        &self.target
    }
    pub fn mode(&self) -> CastMode {
        self.mode
    }
}
