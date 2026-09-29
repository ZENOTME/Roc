use super::BoundScalarExprRef;
#[derive(Clone, Debug)]
pub struct BoundNotExpression {
    input: BoundScalarExprRef,
}
impl BoundNotExpression {
    pub fn new(input: BoundScalarExprRef) -> Self {
        Self { input }
    }
    pub fn input(&self) -> &BoundScalarExprRef {
        &self.input
    }
}
