use super::BoundScalarExprRef;
#[derive(Clone, Debug)]
pub struct BoundCoalesceExpression {
    arguments: Vec<BoundScalarExprRef>,
}
impl BoundCoalesceExpression {
    pub fn new(arguments: Vec<BoundScalarExprRef>) -> Self {
        Self { arguments }
    }
    pub fn arguments(&self) -> &[BoundScalarExprRef] {
        &self.arguments
    }
}
