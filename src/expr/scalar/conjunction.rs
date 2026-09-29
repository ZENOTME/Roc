use super::BoundScalarExprRef;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}
#[derive(Clone, Debug)]
pub struct BoundConjunctionExpression {
    conjunction: Conjunction,
    arguments: Vec<BoundScalarExprRef>,
}
impl BoundConjunctionExpression {
    pub fn new(conjunction: Conjunction, arguments: Vec<BoundScalarExprRef>) -> Self {
        Self {
            conjunction,
            arguments,
        }
    }
    pub fn conjunction(&self) -> Conjunction {
        self.conjunction
    }
    pub fn arguments(&self) -> &[BoundScalarExprRef] {
        &self.arguments
    }
}
