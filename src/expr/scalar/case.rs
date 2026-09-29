use super::BoundScalarExprRef;
/// Searched CASE; all result branches must have the same type.
#[derive(Clone, Debug)]
pub struct BoundCaseExpression {
    branches: Vec<(BoundScalarExprRef, BoundScalarExprRef)>,
    else_expr: BoundScalarExprRef,
}
impl BoundCaseExpression {
    pub fn new(
        branches: Vec<(BoundScalarExprRef, BoundScalarExprRef)>,
        else_expr: BoundScalarExprRef,
    ) -> Self {
        Self {
            branches,
            else_expr,
        }
    }
    pub fn branches(&self) -> &[(BoundScalarExprRef, BoundScalarExprRef)] {
        &self.branches
    }
    pub fn else_expr(&self) -> &BoundScalarExprRef {
        &self.else_expr
    }
}
