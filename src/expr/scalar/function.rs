use super::BoundScalarExprRef;

/// An already selected built-in implementation; no name lookup or coercion occurs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarFunction {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Negate,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Clone, Debug)]
pub struct BoundFunctionExpression {
    function: ScalarFunction,
    arguments: Vec<BoundScalarExprRef>,
}
impl BoundFunctionExpression {
    pub fn new(function: ScalarFunction, arguments: Vec<BoundScalarExprRef>) -> Self {
        Self {
            function,
            arguments,
        }
    }
    pub fn function(&self) -> ScalarFunction {
        self.function
    }
    pub fn arguments(&self) -> &[BoundScalarExprRef] {
        &self.arguments
    }
}
