use crate::expr::scalar::BoundScalarExprRef;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CovarPop,
}

impl AggregateFunction {
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::CovarPop => "covar_pop",
        }
    }
}

/// Static aggregate call. COUNT with no arguments means COUNT(*).
#[derive(Clone, Debug)]
pub struct BoundAggregateExpression {
    alias: Option<String>,
    function: AggregateFunction,
    arguments: Vec<BoundScalarExprRef>,
    distinct: bool,
    filter: Option<BoundScalarExprRef>,
}
impl BoundAggregateExpression {
    pub fn new(function: AggregateFunction, arguments: Vec<BoundScalarExprRef>) -> Self {
        Self {
            alias: None,
            function,
            arguments,
            distinct: false,
            filter: None,
        }
    }
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
    /// Output field name; aliases do not affect aggregate evaluation.
    pub fn output_name(&self) -> &str {
        self.alias().unwrap_or_else(|| self.function.name())
    }
    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }
    pub fn with_filter(mut self, filter: BoundScalarExprRef) -> Self {
        self.filter = Some(filter);
        self
    }
    pub fn function(&self) -> AggregateFunction {
        self.function
    }
    pub fn arguments(&self) -> &[BoundScalarExprRef] {
        &self.arguments
    }
    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
    pub fn filter(&self) -> Option<&BoundScalarExprRef> {
        self.filter.as_ref()
    }
}
