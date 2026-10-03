use crate::expr::ExpressionResultType;
use crate::expr::scalar::ScalarExprRef;
use arrow::datatypes::DataType;

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

/// An aggregate call such as COUNT, SUM, or MIN, with optional DISTINCT and
/// FILTER. COUNT without arguments is COUNT(*).
#[derive(Clone, Debug)]
pub struct AggregateExpression {
    alias: Option<String>,
    function: AggregateFunction,
    arguments: Vec<ScalarExprRef>,
    result_type: ExpressionResultType,
    distinct: bool,
    filter: Option<ScalarExprRef>,
}
impl AggregateExpression {
    pub fn new(
        function: AggregateFunction,
        arguments: Vec<ScalarExprRef>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            alias: None,
            function,
            arguments,
            result_type: ExpressionResultType {
                data_type,
                nullable,
            },
            distinct: false,
            filter: None,
        }
    }
    pub fn result_type(&self) -> &ExpressionResultType {
        &self.result_type
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
    pub fn with_filter(mut self, filter: ScalarExprRef) -> Self {
        self.filter = Some(filter);
        self
    }
    pub fn function(&self) -> AggregateFunction {
        self.function
    }
    pub fn arguments(&self) -> &[ScalarExprRef] {
        &self.arguments
    }
    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
    pub fn filter(&self) -> Option<&ScalarExprRef> {
        self.filter.as_ref()
    }
}
