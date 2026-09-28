mod aggregate;
mod exchange;
mod filter;
mod project;
mod scan;

pub use aggregate::{AggregateExpr, AggregateFunction, AggregateLayout, AggregateOperator};
pub use exchange::{
    ExchangeConsumer, ExchangeHandle, ExchangeId, ExchangeService, ExchangeSink,
    ExchangeSinkOperator, ExchangeSourceOperator,
};
pub use filter::FilterOperator;
pub use project::ProjectOperator;
pub use scan::{
    ScanConsumer, ScanHandle, ScanOperator, ScanReceiver, ScanRequest, ScanSendError, ScanSender,
    ScanStorage, scan_channel,
};

use crate::{
    Result,
    pipeline::{PipelineGraphBuilder, PipelineId},
};
use std::{fmt::Debug, sync::Arc};

pub trait Operator: Debug + Send + Sync {
    fn name(&self) -> &'static str;

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct OperatorTree {
    root: OperatorTreeNode,
}

impl OperatorTree {
    pub fn new(root: OperatorTreeNode) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &OperatorTreeNode {
        &self.root
    }
}

#[derive(Clone, Debug)]
pub struct OperatorTreeNode {
    operator: Arc<dyn Operator>,
    children: Vec<OperatorTreeNode>,
}

impl OperatorTreeNode {
    pub fn new(operator: impl Operator + 'static, children: Vec<OperatorTreeNode>) -> Self {
        Self {
            operator: Arc::new(operator),
            children,
        }
    }

    pub fn children(&self) -> &[OperatorTreeNode] {
        &self.children
    }

    pub fn operator(&self) -> &Arc<dyn Operator> {
        &self.operator
    }
}
