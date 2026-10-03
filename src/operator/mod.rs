// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod aggregate;
mod exchange;
mod filter;
mod project;
mod scan;

pub use aggregate::AggregateOperator;
pub use exchange::{
    ExchangeConsumer, ExchangeHandle, ExchangeId, ExchangeService, ExchangeSink,
    ExchangeSinkOperator, ExchangeSourceOperator,
};
pub use filter::FilterOperator;
pub use project::{ProjectOperator, Projection, ProjectionExpression};
pub use scan::{
    ScanConsumer, ScanHandle, ScanOperator, ScanReceiver, ScanRequest, ScanSendError, ScanSender,
    ScanStorage, scan_channel,
};

use crate::{
    error::Result,
    pipeline::{PipelineGraphBuilder, PipelineId},
};
use std::{fmt::Debug, sync::Arc};

pub trait Operator: Debug + Send + Sync {
    /// Operator name.
    fn name(&self) -> &'static str;

    /// Builds pipeline base on this operator. Operator is responsible to call this function on its child operator.
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
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
