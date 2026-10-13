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

mod service;

pub use service::{ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink};

use super::Operator;
use crate::{
    column::ColumnId,
    error::{Error, Result},
    exec::{ExchangeSinkExec, ExchangeSourceExec},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use std::{fmt, sync::Arc};

pub type ExchangeId = usize;

/// Reads one exchange; its batches bind `outputs` in order.
#[derive(Clone)]
pub struct ExchangeSourceOperator {
    exchange: ExchangeId,
    service: Arc<dyn ExchangeService>,
    outputs: Vec<ColumnId>,
}

/// Sends the child's output columns to every listed exchange.
#[derive(Clone)]
pub struct ExchangeSinkOperator {
    exchanges: Vec<ExchangeId>,
    service: Arc<dyn ExchangeService>,
}

impl ExchangeSourceOperator {
    pub fn new(
        exchange: ExchangeId,
        service: Arc<dyn ExchangeService>,
        outputs: Vec<ColumnId>,
    ) -> Self {
        Self {
            exchange,
            service,
            outputs,
        }
    }

    pub fn exchange(&self) -> ExchangeId {
        self.exchange
    }
}

impl ExchangeSinkOperator {
    pub fn new(exchanges: Vec<ExchangeId>, service: Arc<dyn ExchangeService>) -> Self {
        Self { exchanges, service }
    }

    pub fn exchanges(&self) -> &[ExchangeId] {
        &self.exchanges
    }
}

impl fmt::Debug for ExchangeSourceOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSourceOperator")
            .field("exchange", &self.exchange)
            .field("outputs", &self.outputs)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ExchangeSinkOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSinkOperator")
            .field("exchanges", &self.exchanges)
            .finish_non_exhaustive()
    }
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &'static str {
        "exchange source"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        self.outputs.clone()
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        if !current_node.children().is_empty() {
            return Err(Error::InvalidPlan(format!(
                "exchange source operator requires 0 children, got {}",
                current_node.children().len(),
            )));
        }
        graph.pipeline_mut(current)?.set_source(
            Box::new(ExchangeSourceExec::new(self.exchange, self.service.clone())),
            &self.outputs,
        )
    }
}

impl Operator for ExchangeSinkOperator {
    fn name(&self) -> &'static str {
        "exchange sink"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        vec![]
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::InvalidPlan(format!(
                "exchange sink operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        build_pipeline_on_node(child, current, graph)?;
        let outputs = child.operator().output_columns();
        let schema = graph.columns().schema(&outputs)?;
        let sink = ExchangeSinkExec::new(self.exchanges.clone(), schema, self.service.clone());
        graph
            .pipeline_mut(current)?
            .set_sink(Box::new(sink), Some(outputs))
    }
}
