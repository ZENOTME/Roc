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
    error::{Error, Result},
    exec::{ExchangeSinkExec, ExchangeSourceExec},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use arrow::datatypes::SchemaRef;
use std::{fmt, sync::Arc};

pub type ExchangeId = usize;

#[derive(Clone)]
pub struct ExchangeSourceOperator {
    exchange: ExchangeId,
    service: Arc<dyn ExchangeService>,
}

#[derive(Clone)]
pub struct ExchangeSinkOperator {
    exchanges: Vec<ExchangeId>,
    schema: SchemaRef,
    service: Arc<dyn ExchangeService>,
}

impl ExchangeSourceOperator {
    pub fn new(exchange: ExchangeId, service: Arc<dyn ExchangeService>) -> Self {
        Self { exchange, service }
    }

    pub fn into_parts(self) -> (ExchangeId, Arc<dyn ExchangeService>) {
        (self.exchange, self.service)
    }
}

impl ExchangeSinkOperator {
    pub fn new(
        exchanges: Vec<ExchangeId>,
        schema: SchemaRef,
        service: Arc<dyn ExchangeService>,
    ) -> Self {
        Self {
            exchanges,
            schema,
            service,
        }
    }

    pub fn into_parts(self) -> (Vec<ExchangeId>, SchemaRef, Arc<dyn ExchangeService>) {
        (self.exchanges, self.schema, self.service)
    }
}

impl fmt::Debug for ExchangeSourceOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSourceOperator")
            .field("exchange", &self.exchange)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ExchangeSinkOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSinkOperator")
            .field("exchanges", &self.exchanges)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &'static str {
        "exchange source"
    }
    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        if !current_node.children().is_empty() {
            return Err(Error::InvalidPlan(format!(
                "exchange source operator requires 0 children, got {}",
                current_node.children().len(),
            )));
        }
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(ExchangeSourceExec::new(self.clone())))
    }
}

impl Operator for ExchangeSinkOperator {
    fn name(&self) -> &'static str {
        "exchange sink"
    }

    fn build_pipeline(
        &self,
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = current_node.children() else {
            return Err(Error::InvalidPlan(format!(
                "exchange sink operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        graph
            .pipeline_mut(current)?
            .set_sink(Box::new(ExchangeSinkExec::new(self.clone())))?;
        build_pipeline_on_node(child, current, graph)
    }
}
