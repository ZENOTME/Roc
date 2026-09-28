mod service;

pub use service::{ExchangeConsumer, ExchangeHandle, ExchangeService, ExchangeSink};

use super::Operator;
use crate::{
    Error, Result,
    exec::{ExchangeSinkExec, ExchangeSourceExec},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_node},
};
use arrow::datatypes::SchemaRef;
use std::{fmt, sync::Arc};

pub type ExchangeId = usize;

#[derive(Clone)]
pub struct ExchangeSourceOperator {
    pub exchange: ExchangeId,
    pub schema: SchemaRef,
    pub service: Arc<dyn ExchangeService>,
}

#[derive(Clone)]
pub struct ExchangeSinkOperator {
    pub exchanges: Vec<ExchangeId>,
    pub schema: SchemaRef,
    pub service: Arc<dyn ExchangeService>,
}

impl fmt::Debug for ExchangeSourceOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSourceOperator")
            .field("exchange", &self.exchange)
            .field("schema", &self.schema)
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
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        if !node.children().is_empty() {
            return Err(Error::Plan("exchange source cannot have children".into()));
        }
        let service = self.service.clone();
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(ExchangeSourceExec::new(self.clone(), service)))
    }
}

impl Operator for ExchangeSinkOperator {
    fn name(&self) -> &'static str {
        "exchange sink"
    }

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        let [child] = node.children() else {
            return Err(Error::Plan(
                "exchange sink operator requires one child".into(),
            ));
        };
        let service = self.service.clone();
        graph
            .pipeline_mut(current)?
            .set_sink(Box::new(ExchangeSinkExec::new(
                self.clone(),
                service,
                self.schema.clone(),
            )))?;
        build_pipeline_node(child, current, graph)
    }
}
