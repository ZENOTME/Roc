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

use super::Operator;
use crate::arrow::record_batch::RecordBatch;
use crate::column::ColumnId;
use crate::exec::{
    ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, StateContext, downcast_local,
};
use crate::{
    error::{Error, Result},
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId, build_pipeline_on_node},
};
use asyncband::shutdown::ShutdownGuard;
use futures::future::BoxFuture;
use std::sync::{Arc, Mutex};

/// The result root: collects the chunks of its child's columns in the order
/// the pipeline produces them.
#[derive(Clone, Debug, Default)]
pub struct CollectOperator {
    output: Arc<Mutex<Vec<RecordBatch>>>,
}

impl CollectOperator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the chunks collected so far.
    pub fn take(&self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.output.lock().unwrap())
    }
}

struct CollectSink {
    output: Arc<Mutex<Vec<RecordBatch>>>,
}

impl LocalSink for CollectSink {
    fn consume(
        &mut self,
        chunk: Option<RecordBatch>,
        _: &mut StateContext<'_>,
    ) -> Result<ConsumeStatus> {
        if let Some(chunk) = chunk {
            self.output.lock().unwrap().push(chunk);
        }
        Ok(ConsumeStatus::End)
    }
    fn finish(
        &mut self,
        _: ConsumeStatus,
        _: &mut StateContext<'_>,
        _: &ShutdownGuard,
    ) -> Result<()> {
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl SinkExec for CollectSink {
    fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
        Ok(Box::new(Self {
            output: self.output.clone(),
        }))
    }
    fn consume<'a>(
        &'a mut self,
        _: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        _: StateContext<'a>,
        _: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            downcast_local::<Self>(local)?;
            Ok(())
        })
    }
    fn finish<'a>(
        &'a mut self,
        _: &'a ShutdownGuard,
        _cpu: &'a dyn crate::exec::Cpu,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl Operator for CollectOperator {
    fn name(&self) -> &'static str {
        "collect"
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
            return Err(Error::invalid_plan(format!(
                "collect operator requires exactly 1 child, got {}",
                current_node.children().len(),
            )));
        };
        build_pipeline_on_node(child, current, graph)?;
        let sink = CollectSink {
            output: self.output.clone(),
        };
        graph
            .pipeline_mut(current)?
            .set_sink(Box::new(sink), Some(child.operator().output_columns()))
    }
}
