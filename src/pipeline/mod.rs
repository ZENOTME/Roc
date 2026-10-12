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

mod build;
mod execution;
mod graph;

pub use build::{build_pipeline_graph, build_pipeline_on_node};
pub use execution::yield_now;
pub use execution::{Executor, PipelineExecutionConfig, PipelineGraphExecutor};
pub use graph::{PipelineGraph, PipelineGraphBuilder, PipelineGraphError, PipelineId};

use crate::{
    error::{Error, Result},
    exec::{SinkExec, SourceExec},
};

/// A complete schedulable pipeline. Every role is represented by its execution trait directly.
pub struct Pipeline {
    pub source: Box<dyn SourceExec>,
    pub program: crate::suboperator::ProgramContext,
    pub sink: Box<dyn SinkExec>,
}

impl Pipeline {
    pub fn new(
        source: Box<dyn SourceExec>,
        builder: crate::suboperator::SuboperatorScopeBuilder,
        sink: Box<dyn SinkExec>,
        mode: crate::suboperator::ExecutionMode,
    ) -> Result<Self> {
        let program = builder.finish(vec![])?.compile(mode)?;
        Ok(Self {
            source,
            program,
            sink,
        })
    }
}

/// Operators append suboperators directly, after building their children.
pub struct PipelineBuilder {
    source: Option<Box<dyn SourceExec>>,
    pub suboperators: crate::suboperator::SuboperatorScopeBuilder,
    sink: Option<Box<dyn SinkExec>>,
}

impl PipelineBuilder {
    pub(super) fn new() -> Self {
        Self {
            source: None,
            suboperators: crate::suboperator::SuboperatorScopeBuilder::new(),
            sink: None,
        }
    }

    pub fn set_source(&mut self, source: Box<dyn SourceExec>) -> Result<()> {
        if self.source.is_some() {
            return Err(Error::InvalidPlan(
                "pipeline has more than one source".into(),
            ));
        }
        self.source = Some(source);
        Ok(())
    }

    pub fn set_sink(&mut self, sink: Box<dyn SinkExec>) -> Result<()> {
        if self.sink.is_some() {
            return Err(Error::InvalidPlan("pipeline has more than one sink".into()));
        }
        self.sink = Some(sink);
        Ok(())
    }

    pub(super) fn finish(mut self, mode: crate::suboperator::ExecutionMode) -> Result<Pipeline> {
        let sink = self
            .sink
            .take()
            .ok_or_else(|| Error::InvalidPlan("pipeline has no sink".into()))?;
        Pipeline::new(
            self.source
                .ok_or_else(|| Error::InvalidPlan("pipeline has no source".into()))?,
            self.suboperators,
            sink,
            mode,
        )
    }
}
