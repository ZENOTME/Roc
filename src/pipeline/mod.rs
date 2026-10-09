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
    exec::{ProcessExec, SinkExec, SourceExec},
};

/// A complete schedulable pipeline. Every role is represented by its execution trait directly.
pub struct Pipeline {
    pub source: Box<dyn SourceExec>,
    pub processors: Vec<Box<dyn ProcessExec>>,
    pub sink: Box<dyn SinkExec>,
}

/// One pipeline under construction. Operators are visited from sink to source.
pub struct PipelineBuilder {
    source: Option<Box<dyn SourceExec>>,
    processors: Vec<Box<dyn ProcessExec>>,
    sink: Option<Box<dyn SinkExec>>,
}

impl PipelineBuilder {
    pub(super) fn new() -> Self {
        Self {
            source: None,
            processors: vec![],
            sink: None,
        }
    }

    pub fn set_source(&mut self, source: Box<dyn SourceExec>) -> Result<()> {
        if self.source.is_some() {
            return Err(Error::invalid_plan(
                "pipeline has more than one source".into(),
            ));
        }
        self.source = Some(source);
        Ok(())
    }

    pub fn add_processor(&mut self, processor: Box<dyn ProcessExec>) {
        self.processors.push(processor);
    }

    pub fn set_sink(&mut self, sink: Box<dyn SinkExec>) -> Result<()> {
        if self.sink.is_some() {
            return Err(Error::invalid_plan(
                "pipeline has more than one sink".into(),
            ));
        }
        self.sink = Some(sink);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Pipeline> {
        self.processors.reverse();
        Ok(Pipeline {
            source: self
                .source
                .ok_or_else(|| Error::invalid_plan("pipeline has no source".into()))?,
            processors: self.processors,
            sink: self
                .sink
                .ok_or_else(|| Error::invalid_plan("pipeline has no sink".into()))?,
        })
    }
}
