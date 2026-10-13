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

pub use build::{build_pipeline_graph, build_pipeline_graph_with_mode, build_pipeline_on_node};
pub use execution::yield_now;
pub use execution::{Executor, PipelineExecutionConfig, PipelineGraphExecutor};
pub use graph::{PipelineGraph, PipelineGraphBuilder, PipelineGraphError, PipelineId};

use crate::{
    column::{ColumnContext, ColumnId},
    error::{Error, Result},
    exec::{SinkExec, SourceExec},
    kernel::{ExecutionMode, KernelBuilder, Program},
};

/// A source, one fused program over its chunks, and a sink.
pub struct Pipeline {
    pub source: Box<dyn SourceExec>,
    pub program: Program,
    pub sink: Box<dyn SinkExec>,
}

impl Pipeline {
    pub fn new(source: Box<dyn SourceExec>, program: Program, sink: Box<dyn SinkExec>) -> Self {
        Self {
            source,
            program,
            sink,
        }
    }
}

/// A sink and the columns of the chunks it consumes; without columns it only
/// combines states.
type Sink = (Box<dyn SinkExec>, Option<Vec<ColumnId>>);

/// Collects one pipeline while operators are built from the sink upward:
/// the leaf binds the source columns, then each operator adds kernels in
/// tree order, and the sink names the output columns, if any.
pub struct PipelineBuilder<'plan> {
    source: Option<Box<dyn SourceExec>>,
    pub kernels: KernelBuilder<'plan>,
    sink: Option<Sink>,
}

impl<'plan> PipelineBuilder<'plan> {
    pub(super) fn new(columns: &'plan ColumnContext) -> Self {
        Self {
            source: None,
            kernels: KernelBuilder::new(columns),
            sink: None,
        }
    }

    /// Sets the source; its chunks bind `columns` in order.
    pub fn set_source(&mut self, source: Box<dyn SourceExec>, columns: &[ColumnId]) -> Result<()> {
        if self.source.is_some() {
            return Err(Error::InvalidPlan(
                "pipeline has more than one source".into(),
            ));
        }
        self.kernels.bind_inputs(columns)?;
        self.source = Some(source);
        Ok(())
    }

    /// Sets the sink. With `outputs`, the program produces a chunk of these
    /// columns from the current domain for `SinkExec::consume`; without, the
    /// sink only combines states.
    pub fn set_sink(
        &mut self,
        sink: Box<dyn SinkExec>,
        outputs: Option<Vec<ColumnId>>,
    ) -> Result<()> {
        if self.sink.is_some() {
            return Err(Error::InvalidPlan("pipeline has more than one sink".into()));
        }
        self.sink = Some((sink, outputs));
        Ok(())
    }

    pub(super) fn finish(self, mode: ExecutionMode) -> Result<Pipeline> {
        let (sink, outputs) = self
            .sink
            .ok_or_else(|| Error::InvalidPlan("pipeline has no sink".into()))?;
        let source = self
            .source
            .ok_or_else(|| Error::InvalidPlan("pipeline has no source".into()))?;
        let program = Program::new(self.kernels.finish(outputs.as_deref())?, mode)?;
        Ok(Pipeline::new(source, program, sink))
    }
}
