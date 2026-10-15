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

//! Immutable pipeline DAG, its validation, and graph construction.
use super::{Pipeline, PipelineBuilder};
use crate::column::ColumnContext;
use crate::error::{Error, Result};
use crate::kernel::ExecutionMode;
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

pub type PipelineId = usize;

pub struct PipelineGraphBuilder<'plan> {
    columns: &'plan ColumnContext,
    pipelines: Vec<PipelineBuilder<'plan>>,
    dependencies: Vec<Vec<PipelineId>>,
    mode: ExecutionMode,
    /// Pipelines that several operators read, with the state they share.
    shared: HashMap<u64, (PipelineId, Arc<dyn Any + Send + Sync>)>,
}

impl<'plan> PipelineGraphBuilder<'plan> {
    /// Starts with result pipeline 0, to be filled from its sink upward.
    pub fn new(columns: &'plan ColumnContext) -> Self {
        Self {
            columns,
            pipelines: vec![PipelineBuilder::new(columns)],
            dependencies: vec![vec![]],
            mode: ExecutionMode::default(),
            shared: HashMap::new(),
        }
    }

    /// Makes `dependent` wait for an existing pipeline.
    pub fn add_dependency(&mut self, dependent: PipelineId, dependency: PipelineId) -> Result<()> {
        if dependent >= self.pipelines.len() || dependency >= self.pipelines.len() {
            return Err(Error::invalid_plan("unknown pipeline".into()));
        }
        if !self.dependencies[dependent].contains(&dependency) {
            self.dependencies[dependent].push(dependency);
        }
        Ok(())
    }

    /// The pipeline and state registered for `key` by an operator read in
    /// several places of the tree.
    pub fn shared(&self, key: u64) -> Option<(PipelineId, Arc<dyn Any + Send + Sync>)> {
        self.shared.get(&key).cloned()
    }

    pub fn set_shared(
        &mut self,
        key: u64,
        pipeline: PipelineId,
        state: Arc<dyn Any + Send + Sync>,
    ) {
        self.shared.insert(key, (pipeline, state));
    }

    /// The mode of every pipeline program built by `finish`.
    pub fn set_execution_mode(&mut self, mode: ExecutionMode) {
        self.mode = mode;
    }

    pub fn columns(&self) -> &'plan ColumnContext {
        self.columns
    }

    pub fn new_dependency(&mut self, dependent: PipelineId) -> Result<PipelineId> {
        if dependent >= self.pipelines.len() {
            return Err(Error::invalid_plan(format!("unknown pipeline {dependent}")));
        }
        let id = self.pipelines.len();
        self.pipelines.push(PipelineBuilder::new(self.columns));
        self.dependencies.push(vec![]);
        self.dependencies[dependent].push(id);
        Ok(id)
    }

    pub fn pipeline_mut(&mut self, id: PipelineId) -> Result<&mut PipelineBuilder<'plan>> {
        self.pipelines
            .get_mut(id)
            .ok_or_else(|| Error::invalid_plan(format!("unknown pipeline {id}")))
    }

    pub fn finish(self) -> Result<PipelineGraph> {
        // Dependencies are created after their consumer, so reversing IDs
        // yields a topological order.
        let count = self.pipelines.len();
        let mode = self.mode;
        let pipelines = self
            .pipelines
            .into_iter()
            .rev()
            .map(|p| p.finish(mode))
            .collect::<Result<Vec<_>>>()?;
        let dependencies = self
            .dependencies
            .into_iter()
            .rev()
            .map(|inputs| inputs.into_iter().map(|id| count - 1 - id).collect())
            .collect();
        PipelineGraph::new(pipelines, dependencies)
            .map_err(|error| Error::invalid_plan(error.to_string()))
    }
}

pub struct PipelineGraph {
    pub(super) pipelines: Vec<Pipeline>,
    pub(super) dependencies: Vec<Vec<PipelineId>>,
    pub(super) dependents: Vec<Vec<PipelineId>>,
}

impl std::fmt::Debug for PipelineGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineGraph")
            .field("pipelines", &self.pipelines.len())
            .field("dependencies", &self.dependencies)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PipelineGraphError {
    #[error("pipeline dependency list has {dependencies} entries for {pipelines} pipelines")]
    DependencyCount {
        pipelines: usize,
        dependencies: usize,
    },
    #[error("pipeline {pipeline} depends on unknown pipeline {dependency}")]
    UnknownDependency {
        pipeline: PipelineId,
        dependency: PipelineId,
    },
    #[error("pipeline {pipeline} depends on itself")]
    SelfDependency { pipeline: PipelineId },
    #[error("pipeline {pipeline} lists dependency {dependency} more than once")]
    DuplicateDependency {
        pipeline: PipelineId,
        dependency: PipelineId,
    },
    #[error("pipeline graph contains a dependency cycle")]
    Cycle,
}

impl PipelineGraph {
    pub fn explain(&self) -> String {
        self.pipelines
            .iter()
            .enumerate()
            .map(|(i, p)| {
                format!(
                    "pipeline {i} depends_on={:?}\n{}",
                    self.dependencies[i],
                    p.program.explain()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn new(
        pipelines: Vec<Pipeline>,
        dependencies: Vec<Vec<PipelineId>>,
    ) -> std::result::Result<Self, PipelineGraphError> {
        if dependencies.len() != pipelines.len() {
            return Err(PipelineGraphError::DependencyCount {
                pipelines: pipelines.len(),
                dependencies: dependencies.len(),
            });
        }

        let mut dependents = vec![vec![]; pipelines.len()];
        for (pipeline, inputs) in dependencies.iter().enumerate() {
            let mut unique = HashSet::with_capacity(inputs.len());
            for &dependency in inputs {
                if dependency >= pipelines.len() {
                    return Err(PipelineGraphError::UnknownDependency {
                        pipeline,
                        dependency,
                    });
                }
                if dependency == pipeline {
                    return Err(PipelineGraphError::SelfDependency { pipeline });
                }
                if !unique.insert(dependency) {
                    return Err(PipelineGraphError::DuplicateDependency {
                        pipeline,
                        dependency,
                    });
                }
                dependents[dependency].push(pipeline);
            }
        }

        let graph = Self {
            pipelines,
            dependencies,
            dependents,
        };
        graph.validate_acyclic()?;
        Ok(graph)
    }

    pub fn pipelines(&self) -> &[Pipeline] {
        &self.pipelines
    }

    pub fn into_pipelines(self) -> Vec<Pipeline> {
        self.pipelines
    }

    pub fn dependencies(&self, pipeline: PipelineId) -> Option<&[PipelineId]> {
        self.dependencies.get(pipeline).map(Vec::as_slice)
    }

    pub fn dependents(&self, pipeline: PipelineId) -> Option<&[PipelineId]> {
        self.dependents.get(pipeline).map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }

    fn validate_acyclic(&self) -> std::result::Result<(), PipelineGraphError> {
        let mut remaining = self.dependencies.iter().map(Vec::len).collect::<Vec<_>>();
        let mut ready = remaining
            .iter()
            .enumerate()
            .filter_map(|(id, &count)| (count == 0).then_some(id))
            .collect::<VecDeque<_>>();
        let mut visited = 0;
        while let Some(id) = ready.pop_front() {
            visited += 1;
            for &dependent in &self.dependents[id] {
                remaining[dependent] -= 1;
                if remaining[dependent] == 0 {
                    ready.push_back(dependent);
                }
            }
        }
        if visited == self.pipelines.len() {
            Ok(())
        } else {
            Err(PipelineGraphError::Cycle)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        error::Result,
        exec::{ConsumeStatus, LocalSink, LocalSinkContext, SinkExec, SourceExec, StateContext},
    };
    use asyncband::shutdown::ShutdownGuard;
    use futures::future::BoxFuture;

    struct UnusedExec;

    impl SourceExec for UnusedExec {
        fn local_source(
            &self,
            _: &crate::exec::LocalSourceContext,
        ) -> Result<Box<dyn crate::exec::LocalSource>> {
            unreachable!("construction only")
        }
    }

    impl SinkExec for UnusedExec {
        fn local_sink(&self, _: &LocalSinkContext) -> Result<Box<dyn LocalSink>> {
            unreachable!("construction only")
        }
        fn consume<'a>(
            &'a mut self,
            _: ConsumeStatus,
            _: &'a mut dyn LocalSink,
            _: StateContext<'a>,
            _: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            unreachable!("construction only")
        }
        fn finish<'a>(
            &'a mut self,
            _: &'a ShutdownGuard,
            _cpu: &'a dyn crate::exec::Cpu,
        ) -> BoxFuture<'a, Result<()>> {
            unreachable!("construction only")
        }
    }

    fn pipelines(count: usize) -> Vec<Pipeline> {
        (0..count)
            .map(|_| {
                Pipeline::new(
                    Box::new(UnusedExec),
                    crate::kernel::Program::empty(),
                    Box::new(UnusedExec),
                )
            })
            .collect()
    }

    #[test]
    fn rejects_cycles() {
        assert_eq!(
            PipelineGraph::new(pipelines(2), vec![vec![1], vec![0]]).unwrap_err(),
            PipelineGraphError::Cycle
        );
    }

    #[test]
    fn rejects_duplicate_dependencies() {
        assert_eq!(
            PipelineGraph::new(pipelines(2), vec![vec![], vec![0, 0]]).unwrap_err(),
            PipelineGraphError::DuplicateDependency {
                pipeline: 1,
                dependency: 0,
            }
        );
    }
}
