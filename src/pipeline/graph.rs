//! Immutable pipeline DAG, its validation, and graph construction.
use super::{Pipeline, PipelineBuilder};
use crate::error::{Error, Result};
use std::collections::{HashSet, VecDeque};

pub type PipelineId = usize;

/// Owns pipeline IDs and completion dependencies. Pipeline contents are
/// changed through the corresponding `PipelineBuilder`.
pub struct PipelineGraphBuilder {
    pipelines: Vec<PipelineBuilder>,
    dependencies: Vec<Vec<PipelineId>>,
}
impl Default for PipelineGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl PipelineGraphBuilder {
    /// Starts with result pipeline 0, to be filled from its sink upward.
    pub fn new() -> Self {
        Self {
            pipelines: vec![PipelineBuilder::new()],
            dependencies: vec![vec![]],
        }
    }

    pub fn new_dependency(&mut self, dependent: PipelineId) -> Result<PipelineId> {
        if dependent >= self.pipelines.len() {
            return Err(Error::InvalidPlan(format!("unknown pipeline {dependent}")));
        }
        let id = self.pipelines.len();
        self.pipelines.push(PipelineBuilder::new());
        self.dependencies.push(vec![]);
        self.dependencies[dependent].push(id);
        Ok(id)
    }

    pub fn pipeline_mut(&mut self, id: PipelineId) -> Result<&mut PipelineBuilder> {
        self.pipelines
            .get_mut(id)
            .ok_or_else(|| Error::InvalidPlan(format!("unknown pipeline {id}")))
    }

    pub fn finish(self) -> Result<PipelineGraph> {
        // Dependencies are created after their consumer, so reversing IDs
        let count = self.pipelines.len();
        let pipelines = self
            .pipelines
            .into_iter()
            .rev()
            .map(PipelineBuilder::finish)
            .collect::<Result<Vec<_>>>()?;
        let dependencies = self
            .dependencies
            .into_iter()
            .rev()
            .map(|inputs| inputs.into_iter().map(|id| count - 1 - id).collect())
            .collect();
        PipelineGraph::new(pipelines, dependencies)
            .map_err(|error| Error::InvalidPlan(error.to_string()))
    }
}

/// An immutable DAG of executable pipelines.
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
        error::{Error, Result},
        exec::{GlobalExecContextRef, SinkExec, SinkExecutor, SourceExec, SourceExecutor},
    };
    use asyncband::shutdown::ShutdownGuard;
    use futures::future::BoxFuture;

    struct UnusedExec;

    impl SourceExec for UnusedExec {
        fn init_global_context(
            &self,
            _shutdown_guard: &ShutdownGuard,
        ) -> Result<GlobalExecContextRef> {
            Err(Error::Execution("not run by graph tests".into()))
        }
        fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SourceExecutor>> {
            Err(Error::Execution("not run by graph tests".into()))
        }

        fn finalize<'a>(
            &'a self,
            _global: GlobalExecContextRef,
            _shutdown_guard: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl SinkExec for UnusedExec {
        fn init_global_context(
            &self,
            _shutdown_guard: &ShutdownGuard,
        ) -> Result<GlobalExecContextRef> {
            Err(Error::Execution("not run by graph tests".into()))
        }

        fn new_executor(&self, _global: GlobalExecContextRef) -> Result<Box<dyn SinkExecutor>> {
            Err(Error::Execution("not run by graph tests".into()))
        }
        fn finalize<'a>(
            &'a self,
            _global: GlobalExecContextRef,
            _shutdown_guard: &'a ShutdownGuard,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn pipelines(count: usize) -> Vec<Pipeline> {
        (0..count)
            .map(|_| Pipeline {
                source: Box::new(UnusedExec),
                processors: vec![],
                sink: Box::new(UnusedExec),
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
