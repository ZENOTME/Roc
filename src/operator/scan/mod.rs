mod channel;
mod storage;

pub use channel::{ScanReceiver, ScanSendError, ScanSender, scan_channel};
pub use storage::{ScanConsumer, ScanHandle, ScanRequest, ScanStorage};

use super::Operator;
use crate::{Error, Result, expr::Expr};
use crate::{
    exec::ScanExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId},
};
use arrow::{
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct ScanOperator<StorageTaskDesc = String> {
    pub source: StorageTaskDesc,
    pub schema: SchemaRef,
    /// Evaluated against the input schema before column pruning.
    pub predicate: Option<Expr>,
    pub projection: Option<Vec<usize>>,
    pub storage: Arc<dyn ScanStorage<StorageTaskDesc>>,
}

impl<StorageTaskDesc: fmt::Debug> fmt::Debug for ScanOperator<StorageTaskDesc> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanOperator")
            .field("source", &self.source)
            .field("schema", &self.schema)
            .field("predicate", &self.predicate)
            .field("projection", &self.projection)
            .finish_non_exhaustive()
    }
}

impl ScanOperator<String> {
    pub fn new(
        source: impl Into<String>,
        schema: SchemaRef,
        storage: Arc<dyn ScanStorage<String>>,
    ) -> Self {
        Self {
            source: source.into(),
            schema,
            predicate: None,
            projection: None,
            storage,
        }
    }
}

impl<StorageTaskDesc> ScanOperator<StorageTaskDesc> {
    fn validate(&self) -> Result<()> {
        if let Some(expr) = &self.predicate
            && expr.data_type(&self.schema)? != DataType::Boolean
        {
            return Err(Error::Plan("scan predicate must be Boolean".into()));
        }
        if let Some(indices) = &self.projection {
            self.schema.project(indices)?;
        }
        Ok(())
    }

    /// Physical reads include predicate columns even when they are not returned.
    pub fn read_projection(&self) -> Result<Vec<usize>> {
        let mut indices = self
            .projection
            .clone()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        if let Some(predicate) = &self.predicate {
            let mut names = std::collections::HashSet::new();
            predicate.column_names(&mut names);
            for name in names {
                indices.push(self.schema.index_of(&name)?);
            }
        }
        indices.sort_unstable();
        indices.dedup();
        Ok(indices)
    }

    /// Rebind output column positions to the physically projected input.
    pub fn projected_input(&self, indices: &[usize]) -> Result<Self>
    where
        StorageTaskDesc: Clone,
    {
        let mut scan = self.clone();
        scan.schema = Arc::new(self.schema.project(indices)?);
        if let Some(output) = &self.projection {
            scan.projection = Some(
                output
                    .iter()
                    .map(|index| {
                        indices.binary_search(index).map_err(|_| {
                            Error::Plan("scan projection omits an output column".into())
                        })
                    })
                    .collect::<Result<_>>()?,
            );
        }
        Ok(scan)
    }

    pub fn apply(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        if let Some(expr) = &self.predicate {
            batch = crate::expr::filter(batch, expr)?;
        }
        if let Some(indices) = &self.projection {
            batch = batch.project(indices)?;
        }
        Ok(batch)
    }
}

impl<StorageTaskDesc> Operator for ScanOperator<StorageTaskDesc>
where
    StorageTaskDesc: Clone + fmt::Debug + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "scan"
    }
    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        if !node.children().is_empty() {
            return Err(Error::Plan("scan cannot have children".into()));
        }
        self.validate()?;
        let storage = self.storage.clone();
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(ScanExec::new(self.clone(), storage)))
    }
}
