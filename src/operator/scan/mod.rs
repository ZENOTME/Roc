mod channel;
mod storage;

pub use channel::{ScanReceiver, ScanSendError, ScanSender, scan_channel};
pub use storage::{ScanConsumer, ScanHandle, ScanRequest, ScanStorage};

use super::Operator;
use crate::{
    Error, Result,
    exec::ScanExec,
    operator::OperatorTreeNode,
    pipeline::{PipelineGraphBuilder, PipelineId},
};
use std::{fmt, sync::Arc};

#[derive(Clone)]
pub struct ScanOperator<StorageTaskDesc> {
    source: StorageTaskDesc,
    storage: Arc<dyn ScanStorage<StorageTaskDesc = StorageTaskDesc>>,
}

impl<StorageTaskDesc: fmt::Debug> fmt::Debug for ScanOperator<StorageTaskDesc> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanOperator")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<StorageTaskDesc> ScanOperator<StorageTaskDesc> {
    pub fn new(
        source: StorageTaskDesc,
        storage: Arc<dyn ScanStorage<StorageTaskDesc = StorageTaskDesc>>,
    ) -> Self {
        Self { source, storage }
    }

    pub(crate) fn source(&self) -> &StorageTaskDesc {
        &self.source
    }

    pub(crate) fn storage(&self) -> &Arc<dyn ScanStorage<StorageTaskDesc = StorageTaskDesc>> {
        &self.storage
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
        current_node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder,
    ) -> Result<()> {
        if !current_node.children().is_empty() {
            return Err(Error::Plan("scan cannot have children".into()));
        }
        graph
            .pipeline_mut(current)?
            .set_source(Box::new(ScanExec::new(self.clone())))
    }
}
