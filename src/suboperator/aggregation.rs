//! Ordinary state objects passed to scalar helpers by generated code.
//! Only partial-state export constructs Arrow arrays.
use super::StateId;
use crate::arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use crate::{
    error::{Error, Result},
    exec::aggregate::group_index::GroupIndex,
    expr::agg::accumulator::Accumulator,
};

#[repr(C)]
pub(crate) struct GroupState {
    pub index: GroupIndex,
    pub schema: SchemaRef,
}
#[repr(C)]
pub(crate) struct AccumulatorState {
    pub accumulator: Accumulator,
}

pub(crate) fn partial(
    local: &mut super::ProgramExecutionContext,
    states: &[StateId],
) -> Result<(RecordBatch, Vec<Vec<ArrayRef>>)> {
    let grouping = local
        .take_state(states[0])
        .ok_or_else(|| Error::Execution("missing group state".into()))?
        .into_inner::<GroupState>()?;
    let groups = RecordBatch::try_new_with_options(
        grouping.schema,
        grouping.index.columns()?,
        &RecordBatchOptions::new().with_row_count(Some(grouping.index.len())),
    )?;
    let values = states[1..]
        .iter()
        .map(|&id| {
            let mut state = local
                .take_state(id)
                .ok_or_else(|| Error::Execution("missing accumulator state".into()))?
                .into_inner::<AccumulatorState>()?;
            state.accumulator.resize(groups.num_rows());
            state.accumulator.state()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((groups, values))
}
