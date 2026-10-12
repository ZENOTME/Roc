//! Group lookup and accumulator updates are separate vector kernels. Their
//! ordinary state types are shared by their builders and kernels; the execution
//! frame does not know their concrete types.
use super::{State, StateId};
use crate::{
    error::{Error, Result},
    exec::aggregate::group_index::GroupIndex,
    expr::agg::accumulator::Accumulator,
};
use arrow::{
    array::{ArrayRef, AsArray, UInt64Array},
    datatypes::{SchemaRef, UInt64Type},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use std::sync::Arc;

pub(crate) struct GroupState {
    pub index: GroupIndex,
    pub schema: SchemaRef,
    pub(crate) ids: Vec<usize>,
}
pub(crate) struct AccumulatorState {
    pub accumulator: Accumulator,
    pub(crate) global: bool,
    pub(crate) ids: Vec<usize>,
}

fn state(slots: &mut [Option<State>], id: StateId) -> Result<&mut State> {
    slots[id.index()]
        .as_mut()
        .ok_or_else(|| Error::Execution("missing kernel state".into()))
}
pub(crate) fn lookup(
    slots: &mut [Option<State>],
    states: &[StateId],
    input: &[ArrayRef],
    rows: usize,
) -> Result<Option<ArrayRef>> {
    let group = state(slots, states[0])?.get_mut::<GroupState>()?;
    group.index.intern(input, rows, &mut group.ids)?;
    Ok(Some(Arc::new(UInt64Array::from_iter_values(
        group.ids.iter().map(|&id| id as u64),
    ))))
}
pub(crate) fn update(
    slots: &mut [Option<State>],
    states: &[StateId],
    input: &[ArrayRef],
    rows: usize,
) -> Result<Option<ArrayRef>> {
    let groups = state(slots, states[0])?.get::<GroupState>()?.index.len();
    let local = state(slots, states[1])?.get_mut::<AccumulatorState>()?;
    local.accumulator.resize(groups);
    if rows == 0 {
        return Ok(None);
    }
    if local.global {
        local.accumulator.update_single(&input[1..], rows)?;
    } else {
        local.ids.clear();
        local.ids.extend(
            input[0]
                .as_primitive::<UInt64Type>()
                .values()
                .iter()
                .map(|&id| id as usize),
        );
        local.accumulator.update(&input[1..], &local.ids)?;
    }
    Ok(None)
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
            local
                .take_state(id)
                .ok_or_else(|| Error::Execution("missing accumulator state".into()))?
                .into_inner::<AccumulatorState>()?
                .accumulator
                .state()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((groups, values))
}
