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

//! State objects updated by GroupLookup and AggregateUpdate. Only partial
//! state export constructs Arrow arrays.
use super::{LocalState, StateId};
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

/// Moves the grouping state (first) and accumulator states out of `local`.
pub(crate) fn partial(
    local: &mut LocalState,
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
