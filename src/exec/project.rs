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

use super::{GlobalExecContextRef, ProcessExec};
use crate::{error::Result, operator::Projection};
use asyncband::shutdown::ShutdownGuard;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ProjectExec {
    projection: Projection,
}
impl ProjectExec {
    pub fn new(projection: Projection) -> Self {
        Self { projection }
    }
}
impl ProcessExec for ProjectExec {
    fn init_global_context(&self, _shutdown_guard: &ShutdownGuard) -> Result<GlobalExecContextRef> {
        Ok(Arc::new(()))
    }
    fn emit_program(
        &self,
        _global: GlobalExecContextRef,
        builder: &mut crate::program::ProgramBuilder,
        input: crate::program::ValueId,
    ) -> Result<crate::program::ValueId> {
        builder.emit_project(input, &self.projection)
    }
}
