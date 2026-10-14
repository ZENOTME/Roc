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

//! Exclusively owned kernel state and the factories that create it.
use crate::error::{Error, Result};
use std::sync::Arc;

/// Meaningful only within its KernelBuilder and the Program compiled from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StateId(pub(crate) usize);

impl StateId {
    pub fn index(self) -> usize {
        self.0
    }
}

/// A factory for ordinary, exclusively owned state. Every execution instance
/// creates its own value; the program runtime knows neither the concrete type
/// nor its update/combine semantics.
#[derive(Clone)]
pub struct StateBuilder {
    name: &'static str,
    factory: Arc<dyn Fn() -> Result<State> + Send + Sync>,
}

impl std::fmt::Debug for StateBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateBuilder")
            .field("type", &self.name)
            .finish_non_exhaustive()
    }
}

impl StateBuilder {
    pub fn new<T: Send + 'static>(factory: impl Fn() -> Result<T> + Send + Sync + 'static) -> Self {
        Self {
            name: std::any::type_name::<T>(),
            factory: Arc::new(move || factory().map(State::new)),
        }
    }
    pub(crate) fn build(&self) -> Result<State> {
        (self.factory)()
    }
}

/// Type erasure happens at the batch boundary, never for each row. Kernels
/// borrow their concrete local state and update it without locks.
pub struct State {
    value: Box<dyn std::any::Any + Send>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("State(..)")
    }
}

impl State {
    pub fn new<T: Send + 'static>(value: T) -> Self {
        Self {
            value: Box::new(value),
        }
    }
    pub fn get<T: 'static>(&self) -> Result<&T> {
        self.value.downcast_ref().ok_or_else(mismatch)
    }
    pub fn get_mut<T: 'static>(&mut self) -> Result<&mut T> {
        self.value.downcast_mut().ok_or_else(mismatch)
    }
    pub fn into_inner<T: 'static>(self) -> Result<T> {
        self.value
            .downcast::<T>()
            .map(|v| *v)
            .map_err(|_| mismatch())
    }
}

fn mismatch() -> Error {
    Error::internal("state binding type mismatch".into())
}
