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

//! Predicate row selection, separate from scalar value evaluation.
use crate::error::{Error, Result};
use arrow::array::{Array, ArrayRef, AsArray};

/// Convert a Boolean result to logical row positions. Only valid TRUE passes;
/// NULL and FALSE do not. Positions correspond to the input batch rows.
pub fn select_true(result: ArrayRef) -> Result<Vec<usize>> {
    let value = result
        .as_boolean_opt()
        .ok_or_else(|| Error::invalid_input("expected Boolean expression".into()))?;
    Ok((0..value.len())
        .filter(|&i| value.is_valid(i) && value.value(i))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray};
    use std::sync::Arc;

    #[test]
    fn selection_uses_batch_positions_and_excludes_nulls() {
        let col: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(true),
        ]));
        assert_eq!(select_true(col).unwrap(), vec![1, 3]);
    }
}
