//! Predicate row selection, separate from scalar value evaluation.
use crate::error::{Error, Result};
use arrow::array::{Array, ArrayRef, AsArray};

/// Convert a Boolean result to logical row positions. Only valid TRUE passes;
/// NULL and FALSE do not. Positions correspond to the input batch rows.
pub fn select_true(result: ArrayRef) -> Result<Vec<usize>> {
    let value = result
        .as_boolean_opt()
        .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?;
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
