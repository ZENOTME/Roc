//! Worker-local scalar execution. Result metadata belongs here, not in expressions.
use super::kernels;
pub use super::kernels::is_number;
pub use super::value::ExpressionValue;

use crate::{
    error::{Error, Result},
    expr::scalar::*,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt64Array, new_empty_array},
    compute::{
        CastOptions, and_kleene, can_cast_types, cast_with_options,
        kernels::interleave::interleave, not, or_kleene, take,
    },
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionResult {
    pub data_type: DataType,
    pub nullable: bool,
}

/// Each instance has its own state tree. Plans may share expression Arcs freely.
#[derive(Debug)]
pub struct ExpressionExecutor {
    input_schema: SchemaRef,
    states: Vec<ExpressionState>,
}
impl ExpressionExecutor {
    pub fn try_new(expressions: Vec<BoundScalarExprRef>, input_schema: SchemaRef) -> Result<Self> {
        let states = expressions
            .into_iter()
            .map(|e| ExpressionState::new(e, &input_schema))
            .collect::<Result<_>>()?;
        Ok(Self {
            input_schema,
            states,
        })
    }
    pub fn results(&self) -> impl ExactSizeIterator<Item = &ExpressionResult> {
        self.states.iter().map(|s| &s.result)
    }
    pub fn evaluate(&mut self, input: &RecordBatch) -> Result<Vec<ExpressionValue>> {
        self.validate_input(input)?;
        self.states.iter_mut().map(|s| s.evaluate(input)).collect()
    }
    /// Select TRUE rows. NULL is rejected only at this predicate boundary.
    pub fn select(&mut self, input: &RecordBatch) -> Result<BooleanArray> {
        self.validate_input(input)?;
        if self.states.len() != 1 || self.states[0].result.data_type != DataType::Boolean {
            return Err(Error::Execution(
                "select requires one Boolean expression".into(),
            ));
        }
        let selected = self.states[0].select_rows(input)?;
        let mut mask = vec![false; input.num_rows()];
        for index in selected {
            mask[index] = true;
        }
        Ok(BooleanArray::from(mask))
    }
    fn validate_input(&self, input: &RecordBatch) -> Result<()> {
        if input.schema_ref().as_ref() != self.input_schema.as_ref() {
            return Err(Error::Execution("expression input schema changed".into()));
        }
        // RecordBatch accepts arrays with nulls even if the field is non-nullable.
        for (field, column) in self.input_schema.fields().iter().zip(input.columns()) {
            if !field.is_nullable() && column.logical_null_count() != 0 {
                return Err(Error::Execution(format!(
                    "non-nullable input {} contains NULL",
                    field.name()
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ExpressionState {
    expression: BoundScalarExprRef,
    result: ExpressionResult,
    children: Vec<ExpressionState>,
    arguments: Vec<ExpressionValue>,
}
impl ExpressionState {
    fn new(expression: BoundScalarExprRef, schema: &Schema) -> Result<Self> {
        use BoundScalarExpression::*;
        let expressions: Vec<BoundScalarExprRef> = match expression.as_ref() {
            Reference(_) | Constant(_) => vec![],
            Function(e) => e.arguments().to_vec(),
            Cast(e) => vec![e.input().clone()],
            Conjunction(e) => e.arguments().to_vec(),
            Not(e) => vec![e.input().clone()],
            Case(e) => e
                .branches()
                .iter()
                .flat_map(|(a, b)| [a.clone(), b.clone()])
                .chain([e.else_expr().clone()])
                .collect(),
            Coalesce(e) => e.arguments().to_vec(),
        };
        let children = expressions
            .into_iter()
            .map(|e| Self::new(e, schema))
            .collect::<Result<Vec<_>>>()?;
        let result = match expression.as_ref() {
            Reference(e) => {
                let field = schema.fields().get(e.index()).ok_or_else(|| {
                    Error::InvalidPlan(format!("column index {} out of bounds", e.index()))
                })?;
                ExpressionResult {
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                }
            }
            Constant(e) => ExpressionResult {
                data_type: e.value().data_type().clone(),
                nullable: e.value().logical_null_count() != 0,
            },
            Function(e) => kernels::prepare(
                e.function(),
                &children
                    .iter()
                    .map(|s| s.result.clone())
                    .collect::<Vec<_>>(),
            )?,
            Cast(e) => {
                if !can_cast_types(&children[0].result.data_type, e.target()) {
                    return Err(Error::InvalidPlan(format!(
                        "unsupported cast to {}",
                        e.target()
                    )));
                }
                ExpressionResult {
                    data_type: e.target().clone(),
                    nullable: children[0].result.nullable || e.mode() == CastMode::Try,
                }
            }
            Conjunction(_) | Not(_) => {
                for child in &children {
                    require_boolean(&child.result)?;
                }
                ExpressionResult {
                    data_type: DataType::Boolean,
                    nullable: children.iter().any(|c| c.result.nullable),
                }
            }
            Case(e) => {
                for i in 0..e.branches().len() {
                    require_boolean(&children[i * 2].result)?;
                }
                let results = children.iter().skip(1).step_by(2).chain(children.last());
                let mut result = children.last().unwrap().result.clone();
                for child in results {
                    require_same_type(&result, &child.result)?;
                    result.nullable |= child.result.nullable;
                }
                result
            }
            Coalesce(_) => {
                let first = children.first().ok_or_else(|| {
                    Error::InvalidPlan("coalesce requires at least one argument".into())
                })?;
                for child in &children {
                    require_same_type(&first.result, &child.result)?;
                }
                ExpressionResult {
                    data_type: first.result.data_type.clone(),
                    nullable: children.iter().all(|c| c.result.nullable),
                }
            }
        };
        Ok(Self {
            expression,
            result,
            children,
            arguments: vec![],
        })
    }

    fn evaluate(&mut self, input: &RecordBatch) -> Result<ExpressionValue> {
        use BoundScalarExpression::*;
        if input.num_rows() == 0 {
            return Ok(ExpressionValue::Array(new_empty_array(
                &self.result.data_type,
            )));
        }
        let expression = self.expression.clone();
        let output = match expression.as_ref() {
            Reference(e) => ExpressionValue::Array(input.column(e.index()).clone()),
            Constant(e) => ExpressionValue::scalar(e.value().clone())?,
            Function(e) => {
                self.arguments.clear();
                for child in &mut self.children {
                    match child.evaluate(input) {
                        Ok(value) => self.arguments.push(value),
                        Err(e) => {
                            self.arguments.clear();
                            return Err(e);
                        }
                    }
                }
                let result = kernels::execute(e.function(), &self.arguments);
                self.arguments.clear();
                result?
            }
            Cast(e) => {
                let value = self.children[0].evaluate(input)?;
                let output = cast_with_options(
                    value.datum().get().0,
                    e.target(),
                    &CastOptions {
                        safe: e.mode() == CastMode::Try,
                        ..Default::default()
                    },
                )?;
                if value.is_scalar() {
                    ExpressionValue::scalar(output)?
                } else {
                    ExpressionValue::Array(output)
                }
            }
            Not(_) => {
                let value = self.children[0].evaluate(input)?;
                let array = boolean(value.datum().get().0)?;
                let output = Arc::new(not(array)?);
                if value.is_scalar() {
                    ExpressionValue::scalar(output)?
                } else {
                    ExpressionValue::Array(output)
                }
            }
            Conjunction(e) => {
                // Value evaluation preserves all three truth values. Predicate selection has a separate path.
                let identity = e.conjunction() == crate::expr::scalar::Conjunction::And;
                let mut result = BooleanArray::from(vec![identity; input.num_rows()]);
                for child in &mut self.children {
                    let array = child.evaluate(input)?.into_array(input.num_rows())?;
                    result = match e.conjunction() {
                        crate::expr::scalar::Conjunction::And => {
                            and_kleene(&result, boolean(array.as_ref())?)?
                        }
                        crate::expr::scalar::Conjunction::Or => {
                            or_kleene(&result, boolean(array.as_ref())?)?
                        }
                    };
                }
                ExpressionValue::Array(Arc::new(result))
            }
            Case(e) => self.evaluate_case(input, e.branches().len())?,
            Coalesce(_) => self.evaluate_coalesce(input)?,
        };
        let (array, scalar) = output.datum().get();
        if array.data_type() != &self.result.data_type
            || array.len() != if scalar { 1 } else { input.num_rows() }
        {
            return Err(Error::Execution(
                "invalid expression result type or length".into(),
            ));
        }
        Ok(output)
    }

    fn evaluate_case(&mut self, input: &RecordBatch, branches: usize) -> Result<ExpressionValue> {
        let mut remaining: Vec<usize> = (0..input.num_rows()).collect();
        let mut pieces = vec![];
        let mut mapping = vec![(0, 0); input.num_rows()];
        for branch in 0..branches {
            if remaining.is_empty() {
                break;
            }
            let batch = select_batch(input, &remaining)?;
            let condition = self.children[branch * 2]
                .evaluate(&batch)?
                .into_array(batch.num_rows())?;
            let condition = boolean(condition.as_ref())?;
            let (yes, no): (Vec<_>, Vec<_>) = remaining
                .into_iter()
                .enumerate()
                .partition(|(i, _)| condition.is_valid(*i) && condition.value(*i));
            let yes: Vec<_> = yes.into_iter().map(|(_, i)| i).collect();
            remaining = no.into_iter().map(|(_, i)| i).collect();
            if !yes.is_empty() {
                let value = self.children[branch * 2 + 1]
                    .evaluate(&select_batch(input, &yes)?)?
                    .into_array(yes.len())?;
                add_piece(value, &yes, &mut pieces, &mut mapping);
            }
        }
        if !remaining.is_empty() {
            let value = self
                .children
                .last_mut()
                .unwrap()
                .evaluate(&select_batch(input, &remaining)?)?
                .into_array(remaining.len())?;
            add_piece(value, &remaining, &mut pieces, &mut mapping);
        }
        merge_pieces(pieces, mapping)
    }

    fn evaluate_coalesce(&mut self, input: &RecordBatch) -> Result<ExpressionValue> {
        let mut remaining: Vec<usize> = (0..input.num_rows()).collect();
        let mut pieces = vec![];
        let mut mapping = vec![(0, 0); input.num_rows()];
        let last = self.children.len() - 1;
        for (child_index, child) in self.children.iter_mut().enumerate() {
            if remaining.is_empty() {
                break;
            }
            let value = child
                .evaluate(&select_batch(input, &remaining)?)?
                .into_array(remaining.len())?;
            let nulls = value.logical_nulls();
            let mut next = vec![];
            for (i, original) in remaining.into_iter().enumerate() {
                if child_index != last && nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    next.push(original);
                } else {
                    mapping[original] = (pieces.len(), i);
                }
            }
            pieces.push(value);
            remaining = next;
        }
        merge_pieces(pieces, mapping)
    }

    fn select_rows(&mut self, input: &RecordBatch) -> Result<Vec<usize>> {
        if let BoundScalarExpression::Conjunction(e) = self.expression.as_ref() {
            let mut remaining: Vec<usize> = (0..input.num_rows()).collect();
            let mut accepted = vec![false; input.num_rows()];
            for child in &mut self.children {
                if remaining.is_empty() {
                    break;
                }
                let selected = child.select_rows(&select_batch(input, &remaining)?)?;
                let mut mask = vec![false; remaining.len()];
                for i in selected {
                    mask[i] = true;
                }
                let mut next = vec![];
                for (i, original) in remaining.into_iter().enumerate() {
                    if e.conjunction() == Conjunction::And {
                        if mask[i] {
                            next.push(original);
                        }
                    } else if mask[i] {
                        accepted[original] = true;
                    } else {
                        next.push(original);
                    }
                }
                remaining = next;
            }
            if e.conjunction() == Conjunction::And {
                return Ok(remaining);
            }
            return Ok(accepted
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| v.then_some(i))
                .collect());
        }
        let array = self.evaluate(input)?.into_array(input.num_rows())?;
        let array = boolean(array.as_ref())?;
        Ok((0..array.len())
            .filter(|i| array.is_valid(*i) && array.value(*i))
            .collect())
    }
}

fn add_piece(
    value: ArrayRef,
    rows: &[usize],
    pieces: &mut Vec<ArrayRef>,
    mapping: &mut [(usize, usize)],
) {
    for (i, row) in rows.iter().enumerate() {
        mapping[*row] = (pieces.len(), i);
    }
    pieces.push(value);
}
fn merge_pieces(pieces: Vec<ArrayRef>, mapping: Vec<(usize, usize)>) -> Result<ExpressionValue> {
    let refs = pieces.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    Ok(ExpressionValue::Array(interleave(&refs, &mapping)?))
}
pub fn select_batch(input: &RecordBatch, rows: &[usize]) -> Result<RecordBatch> {
    if rows.len() == input.num_rows() && rows.iter().copied().eq(0..input.num_rows()) {
        return Ok(input.clone());
    }
    let indices = UInt64Array::from_iter_values(rows.iter().map(|i| *i as u64));
    let columns = input
        .columns()
        .iter()
        .map(|a| take(a.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        input.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}
pub fn boolean(array: &dyn Array) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| Error::Execution("expected Boolean expression".into()))
}
pub fn require_boolean(result: &ExpressionResult) -> Result<()> {
    if result.data_type != DataType::Boolean {
        return Err(Error::InvalidPlan("expected Boolean expression".into()));
    }
    Ok(())
}
fn require_same_type(a: &ExpressionResult, b: &ExpressionResult) -> Result<()> {
    if a.data_type != b.data_type {
        return Err(Error::InvalidPlan(
            "branch types must match; supply explicit casts".into(),
        ));
    }
    Ok(())
}
