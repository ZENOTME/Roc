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

//! Experimental data-centric query compilation (`jit` feature).
//!
//! A filter and its following projection are lowered from Roc's existing scalar
//! IR into native bitmap construction and projection kernels over an Arrow batch.
//! A compact bitmap is materialized; arithmetic intermediates stay in SSA values. Compile once and
//! reuse across batches. This is an explicit experiment, not an automatic plan
//! rewrite or an adaptive execution policy.
//!
//! Supported: non-null Int64 references/constants, checked +, -, *, comparisons,
//! Boolean constants as predicates, and Int64 projections. Unsupported expressions
//! return `Ok(None)` so callers can select the existing Arrow executors. Invalid
//! runtime input and compilation failures return errors. No Rust unwinding or
//! machine traps cross the generated-code boundary.

mod emit;

use crate::{
    error::{Error, Result},
    exec::{Batch, ProcessResult},
    expr::scalar::{FunctionKind, ScalarExprRef, ScalarExpression, ScalarValue},
    operator::Projection,
};
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use cranelift_jit::JITModule;
use std::{collections::BTreeSet, sync::Arc};

// usize::MAX denotes arithmetic overflow; all other returns are output lengths.
// Input/output pointer tables have a compile-time known length. No pointers escape.
#[doc(hidden)]
pub type PredicateKernel = unsafe extern "C" fn(*const *const i64, usize, *mut u64) -> usize;
#[doc(hidden)]
pub type ProjectKernel =
    unsafe extern "C" fn(*const *const i64, usize, *const u64, *const *mut i64) -> usize;

// JITModule deliberately does not free executable pages on drop. Keep an owner
// even during fallible compilation so both successful and failed attempts clean up.
struct CodeMemory(Option<JITModule>);
impl Drop for CodeMemory {
    fn drop(&mut self) {
        if let Some(module) = self.0.take() {
            // SAFETY: the kernel pointer is private; calls borrow its owner and
            // finish synchronously. No other generated function refers to it.
            unsafe { module.free_memory() };
        }
    }
}

/// An owned, reusable native filter + projection fragment.
///
/// Provides batch execution for integration with a custom
/// processor. Each instance owns its code; shared compilation/caching and
/// automatic pipeline discovery are intentionally deferred.
pub struct NativeBatchKernel {
    _memory: CodeMemory,
    predicate: PredicateKernel,
    project: ProjectKernel,
    references: Vec<usize>,
    output_schema: SchemaRef,
    clif: String,
    disassembly: Option<String>,
}

impl NativeBatchKernel {
    pub(crate) fn output_schema(&self)->&SchemaRef{&self.output_schema}
    /// Compile a supported fragment, or return `None` without generating code.
    /// Projection column indices refer to the original input (filter preserves
    /// columns). A preceding FilterOperator with output_projection requires
    /// remapping before using this API.
    pub fn compile(predicate: &ScalarExprRef, projection: &Projection) -> Result<Option<Self>> {
        Self::compile_inner(predicate, projection, false)
    }

    /// Compile while retaining Cranelift's post-register-allocation disassembly.
    /// This opts into formatting overhead; use `compile` for startup benchmarks.
    pub fn compile_with_disassembly(
        predicate: &ScalarExprRef,
        projection: &Projection,
    ) -> Result<Option<Self>> {
        Self::compile_inner(predicate, projection, true)
    }

    fn compile_inner(
        predicate: &ScalarExprRef,
        projection: &Projection,
        disassemble: bool,
    ) -> Result<Option<Self>> {
        let Some(predicate) = Expr::lower(predicate, &DataType::Boolean) else {
            return Ok(None);
        };
        let Some(outputs) = projection
            .expressions()
            .iter()
            .map(|e| Expr::lower(e.expression(), &DataType::Int64))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        Self::compile_values(predicate, outputs, projection.output_schema(), disassemble).map(Some)
    }

    /// Backend entry for typed value-slot regions. It never reconstructs a
    /// ScalarExpression or invokes expression evaluation.
    pub(crate) fn compile_values(
        predicate: Expr,
        outputs: Vec<Expr>,
        output_schema: SchemaRef,
        disassemble: bool,
    ) -> Result<Self> {
        let mut references = BTreeSet::new();
        predicate.references(&mut references);
        for expr in &outputs {
            expr.references(&mut references);
        }
        let references: Vec<_> = references.into_iter().collect();
        let code = emit::compile(&predicate, &outputs, &references, disassemble)?;
        Ok(Self {
            _memory: code.memory,
            predicate: code.predicate,
            project: code.project,
            references,
            output_schema,
            clif: code.clif,
            disassembly: code.disassembly,
        })
    }

    /// Construct the AOT control for the paired compilation benchmark.
    ///
    /// This experimental hook lets both backends use exactly the same batch
    /// validation, pointer ABI, allocations, materialization and error handling.
    /// It is not a stable backend extension API.
    ///
    /// # Safety
    /// Functions must remain executable for this object's lifetime. `predicate`
    /// must read only n values from each referenced column, initialize ceil(n/64)
    /// mask words (unused tail bits zero), and return their popcount or usize::MAX.
    /// `project` must visit only set bits below n, write exactly that popcount to
    /// every output column in input order, and return it or usize::MAX. Both must
    /// use the supplied column-table order and schema, never retain pointers, and
    /// neither unwind nor access other memory. Arithmetic must be checked.
    #[doc(hidden)]
    pub unsafe fn from_aot_kernels(
        references: Vec<usize>,
        output_schema: SchemaRef,
        predicate: PredicateKernel,
        project: ProjectKernel,
    ) -> Self {
        Self {
            _memory: CodeMemory(None),
            predicate,
            project,
            references,
            output_schema,
            clif: String::new(),
            disassembly: None,
        }
    }

    /// The generated Cranelift IR, useful for inspecting fusion and branches.
    pub fn clif(&self) -> &str {
        &self.clif
    }

    /// Post-register-allocation machine instructions, when explicitly requested.
    pub fn disassembly(&self) -> Option<&str> {
        self.disassembly.as_deref()
    }

    /// Prepare a predicate-only benchmark, excluding validation and allocation.
    /// The returned closure borrows both input and code, keeping them alive.
    /// Every call must provide exactly ceil(n/64) mask words. Overflow is returned
    /// as usize::MAX, following the internal kernel ABI.
    #[doc(hidden)]
    pub fn prepare_predicate_benchmark<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<impl FnMut(&mut [u64]) -> usize + 'a> {
        let pointers = self.input_pointers(input)?;
        let n = input.num_rows();
        Ok(move |masks: &mut [u64]| {
            assert_eq!(masks.len(), n.div_ceil(64));
            // SAFETY: input is checked once and borrowed for the closure's
            // lifetime; mask capacity is checked each call; self owns live code.
            unsafe { (self.predicate)(pointers.as_ptr(), n, masks.as_mut_ptr()) }
        })
    }

    fn input_pointers(&self, input: &RecordBatch) -> Result<Vec<*const i64>> {
        // Unlike ordinary Roc IR execution, the native-code boundary must check
        // even inconsistent host-supplied IR before using unchecked memory loads.
        self.references
            .iter()
            .map(|&index| {
                let array = input
                    .columns()
                    .get(index)
                    .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                    .ok_or_else(|| Error::Execution(format!("JIT column {index} must be Int64")))?;
                if array.null_count() != 0 || array.len() != input.num_rows() {
                    return Err(Error::Execution(format!(
                        "JIT column {index} must have no nulls and match the batch length"
                    )));
                }
                // Arrow's values() includes the logical slice offset.
                Ok(array.values().as_ptr())
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn execute_batch(&self, input: &RecordBatch) -> Result<RecordBatch> {
        self.execute_domain(input, None)
    }
    /// Native value regions with a true predicate can use the caller's existing
    /// selection directly. No discarded row is evaluated by projection code.
    pub(crate) fn execute_selected(&self, input: &Batch) -> Result<RecordBatch> {
        self.execute_domain(input.physical(), input.selection())
    }
    fn execute_domain(
        &self,
        input: &RecordBatch,
        selection: Option<&arrow::buffer::BooleanBuffer>,
    ) -> Result<RecordBatch> {
        let pointers = self.input_pointers(input)?;
        let mut masks = vec![0_u64; input.num_rows().div_ceil(64)];
        // SAFETY: checked input pointers and n logical values per column; the
        // mask allocation holds ceil(n/64) words. Code ownership lasts this call.
        let mut rows =
            unsafe { (self.predicate)(pointers.as_ptr(), input.num_rows(), masks.as_mut_ptr()) };
        if rows == usize::MAX {
            return Err(Error::Execution("JIT Int64 arithmetic overflow".into()));
        }
        if let Some(selection) = selection {
            // This entry is used only for regions with a constant true native
            // predicate. Intersect before any potentially failing expression.
            rows = 0;
            let mut chunks = selection.bit_chunks().iter();
            for (i, mask) in masks.iter_mut().enumerate() {
                let start = i * 64;
                let width = (input.num_rows() - start).min(64);
                let selected = chunks.next().unwrap_or_else(|| {
                    (0..width).fold(0u64, |bits, j| {
                        bits | ((selection.value(start + j) as u64) << j)
                    })
                });
                *mask &= selected;
                rows += mask.count_ones() as usize;
            }
        }
        // The all-rejected case allocates no value buffer and skips projection.
        let mut outputs = vec![vec![0_i64; rows]; self.output_schema.fields().len()];
        if rows != 0 {
            let output_pointers: Vec<_> = outputs.iter_mut().map(|v| v.as_mut_ptr()).collect();
            // SAFETY: predicate initialized masks with zero tail bits and their
            // exact popcount; each distinct output has that many elements. The
            // generated projector writes once per selected row, in input order.
            let written = unsafe {
                (self.project)(
                    pointers.as_ptr(),
                    input.num_rows(),
                    masks.as_ptr(),
                    output_pointers.as_ptr(),
                )
            };
            if written == usize::MAX {
                return Err(Error::Execution("JIT Int64 arithmetic overflow".into()));
            }
            debug_assert_eq!(written, rows);
        }
        let columns: Vec<ArrayRef> = outputs
            .into_iter()
            .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
            .collect();
        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)
    }
}

impl NativeBatchKernel {

    pub fn execute(&mut self, input: &Batch) -> Result<ProcessResult> {
        // An upstream selection must suppress evaluation of potentially failing
        // predicate arithmetic on inactive rows. The current JIT ABI is dense.
        Ok(ProcessResult::NeedMoreInput(
            self.execute_batch(input.materialize()?.as_ref())?.into(),
        ))
    }
    pub fn finish(&mut self) -> Result<Option<Batch>> {
        Ok(None)
    }
}

// A tiny, validated subset of the existing expression tree, not a new public IR.
// Validation keeps the emitter total and prevents malformed type metadata from
// producing an invalid native ABI or an incorrectly sized load/store.
#[derive(Clone, Debug)]
pub(crate) enum Expr {
    Column(usize),
    Int(i64),
    Bool(bool),
    Binary(FunctionKind, Box<Expr>, Box<Expr>),
}
impl Expr {
    fn lower(expr: &ScalarExpression, expected: &DataType) -> Option<Self> {
        if expr.result_type().is_nullable() || expr.result_type().data_type() != expected {
            return None;
        }
        match expr {
            ScalarExpression::Reference(r) if expected == &DataType::Int64 => {
                Some(Self::Column(r.index()))
            }
            ScalarExpression::Constant(c) => match c.value() {
                ScalarValue::Int64(Some(v)) if expected == &DataType::Int64 => Some(Self::Int(*v)),
                ScalarValue::Boolean(Some(v)) if expected == &DataType::Boolean => {
                    Some(Self::Bool(*v))
                }
                _ => None,
            },
            ScalarExpression::Function(f) => {
                use FunctionKind::*;
                let result = match f.function() {
                    Add | Subtract | Multiply => DataType::Int64,
                    Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan
                    | GreaterThanOrEqual => DataType::Boolean,
                    _ => return None,
                };
                if &result != expected {
                    return None;
                }
                let [left, right] = f.arguments() else {
                    return None;
                };
                Some(Self::Binary(
                    f.function(),
                    Box::new(Self::lower(left, &DataType::Int64)?),
                    Box::new(Self::lower(right, &DataType::Int64)?),
                ))
            }
            _ => None,
        }
    }

    fn references(&self, refs: &mut BTreeSet<usize>) {
        match self {
            Self::Column(i) => {
                refs.insert(*i);
            }
            Self::Binary(_, left, right) => {
                left.references(refs);
                right.references(refs);
            }
            _ => {}
        }
    }
}
