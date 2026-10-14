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

//! Per-pipeline registration and validation of variables, domains and kernels.
use super::*;
use crate::arrow::datatypes::{DataType, SchemaRef};
use crate::column::{ColumnContext, ColumnId, ColumnType};
use crate::error::{Error, Result};

/// A fallible condition that has not been raised yet. Expressions propagate
/// these like validity; the operator that consumes a root expression raises
/// them with one RaiseIf each.
#[derive(Clone, Debug)]
pub struct PendingError {
    /// True on rows that must fail. Already masked by validity and by the
    /// branch conditions of enclosing CASE / COALESCE expressions.
    pub flag: VariableId,
    pub error: KernelError,
    pub args: Vec<VariableId>,
}

/// Empty when the expression cannot fail.
pub type PendingErrors = Vec<PendingError>;

#[derive(Clone, Debug)]
struct BoundColumn {
    binding: ColumnVariableBinding,
    errors: PendingErrors,
}

/// One column of the pipeline input chunk.
#[derive(Clone, Debug)]
pub struct GraphInput {
    /// Position in the input chunk.
    pub position: usize,
    pub ty: ColumnType,
    pub binding: ColumnVariableBinding,
}

/// The output boundary: one chunk of these columns per input chunk, with one
/// row per row of `domain`.
#[derive(Clone, Debug)]
pub struct GraphOutput {
    pub domain: DomainId,
    pub schema: SchemaRef,
    pub bindings: Vec<ColumnVariableBinding>,
}

/// The validated result of one pipeline's kernel construction.
#[derive(Debug)]
pub struct KernelGraph {
    pub variables: Vec<Variable>,
    pub domains: Vec<RowDomain>,
    pub kernels: Vec<(DomainId, Kernel)>,
    pub states: Vec<StateBuilder>,
    pub inputs: Vec<GraphInput>,
    pub output: Option<GraphOutput>,
}

/// Built separately for every pipeline. It only registers, allocates and
/// validates; which kernels are added is decided by expressions and operators.
/// A column may be bound to different variables in different pipelines.
pub struct KernelBuilder<'plan> {
    columns: &'plan ColumnContext,
    /// Indexed by ColumnId; None while the column is not bound here.
    bindings: Vec<Option<BoundColumn>>,
    /// Indexed by VariableId, a space independent of ColumnId.
    variables: Vec<Variable>,
    defined: Vec<bool>,
    /// Indexed by DomainId; entry 0 is the root.
    domains: Vec<RowDomain>,
    domain_defined: Vec<bool>,
    /// Per domain: the input domain when an Expand produced it.
    expanded_from: Vec<Option<DomainId>>,
    current: DomainId,
    /// Each kernel with the domain it runs in. RaiseIf and AggregateUpdate may
    /// read only ancestor variables, so the domain cannot be inferred.
    kernels: Vec<(DomainId, Kernel)>,
    states: Vec<StateBuilder>,
    inputs: Vec<GraphInput>,
    constants: std::collections::HashMap<(DomainId, bool), VariableId>,
    /// Conditions already raised, with the domain of the RaiseIf.
    raised: std::collections::HashSet<(VariableId, DomainId)>,
}

impl<'plan> KernelBuilder<'plan> {
    /// Starts with the root domain as the current domain.
    pub fn new(columns: &'plan ColumnContext) -> Self {
        Self {
            columns,
            bindings: vec![None; columns.len()],
            variables: vec![],
            defined: vec![],
            domains: vec![RowDomain {
                parent: None,
                parent_row: None,
            }],
            domain_defined: vec![true],
            expanded_from: vec![None],
            current: DomainId::ROOT,
            kernels: vec![],
            states: vec![],
            inputs: vec![],
            constants: Default::default(),
            raised: Default::default(),
        }
    }

    pub fn columns(&self) -> &'plan ColumnContext {
        self.columns
    }

    pub fn current_domain(&self) -> DomainId {
        self.current
    }

    /// Only the root or a domain already produced by a DomainKernel.
    pub fn set_current_domain(&mut self, domain: DomainId) -> Result<()> {
        if !self.domain_defined.get(domain.0).copied().unwrap_or(false) {
            return Err(Error::invalid_plan(format!(
                "domain {} has no producer",
                domain.0
            )));
        }
        self.current = domain;
        Ok(())
    }

    /// Declares a child domain of `parent` and its parent_row variable. A
    /// DomainKernel must produce it before it can be used.
    pub fn allocate_domain(&mut self, parent: DomainId) -> Result<DomainId> {
        self.domain(parent)?;
        let id = DomainId(self.domains.len());
        self.domains.push(RowDomain {
            parent: Some(parent),
            parent_row: None,
        });
        self.domain_defined.push(false);
        self.expanded_from.push(None);
        let parent_row = self.allocate_variable(VariableType::Usize, id)?;
        self.domains[id.0].parent_row = Some(parent_row);
        Ok(id)
    }

    /// The domain is explicit: DomainKernel outputs belong to a new domain that
    /// is not current. Allocation does not define a producer.
    pub fn allocate_variable(&mut self, ty: VariableType, domain: DomainId) -> Result<VariableId> {
        self.domain(domain)?;
        self.variables.push(Variable { ty, domain });
        self.defined.push(false);
        Ok(VariableId(self.variables.len() - 1))
    }

    pub fn variable(&self, id: VariableId) -> Result<&Variable> {
        self.variables
            .get(id.0)
            .ok_or_else(|| Error::invalid_plan(format!("undefined variable {}", id.0)))
    }

    pub fn variable_type(&self, id: VariableId) -> Result<VariableType> {
        Ok(self.variable(id)?.ty)
    }

    pub fn state(&mut self, factory: StateBuilder) -> StateId {
        self.states.push(factory);
        StateId(self.states.len() - 1)
    }

    fn domain(&self, id: DomainId) -> Result<&RowDomain> {
        self.domains
            .get(id.0)
            .ok_or_else(|| Error::invalid_plan(format!("undefined domain {}", id.0)))
    }

    /// Whether `ancestor` is `domain` or one of its ancestors.
    pub fn is_ancestor(&self, ancestor: DomainId, mut domain: DomainId) -> bool {
        loop {
            if domain == ancestor {
                return true;
            }
            match self.domains[domain.0].parent {
                Some(parent) => domain = parent,
                None => return false,
            }
        }
    }

    fn check_visible(&self, id: VariableId) -> Result<VariableType> {
        let v = self.variable(id)?;
        if !self.defined[id.0] {
            return Err(Error::invalid_plan(format!(
                "variable {} is used before its producer",
                id.0
            )));
        }
        if !self.is_ancestor(v.domain, self.current) {
            return Err(Error::invalid_plan(format!(
                "variable {} is not visible in domain {}",
                id.0, self.current.0
            )));
        }
        Ok(v.ty)
    }

    fn define(&mut self, id: VariableId, ty: VariableType) -> Result<()> {
        let v = *self.variable(id)?;
        if v.domain != self.current {
            return Err(Error::invalid_plan(format!(
                "output variable {} does not belong to the current domain",
                id.0
            )));
        }
        if v.ty != ty {
            return Err(Error::invalid_plan(format!(
                "output variable {} is {:?}, expected {ty:?}",
                id.0, v.ty
            )));
        }
        if std::mem::replace(&mut self.defined[id.0], true) {
            return Err(Error::invalid_plan(format!(
                "variable {} has more than one producer",
                id.0
            )));
        }
        Ok(())
    }

    fn check_binding(&self, binding: &ColumnVariableBinding) -> Result<()> {
        for v in binding.variables() {
            self.check_visible(v)?;
        }
        if let Some(validity) = binding.validity()
            && self.variable_type(validity)? != VariableType::Boolean
        {
            return Err(Error::invalid_plan("validity must be Boolean".into()));
        }
        let expected = match binding {
            ColumnVariableBinding::Null { .. } => vec![],
            ColumnVariableBinding::Bytes { .. } => {
                vec![VariableType::BytePointer, VariableType::Usize]
            }
            _ => vec![binding_value_type(binding)],
        };
        let actual = binding
            .values()
            .into_iter()
            .map(|v| self.variable_type(v))
            .collect::<Result<Vec<_>>>()?;
        if actual != expected {
            return Err(Error::invalid_plan(format!(
                "binding variables {actual:?} do not match {binding:?}"
            )));
        }
        Ok(())
    }

    /// The kernel belongs to the current domain. Inputs must be visible there;
    /// outputs must belong to it and have no producer yet.
    pub fn add_scalar_kernel(&mut self, kernel: ScalarKernel) -> Result<()> {
        let inputs = kernel
            .inputs()
            .into_iter()
            .map(|v| self.check_visible(v))
            .collect::<Result<Vec<_>>>()?;
        for state in kernel.states() {
            if state.0 >= self.states.len() {
                return Err(Error::invalid_plan(format!("undefined state {}", state.0)));
            }
        }
        let outputs: Vec<VariableType> = match &kernel {
            ScalarKernel::Constant { value, .. } => vec![value.ty()],
            ScalarKernel::Call {
                function, outputs, ..
            } => {
                let types = function.output_types(&inputs)?;
                if types.len() != outputs.len() {
                    return Err(Error::invalid_plan(format!(
                        "{function:?} produces {} outputs, got {}",
                        types.len(),
                        outputs.len()
                    )));
                }
                types
            }
            ScalarKernel::Select { .. } => {
                if inputs[0] != VariableType::Boolean || inputs[1] != inputs[2] {
                    return Err(Error::invalid_plan(format!(
                        "invalid Select operands {inputs:?}"
                    )));
                }
                vec![inputs[1]]
            }
            ScalarKernel::RaiseIf { .. } => {
                if inputs[0] != VariableType::Boolean {
                    return Err(Error::invalid_plan("RaiseIf requires a Boolean".into()));
                }
                vec![]
            }
            ScalarKernel::GroupLookup { keys, .. } => {
                for key in keys {
                    self.check_binding(key)?;
                }
                vec![VariableType::UInt64]
            }
            ScalarKernel::AggregateUpdate {
                group, arguments, ..
            } => {
                if self.variable_type(*group)? != VariableType::UInt64 {
                    return Err(Error::invalid_plan("group ID must be UInt64".into()));
                }
                for argument in arguments {
                    self.check_binding(argument)?;
                }
                vec![]
            }
            ScalarKernel::TableGather { row, output, .. } => {
                if self.variable_type(*row)? != VariableType::Usize {
                    return Err(Error::invalid_plan("table rows must be Usize".into()));
                }
                if output.validity().is_none() || output.is_null_type() {
                    return Err(Error::invalid_plan(
                        "a gathered value has a validity".into(),
                    ));
                }
                output
                    .variables()
                    .iter()
                    .map(|v| self.variable_type(*v))
                    .collect::<Result<_>>()?
            }
        };
        for (id, ty) in kernel.outputs().into_iter().zip(outputs) {
            self.define(id, ty)?;
        }
        self.kernels.push((self.current, Kernel::Scalar(kernel)));
        Ok(())
    }

    /// Checks that the produced domain is declared, has no producer and is a
    /// child of the current domain; for Reduce, of its parent. Does not switch
    /// the current domain.
    pub fn add_domain_kernel(&mut self, kernel: DomainKernel) -> Result<()> {
        let output = kernel.output();
        let domain = *self.domain(output)?;
        let parent = match &kernel {
            DomainKernel::Filter { predicate, .. } => {
                if self.check_visible(*predicate)? != VariableType::Boolean {
                    return Err(Error::invalid_plan(
                        "filter predicate must be Boolean".into(),
                    ));
                }
                self.current
            }
            DomainKernel::Expand {
                input,
                kind,
                outputs,
                ..
            } => {
                if *input != self.current {
                    return Err(Error::invalid_plan(
                        "an expansion reads the current domain".into(),
                    ));
                }
                if let ExpandKind::Lookup { keys, .. } = kind {
                    if keys.is_empty() {
                        return Err(Error::invalid_plan("a lookup needs keys".into()));
                    }
                    for key in keys {
                        self.check_binding(key)?;
                    }
                }
                if kind.state().0 >= self.states.len() {
                    return Err(Error::invalid_plan(format!(
                        "undefined state {}",
                        kind.state().0
                    )));
                }
                self.define_in(outputs, &[VariableType::Usize], output)?;
                self.expanded_from[output.0] = Some(*input);
                *input
            }
            DomainKernel::Reduce {
                parent,
                descendant,
                aggregates,
                outputs,
                ..
            } => {
                if *parent != self.current {
                    return Err(Error::invalid_plan(
                        "a reduction produces a child of the current domain".into(),
                    ));
                }
                // The descendant is in the rows of an expansion of `parent`.
                let mut d = *descendant;
                let expanded = loop {
                    if let Some(input) = self.expanded_from.get(d.0).copied().flatten() {
                        break Some(input);
                    }
                    match self.domain(d)?.parent {
                        Some(p) if p != *parent => d = p,
                        _ => break None,
                    }
                };
                if expanded != Some(*parent) || !self.domain_defined[descendant.0] {
                    return Err(Error::invalid_plan(
                        "a reduction reads the rows of an expansion of its parent".into(),
                    ));
                }
                let saved = std::mem::replace(&mut self.current, *descendant);
                let types = aggregates
                    .iter()
                    .map(|(kind, v)| {
                        let ty = self.check_visible(*v)?;
                        match (kind, ty) {
                            (ReduceKind::Any, VariableType::Boolean) => Ok(VariableType::Boolean),
                            _ => Err(Error::invalid_plan(format!(
                                "invalid reduction {kind:?} of {ty:?}"
                            ))),
                        }
                    })
                    .collect::<Result<Vec<_>>>();
                self.current = saved;
                self.define_in(outputs, &types?, output)?;
                *parent
            }
            DomainKernel::Union { .. } => {
                return Err(Error::unsupported("union domains are not supported".into()));
            }
        };
        if domain.parent != Some(parent) || self.domain_defined[output.0] {
            return Err(Error::invalid_plan(
                "a domain kernel must produce a new child of its input domain".into(),
            ));
        }
        self.domain_defined[output.0] = true;
        if let Some(parent_row) = domain.parent_row {
            self.defined[parent_row.0] = true;
        }
        self.kernels.push((self.current, Kernel::Domain(kernel)));
        Ok(())
    }

    /// Defines the outputs of a domain kernel, variables of its new domain.
    fn define_in(
        &mut self,
        outputs: &[VariableId],
        types: &[VariableType],
        domain: DomainId,
    ) -> Result<()> {
        if outputs.len() != types.len() {
            return Err(Error::invalid_plan(format!(
                "a domain kernel produces {} outputs, got {}",
                types.len(),
                outputs.len()
            )));
        }
        let saved = std::mem::replace(&mut self.current, domain);
        let result = outputs
            .iter()
            .zip(types)
            .try_for_each(|(v, ty)| self.define(*v, *ty));
        self.current = saved;
        result
    }

    /// Adds an Expand of the current domain; returns the new domain and its
    /// table row, without switching to it.
    pub fn expand(&mut self, kind: ExpandKind) -> Result<(DomainId, VariableId)> {
        let output = self.allocate_domain(self.current)?;
        let row = self.allocate_variable(VariableType::Usize, output)?;
        self.add_domain_kernel(DomainKernel::Expand {
            input: self.current,
            kind,
            outputs: vec![row],
            output,
        })?;
        Ok((output, row))
    }

    /// Adds a Reduce of `descendant` into a new child of the current domain;
    /// returns it and whether each Boolean is true on some descendant row.
    pub fn reduce_any(
        &mut self,
        descendant: DomainId,
        values: Vec<VariableId>,
    ) -> Result<(DomainId, Vec<VariableId>)> {
        let output = self.allocate_domain(self.current)?;
        let outputs = values
            .iter()
            .map(|_| self.allocate_variable(VariableType::Boolean, output))
            .collect::<Result<Vec<_>>>()?;
        self.add_domain_kernel(DomainKernel::Reduce {
            parent: self.current,
            descendant,
            aggregates: values.into_iter().map(|v| (ReduceKind::Any, v)).collect(),
            outputs: outputs.clone(),
            output,
        })?;
        Ok((output, outputs))
    }

    /// The value of a table column at `row`, as a binding of `data_type`.
    pub fn table_gather(
        &mut self,
        state: StateId,
        row: VariableId,
        column: usize,
        data_type: &DataType,
    ) -> Result<ColumnVariableBinding> {
        let validity = Some(self.allocate_variable(VariableType::Boolean, self.current)?);
        let output = if is_bytes(data_type) {
            ColumnVariableBinding::Bytes {
                ptr: self.allocate_variable(VariableType::BytePointer, self.current)?,
                len: self.allocate_variable(VariableType::Usize, self.current)?,
                validity,
            }
        } else {
            let ty = VariableType::of(data_type).ok_or_else(|| {
                Error::unsupported(format!("unsupported column type: {data_type}"))
            })?;
            let value = self.allocate_variable(ty, self.current)?;
            ColumnVariableBinding::fixed(ty, value, validity)?
        };
        self.add_scalar_kernel(ScalarKernel::TableGather {
            state,
            row,
            column,
            output,
        })?;
        Ok(output)
    }

    pub fn constant(&mut self, value: ConstantValue) -> Result<VariableId> {
        let output = self.allocate_variable(value.ty(), self.current)?;
        self.add_scalar_kernel(ScalarKernel::Constant { value, output })?;
        Ok(output)
    }

    /// One shared Boolean constant per domain.
    pub fn boolean(&mut self, value: bool) -> Result<VariableId> {
        let mut domain = self.current;
        loop {
            if let Some(&id) = self.constants.get(&(domain, value)) {
                return Ok(id);
            }
            match self.domains[domain.0].parent {
                Some(parent) => domain = parent,
                None => break,
            }
        }
        let id = self.constant(ConstantValue::Boolean(value))?;
        self.constants.insert((self.current, value), id);
        Ok(id)
    }

    pub fn call(
        &mut self,
        function: ScalarFunction,
        inputs: Vec<VariableId>,
    ) -> Result<Vec<VariableId>> {
        let types = inputs
            .iter()
            .map(|&v| self.check_visible(v))
            .collect::<Result<Vec<_>>>()?;
        let outputs = function
            .output_types(&types)?
            .into_iter()
            .map(|ty| self.allocate_variable(ty, self.current))
            .collect::<Result<Vec<_>>>()?;
        self.add_scalar_kernel(ScalarKernel::Call {
            function,
            inputs,
            outputs: outputs.clone(),
        })?;
        Ok(outputs)
    }

    pub fn select(
        &mut self,
        cond: VariableId,
        then: VariableId,
        otherwise: VariableId,
    ) -> Result<VariableId> {
        if then == otherwise {
            return Ok(then);
        }
        let ty = self.check_visible(then)?;
        let output = self.allocate_variable(ty, self.current)?;
        self.add_scalar_kernel(ScalarKernel::Select {
            cond,
            then,
            otherwise,
            output,
        })?;
        Ok(output)
    }

    /// A condition already raised in this domain or an ancestor is skipped:
    /// columns keep their pending errors, so references may raise them again.
    pub fn raise_if(
        &mut self,
        cond: VariableId,
        error: KernelError,
        args: Vec<VariableId>,
    ) -> Result<()> {
        let mut domain = self.current;
        loop {
            if self.raised.contains(&(cond, domain)) {
                return Ok(());
            }
            match self.domains[domain.0].parent {
                Some(parent) => domain = parent,
                None => break,
            }
        }
        self.add_scalar_kernel(ScalarKernel::RaiseIf { cond, error, args })?;
        self.raised.insert((cond, self.current));
        Ok(())
    }

    pub fn group_lookup(
        &mut self,
        state: StateId,
        keys: Vec<ColumnVariableBinding>,
    ) -> Result<VariableId> {
        let output = self.allocate_variable(VariableType::UInt64, self.current)?;
        self.add_scalar_kernel(ScalarKernel::GroupLookup {
            state,
            keys,
            output,
        })?;
        Ok(output)
    }

    pub fn aggregate_update(
        &mut self,
        state: StateId,
        kind: AggregateUpdateKind,
        group: VariableId,
        arguments: Vec<ColumnVariableBinding>,
    ) -> Result<()> {
        self.add_scalar_kernel(ScalarKernel::AggregateUpdate {
            state,
            kind,
            group,
            arguments,
        })
    }

    /// Adds a Filter of the current domain and returns the new domain without
    /// switching to it.
    pub fn filter(&mut self, predicate: VariableId) -> Result<DomainId> {
        let output = self.allocate_domain(self.current)?;
        self.add_domain_kernel(DomainKernel::Filter { predicate, output })?;
        Ok(output)
    }

    /// The logical type selects the lowering rule; the column may not be bound
    /// or visible here.
    pub fn column_type(&self, column: ColumnId) -> Result<&'plan ColumnType> {
        self.columns.column_type(column)
    }

    fn bound(&self, column: ColumnId) -> Result<&BoundColumn> {
        let bound = self
            .bindings
            .get(column.index())
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                Error::invalid_plan(format!(
                    "column {} is not available in this pipeline",
                    column.index()
                ))
            })?;
        for v in bound.binding.variables() {
            if !self.is_ancestor(self.variables[v.0].domain, self.current) {
                return Err(Error::invalid_plan(format!(
                    "column {} is not visible in the current domain",
                    column.index()
                )));
            }
        }
        Ok(bound)
    }

    pub fn is_bound(&self, column: ColumnId) -> bool {
        self.bindings
            .get(column.index())
            .is_some_and(Option::is_some)
    }

    /// Fails when the column is unbound or bound outside the current domain and
    /// its ancestors.
    pub fn column_binding(&self, column: ColumnId) -> Result<ColumnVariableBinding> {
        Ok(self.bound(column)?.binding)
    }

    /// The pending errors recorded with the binding.
    pub fn column_errors(&self, column: ColumnId) -> Result<PendingErrors> {
        Ok(self.bound(column)?.errors.clone())
    }

    /// Registers a producer's output. Checks the binding against the column
    /// type and rejects a second definition. Forwarding reuses the binding.
    pub fn bind_column(
        &mut self,
        column: ColumnId,
        binding: ColumnVariableBinding,
        errors: PendingErrors,
    ) -> Result<()> {
        let ty = self.column_type(column)?;
        if !binding_matches(&binding, &ty.data_type) {
            return Err(Error::invalid_plan(format!(
                "column {} of type {} cannot be bound to {binding:?}",
                column.index(),
                ty.data_type
            )));
        }
        self.check_binding(&binding)?;
        for e in &errors {
            self.check_visible(e.flag)?;
        }
        let slot = &mut self.bindings[column.index()];
        if slot.is_some() {
            return Err(Error::invalid_plan(format!(
                "column {} is defined more than once",
                column.index()
            )));
        }
        *slot = Some(BoundColumn { binding, errors });
        Ok(())
    }

    /// Binds the columns of every input chunk to new root variables. Position i
    /// of the chunk carries `columns[i]`; a column may be listed more than
    /// once.
    pub fn bind_inputs(&mut self, columns: &[ColumnId]) -> Result<()> {
        if !self.inputs.is_empty() {
            return Err(Error::invalid_plan("pipeline input is bound twice".into()));
        }
        let saved = std::mem::replace(&mut self.current, DomainId::ROOT);
        let result = (|| {
            for (position, &column) in columns.iter().enumerate() {
                // A column at several positions carries the same values at
                // each; the first binds it.
                if columns[..position].contains(&column) {
                    continue;
                }
                let ty = self.column_type(column)?.clone();
                let binding = self.input_binding(&ty)?;
                self.bind_column(column, binding, vec![])?;
                self.inputs.push(GraphInput {
                    position,
                    ty,
                    binding,
                });
            }
            Ok(())
        })();
        self.current = saved;
        result
    }

    fn input_binding(&mut self, ty: &ColumnType) -> Result<ColumnVariableBinding> {
        let input = |builder: &mut Self, t| {
            let id = builder.allocate_variable(t, DomainId::ROOT)?;
            builder.defined[id.0] = true;
            Ok::<_, Error>(id)
        };
        if ty.data_type == DataType::Null {
            return Ok(ColumnVariableBinding::Null {
                validity: self.boolean(false)?,
            });
        }
        let validity = if ty.nullable {
            Some(input(self, VariableType::Boolean)?)
        } else {
            None
        };
        if is_bytes(&ty.data_type) {
            return Ok(ColumnVariableBinding::Bytes {
                ptr: input(self, VariableType::BytePointer)?,
                len: input(self, VariableType::Usize)?,
                validity,
            });
        }
        let vt = VariableType::of(&ty.data_type).ok_or_else(|| {
            Error::unsupported(format!("unsupported column type: {}", ty.data_type))
        })?;
        ColumnVariableBinding::fixed(vt, input(self, vt)?, validity)
    }

    /// Seals the graph. The output boundary is the current domain; every
    /// output column must be visible there.
    pub fn finish(self, outputs: Option<&[ColumnId]>) -> Result<KernelGraph> {
        let output = outputs
            .map(|columns| {
                let bindings = columns
                    .iter()
                    .map(|&c| self.column_binding(c))
                    .collect::<Result<Vec<_>>>()?;
                Ok::<_, Error>(GraphOutput {
                    domain: self.current,
                    schema: self.columns.schema(columns)?,
                    bindings,
                })
            })
            .transpose()?;
        Ok(KernelGraph {
            variables: self.variables,
            domains: self.domains,
            kernels: self.kernels,
            states: self.states,
            inputs: self.inputs,
            output,
        })
    }
}

pub(crate) fn is_bytes(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    )
}

fn binding_value_type(binding: &ColumnVariableBinding) -> VariableType {
    use ColumnVariableBinding as B;
    match binding {
        B::Boolean { .. } => VariableType::Boolean,
        B::Int8 { .. } => VariableType::Int8,
        B::Int16 { .. } => VariableType::Int16,
        B::Int32 { .. } => VariableType::Int32,
        B::Int64 { .. } => VariableType::Int64,
        B::Int128 { .. } => VariableType::Int128,
        B::UInt8 { .. } => VariableType::UInt8,
        B::UInt16 { .. } => VariableType::UInt16,
        B::UInt32 { .. } => VariableType::UInt32,
        B::UInt64 { .. } => VariableType::UInt64,
        B::Float32 { .. } => VariableType::Float32,
        B::Float64 { .. } => VariableType::Float64,
        B::Bytes { .. } => VariableType::BytePointer,
        B::Null { .. } => VariableType::Boolean,
    }
}

/// The one rule from logical type to binding shape, shared by inputs and
/// expression outputs.
pub(crate) fn binding_matches(binding: &ColumnVariableBinding, t: &DataType) -> bool {
    match binding {
        ColumnVariableBinding::Null { .. } => *t == DataType::Null,
        ColumnVariableBinding::Bytes { .. } => is_bytes(t),
        _ => VariableType::of(t) == Some(binding_value_type(binding)),
    }
}
