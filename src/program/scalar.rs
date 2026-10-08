//! Expression emission binds a concrete function once. Calls never dispatch on
//! an expression/opcode enum; lowering metadata is consulted only by compilation.
use super::*;
use crate::expr::scalar::{
    kernels::{self, EvalFn},
    selected, *,
};
use arrow::{
    array::{ArrayRef, AsArray, UInt64Array, new_empty_array},
    compute::{CastOptions, cast_with_options, kernels::interleave::interleave, not, take},
    datatypes::DataType,
};
use std::sync::Arc;

struct ValueProgram<P> {
    inputs: Vec<ValueId>,
    output: [ValueId; 1],
    data_type: DataType,
    nullable: bool,
    params: P,
    eval_fn: fn(&mut ValueProgram<P>, &mut ProgramContext) -> Result<Value>,
    ir_fn: fn(&P) -> Option<ScalarOperation<'_>>,
    skip_empty: bool,
    last_use: Vec<bool>,
}
impl<P: Send + 'static> Program for ValueProgram<P> {
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &self.output
    }
    fn bind_lifetimes(&mut self, last: &[bool]) {
        self.last_use = last.to_vec();
    }
    fn empty_output(&self) -> Option<ColumnValue> {
        self.skip_empty
            .then(|| ColumnValue::Array(new_empty_array(&self.data_type)))
    }
    fn lowering(&self) -> Option<Lowering<'_>> {
        Some(Lowering::Scalar {
            domain: self.inputs[0],
            operands: &self.inputs[1..],
            output: self.output[0],
            data_type: &self.data_type,
            nullable: self.nullable,
            op: (self.ir_fn)(&self.params)?,
        })
    }
    fn call(&mut self, context: &mut ProgramContext) -> Result<()> {
        let value = if self.skip_empty && context.rows(self.inputs[0])? == 0 {
            Value::Column(ColumnValue::Array(new_empty_array(&self.data_type)))
        } else {
            (self.eval_fn)(self, context)?
        };
        context.set(self.output[0], value);
        Ok(())
    }
}
fn emit_value<P: Send + 'static>(
    builder: &mut ProgramBuilder,
    inputs: Vec<ValueId>,
    ty: &crate::expr::ExpressionResultType,
    params: P,
    eval_fn: fn(&mut ValueProgram<P>, &mut ProgramContext) -> Result<Value>,
    ir_fn: fn(&P) -> Option<ScalarOperation<'_>>,
    skip_empty: bool,
) -> ValueId {
    let output = builder.value();
    builder.emit(ValueProgram {
        inputs,
        output: [output],
        data_type: ty.data_type().clone(),
        nullable: ty.is_nullable(),
        params,
        eval_fn,
        ir_fn,
        skip_empty,
        last_use: vec![],
    });
    output
}
fn reference(p: &mut ValueProgram<usize>, c: &mut ProgramContext) -> Result<Value> {
    let a = c
        .columns(p.inputs[0])?
        .get(p.params)
        .ok_or_else(|| Error::Execution(format!("column index {} out of bounds", p.params)))?;
    Ok(Value::Physical(a.clone()))
}
fn constant(p: &mut ValueProgram<ScalarValue>, _: &mut ProgramContext) -> Result<Value> {
    Ok(Value::Column(ColumnValue::Scalar(p.params.clone())))
}
struct FunctionProgram {
    inputs: Vec<ValueId>,
    output: [ValueId; 1],
    data_type: DataType,
    nullable: bool,
    kind: FunctionKind,
    eval_fn: EvalFn,
    selected: Option<selected::Kernel>,
    last_use: Vec<bool>,
}
impl Program for FunctionProgram {
    fn inputs(&self) -> &[ValueId] {
        &self.inputs
    }
    fn outputs(&self) -> &[ValueId] {
        &self.output
    }
    fn bind_lifetimes(&mut self, last: &[bool]) {
        self.last_use = last.to_vec();
    }
    fn empty_output(&self) -> Option<ColumnValue> {
        Some(ColumnValue::Array(new_empty_array(&self.data_type)))
    }
    fn lowering(&self) -> Option<Lowering<'_>> {
        Some(Lowering::Scalar {
            domain: self.inputs[0],
            operands: &self.inputs[1..],
            output: self.output[0],
            data_type: &self.data_type,
            nullable: self.nullable,
            op: ScalarOperation::Function(self.kind),
        })
    }
    fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
        let domain = self.inputs[0];
        let rows = c.rows(domain)?;
        let result = if rows == 0 {
            ColumnValue::Array(new_empty_array(&self.data_type))
        } else if let (Some(kernel), Some(mask)) = (self.selected, c.selection(domain)?) {
            let get = |id| -> Result<selected::Value> {
                Ok(match c.get(id)? {
                    Value::Physical(v) => selected::Value::Physical(v.clone()),
                    Value::Column(v) => selected::Value::Compact(v.clone()),
                    _ => return Err(Error::Execution("expected kernel operand".into())),
                })
            };
            kernel(&get(self.inputs[1])?, &get(self.inputs[2])?, mask, rows)?
        } else {
            let left = c.operand(self.inputs[1], domain, self.last_use[1])?;
            if self.inputs.len() == 2 {
                (self.eval_fn)(&[left])?
            } else {
                let right = c.operand(self.inputs[2], domain, self.last_use[2])?;
                (self.eval_fn)(&[left, right])?
            }
        };
        c.set(self.output[0], Value::Column(result));
        Ok(())
    }
}
struct CastParameters{target:DataType,mode:CastMode,options:CastOptions<'static>}
fn cast(p: &mut ValueProgram<CastParameters>, c: &mut ProgramContext) -> Result<Value> {
    let input = c.operand(p.inputs[1], p.inputs[0], p.last_use[1])?;
    Ok(Value::Column(match input {
        ColumnValue::Array(a) => {
            ColumnValue::Array(cast_with_options(a.as_ref(), &p.params.target, &p.params.options)?)
        }
        ColumnValue::Scalar(s) => ColumnValue::Scalar(ScalarValue::try_from_array(
            &cast_with_options(s.to_array()?.as_ref(), &p.params.target, &p.params.options)?,
            0,
        )?),
    }))
}
fn negate_boolean(p: &mut ValueProgram<()>, c: &mut ProgramContext) -> Result<Value> {
    Ok(Value::Column(
        match c.operand(p.inputs[1], p.inputs[0], p.last_use[1])? {
            ColumnValue::Scalar(s) => {
                ColumnValue::Scalar(ScalarValue::Boolean(s.as_boolean()?.map(|v| !v)))
            }
            ColumnValue::Array(a) => ColumnValue::Array(Arc::new(not(a
                .as_boolean_opt()
                .ok_or_else(|| Error::Execution("expected Boolean expression".into()))?)?)),
        },
    ))
}
fn fold_start(p: &mut ValueProgram<bool>, c: &mut ProgramContext) -> Result<Value> {
    Ok(Value::BooleanFold(Box::new(
        crate::expr::scalar::conjunction::FoldState::new(c.rows(p.inputs[0])?, p.params),
    )))
}
fn fold_update(p: &mut ValueProgram<()>, c: &mut ProgramContext) -> Result<Value> {
    let v = c.operand(p.inputs[2], p.inputs[0], p.last_use[2])?;
    let Value::BooleanFold(mut state) = c.take(p.inputs[1])? else {
        return Err(Error::Execution("expected Boolean workspace".into()));
    };
    state.push(v)?;
    Ok(Value::BooleanFold(state))
}
fn fold_finish(p: &mut ValueProgram<()>, c: &mut ProgramContext) -> Result<Value> {
    let Value::BooleanFold(state) = c.take(p.inputs[1])? else {
        return Err(Error::Execution("expected Boolean workspace".into()));
    };
    Ok(Value::Column(state.finish()))
}
struct CaseRegions {
    branches: Vec<(ProcessProgram, ProcessProgram)>,
    otherwise: ProcessProgram,
}
fn case(p: &mut ValueProgram<CaseRegions>, c: &mut ProgramContext) -> Result<Value> {
    let domain = p.inputs[0];
    let rows = c.rows(domain)?;
    let CaseRegions {
        branches,
        otherwise,
    } = &mut p.params;

    let columns = dense_columns(c, domain)?;
    let mut remaining: Vec<usize> = (0..rows).collect();
    let mut mapping = vec![(0, 0); rows];
    let mut pieces = vec![];
    for (condition, branch) in branches {
        if remaining.is_empty() {
            break;
        }
        let input = gather(&columns, rows, &remaining)?;
        let selected = crate::expr::predicate::select_true(
            condition
                .run_value(&Value::input(&input, remaining.len()))?
                .into_array(remaining.len())?,
        )?;
        let mut picked = selected.into_iter().peekable();
        let mut matched = vec![];
        let mut next = vec![];
        for (i, p) in remaining.into_iter().enumerate() {
            if picked.peek() == Some(&i) {
                picked.next();
                matched.push(p);
            } else {
                next.push(p);
            }
        }
        remaining = next;
        if !matched.is_empty() {
            let v = branch
                .run_value(&Value::input(
                    &gather(&columns, rows, &matched)?,
                    matched.len(),
                ))?
                .into_array(matched.len())?;
            for (i, p) in matched.into_iter().enumerate() {
                mapping[p] = (pieces.len(), i);
            }
            pieces.push(v);
        }
    }
    if !remaining.is_empty() {
        let v = otherwise
            .run_value(&Value::input(
                &gather(&columns, rows, &remaining)?,
                remaining.len(),
            ))?
            .into_array(remaining.len())?;
        for (i, p) in remaining.into_iter().enumerate() {
            mapping[p] = (pieces.len(), i);
        }
        pieces.push(v);
    }
    Ok(Value::Column(ColumnValue::Array(interleave(
        &pieces.iter().map(|v| v.as_ref()).collect::<Vec<_>>(),
        &mapping,
    )?)))
}
fn coalesce(p: &mut ValueProgram<Vec<ProcessProgram>>, c: &mut ProgramContext) -> Result<Value> {
    let domain = p.inputs[0];
    let rows = c.rows(domain)?;
    let graphs = &mut p.params;

    let columns = dense_columns(c, domain)?;
    let mut remaining: Vec<usize> = (0..rows).collect();
    let mut mapping = vec![(0, 0); rows];
    let mut pieces = vec![];
    let arguments = graphs.len();
    for (i, g) in graphs.iter_mut().enumerate() {
        if remaining.is_empty() {
            break;
        }
        let v = g
            .run_value(&Value::input(
                &gather(&columns, rows, &remaining)?,
                remaining.len(),
            ))?
            .into_array(remaining.len())?;
        let nulls = v.logical_nulls();
        let mut next = vec![];
        for (j, p) in remaining.into_iter().enumerate() {
            if i + 1 < arguments && nulls.as_ref().is_some_and(|n| n.is_null(j)) {
                next.push(p);
            } else {
                mapping[p] = (pieces.len(), j);
            }
        }
        remaining = next;
        pieces.push(v);
    }
    Ok(Value::Column(ColumnValue::Array(interleave(
        &pieces.iter().map(|v| v.as_ref()).collect::<Vec<_>>(),
        &mapping,
    )?)))
}
fn dense_columns(c: &ProgramContext, domain: ValueId) -> Result<Vec<ArrayRef>> {
    if let Some(mask) = c.selection(domain)? {
        let mask = arrow::array::BooleanArray::new(mask.clone(), None);
        c.columns(domain)?
            .iter()
            .map(|a| Ok(arrow::compute::filter(a.as_ref(), &mask)?))
            .collect()
    } else {
        Ok(c.columns(domain)?.to_vec())
    }
}
fn gather(columns: &[ArrayRef], rows: usize, selected: &[usize]) -> Result<Vec<ArrayRef>> {
    if rows == selected.len() {
        return Ok(columns.to_vec());
    }
    let ids = UInt64Array::from_iter_values(selected.iter().map(|&i| i as u64));
    columns
        .iter()
        .map(|a| Ok(take(a.as_ref(), &ids, None)?))
        .collect()
}

impl ScalarExpression {
    /// Binding and expression-kind dispatch happen once, while emitting the DAG.
    pub fn emit(&self, builder: &mut ProgramBuilder, domain: ValueId) -> Result<ValueId> {
        let ty = self.result_type();
        Ok(match self {
            Self::Reference(e) => {
                if let Some(&id) = builder.references.get(&(domain, e.index())) {
                    return Ok(id);
                }
                let id = emit_value(
                    builder,
                    vec![domain],
                    ty,
                    e.index(),
                    reference,
                    |i| Some(ScalarOperation::Reference(*i)),
                    false,
                );
                builder.references.insert((domain, e.index()), id);
                id
            }
            Self::Constant(e) => emit_value(
                builder,
                vec![domain],
                ty,
                e.value().clone(),
                constant,
                |v| Some(ScalarOperation::Constant(v)),
                false,
            ),
            Self::Function(e) => {
                let mut inputs = vec![domain];
                for arg in e.arguments() {
                    inputs.push(arg.emit(builder, domain)?);
                }
                let arg_ty = e.arguments()[0].result_type().data_type();
                let eval_fn = if e.arguments().len() == 1 {
                    kernels::bind_unary(e.function(), arg_ty)?
                } else {
                    kernels::bind_binary(e.function(), arg_ty)?
                };
                let selected = if e.arguments().len() == 2
                    && e.arguments()
                        .iter()
                        .all(|a| a.result_type().data_type() == &DataType::Int64)
                {
                    use FunctionKind::*;
                    match e.function() {
                        Add => Some(selected::kernel::<0> as selected::Kernel),
                        Subtract => Some(selected::kernel::<1> as selected::Kernel),
                        Multiply => Some(selected::kernel::<2> as selected::Kernel),
                        Equal => Some(selected::kernel::<3> as selected::Kernel),
                        NotEqual => Some(selected::kernel::<4> as selected::Kernel),
                        LessThan => Some(selected::kernel::<5> as selected::Kernel),
                        LessThanOrEqual => Some(selected::kernel::<6> as selected::Kernel),
                        GreaterThan => Some(selected::kernel::<7> as selected::Kernel),
                        GreaterThanOrEqual => Some(selected::kernel::<8> as selected::Kernel),
                        _ => None,
                    }
                } else {
                    None
                };
                let output = builder.value();
                builder.emit(FunctionProgram {
                    inputs,
                    output: [output],
                    data_type: ty.data_type().clone(),
                    nullable: ty.is_nullable(),
                    kind: e.function(),
                    eval_fn,
                    selected,
                    last_use: vec![],
                });
                output
            }
            Self::Cast(e) => {
                let v = e.input().emit(builder, domain)?;
                emit_value(
                    builder,
                    vec![domain, v],
                    ty,
                    CastParameters{target:e.target().clone(),mode:e.mode(),options:CastOptions{safe:e.mode()==CastMode::Try,..Default::default()}},
                    cast,
                    |p| Some(ScalarOperation::Cast(&p.target, p.mode)),
                    true,
                )
            }
            Self::Not(e) => {
                let v = e.input().emit(builder, domain)?;
                emit_value(
                    builder,
                    vec![domain, v],
                    ty,
                    (),
                    negate_boolean,
                    |_| Some(ScalarOperation::Not),
                    true,
                )
            }
            Self::Conjunction(e) => {
                let mut state = emit_value(
                    builder,
                    vec![domain],
                    ty,
                    e.conjunction() == Conjunction::And,
                    fold_start,
                    |v| Some(ScalarOperation::FoldStart(*v)),
                    false,
                );
                for arg in e.arguments() {
                    let v = arg.emit(builder, domain)?;
                    state = emit_value(
                        builder,
                        vec![domain, state, v],
                        ty,
                        (),
                        fold_update,
                        |_| Some(ScalarOperation::FoldUpdate),
                        false,
                    );
                }
                emit_value(
                    builder,
                    vec![domain, state],
                    ty,
                    (),
                    fold_finish,
                    |_| Some(ScalarOperation::FoldFinish),
                    true,
                )
            }
            Self::Case(e) => {
                let branches = e
                    .branches()
                    .iter()
                    .map(|(a, b)| Ok((region(a)?, region(b)?)))
                    .collect::<Result<_>>()?;
                let otherwise = region(e.else_expr())?;
                emit_value(
                    builder,
                    vec![domain],
                    ty,
                    CaseRegions {
                        branches,
                        otherwise,
                    },
                    case,
                    |_| None,
                    true,
                )
            }
            Self::Coalesce(e) => {
                if e.arguments().is_empty() {
                    return Err(Error::InvalidPlan(
                        "coalesce requires at least one argument".into(),
                    ));
                }
                let regions = e
                    .arguments()
                    .iter()
                    .map(|e| region(e))
                    .collect::<Result<_>>()?;
                emit_value(builder, vec![domain], ty, regions, coalesce, |_| None, true)
            }
        })
    }
}
fn region(expr: &ScalarExpression) -> Result<ProcessProgram> {
    let mut builder = ProgramBuilder::default();
    let input = builder.value();
    let output = expr.emit(&mut builder, input)?;
    builder.build_value(input, output)
}
impl ProgramBuilder {
    pub fn build_value(self, input: ValueId, output: ValueId) -> Result<ProcessProgram> {
        self.build_batch(input, Some(output))
    }
}
impl ProcessProgram {
    /// Invoke a value-producing program with a batch or a column row domain.
    /// The worker exclusively owns the program; no locks or scalar executor.
    pub fn run_value(&mut self, input: &Value) -> Result<ColumnValue> {
        let domain = self
            .input
            .ok_or_else(|| Error::Execution("program input is not bound".into()))?;
        let output = self
            .output
            .ok_or_else(|| Error::Execution("program has no value output".into()))?;
        if input.num_rows()? == 0 {
            if let Some(empty) = self.programs.last().and_then(|p| p.empty_output()) {
                return Ok(empty);
            }
        }
        let input = match input {
            Value::Batch(b) => Value::Batch(b.clone()),
            Value::Input { columns, rows } => Value::input(columns, *rows),
            _ => return Err(Error::Execution("expected input row domain".into())),
        };
        self.context.set(domain, input);
        let result = self
            .call()
            .and_then(|_| self.context.operand(output, domain, true));
        self.context.clear();
        result
    }
}
