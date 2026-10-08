//! Worker-local programs. Programs own bindings and state; the context owns only
//! values for the current invocation. A schedule is built once, in dependency order.
use crate::{
    error::{Error, Result},
    exec::Batch,
    expr::scalar::ColumnValue,
};
use std::fmt::Debug;

mod fusion;
mod operators;
pub mod scalar;
pub use fusion::{FusionReport, Lowering, ScalarOperation};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ValueId(pub usize);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgramId(pub usize);

#[derive(Debug)]
pub enum Value {
    Batch(Batch),
    BooleanFold(Box<crate::expr::scalar::conjunction::FoldState>),
    Input {
        columns: Vec<arrow::array::ArrayRef>,
        rows: usize,
    },
    Column(ColumnValue),
    Groups {
        ids: std::sync::Arc<Vec<usize>>,
        count: usize,
    },
    /// An Arrow column in the physical row domain of the corresponding batch.
    Physical(arrow::array::ArrayRef),
}

#[derive(Debug, Default)]
pub struct ProgramContext {
    values: Vec<Option<Value>>,
    persistent:Vec<bool>,
    transient:Vec<usize>,
    pub(crate) halted: Option<Batch>,
    pub(crate) control: Option<crate::exec::ProcessResult>,
}
impl ProgramContext {
    pub fn with_capacity(slots: usize) -> Self {
        Self {
            values: (0..slots).map(|_| None).collect(),
            persistent:vec![false;slots],transient:(0..slots).collect(),
            ..Default::default()
        }
    }
    #[inline]
    pub fn set(&mut self, id: ValueId, value: Value) {
        self.values[id.0] = Some(value);
    }
    #[inline]
    pub fn take(&mut self, id: ValueId) -> Result<Value> {
        if self.persistent[id.0] {
            if let Some(Value::Column(value))=&self.values[id.0]{return Ok(Value::Column(value.clone()));}
        }

        self.values[id.0]
            .take()
            .ok_or_else(|| Error::Execution("value slot is unavailable".into()))
    }
    #[inline]
    pub fn get(&self, id: ValueId) -> Result<&Value> {
        self.values
            .get(id.0)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::Execution(format!("value slot {} is not available", id.0)))
    }
    #[inline]
    pub fn batch(&self, id: ValueId) -> Result<&Batch> {
        match self.get(id)? {
            Value::Batch(b) => Ok(b),
            _ => Err(Error::Execution("expected batch slot".into())),
        }
    }
    #[inline]
    pub fn rows(&self, id: ValueId) -> Result<usize> {
        match self.get(id)? {
            Value::Batch(b) => Ok(b.num_rows()),
            Value::Input { rows, .. } => Ok(*rows),
            _ => Err(Error::Execution("expected row domain".into())),
        }
    }
    #[inline]
    pub fn columns(&self, id: ValueId) -> Result<&[arrow::array::ArrayRef]> {
        match self.get(id)? {
            Value::Batch(b) => Ok(b.physical().columns()),
            Value::Input { columns, .. } => Ok(columns),
            _ => Err(Error::Execution("expected row domain".into())),
        }
    }
    #[inline]
    pub fn selection(&self, id: ValueId) -> Result<Option<&arrow::buffer::BooleanBuffer>> {
        match self.get(id)? {
            Value::Batch(b) => Ok(b.selection()),
            Value::Input { .. } => Ok(None),
            _ => Err(Error::Execution("expected row domain".into())),
        }
    }
    #[inline]
    pub fn column(&self, id: ValueId, domain: ValueId) -> Result<ColumnValue> {
        match self.get(id)? {
            Value::Column(v) => Ok(v.clone()),
            Value::Physical(a) => Ok(ColumnValue::Array(
                if let Some(mask) = self.selection(domain)? {
                    arrow::compute::filter(
                        a.as_ref(),
                        &arrow::array::BooleanArray::new(mask.clone(), None),
                    )?
                } else {
                    a.clone()
                },
            )),
            _ => Err(Error::Execution("expected column slot".into())),
        }
    }
    #[inline]
    pub fn operand(&mut self, id: ValueId, domain: ValueId, consume: bool) -> Result<ColumnValue> {
        if !consume {
            return self.column(id, domain);
        }
        let value = self.take(id)?;
        match value {
            Value::Column(v) => Ok(v),
            Value::Physical(a) => Ok(ColumnValue::Array(
                if let Some(mask) = self.selection(domain)? {
                    arrow::compute::filter(
                        a.as_ref(),
                        &arrow::array::BooleanArray::new(mask.clone(), None),
                    )?
                } else {
                    a
                },
            )),
            _ => Err(Error::Execution("expected column slot".into())),
        }
    }
    #[inline]
    pub fn clear(&mut self) {
        for &i in &self.transient {self.values[i]=None;}
        self.halted = None;
        self.control = None;
    }
}

/// No node wrapper or external state table: each program owns its bindings.
pub trait Program: Send + 'static {
    fn empty_batch(&self)->Option<Batch>{None}
    fn empty_output(&self) -> Option<ColumnValue> {
        None
    }
    fn bind_lifetimes(&mut self, _last_use: &[bool]) {}
    fn lowering(&self) -> Option<Lowering<'_>> {
        None
    }
    fn inputs(&self) -> &[ValueId];
    fn outputs(&self) -> &[ValueId];
    fn call(&mut self, context: &mut ProgramContext) -> Result<()>;
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
    fn finish_batch(&mut self, _context: &mut ProgramContext) -> Result<Option<Batch>> {
        self.finish()?;
        Ok(None)
    }
}

#[derive(Default)]
pub struct ProgramBuilder {
    pub(crate) programs: Vec<Box<dyn Program>>,
    slots: usize,
    retained:Vec<ValueId>,
    references: std::collections::HashMap<(ValueId, usize), ValueId>,
}
impl ProgramBuilder {
    /// Preserve a result used by the caller outside the DAG. Declare before fusion.
    pub fn retain(&mut self,value:ValueId){self.retained.push(value);}
    pub fn value(&mut self) -> ValueId {
        let v = ValueId(self.slots);
        self.slots += 1;
        v
    }
    pub fn emit(&mut self, program: impl Program) -> ProgramId {
        let id = ProgramId(self.programs.len());
        self.programs.push(Box::new(program));
        id
    }
    pub fn build(mut self) -> Result<ProcessProgram> {
        // A value can have one producer. Inputs with no producer are invocation
        // arguments; forward edges are rejected instead of reading stale slots.
        let mut producers = vec![None; self.slots];
        for (i, p) in self.programs.iter().enumerate() {
            for v in p.outputs() {
                if v.0 >= self.slots || producers[v.0].replace(i).is_some() {
                    return Err(Error::InvalidPlan(
                        "duplicate or invalid value producer".into(),
                    ));
                }
            }
        }
        for (i, p) in self.programs.iter().enumerate() {
            for v in p.inputs() {
                if v.0 >= self.slots || producers[v.0].is_some_and(|j| j >= i) {
                    return Err(Error::InvalidPlan(
                        "program dependencies are not topological".into(),
                    ));
                }
            }
        }
        let mut context=ProgramContext::with_capacity(self.slots);
        let mut schedule=Vec::with_capacity(self.programs.len());
        for (i,p) in self.programs.iter().enumerate(){
            if let Some(Lowering::Scalar{op:ScalarOperation::Constant(value),output,..})=p.lowering(){
                context.set(output,Value::Column(ColumnValue::Scalar(value.clone())));
                context.persistent[output.0]=true;
            }else{schedule.push(i);}
        }
        context.transient.retain(|&i|!context.persistent[i]);
        let mut uses = vec![0usize; self.slots];
        for p in &self.programs {
            for v in p.inputs() {
                uses[v.0] += 1;
            }
        }
        for (i,&persistent) in context.persistent.iter().enumerate(){if persistent{uses[i]+=1;}}
        // Invocation arguments and terminal outputs remain visible to the caller.
        for (v, producer) in producers.iter().enumerate() {
            if producer.is_none() {
                uses[v] += 1;
            }
        }
        if let Some(p) = self.programs.last() {
            for v in p.outputs() {
                uses[v.0] += 1;
            }
        }
        for v in &self.retained {
            if v.0>=self.slots{return Err(Error::InvalidPlan("invalid retained value".into()));}
            uses[v.0]+=1;
        }
        let mut releases = Vec::with_capacity(self.programs.len());
        for p in &mut self.programs {
            let mut last = vec![];
            let mut release = vec![];
            for &v in p.inputs() {
                uses[v.0] -= 1;
                let final_use = uses[v.0] == 0;
                last.push(final_use);
                if final_use {
                    release.push(v);
                }
            }
            p.bind_lifetimes(&last);
            releases.push(release);
        }
        Ok(ProcessProgram {
            programs: self.programs,
            context,
            schedule,
            input: None,
            output: None,
            releases,
        })
    }
}

pub struct ProcessProgram {
    pub(crate) programs: Vec<Box<dyn Program>>,
    pub context: ProgramContext,
    input: Option<ValueId>,
    output: Option<ValueId>,
    releases: Vec<Vec<ValueId>>,
    schedule:Vec<usize>,
}
impl ProcessProgram {
    pub fn call(&mut self) -> Result<()> {
        for &i in &self.schedule {
            self.programs[i].call(&mut self.context)?;
            for v in &self.releases[i] {
                self.context.values[v.0] = None;
            }
            if self.context.halted.is_some() || self.context.control.is_some() {
                break;
            }
        }
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        for p in &mut self.programs {
            p.finish()?;
        }
        Ok(())
    }
    pub fn program_count(&self) -> usize {
        self.programs.len()
    }
}

impl std::fmt::Debug for ProcessProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ProcessProgram")
            .field("programs", &self.programs.len())
            .finish()
    }
}

impl Value {
    pub fn input(columns: &[arrow::array::ArrayRef], rows: usize) -> Self {
        Self::Input {
            columns: columns.to_vec(),
            rows,
        }
    }
    #[inline]
    pub fn num_rows(&self) -> Result<usize> {
        match self {
            Self::Input { rows, .. } => Ok(*rows),
            Self::Batch(b) => Ok(b.num_rows()),
            _ => Err(Error::Execution("expected input row domain".into())),
        }
    }
}
#[cfg(test)]
#[path = "../../tests/support/program.rs"]
pub(crate) mod test_support;
#[cfg(test)]
mod value_tests;
