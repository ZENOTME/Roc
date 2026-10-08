//! Typed per-instruction lowering. Region discovery follows ValueId edges and
//! row domains, independently of the original logical operator boundaries.
use super::*;
use crate::expr::scalar::{CastMode, FunctionKind, ScalarValue};
use arrow::datatypes::{DataType, SchemaRef};

pub enum ScalarOperation<'a> {
    Reference(usize),
    Constant(&'a ScalarValue),
    Function(FunctionKind),
    Cast(&'a DataType, CastMode),
    Not,
    FoldStart(bool),
    FoldUpdate,
    FoldFinish,
}
pub enum Lowering<'a> {
    Scalar {
        domain: ValueId,
        operands: &'a [ValueId],
        output: ValueId,
        data_type: &'a DataType,
        nullable: bool,
        op: ScalarOperation<'a>,
    },
    Filter {
        input: ValueId,
        output: ValueId,
        predicate: ValueId,
        indices: Option<&'a [usize]>,
    },
    Project {
        input: ValueId,
        output: ValueId,
        values: &'a [ValueId],
        schema: &'a SchemaRef,
    },
}
#[derive(Clone, Copy, Debug, Default)]
pub struct FusionReport {
    pub regions: usize,
    pub replaced_programs: usize,
}

#[cfg(feature = "jit")]
mod native {
    use super::*;
    use crate::jit::{Expr, NativeBatchKernel};
    use arrow::datatypes::{Field, Schema};
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    struct FusedProgram {
        inputs: [ValueId; 1],
        outputs: Vec<ValueId>,
        batch_output: bool,
        accepts_selection: bool,
        kernel: NativeBatchKernel,
        members: Vec<Box<dyn Program>>,
    }
    impl Program for FusedProgram {
        fn empty_output(&self)->Option<crate::expr::scalar::ColumnValue>{
            (!self.batch_output).then(||crate::expr::scalar::ColumnValue::Array(arrow::array::new_empty_array(&DataType::Int64)))
        }
        fn empty_batch(&self)->Option<Batch>{self.batch_output.then(||arrow::record_batch::RecordBatch::new_empty(self.kernel.output_schema().clone()).into())}

        fn bind_lifetimes(&mut self, _: &[bool]) {
            for p in &mut self.members {
                p.bind_lifetimes(&vec![false; p.inputs().len()]);
            }
        }

        fn inputs(&self) -> &[ValueId] {
            &self.inputs
        }
        fn outputs(&self) -> &[ValueId] {
            &self.outputs
        }
        fn call(&mut self, c: &mut ProgramContext) -> Result<()> {
            let input = c.batch(self.inputs[0]);
            // Keep the vector path for selected input or inconsistent runtime type /
            // nullability. Native errors have no effects, so replay is safe and
            // retains the vector path's error ordering.
            if let Ok(input) = input {
                if input.selection().is_none() || self.accepts_selection {
                    let result = if input.selection().is_some() {
                        self.kernel.execute_selected(input)
                    } else {
                        self.kernel.execute_batch(input.physical())
                    };
                    if let Ok(output) = result {
                        if self.batch_output {
                            let output: Batch = output.into();
                            if output.num_rows() == 0 {
                                c.halted = Some(output.clone());
                            }
                            c.set(self.outputs[0], Value::Batch(output));
                        } else {
                            for (&id, array) in self.outputs.iter().zip(output.columns()) {
                                c.set(
                                    id,
                                    Value::Column(crate::expr::scalar::ColumnValue::Array(
                                        array.clone(),
                                    )),
                                );
                            }
                        }
                        return Ok(());
                    }
                }
            }
            for p in &mut self.members {
                p.call(c)?;
                if c.halted.is_some() {
                    break;
                }
            }
            Ok(())
        }
        fn finish(&mut self) -> Result<()> {
            for p in &mut self.members {
                p.finish()?;
            }
            Ok(())
        }
    }

    fn scalar(
        op: ScalarOperation<'_>,
        operands: &[ValueId],
        values: &HashMap<ValueId, (Expr, DataType)>,
        columns: Option<&[Expr]>,
        ty: &DataType,
    ) -> Option<Expr> {
        Some(match op {
            ScalarOperation::Reference(i) if ty == &DataType::Int64 => match columns {
                Some(cols) => cols.get(i)?.clone(),
                None => Expr::Column(i),
            },
            ScalarOperation::Constant(ScalarValue::Int64(Some(v))) if ty == &DataType::Int64 => {
                Expr::Int(*v)
            }
            ScalarOperation::Constant(ScalarValue::Boolean(Some(v)))
                if ty == &DataType::Boolean =>
            {
                Expr::Bool(*v)
            }
            ScalarOperation::Function(kind) => {
                use FunctionKind::*;
                let expected = match kind {
                    Add | Subtract | Multiply => DataType::Int64,
                    Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan
                    | GreaterThanOrEqual => DataType::Boolean,
                    _ => return None,
                };
                if ty != &expected {
                    return None;
                }
                let [left, right] = operands else {
                    return None;
                };
                let (left, lt) = values.get(left)?;
                let (right, rt) = values.get(right)?;
                if lt != &DataType::Int64 || rt != &DataType::Int64 {
                    return None;
                }
                Expr::Binary(kind, Box::new(left.clone()), Box::new(right.clone()))
            }
            _ => return None,
        })
    }
    struct Candidate {
        end: usize,
        domain: ValueId,
        predicate: Expr,
        values: Vec<Expr>,
        outputs: Vec<ValueId>,
        schema: SchemaRef,
        batch_output: bool,
    }
    fn discover(programs: &[Box<dyn Program>], start: usize, retained:&[ValueId]) -> Option<Candidate> {
        let domain = match programs[start].lowering()? {
            Lowering::Scalar { domain, .. } => domain,
            _ => return None,
        };
        let mut current = domain;
        let mut columns: Option<Vec<Expr>> = None;
        let mut values = HashMap::new();
        let mut predicate = Expr::Bool(true);
        let mut filtered = false;
        let mut last = None;
        let mut arithmetic = 0;
        for (offset, p) in programs[start..].iter().enumerate() {
            let index = start + offset;
            match p.lowering() {
                Some(Lowering::Scalar {
                    domain: d,
                    operands,
                    output,
                    data_type,
                    nullable: false,
                    op,
                }) if d == current => {
                    if matches!(op, ScalarOperation::Function(_)) {
                        arithmetic += 1;
                    }
                    let Some(expr) = scalar(op, operands, &values, columns.as_deref(), data_type)
                    else {
                        break;
                    };
                    values.insert(output, (expr, data_type.clone()));
                    last = Some(index);
                }
                Some(Lowering::Filter {
                    input,
                    output,
                    predicate: mask,
                    indices,
                }) if input == current && !filtered => {
                    let (expr, ty) = values.get(&mask)?;
                    if ty != &DataType::Boolean {
                        return None;
                    }
                    predicate = expr.clone();
                    current = output;
                    filtered = true;
                    if let Some(indices) = indices {
                        columns = Some(
                            indices
                                .iter()
                                .map(|&i| {
                                    columns
                                        .as_ref()
                                        .map_or(Some(Expr::Column(i)), |c| c.get(i).cloned())
                                })
                                .collect::<Option<Vec<_>>>()?,
                        );
                    }
                }
                Some(Lowering::Project {
                    input,
                    output,
                    values: outputs,
                    schema,
                }) if input == current => {
                    if !filtered && arithmetic<2 {break;}
                    if outputs.is_empty() {
                        break;
                    }
                    let projected = outputs
                        .iter()
                        .map(|id| {
                            let (e, t) = values.get(id)?;
                            (t == &DataType::Int64).then(|| e.clone())
                        })
                        .collect::<Option<Vec<_>>>()?;
                    return Some(Candidate {
                        end: index,
                        domain,
                        predicate,
                        values: projected,
                        outputs: vec![output],
                        schema: schema.clone(),
                        batch_output: true,
                    });
                }
                _ => break,
            }
        }
        // Also compile an expression region whose results feed another program,
        // e.g. arithmetic feeding SUM, without needing a Filter or a Project.
        if filtered || arithmetic < 2 {
            return None;
        }
        let end = last?;
        let used: HashSet<_> = programs[end + 1..]
            .iter()
            .flat_map(|p| p.inputs())
            .copied().chain(retained.iter().copied())
            .collect();
        let mut outputs: Vec<_> = values
            .keys()
            .copied()
            .filter(|v| used.contains(v))
            .collect();
        // Standalone graphs may return a slot without a following materializer.
        for &output in programs[end].outputs() {
            if !outputs.contains(&output){outputs.push(output);}
        }
        outputs.sort_by_key(|v| v.0);
        let expressions = outputs
            .iter()
            .map(|id| {
                let (e, t) = values.get(id)?;
                (t == &DataType::Int64).then(|| e.clone())
            })
            .collect::<Option<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(
            outputs
                .iter()
                .map(|v| Field::new(format!("v{}", v.0), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        Some(Candidate {
            end,
            domain,
            predicate,
            values: expressions,
            outputs,
            schema,
            batch_output: false,
        })
    }
    pub(super) fn fuse(programs: &mut Vec<Box<dyn Program>>,retained:&[ValueId]) -> Result<FusionReport> {
        let mut report = FusionReport::default();
        let mut start = 0;
        while start < programs.len() {
            let Some(c) = discover(programs, start,retained) else {
                start += 1;
                continue;
            };
            let internal: HashSet<_> = programs[start..=c.end]
                .iter()
                .flat_map(|p| p.outputs())
                .copied()
                .filter(|v| !c.outputs.contains(v))
                .collect();
            // Preserve escaped values, including fanout to later side effects.
            if retained.iter().any(|v|internal.contains(v)) || programs[..start]
                .iter()
                .chain(&programs[c.end + 1..])
                .any(|p| p.inputs().iter().any(|v| internal.contains(v)))
            {
                start += 1;
                continue;
            }
            // A pure instruction may still raise an error. Do not silently drop
            // a disconnected computation when selecting native region outputs.
            let producers:HashMap<_,_>=programs[start..=c.end].iter().enumerate().flat_map(|(i,p)|p.outputs().iter().map(move |&v|(v,i))).collect();
            let mut pending=c.outputs.clone();let mut reached=HashSet::new();
            while let Some(value)=pending.pop(){
                if let Some(&i)=producers.get(&value){if reached.insert(i){pending.extend(programs[start+i].inputs());}}
            }
            if programs[start..=c.end].iter().enumerate().any(|(i,p)|!reached.contains(&i)&&!matches!(p.lowering(),Some(Lowering::Scalar{op:ScalarOperation::Constant(_),..}))){start+=1;continue;}
            let accepts_selection = matches!(&c.predicate, Expr::Bool(true));
            let kernel = NativeBatchKernel::compile_values(c.predicate, c.values, c.schema, false)?;
            let members = programs.drain(start..=c.end).collect::<Vec<_>>();
            report.regions += 1;
            report.replaced_programs += members.len();
            programs.insert(
                start,
                Box::new(FusedProgram {
                    inputs: [c.domain],
                    outputs: c.outputs,
                    batch_output: c.batch_output,
                    accepts_selection,
                    kernel,
                    members,
                }),
            );
            start += 1;
        }
        Ok(report)
    }
}
impl ProgramBuilder {
    pub fn fuse(&mut self) -> Result<FusionReport> {
        #[cfg(not(feature = "jit"))]
        {
            Ok(FusionReport::default())
        }
        #[cfg(feature = "jit")]
        {
            native::fuse(&mut self.programs,&self.retained)
        }
    }
}
