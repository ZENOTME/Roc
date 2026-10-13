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

//! Per-kernel generated code for zero-compile execution. One function is
//! generated per Call signature with the instruction selection of fused
//! programs, and kept in a process-wide cache for the life of the process.
use super::*;
use cranelift_module::FuncId;
use std::sync::LazyLock;

/// `(runtime, inputs, outputs, rows, n) -> status`.
///
/// Input k is `[pointer, mask]`: the word of root row r is at
/// `pointer + ((r & mask) << 3)`, so a constant has mask 0. Output j is a
/// column pointer written at every computed row. `rows` lists the `n` root
/// rows to compute, or is null for rows `0..n`. Words are canonical: signed
/// integers sign-extended, other integers and Booleans zero-extended, floats as
/// their IEEE bits.
pub(crate) type KernelEntry =
    unsafe extern "C" fn(*mut Runtime, *const [u64; 2], *const *mut u64, *const u32, usize) -> i32;

type Signature = (ScalarFunction, Vec<VariableType>);

static CACHE: LazyLock<Mutex<HashMap<Signature, usize>>> = LazyLock::new(Default::default);

/// The generated function of a Call signature; generated now on a cache miss.
pub(crate) fn generated(function: ScalarFunction, inputs: &[VariableType]) -> Result<KernelEntry> {
    let key = (function, inputs.to_vec());
    let cached = CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .copied();
    let address = match cached {
        Some(address) => address,
        None => {
            let address = generate(std::slice::from_ref(&key))?[0];
            *CACHE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(key)
                .or_insert(address)
        }
    };
    // SAFETY: every cached address is a finalized function of this signature.
    Ok(unsafe { std::mem::transmute::<usize, KernelEntry>(address) })
}

/// Generates the code of every supported Call signature into the cache, so
/// that preparing zero-compile programs generates nothing. Returns the number
/// of cached signatures.
pub fn prewarm_kernels() -> Result<usize> {
    let missing = {
        let cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
        signatures()
            .into_iter()
            .filter(|s| !cache.contains_key(s))
            .collect::<Vec<_>>()
    };
    let addresses = generate(&missing)?;
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    for (key, address) in missing.into_iter().zip(addresses) {
        cache.entry(key).or_insert(address);
    }
    Ok(cache.len())
}

/// Every Call signature accepted by `ScalarFunction::output_types`, except
/// FormatText, which zero-compile execution runs as a runtime call.
fn signatures() -> Vec<Signature> {
    use ScalarFunction::*;
    use VariableType as T;
    let values = [
        T::Boolean,
        T::Int8,
        T::Int16,
        T::Int32,
        T::Int64,
        T::UInt8,
        T::UInt16,
        T::UInt32,
        T::UInt64,
        T::Float32,
        T::Float64,
    ];
    let mut functions = vec![
        Add,
        Subtract,
        Multiply,
        Negate,
        Divide,
        Remainder,
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        And,
        Or,
        Not,
        ValidateUtf8,
    ];
    functions.extend(values.map(|to| Cast { to }));
    functions.extend(values.map(|to| ParseText { to }));
    let mut shapes = values
        .iter()
        .flat_map(|&t| [vec![t], vec![t, t]])
        .collect::<Vec<_>>();
    shapes.push(vec![T::BytePointer, T::Usize]);
    shapes.push(vec![T::BytePointer, T::Usize, T::BytePointer, T::Usize]);
    functions
        .into_iter()
        .flat_map(|f| {
            shapes
                .iter()
                .filter(move |s| f.output_types(s).is_ok())
                .map(move |s| (f, s.clone()))
        })
        .collect()
}

fn generate(signatures: &[Signature]) -> Result<Vec<usize>> {
    if signatures.is_empty() {
        return Ok(vec![]);
    }
    let mut module = JITModule::new(
        JITBuilder::with_flags(
            &[("opt_level", "speed")],
            cranelift_module::default_libcall_names(),
        )
        .map_err(error)?,
    );
    if module.target_config().pointer_type() != types::I64 {
        return Err(Error::InvalidPlan(
            "generated code requires a 64-bit target".into(),
        ));
    }
    let ids = signatures
        .iter()
        .enumerate()
        .map(|(i, (function, inputs))| define(&mut module, i, *function, inputs))
        .collect::<Result<Vec<_>>>()?;
    module.finalize_definitions().map_err(error)?;
    let addresses = ids
        .iter()
        .map(|id| module.get_finalized_function(*id) as usize)
        .collect();
    // Cached functions are never freed.
    std::mem::forget(module);
    Ok(addresses)
}

fn define(
    module: &mut JITModule,
    index: usize,
    function: ScalarFunction,
    inputs: &[VariableType],
) -> Result<FuncId> {
    let outputs = function.output_types(inputs)?;
    let (n_in, n_out) = (inputs.len(), outputs.len());
    let graph = KernelGraph {
        variables: inputs
            .iter()
            .chain(&outputs)
            .map(|&ty| Variable {
                ty,
                domain: DomainId::ROOT,
            })
            .collect(),
        domains: vec![RowDomain {
            parent: None,
            parent_row: None,
        }],
        kernels: vec![(
            DomainId::ROOT,
            Kernel::Scalar(ScalarKernel::Call {
                function,
                inputs: (0..n_in).map(VariableId).collect(),
                outputs: (n_in..n_in + n_out).map(VariableId).collect(),
            }),
        )],
        states: vec![],
        inputs: vec![],
        output: None,
    };
    let plan = Plan::new(&graph);
    let mut ctx = module.make_context();
    ctx.func.signature.params = vec![AbiParam::new(types::I64); 5];
    ctx.func.signature.returns = vec![AbiParam::new(types::I32)];
    let id = module
        .declare_function(
            &format!("roc_kernel_{index}"),
            Linkage::Local,
            &ctx.func.signature,
        )
        .map_err(error)?;
    let mut fb = FunctionBuilderContext::new();
    let mut b = FunctionBuilder::new(&mut ctx.func, &mut fb);
    let entry = b.create_block();
    b.append_block_params_for_function_params(entry);
    b.switch_to_block(entry);
    let args = b.block_params(entry).to_vec();
    let fail = b.create_block();
    b.append_block_param(fail, types::I32);
    let sources = (0..n_in)
        .map(|k| {
            let at = (k * 16) as i32;
            let pointer = b.ins().load(types::I64, MemFlags::new(), args[1], at);
            let mask = b.ins().load(types::I64, MemFlags::new(), args[1], at + 8);
            (pointer, mask)
        })
        .collect::<Vec<_>>();
    let sinks = (0..n_out)
        .map(|j| {
            b.ins()
                .load(types::I64, MemFlags::new(), args[2], (j * 8) as i32)
        })
        .collect::<Vec<_>>();
    let zero = b.ins().iconst(types::I64, 0);
    let count = b.declare_var(types::I64);
    b.def_var(count, zero);
    let dense = b.create_block();
    let dense_body = b.create_block();
    let selected = b.create_block();
    let selected_body = b.create_block();
    let done = b.create_block();
    b.append_block_param(dense, types::I64);
    b.append_block_param(selected, types::I64);
    let is_dense = b.ins().icmp_imm(IntCC::Equal, args[3], 0);
    b.ins()
        .brif(is_dense, dense, &[zero.into()], selected, &[zero.into()]);
    let mut c = Compiler {
        b,
        module: &mut *module,
        graph: &graph,
        plan: &plan,
        vars: vec![None; graph.variables.len()],
        args,
        row: zero,
        setup: entry,
        fail,
        hoisted: HashMap::new(),
        regions: vec![],
        count,
        sites: vec![],
        constants: vec![],
        aliases: &[],
    };
    for (head, body) in [(dense, dense_body), (selected, selected_body)] {
        c.b.switch_to_block(head);
        let i = c.b.block_params(head)[0];
        let more = c.b.ins().icmp(IntCC::UnsignedLessThan, i, c.args[4]);
        c.b.ins().brif(more, body, &[], done, &[]);
        c.b.switch_to_block(body);
        let row = if head == dense {
            i
        } else {
            let at = c.b.ins().ishl_imm(i, 2);
            let at = c.b.ins().iadd(c.args[3], at);
            c.b.ins().uload32(MemFlags::new(), at, 0)
        };
        for (k, &(pointer, mask)) in sources.iter().enumerate() {
            let r = c.b.ins().band(row, mask);
            let offset = c.b.ins().ishl_imm(r, 3);
            let at = c.b.ins().iadd(pointer, offset);
            let word = c.b.ins().load(types::I64, MemFlags::new(), at, 0);
            let v = c.decode(word, inputs[k]);
            c.set(VariableId(k), v);
        }
        c.kernel(0)?;
        let offset = c.b.ins().ishl_imm(row, 3);
        for (j, &sink) in sinks.iter().enumerate() {
            let v = c.get(VariableId(n_in + j));
            let word = c.bits(v, outputs[j]);
            let at = c.b.ins().iadd(sink, offset);
            c.b.ins().store(MemFlags::new(), word, at, 0);
        }
        let next = c.b.ins().iadd_imm(i, 1);
        c.b.ins().jump(head, &[next.into()]);
    }
    c.b.switch_to_block(done);
    let ok = c.b.ins().iconst(types::I32, 0);
    c.b.ins().return_(&[ok]);
    c.b.switch_to_block(fail);
    let status = c.b.block_params(fail)[0];
    c.b.ins().return_(&[status]);
    c.b.seal_all_blocks();
    c.b.finalize();
    module.define_function(id, &mut ctx).map_err(error)?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prewarm_caches_every_signature_once() {
        let expected = signatures().len();
        assert_eq!(prewarm_kernels().unwrap(), expected);
        assert_eq!(prewarm_kernels().unwrap(), expected);
        let add = (ScalarFunction::Add, vec![VariableType::Int64; 2]);
        let first = generated(add.0, &add.1).unwrap() as usize;
        assert_eq!(generated(add.0, &add.1).unwrap() as usize, first);
        assert_eq!(CACHE.lock().unwrap().len(), expected);
    }

    #[test]
    fn generated_kernels_compute_dense_selected_and_constant_rows() {
        let entry = generated(ScalarFunction::Add, &[VariableType::Int32; 2]).unwrap();
        let left = [1u64, 2, 3, (-4i64) as u64];
        let right = [10u64];
        let sources = [[left.as_ptr() as u64, u64::MAX], [right.as_ptr() as u64, 0]];
        let run = |rows: Option<&[u32]>| {
            let mut sum = [0u64; 4];
            let mut overflow = [0u64; 4];
            let sinks = [sum.as_mut_ptr(), overflow.as_mut_ptr()];
            let (selection, n) = rows.map_or((std::ptr::null(), 4), |r| (r.as_ptr(), r.len()));
            let mut runtime = Runtime::default();
            // SAFETY: every listed row has a word in each input and output.
            let status =
                unsafe { entry(&mut runtime, sources.as_ptr(), sinks.as_ptr(), selection, n) };
            assert_eq!(status, 0);
            sum
        };
        assert_eq!(run(None), [11, 12, 13, 6]);
        assert_eq!(run(Some(&[1, 3])), [0, 12, 0, 6]);
    }
}
