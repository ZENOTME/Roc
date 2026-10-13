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
    let mut module = JitModule::new()?;
    let ids = signatures
        .iter()
        .enumerate()
        .map(|(i, (function, inputs))| define(&module, i, *function, inputs))
        .collect::<Result<Vec<_>>>()?;
    module.finalize()?;
    let addresses = ids
        .iter()
        .map(|id| module.address(*id))
        .collect::<Result<Vec<_>>>()?;
    // Cached functions are never freed.
    std::mem::forget(module);
    Ok(addresses)
}

fn define(
    module: &JitModule,
    index: usize,
    function: ScalarFunction,
    inputs: &[VariableType],
) -> Result<llvm::FuncId> {
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
    let id = module.function(&format!("roc_kernel_{index}"), 5);
    let mut b = module.builder(id);
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
    let done = b.create_block();
    // Scalar broadcasting is an input shape, not a per-row address mask.
    // Lift the same scalar kernel for every shape in the finite signature.
    // Dispatch once per invocation; scalar loads dominate the row loops.
    let mut shape = zero;
    for (k, &(_, mask)) in sources.iter().enumerate() {
        let scalar = b.ins().icmp_imm(IntCC::Equal, mask, 0);
        let scalar = b.ins().uextend(types::I64, scalar);
        let bit = b.ins().ishl_imm(scalar, k as i64);
        shape = b.ins().bor(shape, bit);
    }
    let shape_entries = (0..1usize << n_in)
        .map(|_| b.create_block())
        .collect::<Vec<_>>();
    for (s, &target) in shape_entries.iter().enumerate() {
        if s + 1 == shape_entries.len() {
            b.ins().jump(target, &[]);
        } else {
            let next = b.create_block();
            let matches = b.ins().icmp_imm(IntCC::Equal, shape, s as i64);
            b.ins().brif(matches, target, &[], next, &[]);
            b.switch_to_block(next);
        }
    }
    for (shape, &shape_entry) in shape_entries.iter().enumerate() {
        b.switch_to_block(shape_entry);
        let scalar_values = sources
            .iter()
            .enumerate()
            .map(|(k, &(pointer, _))| {
                (shape & (1 << k) != 0)
                    .then(|| b.ins().load(types::I64, MemFlags::new(), pointer, 0))
            })
            .collect::<Vec<_>>();
        let count = b.declare_var(types::I64);
        b.def_var(count, zero);
        let dense = b.create_block();
        let dense_body = b.create_block();
        let selected = b.create_block();
        let selected_body = b.create_block();
        b.append_block_param(dense, types::I64);
        b.append_block_param(selected, types::I64);
        let is_dense = b.ins().icmp_imm(IntCC::Equal, args[3], 0);
        b.ins()
            .brif(is_dense, dense, &[zero.into()], selected, &[zero.into()]);
        let mut c = Compiler {
            b,
            module,
            graph: &graph,
            plan: &plan,
            vars: vec![None; graph.variables.len()],
            args: args.clone(),
            row: zero,
            setup: shape_entry,
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
                let at = c.b.ins().address(c.args[3], at);
                c.b.ins().uload32(MemFlags::new(), at, 0)
            };
            for (k, &(pointer, _)) in sources.iter().enumerate() {
                let word = if let Some(value) = scalar_values[k] {
                    value
                } else {
                    let offset = c.b.ins().ishl_imm(row, 3);
                    let at = c.b.ins().address(pointer, offset);
                    c.b.ins().load(types::I64, MemFlags::new(), at, 0)
                };
                let v = c.decode(word, inputs[k]);
                c.set(VariableId(k), v);
            }
            c.kernel(0)?;
            let offset = c.b.ins().ishl_imm(row, 3);
            for (j, &sink) in sinks.iter().enumerate() {
                let v = c.get(VariableId(n_in + j));
                let word = c.bits(v, outputs[j]);
                let at = c.b.ins().address(sink, offset);
                c.b.ins().store(MemFlags::new(), word, at, 0);
            }
            let next = c.b.ins().iadd_imm(i, 1);
            c.b.ins().jump(head, &[next.into()]);
        }
        b = c.b;
    }
    b.switch_to_block(done);
    let ok = b.ins().iconst(types::I32, 0);
    b.ins().return_(&[ok]);
    b.switch_to_block(fail);
    let status = b.block_params(fail)[0];
    b.ins().return_(&[status]);
    b.seal_all_blocks();
    b.finalize();
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
    fn generated_kernels_preserve_broadcast_selection_aliases_and_overflow() {
        let entry = generated(ScalarFunction::Add, &[VariableType::Int32; 2]).unwrap();
        let n = 4099;
        let left = (0..n)
            .map(|i| [i32::MAX, i32::MIN, 17, -29][i % 4] as i64 as u64)
            .collect::<Vec<_>>();
        let right = (0..n)
            .map(|i| [1i32, -1, -100, 200][i % 4] as i64 as u64)
            .collect::<Vec<_>>();
        let picked = [4098u32, 0, 31, 2, 4097, 100];
        for alias in [false, true] {
            let right = if alias { &left } else { &right };
            for shape in 0..4 {
                let scalar = [shape & 1 != 0, shape & 2 != 0];
                let sources = [
                    [left.as_ptr() as u64, if scalar[0] { 0 } else { u64::MAX }],
                    [right.as_ptr() as u64, if scalar[1] { 0 } else { u64::MAX }],
                ];
                for selection in [None, Some(picked.as_slice()), Some(&[][..])] {
                    let mut sum = vec![0xdead; n];
                    let mut overflow = vec![0xbeef; n];
                    let sinks = [sum.as_mut_ptr(), overflow.as_mut_ptr()];
                    let (rows, count) =
                        selection.map_or((std::ptr::null(), n), |s| (s.as_ptr(), s.len()));
                    let mut runtime = Runtime::default();
                    // SAFETY: all inputs cover the selected indices, output
                    // buffers are disjoint, and broadcast words live here.
                    assert_eq!(
                        unsafe {
                            entry(&mut runtime, sources.as_ptr(), sinks.as_ptr(), rows, count)
                        },
                        0
                    );
                    for r in 0..n {
                        if selection.is_none_or(|s| s.contains(&(r as u32))) {
                            let a = left[if scalar[0] { 0 } else { r }] as i32;
                            let b = right[if scalar[1] { 0 } else { r }] as i32;
                            let (value, flag) = a.overflowing_add(b);
                            assert_eq!(sum[r], value as i64 as u64);
                            assert_eq!(overflow[r], flag as u64);
                        } else {
                            assert_eq!(sum[r], 0xdead);
                            assert_eq!(overflow[r], 0xbeef);
                        }
                    }
                }
            }
        }
    }
}
