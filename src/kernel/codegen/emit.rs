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

//! LLVM IR of a `Group`, in explicit vectors.
//!
//! The main loop runs chunks of `C = max(L, 16)` rows as `C / L` steps of `L`
//! lanes; `L` fills a vector register with the widest lane type. Bitmaps are
//! little-endian words, so the bits of a chunk are one `iC` at byte `row / 8`.
//! Remaining rows run one at a time with one lane. Both loops emit every
//! operation through the same lane-generic code, over `<L x T>` vectors.
//!
//! A group with lazy flags first runs a pass that only ORs their lanes, and
//! reruns exactly, storing them, when one is set.
use super::ir::*;
use inkwell::{
    AddressSpace, FloatPredicate, IntPredicate,
    attributes::{Attribute, AttributeLoc},
    builder::Builder,
    context::Context,
    intrinsics::Intrinsic,
    module::Module,
    types::{BasicTypeEnum, VectorType},
    values::{
        BasicValue, BasicValueEnum, FunctionValue, IntValue, PhiValue, PointerValue, VectorValue,
    },
};
use std::collections::HashMap;

pub type R<T> = std::result::Result<T, String>;

fn err(e: impl std::fmt::Debug) -> String {
    format!("{e:?}")
}

macro_rules! t {
    ($e:expr) => {
        $e.map_err(err)?
    };
}

/// The code generation target of a function.
#[derive(Clone, Debug)]
pub struct Isa {
    pub cpu: String,
    pub features: String,
    /// Bits of a vector register.
    pub vector_bits: u32,
}

/// The rows a lane-generic step computes.
struct Rows<'c> {
    lanes: u32,
    /// The first row.
    row: IntValue<'c>,
    bits: Bits<'c>,
    /// Bitmap argument chunks or words, loaded once.
    loaded: HashMap<usize, IntValue<'c>>,
    /// Bitmap argument chunks broadcast to every lane.
    splats: HashMap<usize, VectorValue<'c>>,
}

/// Where the bits of the rows are in a bitmap.
#[derive(Clone, Copy)]
enum Bits<'c> {
    /// Bit `shift` of the chunk starting at row `base`.
    Chunk { base: IntValue<'c>, shift: u32 },
    /// Bit `shift` of word `word`.
    Word {
        word: IntValue<'c>,
        shift: IntValue<'c>,
    },
}

type Memo<'c> = HashMap<Src, VectorValue<'c>>;

struct Emit<'a, 'c> {
    cx: &'c Context,
    module: &'a Module<'c>,
    f: FunctionValue<'c>,
    b: Builder<'c>,
    g: &'a Group,
    ptrs: Vec<PointerValue<'c>>,
    scalars: Vec<Option<BasicValueEnum<'c>>>,
    lanes: u32,
    rows: IntValue<'c>,
    /// A scalar argument and whether it is known to be non-negative.
    known: Option<(usize, bool)>,
    widest: u32,
}

/// What one pass found, as values of its last block.
struct Found<'c> {
    any: Vec<IntValue<'c>>,
    all: Vec<IntValue<'c>>,
    raise: IntValue<'c>,
    sum: IntValue<'c>,
}

/// Adds `name` to `module`; see `Group` for its signature.
pub fn function<'c>(
    cx: &'c Context,
    module: &Module<'c>,
    name: &str,
    g: &Group,
    isa: &Isa,
) -> R<FunctionValue<'c>> {
    let i64t = cx.i64_type();
    let ptr = cx.ptr_type(AddressSpace::default());
    let f = module.add_function(name, i64t.fn_type(&[ptr.into(), i64t.into()], false), None);
    f.add_attribute(
        AttributeLoc::Function,
        cx.create_string_attribute("target-cpu", &isa.cpu),
    );
    if !isa.features.is_empty() {
        f.add_attribute(
            AttributeLoc::Function,
            cx.create_string_attribute("target-features", &isa.features),
        );
    }
    for attribute in ["nounwind", "nofree", "nosync"] {
        f.add_attribute(
            AttributeLoc::Function,
            cx.create_enum_attribute(Attribute::get_named_enum_kind_id(attribute), 0),
        );
    }
    let args = f.get_nth_param(0).unwrap().into_pointer_value();
    let mut e = Emit {
        cx,
        module,
        f,
        b: cx.create_builder(),
        g,
        ptrs: vec![],
        scalars: vec![],
        lanes: 1,
        rows: f.get_nth_param(1).unwrap().into_int_value(),
        known: None,
        widest: 8,
    };
    e.widest = e.widest();
    e.lanes = (isa.vector_bits / e.widest).clamp(1, 64);
    let entry = cx.append_basic_block(f, "entry");
    e.b.position_at_end(entry);
    for (i, arg) in g.args.iter().enumerate() {
        // SAFETY: the caller passes one pointer per argument.
        let at = t!(unsafe {
            e.b.build_in_bounds_gep(ptr, args, &[i64t.const_int(i as u64, false)], "")
        });
        let p = t!(e.b.build_load(ptr, at, &format!("arg{i}"))).into_pointer_value();
        e.ptrs.push(p);
        e.scalars.push(match *arg {
            Arg::Scalar(ty) => {
                let v = t!(e.b.build_load(e.elem(ty), p, ""));
                v.as_instruction_value()
                    .unwrap()
                    .set_alignment(ty.align())
                    .map_err(err)?;
                Some(v)
            }
            _ => None,
        });
    }
    match e.sign_split() {
        // An overflow check against a scalar is one comparison once its
        // sign is known, so each sign gets its own loops.
        Some(arg) => {
            let c = e.scalars[arg].unwrap().into_int_value();
            let non_negative =
                t!(e.b.build_int_compare(IntPredicate::SGE, c, c.get_type().const_zero(), ""));
            let (positive, negative) = (
                cx.append_basic_block(f, "non_negative"),
                cx.append_basic_block(f, "negative"),
            );
            t!(e.b.build_conditional_branch(non_negative, positive, negative));
            for (block, sign) in [(positive, true), (negative, false)] {
                e.b.position_at_end(block);
                e.known = Some((arg, sign));
                e.body()?;
            }
        }
        None => e.body()?,
    }
    Ok(f)
}

fn phi_int<'c>(p: PhiValue<'c>) -> IntValue<'c> {
    p.as_basic_value().into_int_value()
}

impl<'c> Emit<'_, 'c> {
    /// The passes and the return, from the current block.
    fn body(&mut self) -> R<()> {
        let (cx, g, f) = (self.cx, self.g, self.f);
        let i64t = cx.i64_type();
        let reported = g.reports.len().min(MAX_REPORTS);
        if g.lazy.iter().all(|l| !g.reports[..reported].contains(l)) {
            let found = self.pass(true)?;
            let result = self.result(&found)?;
            t!(self.b.build_return(Some(&result)));
            return Ok(());
        }
        let fast = self.pass(false)?;
        let fast_result = self.result(&fast)?;
        let mut set = cx.bool_type().const_zero();
        for (j, src) in g.reports[..reported].iter().enumerate() {
            if g.lazy.contains(src) {
                set = t!(self.b.build_or(set, fast.any[j], ""));
            }
        }
        let fast_end = self.b.get_insert_block().unwrap();
        let exact = cx.append_basic_block(f, "exact");
        let done = cx.append_basic_block(f, "done");
        t!(self.b.build_conditional_branch(set, exact, done));
        self.b.position_at_end(exact);
        let found = self.pass(true)?;
        let exact_result = self.result(&found)?;
        let exact_end = self.b.get_insert_block().unwrap();
        t!(self.b.build_unconditional_branch(done));
        self.b.position_at_end(done);
        let result = t!(self.b.build_phi(i64t, "result"));
        result.add_incoming(&[(&fast_result, fast_end), (&exact_result, exact_end)]);
        t!(self.b.build_return(Some(&result.as_basic_value())));
        Ok(())
    }

    /// A signed scalar argument checked by AddOverflow or SubOverflow.
    fn sign_split(&self) -> Option<usize> {
        let scalar = |src: Src| match src {
            Src::Arg(i) if matches!(self.g.args[i], Arg::Scalar(t) if t.is_signed()) => Some(i),
            _ => None,
        };
        self.g.nodes.iter().find_map(|node| match node.op {
            Op::AddOverflow => scalar(node.args[1]).or_else(|| scalar(node.args[0])),
            Op::SubOverflow => scalar(node.args[1]),
            _ => None,
        })
    }

    /// Emits the block and row loops from the current block. Without
    /// `exact`, lazy flags are only ORed and neither stored nor reported as
    /// true on every row.
    fn pass(&mut self, exact: bool) -> R<Found<'c>> {
        let (cx, g, lanes, n) = (self.cx, self.g, self.lanes, self.rows);
        let chunk = self.chunk();
        let i64t = cx.i64_type();
        let ct = cx.custom_width_int_type(chunk);
        let bit = cx.bool_type();
        let zero = i64t.const_zero();
        let mask_ty = self.ints(1, lanes);
        let sum_ty = i64t.vec_type(lanes);
        let reported = g.reports.len().min(MAX_REPORTS);
        let lazy = |src: &Src| !exact && g.lazy.contains(src);
        let packed = |src: &Src| !lazy(src);
        let log = i64t.const_int(chunk.trailing_zeros() as u64, false);
        let chunks = t!(self.b.build_right_shift(n, log, false, "chunks"));
        let entry = self.b.get_insert_block().unwrap();

        // Chunks of rows.
        let head = cx.append_basic_block(self.f, "chunk");
        let body = cx.append_basic_block(self.f, "chunk_body");
        let tail_entry = cx.append_basic_block(self.f, "tail_entry");
        t!(self.b.build_unconditional_branch(head));
        self.b.position_at_end(head);
        let index = t!(self.b.build_phi(i64t, "c"));
        let raise = t!(self.b.build_phi(mask_ty, "raise"));
        let mut any = vec![];
        let mut all = vec![];
        for src in &g.reports[..reported] {
            if lazy(src) {
                any.push(t!(self.b.build_phi(mask_ty, "lazy")));
            } else {
                any.push(t!(self.b.build_phi(ct, "any")));
                all.push(t!(self.b.build_phi(ct, "all")));
            }
        }
        let sum = t!(self.b.build_phi(sum_ty, "sum"));
        let iv = phi_int(index);
        let more = t!(self.b.build_int_compare(IntPredicate::ULT, iv, chunks, ""));
        t!(self.b.build_conditional_branch(more, body, tail_entry));
        self.b.position_at_end(body);
        let base = t!(self.b.build_left_shift(iv, log, "base"));
        let mut pending: Vec<(Src, Vec<VectorValue<'c>>)> = vec![];
        let mut raise_v = raise.as_basic_value().into_vector_value();
        let mut lazy_v: Vec<Option<VectorValue<'c>>> = vec![None; reported];
        let mut sum_v = sum.as_basic_value().into_vector_value();
        let mut loaded = HashMap::new();
        let mut splats = HashMap::new();
        for k in 0..chunk / lanes {
            let mut rows = Rows {
                lanes,
                row: t!(self
                    .b
                    .build_int_add(base, i64t.const_int((k * lanes) as u64, false), "")),
                bits: Bits::Chunk {
                    base,
                    shift: k * lanes,
                },
                loaded: std::mem::take(&mut loaded),
                splats: std::mem::take(&mut splats),
            };
            let mut memo = Memo::new();
            let collect =
                |src: Src, v: VectorValue<'c>, pending: &mut Vec<(Src, Vec<VectorValue<'c>>)>| {
                    match pending.iter_mut().find(|(s, _)| *s == src) {
                        Some((_, vs)) if vs.len() as u32 > k => {}
                        Some((_, vs)) => vs.push(v),
                        None => pending.push((src, vec![v])),
                    }
                };
            for &(src, out) in &g.stores {
                let v = self.get(src, &mut rows, &mut memo)?;
                if g.ty(src) != Ty::Bool {
                    self.store_column(out, v, &rows)?;
                } else if packed(&src) {
                    collect(src, v, &mut pending);
                }
            }
            raise_v = t!(self
                .b
                .build_or(raise_v, self.raised(&mut rows, &mut memo)?, ""));
            for (j, src) in g.reports[..reported].iter().enumerate() {
                let v = self.get(*src, &mut rows, &mut memo)?;
                if lazy(src) {
                    let old = lazy_v[j].unwrap_or(any[j].as_basic_value().into_vector_value());
                    lazy_v[j] = Some(t!(self.b.build_or(old, v, "")));
                } else {
                    collect(*src, v, &mut pending);
                }
            }
            if let Some((value, mask)) = g.sum {
                let v = self.get(value, &mut rows, &mut memo)?;
                let m = self.get(mask, &mut rows, &mut memo)?;
                let m = t!(self.b.build_int_s_extend(m, v.get_type(), ""));
                sum_v = t!(self
                    .b
                    .build_int_add(sum_v, t!(self.b.build_and(v, m, "")), ""));
            }
            loaded = rows.loaded;
            splats = rows.splats;
        }
        let mut word = HashMap::new();
        for (src, vs) in &pending {
            word.insert(*src, self.pack(vs)?);
        }
        for &(src, out) in &g.stores {
            if g.ty(src) == Ty::Bool && packed(&src) {
                let at = self.chunk_ptr(out, base)?;
                let store = t!(self.b.build_store(at, word[&src]));
                store.set_alignment(chunk / 8).map_err(err)?;
            }
        }
        let mut any_next = vec![];
        let mut all_next = vec![];
        let mut a = 0;
        for (j, src) in g.reports[..reported].iter().enumerate() {
            if lazy(src) {
                any_next.push(lazy_v[j].unwrap().as_basic_value_enum());
            } else {
                any_next.push(
                    t!(self.b.build_or(phi_int(any[j]), word[src], "")).as_basic_value_enum(),
                );
                all_next.push(t!(self.b.build_and(phi_int(all[a]), word[src], "")));
                a += 1;
            }
        }
        let next = t!(self.b.build_int_add(iv, i64t.const_int(1, false), ""));
        let body_end = self.b.get_insert_block().unwrap();
        t!(self.b.build_unconditional_branch(head));
        index.add_incoming(&[(&zero, entry), (&next, body_end)]);
        raise.add_incoming(&[(&mask_ty.const_zero(), entry), (&raise_v, body_end)]);
        let mut a = 0;
        for (j, src) in g.reports[..reported].iter().enumerate() {
            if lazy(src) {
                any[j].add_incoming(&[(&mask_ty.const_zero(), entry), (&any_next[j], body_end)]);
            } else {
                any[j].add_incoming(&[(&ct.const_zero(), entry), (&any_next[j], body_end)]);
                all[a].add_incoming(&[(&ct.const_all_ones(), entry), (&all_next[a], body_end)]);
                a += 1;
            }
        }
        sum.add_incoming(&[(&sum_ty.const_zero(), entry), (&sum_v, body_end)]);

        // Remaining rows, one lane at a time; their bits are set or cleared
        // in place, as earlier rows of their word are already stored.
        self.b.position_at_end(tail_entry);
        let start = t!(self.b.build_left_shift(chunks, log, "tail"));
        let thead = cx.append_basic_block(self.f, "tail_head");
        let tbody = cx.append_basic_block(self.f, "tail_body");
        let exit = cx.append_basic_block(self.f, "exit");
        t!(self.b.build_unconditional_branch(thead));
        self.b.position_at_end(thead);
        let row = t!(self.b.build_phi(i64t, "r"));
        let traise = t!(self.b.build_phi(bit, "traise"));
        let tany = (0..reported)
            .map(|_| self.b.build_phi(bit, "tany").map_err(err))
            .collect::<R<Vec<_>>>()?;
        let tall = (0..reported)
            .map(|_| self.b.build_phi(bit, "tall").map_err(err))
            .collect::<R<Vec<_>>>()?;
        let tsum = t!(self.b.build_phi(i64t, "tsum"));
        let rv = phi_int(row);
        let more = t!(self.b.build_int_compare(IntPredicate::ULT, rv, n, ""));
        t!(self.b.build_conditional_branch(more, tbody, exit));
        self.b.position_at_end(tbody);
        let word_index = t!(self
            .b
            .build_right_shift(rv, i64t.const_int(6, false), false, ""));
        let shift = t!(self.b.build_and(rv, i64t.const_int(63, false), ""));
        let mut rows = Rows {
            lanes: 1,
            row: rv,
            bits: Bits::Word {
                word: word_index,
                shift,
            },
            loaded: HashMap::new(),
            splats: HashMap::new(),
        };
        let mut memo = Memo::new();
        let lane = |e: &Self, v: VectorValue<'c>| -> R<IntValue<'c>> {
            Ok(t!(e.b.build_extract_element(v, i64t.const_zero(), "")).into_int_value())
        };
        for &(src, out) in &g.stores {
            let v = self.get(src, &mut rows, &mut memo)?;
            if g.ty(src) != Ty::Bool {
                self.store_column(out, v, &rows)?;
            } else if packed(&src) {
                let flag = t!(self.b.build_int_z_extend(lane(self, v)?, i64t, ""));
                let flag = t!(self.b.build_left_shift(flag, shift, ""));
                let clear = t!(self.b.build_not(
                    t!(self.b.build_left_shift(i64t.const_int(1, false), shift, "")),
                    ""
                ));
                let at = self.word_ptr(out, word_index)?;
                let old = t!(self.b.build_load(i64t, at, "")).into_int_value();
                let new = t!(self
                    .b
                    .build_or(t!(self.b.build_and(old, clear, "")), flag, ""));
                t!(self.b.build_store(at, new));
            }
        }
        let raised = lane(self, self.raised(&mut rows, &mut memo)?)?;
        let traise_v = t!(self.b.build_or(phi_int(traise), raised, ""));
        let mut tany_v = vec![];
        let mut tall_v = vec![];
        for (j, src) in g.reports[..reported].iter().enumerate() {
            let v = lane(self, self.get(*src, &mut rows, &mut memo)?)?;
            tany_v.push(t!(self.b.build_or(phi_int(tany[j]), v, "")));
            tall_v.push(t!(self.b.build_and(phi_int(tall[j]), v, "")));
        }
        let mut tsum_v = phi_int(tsum);
        if let Some((value, mask)) = g.sum {
            let v = self.get(value, &mut rows, &mut memo)?;
            let m = self.get(mask, &mut rows, &mut memo)?;
            let picked =
                t!(self.b.build_select(m, v, v.get_type().const_zero(), "")).into_vector_value();
            tsum_v = t!(self.b.build_int_add(tsum_v, lane(self, picked)?, ""));
        }
        let tnext = t!(self.b.build_int_add(rv, i64t.const_int(1, false), ""));
        let tbody_end = self.b.get_insert_block().unwrap();
        t!(self.b.build_unconditional_branch(thead));
        row.add_incoming(&[(&start, tail_entry), (&tnext, tbody_end)]);
        traise.add_incoming(&[(&bit.const_zero(), tail_entry), (&traise_v, tbody_end)]);
        tsum.add_incoming(&[(&zero, tail_entry), (&tsum_v, tbody_end)]);
        for j in 0..reported {
            tany[j].add_incoming(&[(&bit.const_zero(), tail_entry), (&tany_v[j], tbody_end)]);
            tall[j].add_incoming(&[(&bit.const_all_ones(), tail_entry), (&tall_v[j], tbody_end)]);
        }

        self.b.position_at_end(exit);
        let mut found = Found {
            any: vec![],
            all: vec![],
            raise: self.reduce_or(raise.as_basic_value().into_vector_value())?,
            sum: zero,
        };
        found.raise = t!(self.b.build_or(found.raise, phi_int(traise), ""));
        let mut a = 0;
        for (j, src) in g.reports[..reported].iter().enumerate() {
            let tail_any = phi_int(tany[j]);
            if lazy(src) {
                let v = self.reduce_or(any[j].as_basic_value().into_vector_value())?;
                found.any.push(t!(self.b.build_or(v, tail_any, "")));
                found.all.push(bit.const_zero());
            } else {
                let v = t!(self.b.build_int_compare(
                    IntPredicate::NE,
                    phi_int(any[j]),
                    ct.const_zero(),
                    ""
                ));
                found.any.push(t!(self.b.build_or(v, tail_any, "")));
                let full = t!(self.b.build_int_compare(
                    IntPredicate::EQ,
                    phi_int(all[a]),
                    ct.const_all_ones(),
                    ""
                ));
                found
                    .all
                    .push(t!(self.b.build_and(full, phi_int(tall[j]), "")));
                a += 1;
            }
        }
        if g.sum.is_some() {
            let reduce = self.intrinsic("llvm.vector.reduce.add", &[sum_ty.into()])?;
            let total = self.call(reduce, &[sum.as_basic_value()])?.into_int_value();
            found.sum = t!(self.b.build_int_add(total, phi_int(tsum), ""));
        }
        Ok(found)
    }

    /// Whether a raise condition holds, as lanes. Conditions that AND a
    /// Boolean argument, such as a validity, are ORed before one AND.
    fn raised(&self, rows: &mut Rows<'c>, memo: &mut Memo<'c>) -> R<VectorValue<'c>> {
        let g = self.g;
        // Per (raise mask, AND-ed argument): the remaining conditions.
        type Masks = (Option<Src>, Option<Src>);
        let mut groups: Vec<(Masks, Vec<Src>)> = vec![];
        for &(cond, mask) in &g.raises {
            let (inner, arg) = match cond {
                Src::Node(n) if g.nodes[n].op == Op::And => match g.nodes[n].args[..] {
                    [x, a @ Src::Arg(_)] | [a @ Src::Arg(_), x] => (x, Some(a)),
                    _ => (cond, None),
                },
                _ => (cond, None),
            };
            match groups.iter_mut().find(|(k, _)| *k == (mask, arg)) {
                Some((_, conds)) => conds.push(inner),
                None => groups.push(((mask, arg), vec![inner])),
            }
        }
        let mut raised = self.splat_int(1, rows.lanes, 0);
        for ((mask, arg), conds) in groups {
            let mut v = self.get(conds[0], rows, memo)?;
            for c in &conds[1..] {
                v = t!(self.b.build_or(v, self.get(*c, rows, memo)?, ""));
            }
            for m in [mask, arg].into_iter().flatten() {
                v = t!(self.b.build_and(v, self.get(m, rows, memo)?, ""));
            }
            raised = t!(self.b.build_or(raised, v, ""));
        }
        Ok(raised)
    }

    /// The chunk of a bitmap argument, loaded once per chunk.
    fn chunk_bits(&self, arg: usize, rows: &mut Rows<'c>) -> R<IntValue<'c>> {
        let Bits::Chunk { base, .. } = rows.bits else {
            return Err("a chunk read outside the main loop".into());
        };
        if let Some(w) = rows.loaded.get(&arg) {
            return Ok(*w);
        }
        let ct = self.cx.custom_width_int_type(self.chunk());
        let at = self.chunk_ptr(arg, base)?;
        let w = t!(self.b.build_load(ct, at, "")).into_int_value();
        w.as_instruction_value()
            .unwrap()
            .set_alignment(self.chunk() / 8)
            .map_err(err)?;
        rows.loaded.insert(arg, w);
        Ok(w)
    }

    /// Rows per main loop iteration.
    fn chunk(&self) -> u32 {
        self.lanes.max(16)
    }

    /// The bitmap chunk of the rows from `base`.
    fn chunk_ptr(&self, arg: usize, base: IntValue<'c>) -> R<PointerValue<'c>> {
        let i64t = self.cx.i64_type();
        let byte = t!(self
            .b
            .build_right_shift(base, i64t.const_int(3, false), false, ""));
        // SAFETY: bitmaps cover every row of the window.
        Ok(t!(unsafe {
            self.b
                .build_in_bounds_gep(self.cx.i8_type(), self.ptrs[arg], &[byte], "")
        }))
    }

    /// The function result of a pass.
    fn result(&self, found: &Found<'c>) -> R<IntValue<'c>> {
        let i64t = self.cx.i64_type();
        if self.g.sum.is_some() {
            return Ok(found.sum);
        }
        let mut result = i64t.const_zero();
        let mut flags: Vec<(IntValue<'c>, u64)> = vec![];
        for (j, (any, all)) in found.any.iter().zip(&found.all).enumerate() {
            flags.push((*any, 2 * j as u64));
            flags.push((*all, 2 * j as u64 + 1));
        }
        if !self.g.raises.is_empty() {
            flags.push((found.raise, 63));
        }
        for (flag, bit) in flags {
            let flag = t!(self.b.build_int_z_extend(flag, i64t, ""));
            let flag = t!(self
                .b
                .build_left_shift(flag, i64t.const_int(bit, false), ""));
            result = t!(self.b.build_or(result, flag, ""));
        }
        Ok(result)
    }

    fn reduce_or(&self, v: VectorValue<'c>) -> R<IntValue<'c>> {
        let bits = t!(self
            .b
            .build_bit_cast(v, self.cx.custom_width_int_type(self.lanes), ""))
        .into_int_value();
        Ok(t!(self.b.build_int_compare(
            IntPredicate::NE,
            bits,
            bits.get_type().const_zero(),
            ""
        )))
    }

    /// The bits of a chunk's Boolean lanes. Lanes are widened to the lane
    /// width, which comparisons produce, and narrowed to bytes as one vector:
    /// a form every backend packs well.
    fn pack(&self, vs: &[VectorValue<'c>]) -> R<IntValue<'c>> {
        let wide = self.ints(self.widest, self.lanes);
        let vs = vs
            .iter()
            .map(|v| self.b.build_int_s_extend(*v, wide, "").map_err(err))
            .collect::<R<Vec<_>>>()?;
        let mut v = self.concat(&vs)?;
        let chunk = self.chunk();
        if self.widest > 8 {
            v = t!(self.b.build_int_truncate(v, self.ints(8, chunk), ""));
        }
        let set =
            t!(self
                .b
                .build_int_compare(IntPredicate::NE, v, self.splat_int(8, chunk, 0), ""));
        Ok(t!(self
            .b
            .build_bit_cast(set, self.cx.custom_width_int_type(chunk), ""))
        .into_int_value())
    }

    fn concat(&self, vs: &[VectorValue<'c>]) -> R<VectorValue<'c>> {
        let i32t = self.cx.i32_type();
        let mut vs = vs.to_vec();
        while vs.len() > 1 {
            let mut next = vec![];
            for pair in vs.chunks(2) {
                let n = pair[0].get_type().get_size();
                let mask = (0..2 * n)
                    .map(|i| i32t.const_int(i as u64, false))
                    .collect::<Vec<_>>();
                next.push(t!(self.b.build_shuffle_vector(
                    pair[0],
                    pair[1],
                    VectorType::const_vector(&mask),
                    ""
                )));
            }
            vs = next;
        }
        Ok(vs[0])
    }
    /// Bits of the widest lane type, at least 8.
    fn widest(&self) -> u32 {
        let g = self.g;
        let mut widest = 8;
        let mut see = |t: Ty| {
            if t != Ty::Bool {
                widest = widest.max(t.bits());
            }
        };
        for arg in &g.args {
            if let Arg::Column(t) | Arg::Scalar(t) | Arg::Output(t) = *arg {
                see(t);
            }
        }
        for (i, node) in g.nodes.iter().enumerate() {
            see(g.ty(Src::Node(i)));
            for &a in &node.args {
                see(g.ty(a));
            }
            if let Op::DivMagic | Op::RemMagic | Op::Cast(_) | Op::CastInvalid(_) = node.op {
                see(Ty::I64);
            }
        }
        if g.sum.is_some() {
            see(Ty::I64);
        }
        widest
    }

    fn elem(&self, t: Ty) -> BasicTypeEnum<'c> {
        match t {
            Ty::F32 => self.cx.f32_type().into(),
            Ty::F64 => self.cx.f64_type().into(),
            t => self.cx.custom_width_int_type(t.bits()).into(),
        }
    }

    fn vec(&self, t: Ty, lanes: u32) -> VectorType<'c> {
        match self.elem(t) {
            BasicTypeEnum::FloatType(f) => f.vec_type(lanes),
            BasicTypeEnum::IntType(i) => i.vec_type(lanes),
            _ => unreachable!("lane types are integers or floats"),
        }
    }

    fn ints(&self, bits: u32, lanes: u32) -> VectorType<'c> {
        self.cx.custom_width_int_type(bits).vec_type(lanes)
    }

    fn splat_int(&self, bits: u32, lanes: u32, value: u64) -> VectorValue<'c> {
        let i = self.cx.custom_width_int_type(bits);
        let c = i.const_int(value, false);
        VectorType::const_vector(&vec![c; lanes as usize])
    }

    /// A signed constant of any lane width, truncated to `bits`.
    fn splat_signed(&self, bits: u32, lanes: u32, value: i128) -> VectorValue<'c> {
        let i = self.cx.custom_width_int_type(bits);
        let words = [value as u64, (value >> 64) as u64];
        let c = i.const_int_arbitrary_precision(&words[..(bits as usize).div_ceil(64)]);
        VectorType::const_vector(&vec![c; lanes as usize])
    }

    fn splat(&self, v: BasicValueEnum<'c>, lanes: u32) -> R<VectorValue<'c>> {
        let ty = match v.get_type() {
            BasicTypeEnum::FloatType(f) => f.vec_type(lanes),
            BasicTypeEnum::IntType(i) => i.vec_type(lanes),
            _ => unreachable!("scalars are integers or floats"),
        };
        let i32t = self.cx.i32_type();
        let one = t!(self
            .b
            .build_insert_element(ty.get_poison(), v, i32t.const_zero(), ""));
        let mask = VectorType::const_vector(&vec![i32t.const_zero(); lanes as usize]);
        Ok(t!(self.b.build_shuffle_vector(
            one,
            ty.get_poison(),
            mask,
            ""
        )))
    }

    fn intrinsic(&self, name: &str, types: &[BasicTypeEnum<'c>]) -> R<FunctionValue<'c>> {
        Intrinsic::find(name)
            .and_then(|i| i.get_declaration(self.module, types))
            .ok_or_else(|| format!("no intrinsic {name}"))
    }

    fn call(&self, f: FunctionValue<'c>, args: &[BasicValueEnum<'c>]) -> R<BasicValueEnum<'c>> {
        let args = args.iter().map(|a| (*a).into()).collect::<Vec<_>>();
        match t!(self.b.build_call(f, &args, "")).try_as_basic_value() {
            inkwell::values::ValueKind::Basic(v) => Ok(v),
            _ => Err("intrinsic returns no value".into()),
        }
    }

    fn word_ptr(&self, arg: usize, word: IntValue<'c>) -> R<PointerValue<'c>> {
        // SAFETY: bitmaps cover every word of the window.
        Ok(t!(unsafe {
            self.b
                .build_in_bounds_gep(self.cx.i64_type(), self.ptrs[arg], &[word], "")
        }))
    }

    fn column_ptr(&self, arg: usize, t: Ty, row: IntValue<'c>) -> R<PointerValue<'c>> {
        // SAFETY: columns cover every row of the window.
        Ok(t!(unsafe {
            self.b
                .build_in_bounds_gep(self.elem(t), self.ptrs[arg], &[row], "")
        }))
    }

    fn store_column(&self, out: usize, v: VectorValue<'c>, rows: &Rows<'c>) -> R<()> {
        let Arg::Output(t) = self.g.args[out] else {
            return Err("a value stored to a non-column output".into());
        };
        let at = self.column_ptr(out, t, rows.row)?;
        let store = t!(self.b.build_store(at, v));
        store.set_alignment(t.align()).map_err(err)?;
        Ok(())
    }

    fn get(&self, src: Src, rows: &mut Rows<'c>, memo: &mut Memo<'c>) -> R<VectorValue<'c>> {
        if let Some(v) = memo.get(&src) {
            return Ok(*v);
        }
        let v = self.compute(src, rows, memo)?;
        memo.insert(src, v);
        Ok(v)
    }

    fn compute(&self, src: Src, rows: &mut Rows<'c>, memo: &mut Memo<'c>) -> R<VectorValue<'c>> {
        let lanes = rows.lanes;
        match src {
            Src::Imm(t, bits) => {
                let v = self.splat_int(t.bits(), lanes, bits);
                if t.is_float() {
                    Ok(t!(self.b.build_bit_cast(v, self.vec(t, lanes), "")).into_vector_value())
                } else {
                    Ok(v)
                }
            }
            Src::Arg(i) => match self.g.args[i] {
                Arg::Column(t) => {
                    let at = self.column_ptr(i, t, rows.row)?;
                    let v = t!(self.b.build_load(self.vec(t, lanes), at, ""));
                    v.as_instruction_value()
                        .unwrap()
                        .set_alignment(t.align())
                        .map_err(err)?;
                    Ok(v.into_vector_value())
                }
                Arg::Scalar(_) => self.splat(self.scalars[i].unwrap(), lanes),
                Arg::Bitmap => {
                    // Lanes of 16 bits or more hold a bit position of the
                    // chunk: broadcast it and test one bit per lane.
                    if let Bits::Chunk { shift, .. } = rows.bits
                        && self.widest >= 16
                    {
                        let et = self.cx.custom_width_int_type(self.widest);
                        let splat = match rows.splats.get(&i) {
                            Some(v) => *v,
                            None => {
                                let whole = self.chunk_bits(i, rows)?;
                                let whole = if self.widest > whole.get_type().get_bit_width() {
                                    t!(self.b.build_int_z_extend(whole, et, ""))
                                } else {
                                    t!(self.b.build_int_truncate_or_bit_cast(whole, et, ""))
                                };
                                let v = self.splat(whole.into(), lanes)?;
                                rows.splats.insert(i, v);
                                v
                            }
                        };
                        let probes = (0..lanes)
                            .map(|l| et.const_int(1u64 << (shift + l), false))
                            .collect::<Vec<_>>();
                        let probes = VectorType::const_vector(&probes);
                        let hit = t!(self.b.build_and(splat, probes, ""));
                        return Ok(t!(self.b.build_int_compare(
                            IntPredicate::NE,
                            hit,
                            et.vec_type(lanes).const_zero(),
                            ""
                        )));
                    }
                    let (whole, shift) = match rows.bits {
                        Bits::Chunk { shift, .. } => {
                            let whole = self.chunk_bits(i, rows)?;
                            (whole, whole.get_type().const_int(shift as u64, false))
                        }
                        Bits::Word { word, shift } => {
                            let at = self.word_ptr(i, word)?;
                            (
                                t!(self.b.build_load(self.cx.i64_type(), at, "")).into_int_value(),
                                shift,
                            )
                        }
                    };
                    let bits = t!(self.b.build_right_shift(whole, shift, false, ""));
                    let bits = if whole.get_type().get_bit_width() == lanes {
                        bits
                    } else {
                        t!(self.b.build_int_truncate(
                            bits,
                            self.cx.custom_width_int_type(lanes),
                            ""
                        ))
                    };
                    Ok(
                        t!(self.b.build_bit_cast(bits, self.ints(1, lanes), ""))
                            .into_vector_value(),
                    )
                }
                Arg::Output(_) | Arg::OutputBitmap => Err("an output read as an operand".into()),
            },
            Src::Node(n) => {
                let node = &self.g.nodes[n];
                let args = node
                    .args
                    .iter()
                    .map(|a| self.get(*a, rows, memo))
                    .collect::<R<Vec<_>>>()?;
                self.op(node, &args, lanes)
            }
        }
    }

    fn op(&self, node: &Node, a: &[VectorValue<'c>], lanes: u32) -> R<VectorValue<'c>> {
        let b = &self.b;
        let g = self.g;
        let t = g.ty(node.args[0]);
        let bits = t.bits();
        let zeros = |bits: u32| self.splat_int(bits, lanes, 0);
        let all = |bits: u32, v: u64| self.splat_int(bits, lanes, v);
        let min = || self.splat_signed(bits, lanes, i128::MIN >> (128 - bits.max(1)));
        let icmp = |p, x: VectorValue<'c>, y: VectorValue<'c>| -> R<VectorValue<'c>> {
            Ok(t!(b.build_int_compare(p, x, y, "")))
        };
        let imm_sign = |i: usize| match node.args[i] {
            Src::Imm(_, v) if bits <= 64 => Some(((v << (64 - bits)) as i64) >= 0),
            Src::Imm(..) => None,
            Src::Arg(a) => self.known.filter(|(k, _)| *k == a).map(|(_, s)| s),
            _ => None,
        };
        Ok(match node.op {
            Op::Add | Op::Sub | Op::Mul if t.is_float() => match node.op {
                Op::Add => t!(b.build_float_add(a[0], a[1], "")),
                Op::Sub => t!(b.build_float_sub(a[0], a[1], "")),
                _ => t!(b.build_float_mul(a[0], a[1], "")),
            },
            Op::Add => t!(b.build_int_add(a[0], a[1], "")),
            Op::Sub => t!(b.build_int_sub(a[0], a[1], "")),
            Op::Mul => t!(b.build_int_mul(a[0], a[1], "")),
            Op::AddOverflow => {
                let r = a[2];
                if !t.is_signed() {
                    icmp(IntPredicate::ULT, r, a[0])?
                } else if let Some(non_negative) = imm_sign(1) {
                    // x + c leaves x downwards iff c >= 0.
                    icmp(
                        if non_negative {
                            IntPredicate::SLT
                        } else {
                            IntPredicate::SGT
                        },
                        r,
                        a[0],
                    )?
                } else if let Some(non_negative) = imm_sign(0) {
                    icmp(
                        if non_negative {
                            IntPredicate::SLT
                        } else {
                            IntPredicate::SGT
                        },
                        r,
                        a[1],
                    )?
                } else {
                    let x = t!(b.build_xor(a[0], r, ""));
                    let y = t!(b.build_xor(a[1], r, ""));
                    icmp(IntPredicate::SLT, t!(b.build_and(x, y, "")), zeros(bits))?
                }
            }
            Op::SubOverflow => {
                let r = a[2];
                if !t.is_signed() {
                    icmp(IntPredicate::ULT, a[0], a[1])?
                } else if let Some(non_negative) = imm_sign(1) {
                    // x - c leaves x upwards iff c >= 0.
                    icmp(
                        if non_negative {
                            IntPredicate::SGT
                        } else {
                            IntPredicate::SLT
                        },
                        r,
                        a[0],
                    )?
                } else {
                    let x = t!(b.build_xor(a[0], a[1], ""));
                    let y = t!(b.build_xor(a[0], r, ""));
                    icmp(IntPredicate::SLT, t!(b.build_and(x, y, "")), zeros(bits))?
                }
            }
            Op::MulOverflow => {
                let name = if t.is_signed() {
                    "llvm.smul.with.overflow"
                } else {
                    "llvm.umul.with.overflow"
                };
                let f = self.intrinsic(name, &[a[0].get_type().into()])?;
                let pair = self
                    .call(f, &[a[0].into(), a[1].into()])?
                    .into_struct_value();
                t!(b.build_extract_value(pair, 1, "")).into_vector_value()
            }
            Op::Neg if t.is_float() => t!(b.build_float_neg(a[0], "")),
            Op::Neg => t!(b.build_int_sub(zeros(bits), a[0], "")),
            Op::NegOverflow => icmp(IntPredicate::EQ, a[0], min())?,
            Op::Div | Op::Rem if t.is_float() => {
                if node.op == Op::Div {
                    t!(b.build_float_div(a[0], a[1], ""))
                } else {
                    t!(b.build_float_rem(a[0], a[1], ""))
                }
            }
            Op::Div | Op::Rem => {
                let mut bad = icmp(IntPredicate::EQ, a[1], zeros(bits))?;
                if t.is_signed() {
                    bad = t!(b.build_or(bad, self.div_overflow(a, bits, lanes)?, ""));
                }
                let d = t!(b.build_select(bad, all(bits, 1), a[1], "")).into_vector_value();
                match (node.op, t.is_signed()) {
                    (Op::Div, true) => t!(b.build_int_signed_div(a[0], d, "")),
                    (Op::Div, false) => t!(b.build_int_unsigned_div(a[0], d, "")),
                    (_, true) => t!(b.build_int_signed_rem(a[0], d, "")),
                    _ => t!(b.build_int_unsigned_rem(a[0], d, "")),
                }
            }
            Op::DivZero => icmp(IntPredicate::EQ, a[0], zeros(bits))?,
            Op::DivOverflow => {
                if t.is_signed() {
                    self.div_overflow(a, bits, lanes)?
                } else {
                    zeros(1)
                }
            }
            Op::DivMagic | Op::RemMagic => self.magic(node.op, t, a, lanes)?,
            Op::Compare(p) => {
                let (x, y, signed) = if t.is_float() {
                    (
                        self.float_key(a[0], bits, lanes)?,
                        self.float_key(a[1], bits, lanes)?,
                        true,
                    )
                } else if t == Ty::Bool {
                    let i8v = self.ints(8, lanes);
                    (
                        t!(b.build_int_z_extend(a[0], i8v, "")),
                        t!(b.build_int_z_extend(a[1], i8v, "")),
                        false,
                    )
                } else {
                    (a[0], a[1], t.is_signed())
                };
                let p = match (p, signed) {
                    (Pred::Eq, _) => IntPredicate::EQ,
                    (Pred::Ne, _) => IntPredicate::NE,
                    (Pred::Lt, true) => IntPredicate::SLT,
                    (Pred::Lt, false) => IntPredicate::ULT,
                    (Pred::Le, true) => IntPredicate::SLE,
                    (Pred::Le, false) => IntPredicate::ULE,
                    (Pred::Gt, true) => IntPredicate::SGT,
                    (Pred::Gt, false) => IntPredicate::UGT,
                    (Pred::Ge, true) => IntPredicate::SGE,
                    (Pred::Ge, false) => IntPredicate::UGE,
                };
                icmp(p, x, y)?
            }
            Op::And => t!(b.build_and(a[0], a[1], "")),
            Op::Or => t!(b.build_or(a[0], a[1], "")),
            Op::Not => t!(b.build_not(a[0], "")),
            Op::Select => t!(b.build_select(a[0], a[1], a[2], "")).into_vector_value(),
            Op::Shr => {
                let n = t!(b.build_and(a[1], all(bits, bits as u64 - 1), ""));
                t!(b.build_right_shift(a[0], n, t.is_signed(), ""))
            }
            Op::Abs if t.is_float() => {
                let f = self.intrinsic("llvm.fabs", &[a[0].get_type().into()])?;
                self.call(f, &[a[0].into()])?.into_vector_value()
            }
            Op::Abs if t.is_signed() => {
                let negative = icmp(IntPredicate::SLT, a[0], zeros(bits))?;
                let negated = t!(b.build_int_sub(zeros(bits), a[0], ""));
                t!(b.build_select(negative, negated, a[0], "")).into_vector_value()
            }
            Op::Abs => a[0],
            Op::Round => {
                let f = self.intrinsic("llvm.round", &[a[0].get_type().into()])?;
                self.call(f, &[a[0].into()])?.into_vector_value()
            }
            Op::Cast(to) => self.cast(a[0], t, to, lanes)?.0,
            Op::CastInvalid(to) => self.cast(a[0], t, to, lanes)?.1,
        })
    }

    fn div_overflow(&self, a: &[VectorValue<'c>], bits: u32, lanes: u32) -> R<VectorValue<'c>> {
        let b = &self.b;
        let min = t!(b.build_int_compare(
            IntPredicate::EQ,
            a[0],
            self.splat_signed(bits, lanes, i128::MIN >> (128 - bits)),
            ""
        ));
        let neg = t!(b.build_int_compare(
            IntPredicate::EQ,
            a[1],
            self.splat_signed(bits, lanes, -1),
            ""
        ));
        Ok(t!(b.build_and(min, neg, "")))
    }

    /// Maps IEEE floats to integers ordered like `total_cmp`.
    fn float_key(&self, v: VectorValue<'c>, bits: u32, lanes: u32) -> R<VectorValue<'c>> {
        let b = &self.b;
        let x = t!(b.build_bit_cast(v, self.ints(bits, lanes), "")).into_vector_value();
        let sign =
            t!(b.build_right_shift(x, self.splat_int(bits, lanes, (bits - 1) as u64), true, ""));
        let mask = t!(b.build_right_shift(sign, self.splat_int(bits, lanes, 1), false, ""));
        Ok(t!(b.build_xor(x, mask, "")))
    }

    /// The 64-bit form of an integer or Boolean lane.
    /// Extends an integer or Boolean lane to `bits`, at least its own width.
    fn widen(&self, v: VectorValue<'c>, t: Ty, bits: u32, lanes: u32) -> R<VectorValue<'c>> {
        let w = self.ints(bits, lanes);
        Ok(if t.bits() == bits {
            v
        } else if t.is_signed() {
            t!(self.b.build_int_s_extend(v, w, ""))
        } else {
            t!(self.b.build_int_z_extend(v, w, ""))
        })
    }

    fn narrow(&self, v: VectorValue<'c>, t: Ty, lanes: u32) -> R<VectorValue<'c>> {
        Ok(
            if v.get_type()
                .get_element_type()
                .into_int_type()
                .get_bit_width()
                == t.bits()
            {
                v
            } else {
                t!(self.b.build_int_truncate(v, self.ints(t.bits(), lanes), ""))
            },
        )
    }

    /// The high 64 bits of a 64 x 64 bit product.
    fn mulh(
        &self,
        x: VectorValue<'c>,
        y: VectorValue<'c>,
        signed: bool,
        lanes: u32,
    ) -> R<VectorValue<'c>> {
        let b = &self.b;
        let w = self.ints(128, lanes);
        let (x, y) = if signed {
            (
                t!(b.build_int_s_extend(x, w, "")),
                t!(b.build_int_s_extend(y, w, "")),
            )
        } else {
            (
                t!(b.build_int_z_extend(x, w, "")),
                t!(b.build_int_z_extend(y, w, "")),
            )
        };
        let p = t!(b.build_int_mul(x, y, ""));
        let p = t!(b.build_right_shift(p, self.splat_int(128, lanes, 64), false, ""));
        Ok(t!(b.build_int_truncate(p, self.ints(64, lanes), "")))
    }

    fn magic(&self, op: Op, t: Ty, a: &[VectorValue<'c>], lanes: u32) -> R<VectorValue<'c>> {
        let b = &self.b;
        let n = self.widen(a[0], t, 64, lanes)?;
        let (m, s, more) = (a[2], a[3], a[4]);
        let q = if t.is_signed() {
            let q = self.mulh(m, n, true, lanes)?;
            let q = t!(b.build_int_add(q, t!(b.build_and(n, more, "")), ""));
            let q = t!(b.build_right_shift(q, s, true, ""));
            let negative = t!(b.build_right_shift(n, self.splat_int(64, lanes, 63), false, ""));
            t!(b.build_int_add(q, negative, ""))
        } else {
            let hi = self.mulh(m, n, false, lanes)?;
            let plain = t!(b.build_right_shift(hi, s, false, ""));
            let half = t!(b.build_right_shift(
                t!(b.build_int_sub(n, hi, "")),
                self.splat_int(64, lanes, 1),
                false,
                ""
            ));
            let added = t!(b.build_right_shift(t!(b.build_int_add(half, hi, "")), s, false, ""));
            let more =
                t!(b.build_int_compare(IntPredicate::NE, more, self.splat_int(64, lanes, 0), ""));
            t!(b.build_select(more, added, plain, "")).into_vector_value()
        };
        let v = if op == Op::DivMagic {
            q
        } else {
            let d = self.widen(a[1], t, 64, lanes)?;
            t!(b.build_int_sub(n, t!(b.build_int_mul(q, d, "")), ""))
        };
        self.narrow(v, t, lanes)
    }

    /// `(value, invalid)` of a conversion.
    fn cast(
        &self,
        v: VectorValue<'c>,
        from: Ty,
        to: Ty,
        lanes: u32,
    ) -> R<(VectorValue<'c>, VectorValue<'c>)> {
        let b = &self.b;
        let ok = self.splat_int(1, lanes, 0);
        if from == to {
            return Ok((v, ok));
        }
        if to == Ty::Bool {
            let bit = if from.is_float() {
                let zero = t!(b.build_bit_cast(
                    self.splat_int(from.bits(), lanes, 0),
                    self.vec(from, lanes),
                    ""
                ))
                .into_vector_value();
                t!(b.build_float_compare(FloatPredicate::UNE, v, zero, ""))
            } else {
                t!(b.build_int_compare(
                    IntPredicate::NE,
                    v,
                    self.splat_int(from.bits(), lanes, 0),
                    ""
                ))
            };
            return Ok((bit, ok));
        }
        let f64v = self.vec(Ty::F64, lanes);
        // Integer conversions run at the wider of 64 bits and both types.
        let wide = 64.max(from.bits()).max(to.bits());
        if to.is_float() {
            let target = self.vec(to, lanes);
            let value = if from.is_float() {
                if to.bits() > from.bits() {
                    t!(b.build_float_ext(v, target, ""))
                } else {
                    t!(b.build_float_trunc(v, target, ""))
                }
            } else {
                let raw = self.widen(v, from, 64.max(from.bits()), lanes)?;
                if from.is_signed() {
                    t!(b.build_signed_int_to_float(raw, target, ""))
                } else {
                    t!(b.build_unsigned_int_to_float(raw, target, ""))
                }
            };
            return Ok((value, ok));
        }
        let bits = to.bits();
        let signed = to.is_signed();
        if from.is_float() {
            let iv = self.ints(wide, lanes);
            let float_value = if from == Ty::F32 {
                t!(b.build_float_ext(v, f64v, ""))
            } else {
                v
            };
            let name = if signed {
                "llvm.fptosi.sat"
            } else {
                "llvm.fptoui.sat"
            };
            let convert = self.intrinsic(name, &[iv.into(), f64v.into()])?;
            let raw = self
                .call(convert, &[float_value.into()])?
                .into_vector_value();
            let trunc = self.intrinsic("llvm.trunc", &[f64v.into()])?;
            let whole = self.call(trunc, &[float_value.into()])?.into_vector_value();
            let float = |x: f64| -> R<VectorValue<'c>> {
                Ok(
                    t!(b.build_bit_cast(self.splat_int(64, lanes, x.to_bits()), f64v, ""))
                        .into_vector_value(),
                )
            };
            let lo = if signed {
                -(2f64).powi(bits as i32 - 1)
            } else {
                0.0
            };
            let hi = (2f64).powi(bits as i32 - i32::from(signed));
            let low = t!(b.build_float_compare(FloatPredicate::OLT, whole, float(lo)?, ""));
            let high = t!(b.build_float_compare(FloatPredicate::OGE, whole, float(hi)?, ""));
            let nan = t!(b.build_float_compare(FloatPredicate::UNO, float_value, float_value, ""));
            let bad = t!(b.build_or(t!(b.build_or(low, high, "")), nan, ""));
            return Ok((self.narrow(raw, to, lanes)?, bad));
        }
        let raw = self.widen(v, from, wide, lanes)?;
        let value = self.narrow(raw, to, lanes)?;
        if from == Ty::Bool {
            return Ok((value, ok));
        }
        let c = |x: i128| self.splat_signed(wide, lanes, x);
        let icmp = |p, x: VectorValue<'c>, y: VectorValue<'c>| -> R<VectorValue<'c>> {
            Ok(t!(b.build_int_compare(p, x, y, "")))
        };
        // The range of the target, as values of the wide type.
        let (high, low) = if signed {
            let max = i128::MAX >> (128 - bits);
            let min = i128::MIN >> (128 - bits);
            let p = if from.is_signed() {
                IntPredicate::SGT
            } else {
                IntPredicate::UGT
            };
            (
                icmp(p, raw, c(max))?,
                from.is_signed()
                    .then(|| icmp(IntPredicate::SLT, raw, c(min)))
                    .transpose()?,
            )
        } else {
            let max = (u128::MAX >> (128 - bits)) as i128;
            (
                icmp(IntPredicate::UGT, raw, c(max))?,
                from.is_signed()
                    .then(|| icmp(IntPredicate::SLT, raw, c(0)))
                    .transpose()?,
            )
        };
        let bad = match low {
            Some(low) => t!(b.build_or(high, low, "")),
            None => high,
        };
        Ok((value, bad))
    }
}
