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

//! Liveness, placement and emission order of one fused program.
//!
//! Pure kernels are total, so they may run in any descendant of their domain
//! that contains all uses: each is placed in the lowest common ancestor of its
//! uses. Effectful kernels (RaiseIf, GroupLookup, AggregateUpdate) stay in
//! their declared domain. Items are emitted in builder order, except that an
//! item never precedes the Filter that creates its placement domain.
//!
//! Both execution modes run the same plan: generated code fuses it into one
//! row loop, zero-compile execution runs each item over a whole chunk.
use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Item {
    Kernel(usize),
    Output,
}

pub(crate) struct Plan {
    pub items: Vec<(Item, DomainId)>,
    /// Indexed by StateId; None for states of dead kernels.
    pub states: Vec<Option<StateKind>>,
    /// Per output column: the input whose array it reuses.
    pub aliases: Vec<Option<usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateKind {
    Group,
    Accumulator,
}

impl Plan {
    pub fn new(graph: &KernelGraph) -> Self {
        let parents = graph.domains.iter().map(|d| d.parent).collect::<Vec<_>>();
        let mut depth = vec![0usize; parents.len()];
        for (i, parent) in parents.iter().enumerate() {
            if let Some(p) = parent {
                depth[i] = depth[p.0] + 1;
            }
        }
        let lca = |a: Option<DomainId>, b: DomainId| -> DomainId {
            let Some(mut a) = a else { return b };
            let mut b = b;
            while depth[a.0] > depth[b.0] {
                a = parents[a.0].unwrap();
            }
            while depth[b.0] > depth[a.0] {
                b = parents[b.0].unwrap();
            }
            while a != b {
                a = parents[a.0].unwrap();
                b = parents[b.0].unwrap();
            }
            a
        };
        let mut uses: Vec<Option<DomainId>> = vec![None; graph.variables.len()];
        let mut domain_live = vec![false; parents.len()];
        let mark = |domain_live: &mut Vec<bool>, mut d: DomainId| loop {
            domain_live[d.0] = true;
            match parents[d.0] {
                Some(p) => d = p,
                None => break,
            }
        };
        if let Some(output) = &graph.output {
            mark(&mut domain_live, output.domain);
            for v in output.bindings.iter().flat_map(|b| b.variables()) {
                uses[v.0] = Some(lca(uses[v.0], output.domain));
            }
        }
        let mut live = vec![false; graph.kernels.len()];
        let mut place = vec![DomainId::ROOT; graph.kernels.len()];
        for (k, (domain, kernel)) in graph.kernels.iter().enumerate().rev() {
            match kernel {
                Kernel::Scalar(s) => {
                    let p = if s.has_effect() {
                        *domain
                    } else {
                        let mut p = None;
                        for v in s.outputs() {
                            if let Some(u) = uses[v.0] {
                                p = Some(lca(p, u));
                            }
                        }
                        let Some(p) = p else { continue };
                        p
                    };
                    live[k] = true;
                    place[k] = p;
                    mark(&mut domain_live, p);
                    for v in s.inputs() {
                        uses[v.0] = Some(lca(uses[v.0], p));
                    }
                }
                Kernel::Domain(DomainKernel::Filter { predicate, output }) => {
                    if domain_live[output.0] {
                        live[k] = true;
                        place[k] = *domain;
                        uses[predicate.0] = Some(lca(uses[predicate.0], *domain));
                    }
                }
                Kernel::Domain(_) => unreachable!("rejected by the builder"),
            }
        }
        // The builder position of the Filter creating each domain. Sort keys are
        // `(position, rank, tie)`: an item sunk below its builder position into
        // a later domain takes the creating Filter's position with rank 1, so it
        // follows that Filter and keeps builder order among sunk items.
        let mut created = vec![-1i64; parents.len()];
        for (k, (_, kernel)) in graph.kernels.iter().enumerate() {
            if let Kernel::Domain(d) = kernel {
                created[d.output().0] = k as i64;
            }
        }
        let mut items = vec![];
        for k in 0..graph.kernels.len() {
            if live[k] {
                let p = place[k];
                let k = k as i64;
                let key = if k >= created[p.0] {
                    (k, 0, k)
                } else {
                    (created[p.0], 1, k)
                };
                items.push((key, Item::Kernel(k as usize), p));
            }
        }
        if let Some(output) = &graph.output {
            let end = graph.kernels.len() as i64;
            items.push(((end, 0, end), Item::Output, output.domain));
        }
        items.sort_by_key(|(key, _, _)| *key);
        let mut states = vec![None; graph.states.len()];
        for (k, (_, kernel)) in graph.kernels.iter().enumerate() {
            match kernel {
                Kernel::Scalar(ScalarKernel::GroupLookup { state, .. }) if live[k] => {
                    states[state.0] = Some(StateKind::Group)
                }
                Kernel::Scalar(ScalarKernel::AggregateUpdate { state, .. }) if live[k] => {
                    states[state.0] = Some(StateKind::Accumulator)
                }
                _ => {}
            }
        }
        let root_output = graph
            .output
            .as_ref()
            .is_none_or(|o| o.domain == DomainId::ROOT);
        let aliases = graph.output.as_ref().map_or(vec![], |o| {
            o.bindings
                .iter()
                .enumerate()
                .map(|(j, b)| {
                    // Casts between byte types share bindings; the array is
                    // reused only when its type is the output's.
                    let alias = graph.inputs.iter().position(|i| {
                        i.binding == *b && i.ty.data_type == *o.schema.field(j).data_type()
                    });
                    alias.filter(|_| root_output)
                })
                .collect()
        });
        Self {
            items: items.into_iter().map(|(_, i, p)| (i, p)).collect(),
            states,
            aliases,
        }
    }
}
