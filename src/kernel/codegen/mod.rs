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

//! The single definition of kernel code. The build script compiles the
//! precompiled library from it; `Compile` programs fuse their groups with it
//! at run time. Depends on nothing in the crate.
pub mod catalog;
pub mod emit;
pub mod ir;

use inkwell::{
    OptimizationLevel,
    context::Context,
    module::Module,
    passes::PassBuilderOptions,
    targets::{CodeModel, InitializationConfig, RelocMode, Target, TargetMachine, TargetTriple},
};

pub use emit::{Isa, R};

/// The precompiled kernel variants of a target: `(symbol suffix, isa)`,
/// widest first; every CPU of the architecture runs the last. Empty for
/// architectures without an LLVM backend in the build.
pub fn variants(arch: &str, os: &str) -> Vec<(&'static str, Isa)> {
    let isa = |cpu: &str, features: &str, vector_bits| Isa {
        cpu: cpu.into(),
        features: features.into(),
        vector_bits,
    };
    match arch {
        "x86_64" => vec![
            ("avx2", isa("x86-64-v3", "", 256)),
            ("sse2", isa("x86-64", "", 128)),
        ],
        "aarch64" if os == "macos" => vec![("neon", isa("apple-m1", "", 128))],
        "aarch64" => vec![("neon", isa("generic", "+neon", 128))],
        _ => vec![],
    }
}

pub fn symbol(name: &str, suffix: &str) -> String {
    format!("roc_kernel_{name}_{suffix}")
}

pub fn initialize() -> R<()> {
    Target::initialize_native(&InitializationConfig::default())?;
    Target::initialize_x86(&InitializationConfig::default());
    Target::initialize_aarch64(&InitializationConfig::default());
    Ok(())
}

pub fn machine(triple: &TargetTriple, isa: &Isa, reloc: RelocMode) -> R<TargetMachine> {
    let target = Target::from_triple(triple).map_err(|e| e.to_string())?;
    target
        .create_target_machine(
            triple,
            &isa.cpu,
            &isa.features,
            OptimizationLevel::Aggressive,
            reloc,
            CodeModel::Default,
        )
        .ok_or_else(|| format!("no LLVM target machine for {}", triple))
}

/// Verifies and optimizes a module of emitted functions with a pass
/// pipeline, such as `default<O2>`.
pub fn optimize(module: &Module<'_>, machine: &TargetMachine, passes: &str) -> R<()> {
    module.set_triple(&machine.get_triple());
    module.set_data_layout(&machine.get_target_data().get_data_layout());
    module.verify().map_err(|e| e.to_string())?;
    module
        .run_passes(passes, machine, PassBuilderOptions::create())
        .map_err(|e| e.to_string())
}

/// A module with every precompiled kernel of one variant.
pub fn library<'c>(cx: &'c Context, suffix: &str, isa: &Isa) -> R<Module<'c>> {
    let module = cx.create_module(&format!("roc_kernels_{suffix}"));
    for key in catalog::all() {
        emit::function(cx, &module, &symbol(&key.name(), suffix), &key.group(), isa)?;
    }
    Ok(module)
}
