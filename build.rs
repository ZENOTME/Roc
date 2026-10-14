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

//! Compiles the precompiled kernel library with the same emitter that fuses
//! `Compile` programs, links it statically and generates its symbol table.
#[allow(dead_code)]
#[path = "src/kernel/codegen/mod.rs"]
mod codegen;

use inkwell::{
    context::Context,
    targets::{FileType, RelocMode, TargetTriple},
};
use std::{env, fmt::Write, path::PathBuf, process::Command};

fn main() -> Result<(), String> {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=src/kernel/codegen");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let triple = TargetTriple::create(&env::var("TARGET").unwrap());
    codegen::initialize()?;
    let variants = codegen::variants(&arch, &os);
    if variants.is_empty() {
        return Err(format!(
            "the kernel library supports x86_64 and aarch64 targets, not {arch}"
        ));
    }
    let mut objects = vec![];
    for (suffix, isa) in &variants {
        let cx = Context::create();
        let module = codegen::library(&cx, suffix, isa)?;
        let machine = codegen::machine(&triple, isa, RelocMode::PIC)?;
        codegen::optimize(&module, &machine, "default<O2>")?;
        let object = out.join(format!("roc_kernels_{suffix}.o"));
        machine
            .write_to_file(&module, FileType::Object, &object)
            .map_err(|e| e.to_string())?;
        objects.push(object);
    }
    let archive = out.join("libroc_kernels.a");
    let _ = std::fs::remove_file(&archive);
    let status = Command::new(env::var("AR").unwrap_or_else(|_| "ar".into()))
        .arg("crs")
        .arg(&archive)
        .args(&objects)
        .status()
        .map_err(|e| format!("cannot run ar: {e}"))?;
    if !status.success() {
        return Err(format!("ar failed: {status}"));
    }
    println!("cargo::rustc-link-search=native={}", out.display());
    println!("cargo::rustc-link-lib=static=roc_kernels");

    let mut names = codegen::catalog::all()
        .iter()
        .map(|k| k.name())
        .collect::<Vec<_>>();
    names.sort();
    let mut table = String::new();
    let suffixes = variants.iter().map(|(s, _)| format!("{s:?}")).collect::<Vec<_>>();
    writeln!(table, "pub(super) const VARIANTS: [&str; {}] = [{}];", variants.len(), suffixes.join(", ")).unwrap();
    writeln!(table, "pub(super) static NAMES: [&str; {}] = [", names.len()).unwrap();
    for name in &names {
        writeln!(table, "    {name:?},").unwrap();
    }
    writeln!(table, "];\nunsafe extern \"C\" {{").unwrap();
    for (suffix, _) in &variants {
        for name in &names {
            let symbol = codegen::symbol(name, suffix);
            writeln!(table, "    fn {symbol}(args: *const *mut u8, rows: u64) -> u64;").unwrap();
        }
    }
    writeln!(
        table,
        "}}\npub(super) static TABLES: [[KernelFn; {}]; {}] = [",
        names.len(),
        variants.len()
    )
    .unwrap();
    for (suffix, _) in &variants {
        writeln!(table, "    [").unwrap();
        for name in &names {
            writeln!(table, "        {},", codegen::symbol(name, suffix)).unwrap();
        }
        writeln!(table, "    ],").unwrap();
    }
    writeln!(table, "];").unwrap();
    std::fs::write(out.join("kernels.rs"), table).map_err(|e| e.to_string())?;
    Ok(())
}
