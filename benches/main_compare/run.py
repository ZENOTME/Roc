#!/usr/bin/env python3
"""Freeze two source trees, build isolated release binaries, alternate measurements.
Usage: python3 benches/main_compare/run.py [--output PATH] [--rows N] [--samples N] [--replicas N]
The current engine is measured in every execution mode: compile (runs of
kernels fused into LLVM functions when the program is prepared) and
zero-compile (kernels precompiled into the binary).
Preparation includes operator construction, graph lowering and JIT compilation.
Execution includes state initialization, scheduling, all output collection and cleanup.
Input generation, full correctness checking and output deallocation are excluded.
Reference engines (engines.py: DuckDB, DataFusion) run the same workloads with
--python, one process per replica like the Roc variants.
On Linux, --cpus1/--cpus4 pin every measured process with taskset.
The current engine builds with LLVM 19 (LLVM_SYS_191_PREFIX); --rustflags applies
to both builds, for example a library path LLVM's system libraries need.
"""
import argparse, hashlib, json, os, pathlib, platform, re, shutil, statistics, subprocess, tarfile, time
ROOT = pathlib.Path(__file__).resolve().parents[2]
MODES = ['compile', 'zero-compile']
ENGINES = ['duckdb', 'duckdb-arrow', 'datafusion']
def machine():
    if platform.system() == 'Darwin':
        return command(['sysctl','-n','machdep.cpu.brand_string','hw.memsize','hw.physicalcpu'])
    return command(['lscpu']) + '\n' + command(['uname','-a'])
def engine_versions(python):
    return command([python,'-c','import duckdb,datafusion,pyarrow;print(duckdb.__version__,datafusion.__version__,pyarrow.__version__)'])
def command(args, **kw):
    return subprocess.check_output(args, cwd=ROOT, **kw).decode().strip()
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def snapshot_hash(path):
    h = hashlib.sha256()
    for p in sorted(path.rglob('*')):
        if p.is_file():
            h.update(str(p.relative_to(path)).encode() + b'\0' + p.read_bytes())
    return h.hexdigest()
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', default=f'target/main-compare-{time.strftime("%Y%m%d-%H%M%S")}')
    p.add_argument('--rows', type=int, default=4_000_000)
    p.add_argument('--samples', type=int, default=10)
    p.add_argument('--replicas', type=int, default=3)
    p.add_argument('--resume-build', action='store_true', help='Reuse frozen snapshots after a build failure; no measured results may exist')
    p.add_argument('--baseline', default='origin/main', help='Git ref or pinned commit to compare')
    p.add_argument('--modes', default=','.join(MODES), help='Comma-separated current-engine modes')
    p.add_argument('--python', help='Python with duckdb, datafusion and pyarrow; enables reference engines')
    p.add_argument('--engines', default=','.join(ENGINES), help='Comma-separated reference engines')
    p.add_argument('--cpus1', help='taskset CPU list for one thread (Linux)')
    p.add_argument('--cpus4', help='taskset CPU list for four threads (Linux)')
    p.add_argument('--rustflags', help='RUSTFLAGS of both builds, such as the LLVM system library path')
    a = p.parse_args()
    if min(a.rows, a.samples, a.replicas) <= 0: p.error('rows, samples, replicas must be positive')
    modes = a.modes.split(',')
    engines = a.engines.split(',') if a.python else []
    cpus = {1: a.cpus1, 4: a.cpus4}
    out = (ROOT / a.output).resolve(); out.mkdir(parents=True, exist_ok=a.resume_build)
    if any(out.glob('*-t*-r*.json')): raise RuntimeError('refusing to overwrite measurements')
    metadata = {'baseline':command(['git','rev-parse',a.baseline]), 'current_head':command(['git','rev-parse','HEAD']), 'current_status':command(['git','status','--short']), 'rustc':command(['rustc','-Vv']), 'machine':machine(), 'args':vars(a), 'builds':{}}
    if engines: metadata['engine_versions'] = engine_versions(a.python)
    (out/'current.patch').write_bytes(subprocess.check_output(['git','diff','--binary','HEAD'],cwd=ROOT))
    binaries = {}
    for label in ['main','current']:
        source = out/label/'source'
        frozen = source.exists()
        source.mkdir(parents=True, exist_ok=a.resume_build)
        if not frozen and label == 'main':
            archive = out/'main.tar'
            with archive.open('wb') as f: subprocess.run(['git','archive',metadata['baseline']],cwd=ROOT,stdout=f,check=True)
            with tarfile.open(archive) as t: t.extractall(source)
        elif not frozen:
            files = subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=ROOT).decode().split('\0')
            for name in set(files)-{''}:
                src = ROOT/name
                if src.is_file():
                    dst = source/name; dst.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(src,dst)
        harness = out/label/'harness'; (harness/'src').mkdir(parents=True, exist_ok=a.resume_build)
        shutil.copy2(ROOT/'benches/main_compare/bench.rs',harness/'src/main.rs')
        (harness/'Cargo.toml').write_text('''[workspace]
[package]
name = "roc-main-compare"
version = "0.1.0"
edition = "2024"
[features]
main-engine = []
[dependencies]
roc = { path = "../source" }
arrow = { version = "=59.2.0", default-features = false }
asyncband = { version = "0.7.3", default-features = false, features = ["shutdown", "mpmc"] }
futures = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "sync", "time"] }
serde_json = "1"
mimalloc = { version = "=0.1.48", default-features = false }
''')
        # Seed both resolutions with exactly the current lockfile's external versions.
        shutil.copy2(ROOT/'Cargo.lock',harness/'Cargo.lock')
        env = os.environ.copy(); env.pop('RUSTFLAGS',None); env.pop('CARGO_ENCODED_RUSTFLAGS',None)
        if a.rustflags: env['RUSTFLAGS'] = a.rustflags
        env['CARGO_TARGET_DIR'] = str(out/label/'build')
        cmd = ['cargo','build','--release','--manifest-path',str(harness/'Cargo.toml')]
        if label == 'main': cmd += ['--features','main-engine']
        print('Building', label, flush=True)
        with (out/f'{label}-build.log').open('w') as log: subprocess.run(cmd,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        binary = out/label/'build/release/roc-main-compare'; binaries[label] = binary
        metadata['builds'][label] = {'source_sha256':snapshot_hash(source),'harness_sha256':digest(harness/'src/main.rs'),'binary_sha256':digest(binary),'lock_sha256':digest(harness/'Cargo.lock'),'command':cmd,'rustflags':a.rustflags}
        (out/'metadata.json').write_text(json.dumps(metadata,indent=2))
    # Compare resolved external versions before accepting any measurements.
    packages = {}
    for label in ['main', 'current']:
        blocks = (out/label/'harness/Cargo.lock').read_text().split('[[package]]')[1:]
        packages[label] = {
            re.search(r'\nname = "([^"]+)"', block)[1]:
            re.search(r'\nversion = "([^"]+)"', block)[1]
            for block in blocks
        }
    shared = packages['main'].keys() & packages['current'].keys()
    differences = {name: [packages['main'][name], packages['current'][name]]
                   for name in shared if packages['main'][name] != packages['current'][name]}
    if differences: raise RuntimeError(f'External dependency versions differ: {differences}')
    (out/'dependency-check.json').write_text(json.dumps({
        'common_package_count': len(shared),
        'common_package_version_differences': differences,
    }, indent=2))
    # Each variant is (label, binary, mode argument); the current engine runs once per mode.
    # Each variant is (label, argv prefix); argv continues with rows, samples, threads.
    variants = [('main', [str(binaries['main'])], [])] + [(m, [str(binaries['current'])], [m]) for m in modes]
    variants += [(e, [a.python, str(ROOT/'benches/main_compare/engines.py'), e], []) for e in engines]
    labels = [v[0] for v in variants]
    for replica in range(a.replicas):
        for threads in [1,4]:
            # Rotate the order so no variant always runs first or last.
            shift = replica % len(variants)
            for label, prefix, mode in variants[shift:] + variants[:shift]:
                name = f'{label}-t{threads}-r{replica+1}'
                print('Measuring',name,flush=True)
                env = os.environ.copy(); env['RAYON_NUM_THREADS'] = str(threads)
                argv = prefix + [str(a.rows),str(a.samples),str(threads)] + mode
                if cpus[threads]: argv = ['taskset','-c',cpus[threads]] + argv
                with (out/f'{name}.json').open('w') as stdout, (out/f'{name}.log').open('w') as stderr:
                    subprocess.run(argv,env=env,stdout=stdout,stderr=stderr,check=True)
    summaries=[]
    for threads in [1,4]:
        reports={label:[json.loads((out/f'{label}-t{threads}-r{r+1}.json').read_text()) for r in range(a.replicas)] for label in labels}
        for i,case in enumerate(reports['main'][0]['cases']):
            row={'case':case['case'],'threads':threads}
            for label,rs in reports.items():
                for field in ['prepare','execute']:
                    values=[r['cases'][i][f'{field}_median_ms'] for r in rs]
                    # Unsupported cases report no time.
                    if None in values:
                        row[f'{label}_{field}_ms']=None; continue
                    row[f'{label}_{field}_ms']=statistics.median(values)
                    row[f'{label}_{field}_range_ms']=[min(values),max(values)]
            # Variants without a time for this case are left out of ratios.
            timed = lambda ls: [l for l in ls if row[f'{l}_execute_ms'] is not None]
            for label in timed(modes):
                if row['main_execute_ms'] is not None:
                    row[f'{label}_over_main']=row[f'{label}_execute_ms']/row['main_execute_ms']
            # The fastest semantically comparable reference: DataFusion wraps on overflow.
            references = timed(['main'] + engines)
            best = min(references, key=lambda l: row[f'{l}_execute_ms'], default=None)
            row['best_reference'] = best
            row['best_reference_ms'] = row[f'{best}_execute_ms'] if best else None
            checked = [l for l in references if l != 'datafusion']
            row['best_checked_reference'] = min(checked, key=lambda l: row[f'{l}_execute_ms'], default=None)
            for label in timed(modes):
                if best:
                    row[f'{label}_over_best']=row[f'{label}_execute_ms']/row['best_reference_ms']
            summaries.append(row)
    (out/'summary.json').write_text(json.dumps({'cases':summaries},indent=2))
    print(f"{'case':<18}{'thr':>4}" + ''.join(f'{l+" exec":>20}' for l in labels) + f"{'best':>14}")
    for row in summaries:
        cell = lambda v: f"{v:>20.3f}" if v is not None else f"{'-':>20}"
        print(f"{row['case']:<18}{row['threads']:>4}" + ''.join(cell(row[l+'_execute_ms']) for l in labels) + f"{row['best_reference'] or '-':>14}")
    print('Results:',out)
if __name__ == '__main__': main()
