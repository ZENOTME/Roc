#!/usr/bin/env python3
"""Freeze two source trees, build isolated release binaries, alternate measurements.
Usage: python3 benches/main_compare/run.py [--output PATH] [--rows N] [--samples N] [--replicas N]
The current engine is measured in every execution mode: compile (one fused
program per pipeline), zero-generated (cached per-kernel generated code, all
signatures prewarmed at startup) and zero-native (precompiled Rust kernels).
Preparation includes operator construction, graph lowering and JIT compilation.
Execution includes state initialization, scheduling, all output collection and cleanup.
Input generation, full correctness checking and output deallocation are excluded.
"""
import argparse, hashlib, json, os, pathlib, platform, re, shutil, statistics, subprocess, tarfile, time
ROOT = pathlib.Path(__file__).resolve().parents[2]
MODES = ['compile', 'zero-generated', 'zero-native']
def command(args, **kw):
    return subprocess.check_output(args, cwd=ROOT, **kw).decode().strip()
def machine_info():
    if platform.system() == 'Darwin':
        return command(['sysctl','-n','machdep.cpu.brand_string','hw.memsize','hw.physicalcpu'])
    if platform.system() == 'Linux':
        return command(['uname','-a']) + '\n' + command(['lscpu']) + '\n' + pathlib.Path('/proc/meminfo').read_text()
    return platform.platform()
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
    a = p.parse_args()
    if min(a.rows, a.samples, a.replicas) <= 0: p.error('rows, samples, replicas must be positive')
    out = (ROOT / a.output).resolve(); out.mkdir(parents=True, exist_ok=a.resume_build)
    if any(out.glob('*-t*-r*.json')): raise RuntimeError('refusing to overwrite measurements')
    metadata = {'baseline':command(['git','rev-parse',a.baseline]), 'current_head':command(['git','rev-parse','HEAD']), 'current_status':command(['git','status','--short']), 'rustc':command(['rustc','-Vv']), 'machine':machine_info(), 'args':vars(a), 'builds':{}}
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
''')
        # Seed both resolutions with exactly the current lockfile's external versions.
        shutil.copy2(ROOT/'Cargo.lock',harness/'Cargo.lock')
        env = os.environ.copy(); env.pop('RUSTFLAGS',None); env.pop('CARGO_ENCODED_RUSTFLAGS',None)
        env['CARGO_TARGET_DIR'] = str(out/label/'build')
        cmd = ['cargo','build','--release','--manifest-path',str(harness/'Cargo.toml')]
        if label == 'main': cmd += ['--features','main-engine']
        print('Building', label, flush=True)
        with (out/f'{label}-build.log').open('w') as log: subprocess.run(cmd,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        binary = out/label/'build/release/roc-main-compare'; binaries[label] = binary
        metadata['builds'][label] = {'source_sha256':snapshot_hash(source),'harness_sha256':digest(harness/'src/main.rs'),'binary_sha256':digest(binary),'lock_sha256':digest(harness/'Cargo.lock'),'command':cmd,'rustflags':None}
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
    variants = [('main', binaries['main'], [])] + [(m, binaries['current'], [m]) for m in MODES]
    labels = [v[0] for v in variants]
    for replica in range(a.replicas):
        for threads in [1,4]:
            # Rotate the order so no variant always runs first or last.
            shift = replica % len(variants)
            for label, binary, mode in variants[shift:] + variants[:shift]:
                name = f'{label}-t{threads}-r{replica+1}'
                print('Measuring',name,flush=True)
                env = os.environ.copy(); env['RAYON_NUM_THREADS'] = str(threads)
                with (out/f'{name}.json').open('w') as stdout, (out/f'{name}.log').open('w') as stderr:
                    subprocess.run([str(binary),str(a.rows),str(a.samples),str(threads)]+mode,env=env,stdout=stdout,stderr=stderr,check=True)
    summaries=[]
    for threads in [1,4]:
        reports={label:[json.loads((out/f'{label}-t{threads}-r{r+1}.json').read_text()) for r in range(a.replicas)] for label in labels}
        for i,case in enumerate(reports['main'][0]['cases']):
            row={'case':case['case'],'threads':threads}
            for label,rs in reports.items():
                for field in ['prepare','execute']:
                    values=[r['cases'][i][f'{field}_median_ms'] for r in rs]
                    row[f'{label}_{field}_ms']=statistics.median(values)
                    row[f'{label}_{field}_range_ms']=[min(values),max(values)]
            for label in MODES:
                row[f'{label}_over_main']=row[f'{label}_execute_ms']/row['main_execute_ms']
            summaries.append(row)
    prewarm={f't{t}':[json.loads((out/f'zero-generated-t{t}-r{r+1}.json').read_text())['prewarm_ms'] for r in range(a.replicas)] for t in [1,4]}
    (out/'summary.json').write_text(json.dumps({'prewarm_ms':prewarm,'cases':summaries},indent=2))
    print(f"{'case':<18}{'thr':>4}" + ''.join(f'{l+" prep":>22}{l+" exec":>22}' for l in labels))
    for row in summaries:
        print(f"{row['case']:<18}{row['threads']:>4}" + ''.join(f"{row[l+'_prepare_ms']:>22.3f}{row[l+'_execute_ms']:>22.3f}" for l in labels))
    print('prewarm_ms', prewarm); print('Results:',out)
if __name__ == '__main__': main()
