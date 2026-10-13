#!/usr/bin/env python3
"""Recheck frozen binaries with per-worker-count CPU affinity; no rebuild."""
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time
import tomllib

BASE = Path('/home/ze/benchmarks/roc-modes-20261013/repo/target/mylinux-modes')
LLVM = Path('/home/ze/benchmarks/roc-llvm-20261013')
OUT = LLVM / 'results'
MODES = ['main', 'cranelift-compile', 'cranelift-generated', 'llvm-compile', 'llvm-generated', 'zero-native']
EXEC_MODES = ['main', 'compile', 'zero-generated', 'compile', 'zero-generated', 'zero-native']
REPLICAS, SAMPLES, ROWS = 5, 20, 4_000_000

def main():
    OUT.mkdir(exist_ok=False)
    binaries = {m: (LLVM / 'harness/target/release/roc-main-compare' if m.startswith('llvm-') else BASE / ('main' if m == 'main' else 'current') / 'build/release/roc-main-compare') for m in MODES}
    exec_modes = dict(zip(MODES, EXEC_MODES))
    metadata = json.loads((BASE / 'metadata.json').read_text())
    for m in ['main', 'current']:
        binary = BASE / m / 'build/release/roc-main-compare'
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == metadata['builds'][m]['binary_sha256']
    def packages(lock):
        result = {}
        for p in tomllib.loads(lock.read_text())['package']:
            if 'source' in p:
                result.setdefault(p['name'], set()).add((p['version'], p['source']))
        return result
    old = packages(BASE / 'current/harness/Cargo.lock')
    new = packages(LLVM / 'harness/Cargo.lock')
    # These older versions belonged only to Cranelift/gimli/regalloc2; their
    # removal is the intended backend change, not a runtime dependency update.
    removed_codegen_versions = {'foldhash': {'0.1.5'}, 'hashbrown': {'0.15.5', '0.16.1'}}
    normalized_old = {k: {v for v in versions if v[0] not in removed_codegen_versions.get(k, set())} for k, versions in old.items()}
    differences = {k: [sorted(normalized_old[k]), sorted(new[k])] for k in old.keys() & new.keys() if normalized_old[k] != new[k]}
    assert not differences, differences
    metadata['llvm'] = {
        'version': subprocess.check_output([str(LLVM/'llvm19/usr/bin/llvm-config'), '--version'], text=True).strip(),
        'inkwell': '0.7.1', 'optimization': 'default<O3> + MCJIT aggressive, host CPU/features',
        'binary_sha256': hashlib.sha256(binaries['llvm-compile'].read_bytes()).hexdigest(),
        'harness_sha256': hashlib.sha256((LLVM/'harness/src/main.rs').read_bytes()).hexdigest(),
        'lock_sha256': hashlib.sha256((LLVM/'harness/Cargo.lock').read_bytes()).hexdigest(),
        'common_dependency_differences': differences,
        'removed_codegen_only_versions': {k: sorted(v) for k, v in removed_codegen_versions.items()},
        'common_dependency_names': len(old.keys() & new.keys()),
    }
    assert metadata['llvm']['harness_sha256'] == metadata['builds']['current']['harness_sha256']
    source_files = sorted([p for p in (LLVM/'repo/src').rglob('*') if p.is_file()] + [LLVM/'repo/Cargo.toml', LLVM/'repo/Cargo.lock'])
    metadata['llvm']['source_files_sha256'] = {str(p.relative_to(LLVM/'repo')): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
    metadata['confirmation'] = dict(rows=ROWS, samples=SAMPLES, replicas=REPLICAS,
        affinity={'1': [0], '4': [0, 2, 4, 6]}, load_before=os.getloadavg(),
        order='rotate variants per replica; alternate thread-count order',
        started_unix=time.time(), source_run=str(BASE))
    (OUT / 'metadata.json').write_text(json.dumps(metadata, indent=2))
    for replica in range(REPLICAS):
        for threads in ([1, 4] if replica % 2 == 0 else [4, 1]):
            cpus = '0' if threads == 1 else '0,2,4,6'
            shift = replica % len(MODES)
            for mode in MODES[shift:] + MODES[:shift]:
                name = f'{mode}-t{threads}-r{replica+1}'
                print('Measuring', name, flush=True)
                env = os.environ.copy()
                env.pop('ROC_BENCH_CASES', None)
                env['RAYON_NUM_THREADS'] = str(threads)
                args = ['taskset', '-c', cpus, str(binaries[mode]), str(ROWS), str(SAMPLES), str(threads)]
                if mode != 'main':
                    args.append(exec_modes[mode])
                with (OUT / f'{name}.json').open('w') as output, (OUT / f'{name}.log').open('w') as log:
                    subprocess.run(args, env=env, stdout=output, stderr=log, check=True)
    summary = []
    prewarm = {}
    for threads in [1, 4]:
        reports = {m: [json.loads((OUT / f'{m}-t{threads}-r{r+1}.json').read_text()) for r in range(REPLICAS)] for m in MODES}
        for mode in ['cranelift-generated', 'llvm-generated']:
            prewarm[f'{mode}-t{threads}'] = [r['prewarm_ms'] for r in reports[mode]]
        names = [c['case'] for c in reports['main'][0]['cases']]
        for rs in reports.values():
            for r in rs:
                assert r['rows'] == ROWS and r['samples'] == SAMPLES
                assert [c['case'] for c in r['cases']] == names
        for i, name in enumerate(names):
            row = {'case': name, 'threads': threads}
            for mode, rs in reports.items():
                for field in ['prepare', 'execute', 'total']:
                    values = [c[f'{field}_median_ms'] if field != 'total' else statistics.median(p + e for p, e in zip(c['prepare_ms'], c['execute_ms'])) for r in rs for c in [r['cases'][i]]]
                    row[f'{mode}_{field}_ms'] = statistics.median(values)
                    row[f'{mode}_{field}_range_ms'] = [min(values), max(values)]
                row[f'{mode}_over_main'] = row[f'{mode}_execute_ms'] / row['main_execute_ms']
            summary.append(row)
    (OUT / 'summary.json').write_text(json.dumps({'prewarm_ms': prewarm, 'cases': summary}, indent=2))
    print('Results:', OUT, flush=True)

if __name__ == '__main__':
    main()
