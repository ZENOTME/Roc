#!/usr/bin/env python3
"""Recheck frozen binaries with per-worker-count CPU affinity; no rebuild."""
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

BASE = Path('/home/ze/benchmarks/roc-modes-20261013/repo/target/mylinux-modes')
OUT = BASE.parent / 'mylinux-confirmation'
MODES = ['main', 'compile', 'zero-generated', 'zero-native']
REPLICAS, SAMPLES, ROWS = 5, 20, 4_000_000

def main():
    OUT.mkdir(exist_ok=False)
    binaries = {m: BASE / ('main' if m == 'main' else 'current') / 'build/release/roc-main-compare' for m in MODES}
    metadata = json.loads((BASE / 'metadata.json').read_text())
    for m in ['main', 'current']:
        binary = BASE / m / 'build/release/roc-main-compare'
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == metadata['builds'][m]['binary_sha256']
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
                    args.append(mode)
                with (OUT / f'{name}.json').open('w') as output, (OUT / f'{name}.log').open('w') as log:
                    subprocess.run(args, env=env, stdout=output, stderr=log, check=True)
    summary = []
    prewarm = {}
    for threads in [1, 4]:
        reports = {m: [json.loads((OUT / f'{m}-t{threads}-r{r+1}.json').read_text()) for r in range(REPLICAS)] for m in MODES}
        prewarm[f't{threads}'] = [r['prewarm_ms'] for r in reports['zero-generated']]
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
