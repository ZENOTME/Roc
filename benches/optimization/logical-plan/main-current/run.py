import json, math, random, statistics, subprocess, time
from pathlib import Path

root = Path(__file__).resolve().parent
data = Path('/Users/zenotme/Project/Roc/target/parquet-snappy/data-1791083916888251000')
assert len(list(data.glob('*.parquet'))) == 8
meta = json.loads((root / 'metadata.json').read_text())
meta['design'] = {
    'replicas': 5, 'samples_per_case_per_engine': 10, 'warmups_per_engine': 2,
    'threads': [1, 4], 'batch_rows': 8192, 'rows': 4000000, 'groups': 256,
    'input': str(data), 'cache': 'warm operating-system page cache; Parquet decode occurs inside every execution',
    'order': 'Four workloads in the recorded fixed order, one thread then four threads. Engine order alternates per sample. Five sequential independent processes; no compilation or other benchmarks during timing.',
}
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
runs = []
for replica in range(5):
    out = root / f'process-{replica}'
    out.mkdir(exist_ok=True)
    started = time.monotonic()
    with (out / 'run.log').open('w') as log:
        r = subprocess.run([str(root / 'benchmark'), '--logical', str(data), str(out), meta['build_label'], '10'], stdout=log, stderr=subprocess.STDOUT)
    if r.returncode:
        print((out / 'run.log').read_text()[-7000:]); raise SystemExit(r.returncode)
    report = json.loads((out / 'results.json').read_text())
    assert report['metadata']['compiled_source_sha256'] == meta['compiled_source_sha256']
    assert report['metadata']['executable_sha256'] == meta['binary_sha256']
    assert report['metadata']['release_build']
    assert report['metadata']['datafusion_version'] == '55.1.0'
    assert report['metadata']['arrow_version'] == '59.3.0'
    assert report['metadata']['runtime_checkout']['cargo_lock_sha256'] == meta['lock_sha256']
    assert len(report['results']) == 8
    for case in report['results']:
        assert len(case['roc_ms']) == len(case['datafusion_ms']) == 10
    if runs:
        assert report['input_files'] == runs[0]['input_files']
    runs.append(report)
    print(f'Completed process {replica + 1}/5 in {time.monotonic() - started:.1f}s', flush=True)
    print('; '.join(f"{x['workload']}/{x['threads']}t DF={x['datafusion_median_ms']:.2f} Roc={x['roc_median_ms']:.2f}ms" for x in report['results']), flush=True)

rng = random.Random(20261007)
summary = {}
for workload in ['scan_only', 'filter_project', 'global_aggregate', 'grouped_aggregate']:
    for threads in [1, 4]:
        values = {engine: [] for engine in ['datafusion', 'roc']}
        for run in runs:
            case = next(x for x in run['results'] if x['workload'] == workload and x['threads'] == threads)
            for engine in values:
                values[engine].append(statistics.median(case[engine + '_ms']))
        logs = [math.log(values['roc'][i] / values['datafusion'][i]) for i in range(5)]
        intervals = sorted((math.exp(statistics.mean(rng.choices(logs, k=5))) - 1) * 100 for _ in range(10000))
        df = statistics.geometric_mean(values['datafusion'])
        roc = statistics.geometric_mean(values['roc'])
        summary[f'{workload}/{threads}t'] = {
            'datafusion_ms': df, 'roc_ms': roc, 'roc_over_datafusion': roc / df,
            'roc_elapsed_gap_percent': (roc / df - 1) * 100,
            'paired_process_bootstrap_95_gap_percent': [intervals[249], intervals[9749]],
            'process_medians_ms': values,
        }
meta['sample_counts'] = {'roc': 400, 'datafusion': 400, 'total': 800}
meta['statistics'] = 'Geometric mean of five process medians, ten timed samples per case/engine/process. Gap=(Roc/DF-1)*100; positive is slower. 10000 paired process cluster bootstrap resamples for 95% intervals; five clusters only. Intervals exclude neither fixed-order nor machine/system confounding.'
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
(root / 'input-files.json').write_text(json.dumps(runs[0]['input_files'], indent=2) + '\n')
print(json.dumps(summary, indent=2), flush=True)
