import json, math, random, statistics, subprocess, time
from pathlib import Path
root = Path(__file__).resolve().parent / 'focused'
meta = json.loads((root / 'metadata.json').read_text())
data = root.parent.parent / 'parquet-snappy/data-1791083916888251000'
replicas = 10
rng = random.Random(202610073)
orders = [['main', 'count'] for _ in range(replicas // 2)] + [['count', 'main'] for _ in range(replicas // 2)]
rng.shuffle(orders)
runs = {}
for replica, labels in enumerate(orders):
    for label in labels:
        v = meta['variants'][label]
        out = root / label / f'process-{replica}'; out.mkdir()
        with (out / 'run.log').open('w') as log:
            subprocess.run([v['binary_path'], '--logical', str(data), str(out), v['build_label'], '10'], stdout=log, stderr=subprocess.STDOUT, check=True)
        report = json.loads((out / 'results.json').read_text())
        assert report['metadata']['executable_sha256'] == v['binary_sha256']
        assert report['metadata']['compiled_source_sha256'] == v['compiled_source_sha256']
        assert report['metadata']['runtime_checkout']['cargo_lock_sha256'] == v['lock_sha256']
        assert report['metadata']['release_build'] and len(report['results']) == 2
        assert all(c['workload'] == 'global_aggregate' and len(c['roc_ms']) == len(c['datafusion_ms']) == 10 for c in report['results'])
        if runs: assert report['input_files'] == next(iter(runs.values()))['input_files']
        runs[(label, replica)] = report
        print(f'Focused {replica + 1}/{replicas} {label}: ' + '; '.join(f"{c['threads']}t DF={c['datafusion_median_ms']:.2f} Roc={c['roc_median_ms']:.2f}ms" for c in report['results']), flush=True)
summary = {}
bootstrap = random.Random(202610074)
for threads in [1, 4]:
    values = {label: {engine: [] for engine in ['roc', 'datafusion']} for label in ['main', 'count']}
    for label in values:
        for replica in range(replicas):
            c = next(c for c in runs[(label, replica)]['results'] if c['threads'] == threads)
            for engine in values[label]: values[label][engine].append(statistics.median(c[engine + '_ms']))
    old = statistics.geometric_mean(values['main']['roc']); new = statistics.geometric_mean(values['count']['roc'])
    old_df = statistics.geometric_mean(values['main']['datafusion']); new_df = statistics.geometric_mean(values['count']['datafusion'])
    logs = [math.log(values['count']['roc'][i] / values['main']['roc'][i]) for i in range(replicas)]
    intervals = sorted((1 - math.exp(statistics.mean(bootstrap.choices(logs, k=replicas)))) * 100 for _ in range(10000))
    summary[f'global_aggregate/{threads}t'] = {'main_roc_ms': old, 'count_roc_ms': new, 'saved_ms': old - new, 'roc_elapsed_reduction_percent': (1 - new / old) * 100, 'paired_process_bootstrap_95_reduction_percent': [intervals[249], intervals[9749]], 'main_datafusion_ms': old_df, 'count_datafusion_ms': new_df, 'main_roc_gap_to_datafusion_percent': (old / old_df - 1) * 100, 'count_roc_gap_to_datafusion_percent': (new / new_df - 1) * 100, 'datafusion_control_change_percent': (new_df / old_df - 1) * 100, 'process_medians_ms': values}
meta['design'] = {'replicas_per_variant': replicas, 'samples_per_case_engine': 10, 'threads': [1, 4], 'warmups': 2, 'order': orders, 'order_description': 'Balanced five main-first/five candidate-first process pairs, randomized with fixed seed; engine order alternates within each sample.', 'rows': 4000000, 'batch_rows': 8192, 'samples': {'roc': 400, 'datafusion': 400, 'total': 800}, 'full_correctness_checks': 40, 'statistics': 'Geometric mean of ten process medians; paired ten-process cluster bootstrap with 10000 resamples. All samples retained.'}
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2), flush=True)
