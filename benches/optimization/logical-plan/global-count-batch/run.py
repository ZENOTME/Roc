import json, math, random, statistics, subprocess, time
from pathlib import Path

root = Path(__file__).resolve().parent
data = Path('/Users/zenotme/Project/Roc/target/parquet-snappy/data-1791083916888251000')
meta = json.loads((root / 'metadata.json').read_text())
meta['design'] = {'replicas_per_variant': 5, 'samples_per_case_engine': 10, 'warmups': 2, 'threads': [1, 4], 'batch_rows': 8192, 'rows': 4000000, 'groups': 256, 'input': str(data), 'order': 'Variant order randomized per paired process replica with seed 202610071; DF/Roc execution order alternates each timed sample; workload/thread order is fixed as in the unchanged harness.', 'scope': 'Full warm-cache Snappy Parquet SQL execution and output collection, planning/conversion excluded. Fresh timing samples for both binaries; earlier main samples are not reused.'}
rng = random.Random(202610071)
runs = {}
for replica in range(5):
    labels = ['main', 'count']; rng.shuffle(labels)
    for label in labels:
        variant = meta['variants'][label]
        out = root / label / f'process-{replica}'
        out.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        with (out / 'run.log').open('w') as log:
            r = subprocess.run([variant['binary_path'], '--logical', str(data), str(out), variant['build_label'], '10'], stdout=log, stderr=subprocess.STDOUT)
        if r.returncode:
            print((out / 'run.log').read_text()[-7000:]); raise SystemExit(r.returncode)
        report = json.loads((out / 'results.json').read_text())
        assert report['metadata']['compiled_source_sha256'] == variant['compiled_source_sha256']
        assert report['metadata']['executable_sha256'] == variant['binary_sha256']
        assert report['metadata']['runtime_checkout']['cargo_lock_sha256'] == variant['lock_sha256']
        assert report['metadata']['release_build']
        assert report['metadata']['datafusion_version'] == '55.1.0' and report['metadata']['arrow_version'] == '59.3.0'
        if runs: assert report['input_files'] == next(iter(runs.values()))['input_files']
        assert len(report['results']) == 8
        for case in report['results']: assert len(case['roc_ms']) == len(case['datafusion_ms']) == 10
        runs[(label, replica)] = report
        print(f'Completed {replica + 1}/5 {label} in {time.monotonic() - started:.1f}s', flush=True)
        print('; '.join(f"{x['workload']}/{x['threads']}t DF={x['datafusion_median_ms']:.2f} Roc={x['roc_median_ms']:.2f}ms" for x in report['results']), flush=True)

bootstrap = random.Random(202610072)
summary = {}
for workload in ['scan_only', 'filter_project', 'global_aggregate', 'grouped_aggregate']:
    for threads in [1, 4]:
        values = {label: {engine: [] for engine in ['roc', 'datafusion']} for label in ['main', 'count']}
        for label in values:
            for replica in range(5):
                case = next(x for x in runs[(label, replica)]['results'] if x['workload'] == workload and x['threads'] == threads)
                for engine in values[label]: values[label][engine].append(statistics.median(case[engine + '_ms']))
        old = statistics.geometric_mean(values['main']['roc']); new = statistics.geometric_mean(values['count']['roc'])
        old_df = statistics.geometric_mean(values['main']['datafusion']); new_df = statistics.geometric_mean(values['count']['datafusion'])
        logs = [math.log(values['count']['roc'][i] / values['main']['roc'][i]) for i in range(5)]
        intervals = sorted((1 - math.exp(statistics.mean(bootstrap.choices(logs, k=5)))) * 100 for _ in range(10000))
        summary[f'{workload}/{threads}t'] = {
            'main_roc_ms': old, 'count_roc_ms': new, 'main_datafusion_ms': old_df, 'count_datafusion_ms': new_df,
            'saved_ms': old - new, 'roc_elapsed_reduction_percent': (1 - new / old) * 100,
            'paired_process_bootstrap_95_reduction_percent': [intervals[249], intervals[9749]],
            'main_roc_gap_to_datafusion_percent': (old / old_df - 1) * 100,
            'count_roc_gap_to_datafusion_percent': (new / new_df - 1) * 100,
            'datafusion_control_change_percent': (new_df / old_df - 1) * 100,
            'process_medians_ms': values,
        }
meta['sample_counts'] = {'roc': 800, 'datafusion': 800, 'total': 1600}
meta['full_correctness_checks'] = 80
meta['statistics'] = 'Geometric mean of five process medians for each variant/case/engine. Reduction=(1-count/main)*100. Paired process log-ratio bootstrap with 10000 five-cluster resamples; intervals describe observed variation, not all system/binary-layout confounders. All native DataFusion control movements are recorded.'
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
(root / 'input-files.json').write_text(json.dumps(next(iter(runs.values()))['input_files'], indent=2) + '\n')
for threads in [1, 4]: print(json.dumps(summary[f'global_aggregate/{threads}t'], indent=2), flush=True)
