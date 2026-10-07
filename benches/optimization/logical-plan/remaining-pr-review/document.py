import hashlib,json,platform,statistics,subprocess,tarfile
from pathlib import Path
root=Path(__file__).resolve().parent
meta=json.loads((root/'metadata.json').read_text());summary=json.loads((root/'summary.json').read_text())
meta['machine']=subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip()
meta['rust']=subprocess.check_output(['rustc','--version'],text=True).strip();meta['platform']=platform.platform()
meta['samples']={'roc':1120,'datafusion':1120,'total':2240}
meta['statistics']='Geometric mean of five process medians per variant/case. Paired process log ratios with 10000 five-cluster bootstrap resamples for 95% intervals. Variant order randomized per replica; native DataFusion versus Roc order alternates per timed sample. Effects are not additive.'
meta['compiler_artifact_control']='Copied path-dependency builds initially reused a Roc artifact; caught by the required Compiling roc log assertion before any timing. Final builds clean only Roc release artifacts, explicitly rebuild Roc and its DataFusion harness, capture separate binaries and verify compiled source and binary hashes in every process report. No compilation overlapped timing.'
meta['decisions']={
    '12':'Retain: replaces a handwritten scalar comparison implementation with existing Arrow kernels, net removes 34 production lines; affected SQL filter/project is about 1.8%-2.0% faster in this setup. Simplification is the principal reason; small control movements limit causal claims about timing.',
    '15':'Close/defer: no stable global Int64 SUM/COUNT query improvement at one or four threads; adds a global route, duplicates FILTER/argument preparation and creates fallback group-ID vectors per batch. No conclusion is claimed for every possible type/workload or future implementation.',
    '16':'Retain: about 4.5%-5.2% lower SQL filter/project elapsed time, removes real predicate-only column filtering work. Optional output-column selection preserves default behavior.',
    '14':'Retain: evidence documentation adds no runtime execution path; records exact measured identities and scope.',
}
for label in ['all','without12','without15','without16']:
    for kind,names in [('local-source',['src','tests','integrations','Cargo.toml','Cargo.lock']),('core-source',['src','tests','Cargo.toml','Cargo.lock'])]:
        p=root/label/(kind+'.tar.gz')
        with tarfile.open(p,'w:gz')as tar:
            for name in names:tar.add(root/label/name,arcname=name)
        meta['variants'][label][kind+'_archive_sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
(root/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
lines=['# Review of remaining optimization PRs','',f"Current source: local-only harness `{meta['local_source']}` over core PR #16 `{meta['core_heads']['16']}`, main `{meta['main']}`. Four separately compiled binaries retain all remaining optimizations or remove exactly #12, #15 or #16. The without16 variant also disables local converter fusion and column remapping so its output semantics remain equivalent.",'',f"Measured on {meta['machine']}, {meta['rust']}, Arrow 59.3.0 and DataFusion 55.1.0. The same 4,000,000 deterministic rows in eight Snappy Parquet files are used by every process; exact file hashes are recorded. SQL planning/conversion are outside timing. Full execution, output collection and Roc cleanup are timed; decoded inputs are not resident-only synthetic arrays.",'','## Decisions and affected workloads','','Positive reduction means the optimization is faster. Values are geometric means of five independent process medians, each with seven timed samples. Intervals use paired-process bootstrap.','','| PR | Workload / threads | Without ms | With ms | Reduction | 95% interval | Decision |','|---|---|---:|---:|---:|---:|---|']
for n in [12,15,16]:
    affected='global_aggregate'if n==15 else'filter_project'
    for t in [1,4]:
        r=summary[str(n)][f'{affected}/{t}t'];lo,hi=r['paired_bootstrap_95_percent']
        lines.append(f"| #{n} | {affected} / {t} | {r['without_ms']:.2f} | {r['with_ms']:.2f} | {r['reduction_percent']:.2f}% | {lo:.2f}% to {hi:.2f}% | {'Close' if n==15 else 'Retain'} |")
lines+=['','PR #12 deletes 34 net lines of handwritten scalar comparison logic and reuses Arrow. Its measured filter/project savings are modest; simplification and shared kernel semantics justify retaining it. PR #15 adds approximately 100 production lines across three files plus eight new tests, but provides no stable benefit on the measured global SUM/COUNT query; it is deferred and removed from the later stack. This does not rule out benefit for every data type or future implementation. PR #16 eliminates filtering/copying a column used only by the predicate, saving about 5.4 ms at one thread and 1.3 ms at four threads on this workload. The documentation PR #14 remains open because evidence does not add execution complexity.','','## All workloads and controls','','Unchanged workloads are controls, not optimization targets. A small bootstrap interval does not eliminate binary-layout, hash-randomization, thermal or system confounders. For example, #12 shows about 0.9% slower single-thread global aggregation even though that workload does not execute its comparison change. Treat #12 timing as a small observed difference, not a proven isolated kernel effect. No unrelated control movement is attributed to a specific algorithm.','','| Removed PR | Workload / threads | Without ms | With ms | Reduction | 95% interval |','|---|---|---:|---:|---:|---:|']
for n,cases in summary.items():
    for case,r in cases.items():
        lo,hi=r['paired_bootstrap_95_percent']
        lines.append(f"| #{n} | {case} | {r['without_ms']:.2f} | {r['with_ms']:.2f} | {r['reduction_percent']:.2f}% | {lo:.2f}% to {hi:.2f}% |")
lines+=['','## Retained stack compared with DataFusion','','The measured `without15` source is verified byte-for-byte against the retained local harness after removal, for all core source and test files. These rows use the original without15 samples; no samples or source identities are relabeled. The individual #12/#16 effects above were measured in the original all-enabled background, not rerun as a factorial experiment after removing #15.','','| Workload / threads | DataFusion ms | Retained Roc ms | DF/Roc |','|---|---:|---:|---:|']
for workload in ['scan_only','filter_project','global_aggregate','grouped_aggregate']:
    for t in [1,4]:
        vals={engine:[]for engine in ['datafusion','roc']}
        for i in range(5):
            results=json.loads((root/'without15'/f'process-{i}/results.json').read_text())['results']
            case=next(c for c in results if c['workload']==workload and c['threads']==t)
            for engine in vals:vals[engine].append(statistics.median(case[engine+'_ms']))
        df=statistics.geometric_mean(vals['datafusion']);roc=statistics.geometric_mean(vals['roc'])
        lines.append(f'| {workload}/{t}t | {df:.2f} | {roc:.2f} | {df/roc:.3f}x |')
lines+=['','This is a synthetic warm-cache local Parquet workload, not a general engine ranking. Roc retains checked integer arithmetic and SUM; generated values do not overflow, and both routes produce equal results. The global workload uses Int64 SUM and COUNT; Float64/UInt64 global SUM performance is not measured here.','','## Evidence and reproduction','','- 20 process runs: four variants times five replicas, seven samples for four workloads at one/four threads; 1120 Roc and 1120 DataFusion timing samples, 2240 total.', '- Every process checks full schema and the unordered output row multiset against native DataFusion before timing each case; every timed run checks row count. Two warm-ups per case. Engine order alternates per sample; variant order is randomized per replica.', '- Metadata records the original core/local commit identities, exact transformations, source/binary hashes, lockfile identity and machine. Every process verifies compiled source and binary hashes, and all input file hashes must match.', '- Core source archives, build logs, exact raw JSON/CSV results and scripts are published. DataFusion integration, full local-source archives and binaries remain local; it is not added to the production workspace by this documentation PR. Core archives are checksum snapshots, not standalone workspaces: their root manifests reference the local-only integration.', '- Full local reproduction uses the preserved local harness commit and build.py/run.py in the original workspace; standalone local-source archives remain under target/remaining-pr-review/<variant>/local-source.tar.gz. Data directory generation and recorded input settings appear in earlier benchmark artifacts.', '- All timing data were collected before removing PR #15. Updated retained heads and fresh post-removal correctness checks are recorded separately in core-source-verification.json; exact measured identities remain unchanged.', '']
(root/'README.md').write_text('\n'.join(lines))
print('\n'.join(lines[:16]),flush=True)
