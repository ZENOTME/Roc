import hashlib, io, json, platform, subprocess, tarfile
from pathlib import Path

root = Path(__file__).resolve().parent
repo = Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
meta = json.loads((root / 'metadata.json').read_text())
summary = json.loads((root / 'summary.json').read_text())
report = json.loads((root / 'process-0/results.json').read_text())
meta['machine'] = subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip()
meta['rust'] = subprocess.check_output(['rustc', '--version'], text=True).strip()
meta['platform'] = platform.platform()
meta['datafusion'] = report['metadata']['datafusion_version']
meta['arrow'] = report['metadata']['arrow_version']
meta['date'] = '2026-10-07'
archive = subprocess.check_output(['git', 'archive', meta['main'], 'src', 'tests', 'benches', 'Cargo.toml', 'Cargo.lock'], cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    with tarfile.open(root / 'core-source.tar.gz', 'w:gz') as out:
        for member in source.getmembers():
            if member.isfile():
                out.addfile(member, source.extractfile(member))
            else:
                out.addfile(member)
with tarfile.open(root / 'local-source.tar.gz', 'w:gz') as out:
    for name in ['src', 'tests', 'integrations', 'Cargo.toml', 'Cargo.lock']:
        out.add(root / 'source' / name, arcname=name)
meta['core_source_archive_sha256'] = hashlib.sha256((root / 'core-source.tar.gz').read_bytes()).hexdigest()
meta['core_source_archive_scope'] = 'Unmodified main src, tests, benches, Cargo.toml and Cargo.lock; no DataFusion integration. Main manifests differ from the locally compiled workspace manifests, which add the harness and its locked dependencies. Core src/tests match exactly.'
meta['local_source_archive_sha256'] = hashlib.sha256((root / 'local-source.tar.gz').read_bytes()).hexdigest()
meta['local_source_archive_scope'] = 'Exact compiled core and local integration, tests and workspace manifests, retained locally only.'
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
lines = [
    '# Merged main versus DataFusion', '',
    f"Fresh measurements on October 7, 2026, after all retained core optimizations were merged. Roc main: `{meta['main']}`. The local-only harness `{meta['local_harness']}` has exactly the same `src` and `tests` Git trees as main. Deferred PRs #9, #11 and #15 are absent. Earlier cumulative and ablation samples remain historical; none are reused or relabeled here.", '',
    f"Environment: {meta['machine']}, {meta['rust']}, {meta['platform']}, DataFusion {meta['datafusion']}, Arrow {meta['arrow']}; release build. The same 4,000,000 deterministic rows in eight Snappy Parquet files, 256 Int64 groups and NULLs are read by both engines. Batch size is 8192. Each thread configuration sets both runtime worker count and DataFusion target partitions to one or four.", '',
    '## Results', '',
    'Times are geometric means of five independent process medians, each based on ten timed samples. Positive elapsed gap means Roc is slower; negative means Roc is faster. Gap = `(Roc / DataFusion - 1) * 100`. Intervals bootstrap the five paired process clusters 10,000 times; they capture observed process variation, not every system or fixed-order confounder.', '',
    '| Workload | Threads | DataFusion ms | Roc ms | Roc elapsed gap | 95% gap interval |',
    '|---|---:|---:|---:|---:|---:|',
]
for case, r in summary.items():
    workload, threads = case.split('/')
    lo, hi = r['paired_process_bootstrap_95_gap_percent']
    lines.append(f"| {workload} | {threads[:-1]} | {r['datafusion_ms']:.2f} | {r['roc_ms']:.2f} | {r['roc_elapsed_gap_percent']:+.2f}% | {lo:+.2f}% to {hi:+.2f}% |")
lines += [
    '', 'Scan and filter/project are close at one thread and faster on Roc at four threads in this setup. Grouped SUM/COUNT is slightly slower on Roc at one thread and close at four threads. Global SUM/COUNT remains the largest gap at both thread counts. This comparison measures query elapsed time; it does not establish a specific cause of the remaining aggregate gap. Follow-up attribution needs an isolated accumulator benchmark or profile on this exact source.', '',
    'These are warm-cache synthetic local Parquet queries, not a general engine ranking or TPC-H result. Int64 arithmetic and SUM remain checked in Roc. The generated values do not overflow, and results match DataFusion exactly. Float64 aggregation, high-cardinality/multiple-column grouping, joins, spilling and cold disk I/O are outside this run.', '',
    '## SQL and execution path', '',
]
for case in report['results'][:4]:
    lines += [f"**{case['workload']}**", '', '```sql', case['sql'], '```', '']
lines += [
    'Both routes start from the same DataFusion SQL and analyzed, optimized logical plan. DataFusion creates its native physical plan; the local LogicalPlanConverter binds expressions and builds the Roc operator tree. Roc uses the DataFusion table provider Parquet scan with projection and pruning. Recorded plans and session flags are in each results.json. Scan-only reads four columns; filter/project reads three; global SUM/COUNT reads one; grouped SUM/COUNT reads two. Random selectors span each row group, so pruning cannot skip substantial data in this particular filter workload.', '',
    'The filter workload still evaluates `value + 1` after filtering rows; the local converter activates the merged optional filter output-column projection so the predicate-only selector column is not filtered into the output. The integration and this activation code remain local. The production core supplies the API and kernels.', '',
    '## Timing and correctness', '',
    '- Five independently started sequential processes, four workloads, two thread counts, ten paired samples: 400 Roc plus 400 DataFusion timings, 800 total. Two warmups per engine/case. DataFusion runs first on even samples, Roc first on odd samples. Workload/thread order is fixed and recorded.',
    '- Every case in every process compares the full output schema and unordered row multiset against native DataFusion before timing. All 40 comparisons pass. Every one of the 800 timed executions checks the expected output row count.',
    '- Timers include scan/plan state reset, execution state initialization, complete output collection and Roc task cleanup. They exclude SQL/logical planning, physical planning/conversion, Roc graph construction, validation, warmups and output deallocation. Single planning observations in raw JSON are not a planning benchmark.',
    '- The operating-system page cache is warm; Parquet decoding occurs on every execution. Both engines share the same files and session settings. File size and SHA-256 identities match across all processes.',
    '- Roc release artifacts were explicitly cleaned before compilation. Build logs confirm both Roc and its local harness were rebuilt. Every process validates the compile-time source digest, captured executable digest, release flag, DataFusion/Arrow versions and Cargo.lock digest. No compilation or other benchmark overlapped timing.',
    '- Runtime Git metadata in raw reports may describe the surrounding primary checkout because the compiled source is an isolated archive. The compiled source digest, preserved binary and explicit main/harness identities are authoritative; the dirty primary checkout was not used for this build.',
    '', '## Evidence and reproduction', '',
    '- [summary.json](summary.json) contains all process medians, paired intervals and exact differences; [metadata.json](metadata.json) records commits, source/binary/archive digests, environment and method; [input-files.json](input-files.json) records the dataset identity.',
    '- process-0 through process-4 preserve full results.json (SQL, plans, settings and raw timings), samples.csv and run.log. Build and clean logs are retained. [core-source.tar.gz](core-source.tar.gz) contains unmodified main core code, tests, benches and main manifests without the integration.',
    '- The DataFusion harness, its full local-source.tar.gz and executable remain local under target/main-datafusion-current. This evidence PR adds no integration or runtime dependency to main. Full engine-comparison reproduction requires that preserved local harness; the core archive alone cannot run DataFusion comparisons.',
    '- In the original workspace, build.py extracts the recorded harness commit, checks core src/tests against recorded main, explicitly rebuilds and captures the binary. run.py uses the recorded eight-file input directory for five processes and computes the summary. document.py creates this evidence. build.py intentionally requires a fresh task directory and the recorded ledger/PR state rather than silently overwriting a previous run.',
    '- To replay the preserved binary in the local workspace: `target/main-datafusion-current/benchmark --logical target/parquet-snappy/data-1791083916888251000 NEW_OUTPUT_DIR main-291317e-current 10`. Check all recorded digests before comparing timings. Input-generation settings and deterministic generator are retained in the local parquet_compare harness and earlier benchmark evidence.',
    '',
]
(root / 'README.md').write_text('\n'.join(lines))
print('\n'.join(lines[:23]), flush=True)
