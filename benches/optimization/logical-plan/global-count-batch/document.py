import hashlib, io, json, platform, subprocess, tarfile
from pathlib import Path

root = Path(__file__).resolve().parent
repo = Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
meta = json.loads((root / 'metadata.json').read_text())
full_summary = json.loads((root / 'summary.json').read_text())
summary = json.loads((root / 'focused/summary.json').read_text())
focused_meta = json.loads((root / 'focused/metadata.json').read_text())
meta['date'] = '2026-10-07'
meta['machine'] = subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip()
meta['rust'] = subprocess.check_output(['rustc', '--version'], text=True).strip()
meta['platform'] = platform.platform()
meta['datafusion'] = '55.1.0'; meta['arrow'] = '59.3.0'
meta['core_release_tests_passed'] = 125
meta['primary_comparison'] = 'focused/summary.json: ten balanced-order paired processes per variant, unchanged global SUM/COUNT query measured alone; all samples retained.'
meta['all_experiment_counts'] = {'pilot_total_timings': 1600, 'complete_repeat_total_timings': 1600, 'focused_total_timings': 800, 'total_timings': 4000, 'full_correctness_checks': 200}
meta['focused'] = focused_meta
meta['new_regression_tests'] = ['Primitive sliced validity and dictionary logical NULLs match row updates across batches', 'COUNT(*) handles empty/nonempty input and COUNT DISTINCT is not rebound', 'Checked overflow reports an error and preserves the same successful prefix as row updates']
for label, sha in [('main', meta['main']), ('count', meta['candidate_core'])]:
    raw = subprocess.check_output(['git', 'archive', sha, 'src', 'tests', 'benches', 'Cargo.toml', 'Cargo.lock'], cwd=repo)
    path = root / label / 'core-source.tar.gz'
    with tarfile.open(fileobj=io.BytesIO(raw)) as source:
        with tarfile.open(path, 'w:gz') as out:
            for member in source.getmembers():
                out.addfile(member, source.extractfile(member) if member.isfile() else None)
    meta['variants'][label]['core_source_archive_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
with tarfile.open(root / 'local-source.tar.gz', 'w:gz') as out:
    for name in ['src', 'tests', 'integrations', 'Cargo.toml', 'Cargo.lock']:
        out.add(root / 'source' / name, arcname=name)
meta['candidate_local_source_archive_sha256'] = hashlib.sha256((root / 'local-source.tar.gz').read_bytes()).hexdigest()
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
lines = [
    '# Global batch COUNT ablation', '',
    f"October 7, 2026. Compare main `{meta['main']}` with candidate core `{meta['candidate_core']}` through the unchanged local-only DataFusion harness. Candidate local source: `{meta['candidate_local_harness']}`; its core source and tests match the candidate core commit exactly. Baseline local source: `{meta['baseline_local_harness']}`; its core source/tests match main. DataFusion integration and executables remain local.", '',
    '## Isolated change', '',
    'When constructing an ungrouped aggregate executor, bind ordinary COUNT to a batch update function. It computes `selected_rows - argument.logical_null_count` (or selected_rows for COUNT(*)) and uses checked addition once per batch. Grouped COUNT and COUNT DISTINCT retain the old update functions. Existing FILTER and argument evaluation are shared, including filtering before fallible argument evaluation. No SUM kernel, group-ID generation, merge, scan, projection or pipeline code changes.', '',
    'The existing group-ID vector still supplies the selected row count and is still built for every batch. This experiment deliberately measures only COUNT reduction; it does not claim to eliminate all ungrouped aggregation overhead. On overflow, COUNT reports the same error and retains the same successful-prefix count as the row loop.', '',
    '## Global SUM/COUNT query', '',
    '```sql', 'SELECT SUM(value) AS total, COUNT(value) AS count FROM t', '```', '',
    'All measurements below are fresh. Primary estimates use the unchanged global SUM/COUNT query alone at one/four threads: ten independent paired processes per variant, ten samples per case/engine, with balanced five main-first/five candidate-first pairs in randomized order. Both focused binaries rebuild the same core snapshots and apply only the identical workload-iterator filter in the local harness. This avoids preceding multi-million-row scan output validation and sorting, while retaining the same SQL, storage, timed execution path, correctness checks and configuration. Values are geometric means of ten process medians. Positive reduction means less Roc elapsed time.', '',
    '| Threads | Main Roc ms | Batch COUNT Roc ms | Saved ms | Roc reduction | 95% paired interval | Main gap vs native DF | Candidate gap vs native DF |',
    '|---:|---:|---:|---:|---:|---:|---:|---:|',
]
for t in [1, 4]:
    r = summary[f'global_aggregate/{t}t']; lo, hi = r['paired_process_bootstrap_95_reduction_percent']
    lines.append(f"| {t} | {r['main_roc_ms']:.2f} | {r['count_roc_ms']:.2f} | {r['saved_ms']:.2f} | {r['roc_elapsed_reduction_percent']:.2f}% | {lo:.2f}% to {hi:.2f}% | {r['main_roc_gap_to_datafusion_percent']:+.2f}% | {r['count_roc_gap_to_datafusion_percent']:+.2f}% |")
lines += [
    '', 'Gaps use the native DataFusion timings collected alongside each respective variant, recorded separately below. They are elapsed-time ratios, not throughput gains. COUNT reduction produces a clear improvement in the measured mixed SUM/COUNT query, but the candidate remains slower than native DataFusion. The unchanged SUM and other ungrouped work are candidates for further attribution, not proven explanations of the entire residual gap.', '',
    '## Complete-query runs and observed interference', '',
    'Two complete four-query experiments preceded the focused comparison; both are preserved in full. In the first run, the final main process slowed markedly in unchanged queries and native DataFusion controls (four-thread native scan ~61 ms, filter ~117 ms rather than ~31/29 ms). The entire five-pair experiment was repeated, not selectively trimmed. The repeat then showed a single-thread candidate global outlier alongside a native DataFusion outlier (~73/61 ms rather than ~47/39 ms). Its unnormalized single-thread reduction is 7.0%, with a wide interval crossing zero. This uncertainty is retained below; it is not presented as a stable isolated estimate.', '',
    'Consequently the focused comparison uses only the target query, ten pairs and balanced variant order. No samples in any experiment are removed. Full-query controls remain useful evidence of interference and semantic consistency; their movements are not attributed to COUNT. Raw initial-run data and summary appear under pilot; the complete repeat is under main/count. The focused run is under focused. Intervals bootstrap the corresponding process clusters 10,000 times and do not eliminate all machine or binary-layout confounders.', '',
    '| Query / threads | Main Roc ms | Candidate Roc ms | Roc reduction | 95% interval | DF with main ms | DF with candidate ms | DF control change |',
    '|---|---:|---:|---:|---:|---:|---:|---:|',
]
for case, r in full_summary.items():
    lo, hi = r['paired_process_bootstrap_95_reduction_percent']
    lines.append(f"| {case} | {r['main_roc_ms']:.2f} | {r['count_roc_ms']:.2f} | {r['roc_elapsed_reduction_percent']:+.2f}% | {lo:+.2f}% to {hi:+.2f}% | {r['main_datafusion_ms']:.2f} | {r['count_datafusion_ms']:.2f} | {r['datafusion_control_change_percent']:+.2f}% |")
lines += [
    '', '## Method, validation and reproduction', '',
    f"Environment: {meta['machine']}, {meta['rust']}, {meta['platform']}, release builds, DataFusion 55.1.0 / Arrow 59.3.0. Same 4M deterministic rows in eight Snappy Parquet files, 256 Int64 groups, batch size 8192, one/four runtime workers and target scan partitions. Warm OS page cache; Parquet decoding is included.", '',
    '- Complete four-query pilot: 1600 timings and 80 full correctness comparisons. Complete repeat: 1600 timings and 80 comparisons. Focused ten-pair run: 800 timings and 40 comparisons. Total: 4000 timings and 200 complete schema/unordered-row comparisons, all passing. Every timed execution checks output row count. Input file identities match; exact SQL, plans and session settings are retained.',
    '- Timed: scan/plan state reset, execution initialization, full output collection and Roc task cleanup. Excluded: SQL/logical/physical planning, conversion, Roc graph construction, validation, warmups and output deallocation.',
    '- Core release tests: 125 pass, including three new regression tests for logical NULLs, slices, COUNT(*), DISTINCT and overflow prefix state. Existing tests cover global/grouped merging and FILTER-before-argument behavior.',
    '- Baseline executable is the previously captured clean main build, verified by SHA256 before measurement. Candidate explicitly cleans Roc release artifacts and rebuilds both Roc and harness. Source, executable and lockfile hashes are verified in every process; no compilation overlaps timing.',
    '- Core archives use the recorded core commits and their production manifests, without a DataFusion workspace member. Compiled workspace manifests add the local harness; that exact lockfile digest is recorded separately. Full local-source archives and binaries remain local.',
    '- [focused/summary.json](focused/summary.json) records primary process medians and paired intervals; [summary.json](summary.json) records the complete repeat; [pilot/summary.json](pilot/summary.json) records the first full run. [metadata.json](metadata.json), [input-files.json](input-files.json), build logs and core-release-tests.json record identities and validation. Every process directory preserves full results.json, samples.csv and run.log.',
    '- In the original workspace, build.py requires the preserved main-datafusion-current executable and core/local candidate branches, verifies equal integration/manifests, then captures the candidate executable. run.py remeasures both complete binaries. preserve_pilot.py retains the first entire run. focused_build.py applies the identical single workload filter and cleanly rebuilds both binaries; focused_run.py runs the balanced ten-pair comparison. document.py produces this evidence. Full reproduction requires the preserved local harness; core archives alone do not provide the DataFusion comparison.',
    '- This is a synthetic warm-cache query comparison, not TPC-H or a general engine ranking. Roc retains checked integer SUM and COUNT. Inputs do not overflow; explicit overflow regression tests exercise the failure path. Neither Float64 SUM nor high-cardinality grouping/spilling is measured.', '',
]
(root / 'README.md').write_text('\n'.join(lines))
print('\n'.join(lines[:25]), flush=True)
