import hashlib, io, json, os, shutil, subprocess, tarfile
from pathlib import Path

root = Path(__file__).resolve().parent
repo = Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
baseline_root = root.parent / 'main-datafusion-current'
baseline = json.loads((baseline_root / 'metadata.json').read_text())
def git(*args):
    return subprocess.check_output(['git', *args], cwd=repo, text=True).strip()
core = git('rev-parse', 'codex/global-count-batch')
local = git('rev-parse', 'codex/local-global-count-benchmark')
assert git('rev-parse', core + '^') == baseline['main']
for folder in ['src', 'tests']:
    assert git('rev-parse', core + ':' + folder) == git('rev-parse', local + ':' + folder)
assert git('diff', baseline['local_harness'], local, '--', 'integrations', 'Cargo.toml', 'Cargo.lock') == ''
assert not git('status', '--porcelain')
assert hashlib.sha256((baseline_root / 'benchmark').read_bytes()).hexdigest() == baseline['binary_sha256']
ledger_path = Path('/Users/zenotme/Project/Roc/target/pr-series/core-only/agg-update-fns-restack.json')
(root / 'prior-ledger.json').write_text(ledger_path.read_text())
dest = root / 'source'
dest.mkdir()
archive = subprocess.check_output(['git', 'archive', local], cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    tar.extractall(dest, filter='data')
paths = sorted(p for p in dest.rglob('*') if p.is_file() and (p.relative_to(dest).parts[0] in ['src', 'integrations'] or p.name in ['Cargo.toml', 'Cargo.lock']))
h = hashlib.sha256()
for p in paths:
    h.update(str(p.relative_to(dest)).encode()); h.update(b'\0'); h.update(p.read_bytes()); h.update(b'\0')
digest = h.hexdigest()
env = os.environ.copy()
env['ROC_ABLATION_SOURCE_SHA256'] = digest
env['ROC_ABLATION_BUILD_LABEL'] = 'global-count-batch'
cache = Path('/Users/zenotme/Project/Roc/target')
for folder in ['src', 'integrations']:
    for p in (dest / folder).rglob('*.rs'): p.touch()
with (root / 'clean.log').open('w') as log:
    subprocess.run(['cargo', 'clean', '--release', '-p', 'roc', '--target-dir', str(cache)], cwd=dest, stdout=log, stderr=subprocess.STDOUT, check=True)
with (root / 'build.log').open('w') as log:
    r = subprocess.run(['cargo', 'build', '--locked', '--release', '-p', 'roc-datafusion', '--example', 'parquet_compare', '--target-dir', str(cache)], cwd=dest, env=env, stdout=log, stderr=subprocess.STDOUT)
if r.returncode:
    print((root / 'build.log').read_text()[-7000:]); raise SystemExit(r.returncode)
log = (root / 'build.log').read_text()
assert 'Compiling roc v' in log and 'Compiling roc-datafusion v' in log
binary = root / 'benchmark'
shutil.copy2(cache / 'release/examples/parquet_compare', binary)
binary.chmod(0o755)
meta = {
    'main': baseline['main'], 'candidate_core': core, 'candidate_local_harness': local,
    'baseline_local_harness': baseline['local_harness'],
    'core_candidate_and_local_source_tests_match': True, 'harness_and_manifests_unchanged': True,
    'variants': {
        'main': {'binary_path': str(baseline_root / 'benchmark'), 'binary_sha256': baseline['binary_sha256'], 'compiled_source_sha256': baseline['compiled_source_sha256'], 'lock_sha256': baseline['lock_sha256'], 'build_label': baseline['build_label']},
        'count': {'binary_path': str(binary), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'compiled_source_sha256': digest, 'lock_sha256': hashlib.sha256((dest / 'Cargo.lock').read_bytes()).hexdigest(), 'build_label': 'global-count-batch'},
    },
    'change': 'Bind a batch COUNT update only for ungrouped non-DISTINCT COUNT. Preserve shared FILTER/argument evaluation, group ID construction, SUM and every other update path. Logical NULLs and checked overflow including successful-prefix state are preserved.',
    'artifact_control': 'Baseline is the preserved freshly rebuilt main binary with verified SHA256; candidate explicitly cleans/rebuilds Roc and harness, preserving its executable. Harness and lockfiles are identical. Every process checks compiled source, executable, versions and lock hashes. No builds overlap timing.',
}
assert meta['variants']['main']['lock_sha256'] == meta['variants']['count']['lock_sha256']
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
print(json.dumps(meta, indent=2), flush=True)
