import hashlib, io, json, os, shutil, subprocess, tarfile
from pathlib import Path

root = Path(__file__).resolve().parent
repo = Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
ledger_path = Path('/Users/zenotme/Project/Roc/target/pr-series/core-only/agg-update-fns-restack.json')
ledger = json.loads(ledger_path.read_text())
(root / 'prior-ledger.json').write_text(json.dumps(ledger, indent=2) + '\n')
def git(*args):
    return subprocess.check_output(['git', *args], cwd=repo, text=True).strip()
assert git('rev-parse', 'origin/main') == ledger['main']
assert git('rev-parse', 'HEAD') == ledger['local_new']
assert not git('status', '--porcelain')
for folder in ['src', 'tests']:
    assert git('rev-parse', ledger['main'] + ':' + folder) == git('rev-parse', ledger['local_new'] + ':' + folder)
pr = json.loads(subprocess.check_output(['gh', 'pr', 'view', '14', '--json', 'headRefOid,baseRefName,title,body,url'], cwd=repo, text=True))
assert pr['headRefOid'] == ledger['report_new'] and pr['baseRefName'] == 'main'
(root / 'pr14-before.json').write_text(json.dumps(pr, indent=2) + '\n')
dest = root / 'source'
dest.mkdir()
archive = subprocess.check_output(['git', 'archive', ledger['local_new']], cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    tar.extractall(dest, filter='data')
paths = sorted(p for p in dest.rglob('*') if p.is_file() and (p.relative_to(dest).parts[0] in ['src', 'integrations'] or p.name in ['Cargo.toml', 'Cargo.lock']))
h = hashlib.sha256()
for p in paths:
    h.update(str(p.relative_to(dest)).encode()); h.update(b'\0'); h.update(p.read_bytes()); h.update(b'\0')
digest = h.hexdigest()
label = 'main-291317e-current'
env = os.environ.copy()
env['ROC_ABLATION_SOURCE_SHA256'] = digest
env['ROC_ABLATION_BUILD_LABEL'] = label
cache = Path('/Users/zenotme/Project/Roc/target')
for folder in ['src', 'integrations']:
    for p in (dest / folder).rglob('*.rs'):
        p.touch()
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
    'main': ledger['main'], 'local_harness': ledger['local_new'],
    'core_source_tree': git('rev-parse', ledger['main'] + ':src'),
    'core_tests_tree': git('rev-parse', ledger['main'] + ':tests'),
    'core_source_and_tests_match_main': True,
    'compiled_source_sha256': digest,
    'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
    'lock_sha256': hashlib.sha256((dest / 'Cargo.lock').read_bytes()).hexdigest(),
    'build_label': label,
    'compiler_artifact_control': 'Clean Roc release artifacts, require both Roc and local harness to compile, preserve binary, verify compiled source and executable hashes in every process. No builds overlap timing.',
}
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
print(json.dumps(meta, indent=2), flush=True)
