import hashlib, io, json, os, shutil, subprocess, tarfile
from pathlib import Path
root = Path(__file__).resolve().parent
repo = Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
meta = json.loads((root / 'metadata.json').read_text())
focused = root / 'focused'
focused.mkdir()
sources = {'main': meta['baseline_local_harness'], 'count': meta['candidate_local_harness']}
build = {'scope': 'Same full global SUM/COUNT Parquet query and timing boundaries, alone at one/four threads; avoids preceding multi-million-row scan result validation/sorting. Production core unchanged from the respective recorded commits.', 'harness_transform': 'Only filter the unchanged WORKLOADS iterator to Workload::GlobalAggregate in logical.rs, identically for both variants.', 'variants': {}}
cache = root.parent
for label, sha in sources.items():
    folder = focused / label
    folder.mkdir()
    source = folder / 'source'; source.mkdir()
    archive = subprocess.check_output(['git', 'archive', sha], cwd=repo)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar: tar.extractall(source, filter='data')
    path = source / 'integrations/datafusion/examples/parquet_compare/logical.rs'
    text = path.read_text()
    old = 'for workload in diagnostics::WORKLOADS {'
    assert text.count(old) == 1
    path.write_text(text.replace(old, 'for workload in diagnostics::WORKLOADS.into_iter().filter(|workload| matches!(workload, Workload::GlobalAggregate)) {'))
    h = hashlib.sha256()
    paths = sorted(p for p in source.rglob('*') if p.is_file() and (p.relative_to(source).parts[0] in ['src', 'integrations'] or p.name in ['Cargo.toml', 'Cargo.lock']))
    for p in paths:
        h.update(str(p.relative_to(source)).encode()); h.update(b'\0'); h.update(p.read_bytes()); h.update(b'\0')
    digest = h.hexdigest()
    env = os.environ.copy(); env['ROC_ABLATION_SOURCE_SHA256'] = digest
    env['ROC_ABLATION_BUILD_LABEL'] = 'focused-global-count-' + label
    for name in ['src', 'integrations']:
        for p in (source / name).rglob('*.rs'): p.touch()
    with (folder / 'clean.log').open('w') as log:
        subprocess.run(['cargo', 'clean', '--release', '-p', 'roc', '--target-dir', str(cache)], cwd=source, stdout=log, stderr=subprocess.STDOUT, check=True)
    with (folder / 'build.log').open('w') as log:
        r = subprocess.run(['cargo', 'build', '--locked', '--release', '-p', 'roc-datafusion', '--example', 'parquet_compare', '--target-dir', str(cache)], cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
    if r.returncode:
        print((folder / 'build.log').read_text()[-6000:]); raise SystemExit(r.returncode)
    log = (folder / 'build.log').read_text()
    assert 'Compiling roc v' in log and 'Compiling roc-datafusion v' in log
    binary = folder / 'benchmark'; shutil.copy2(cache / 'release/examples/parquet_compare', binary); binary.chmod(0o755)
    build['variants'][label] = {'original_local_commit': sha, 'compiled_source_sha256': digest, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'binary_path': str(binary), 'build_label': env['ROC_ABLATION_BUILD_LABEL'], 'lock_sha256': hashlib.sha256((source / 'Cargo.lock').read_bytes()).hexdigest()}
    print(label, 'focused build completed', flush=True)
assert build['variants']['main']['lock_sha256'] == build['variants']['count']['lock_sha256']
(focused / 'metadata.json').write_text(json.dumps(build, indent=2) + '\n')
