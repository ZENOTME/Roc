import hashlib, io, json, subprocess, tarfile
from pathlib import Path
root=Path(__file__).resolve().parent
repo=Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
ledger=json.loads(Path('/Users/zenotme/Project/Roc/target/pr-series/core-only/agg-update-fns-restack.json').read_text())
sources={'main':ledger['main'],'pr11':ledger['records'][0]['new_sha']}
assert ledger['records'][0]['number']==11
metadata={'source_commits':sources,'scope':'Actual public ScanExec next_batch path only; not full queries. 2048-row Int64 batches. 5 independent process pairs, 9 samples per mode/process, 100000 batch reads per sample; setup outside timing. pending_once is an artificial wake/re-poll, not actual I/O waiting. ready_sum includes Arrow SUM on each batch, not Roc aggregate or pipeline scheduling. Correctness and column identity assertions run in timed loop identically for both variants.','samples':360}
cache=Path('/Users/zenotme/Project/Roc/target/column-value-checks/build')
for label,sha in sources.items():
    dest=root/label;dest.mkdir(exist_ok=True)
    data=subprocess.check_output(['git','archive',sha],cwd=repo)
    with tarfile.open(fileobj=io.BytesIO(data))as archive:archive.extractall(dest,filter='data')
    with (dest/'Cargo.toml').open('a') as f:f.write('\n[workspace]\n')
    (dest/'examples').mkdir(exist_ok=True)
    (dest/'examples/ready_poll_probe.rs').write_bytes((root/'ready_poll_probe.rs').read_bytes())
    for source in (dest/'src').rglob('*.rs'):source.touch()
    cmd=['cargo','build','--locked','--release','--example','ready_poll_probe','--target-dir',str(cache)]
    with (dest/'build.log').open('w')as f:r=subprocess.run(cmd,cwd=dest,stdout=f,stderr=subprocess.STDOUT)
    if r.returncode:print((dest/'build.log').read_text()[-6000:]);raise SystemExit(r.returncode)
    assert 'Compiling roc v' in (dest/'build.log').read_text()
    binary=dest/'benchmark';binary.write_bytes((cache/'release/examples/ready_poll_probe').read_bytes());binary.chmod(0o755)
    metadata.setdefault('binaries',{})[label]=hashlib.sha256(binary.read_bytes()).hexdigest()
    metadata.setdefault('lockfiles',{})[label]=hashlib.sha256((dest/'Cargo.lock').read_bytes()).hexdigest()
    print(label,'built',flush=True)
assert len(set(metadata['lockfiles'].values()))==1
(root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
