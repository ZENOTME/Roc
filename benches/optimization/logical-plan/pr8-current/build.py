import hashlib,json,subprocess,os
from pathlib import Path
root=Path(__file__).resolve().parent;d=json.loads((root/'metadata.json').read_text());cache=os.environ.get('PR8_BUILD_CACHE',str(root/'build'))
for label in d['source_commits']:
 if label!='main': (root/label/'Cargo.lock').write_bytes((root/'main/Cargo.lock').read_bytes())
 # Shared Cargo target cache uses relative source paths; force every copied Roc source to be newer than its previous fingerprint.
 for source in (root/label/'roc/src').rglob('*.rs'): source.touch()
 cmd=['cargo','build','--release','--target-dir',cache];
 if label!='main': cmd+=['--locked']
 if label=='main':cmd+=['--features','baseline']
 with (root/label/'build.log').open('w')as f:result=subprocess.run(cmd,cwd=root/label,stdout=f,stderr=subprocess.STDOUT)
 if result.returncode:print((root/label/'build.log').read_text()[-6000:]);raise SystemExit(result.returncode)
 assert 'Compiling roc v' in (root/label/'build.log').read_text(), 'Roc source was unexpectedly reused'
 binary=root/label/'benchmark';binary.write_bytes((Path(cache)/'release/pr8-ablation').read_bytes());binary.chmod(0o755);d.setdefault('cargo_lock_sha256',{})[label]=hashlib.sha256((root/label/'Cargo.lock').read_bytes()).hexdigest();d.setdefault('binary_sha256',{})[label]=hashlib.sha256(binary.read_bytes()).hexdigest();print(label+': built',flush=True)
 (root/'metadata.json').write_text(json.dumps(d,indent=2)+'\n')
