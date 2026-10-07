import hashlib, io, json, os, shutil, subprocess, tarfile
from pathlib import Path
root=Path(__file__).resolve().parent/'paired'
root.mkdir(exist_ok=True)
repo=Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
cache=Path('/Users/zenotme/Project/Roc/target')
probe=(repo/'integrations/datafusion/examples/global_probe.rs').read_bytes()
sources={'reuse':'da4b0e0db2ba1b84008f485e931f7481dde63317','fused':'b57681fa2fb5aaca1b31a2cf889b8be459ac8bd7'}
meta={'variants':{},'probe_sha256':hashlib.sha256(probe).hexdigest(),'purpose':'Paired ablation of the same native checked global SUM and the fused prefix-safety bound. Identical local SQL converter and probe; kernel controls are exploratory, all full-path cases execute the actual core library.'}
for label,sha in sources.items():
 folder=root/label; folder.mkdir(exist_ok=True); source=folder/'source'; source.mkdir(exist_ok=True)
 archive=subprocess.check_output(['git','archive',sha],cwd=repo)
 with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(source)
 (source/'integrations/datafusion/examples/global_probe.rs').write_bytes(probe)
 h=hashlib.sha256(); core=hashlib.sha256()
 for p in sorted(p for p in source.rglob('*') if p.is_file() and (p.relative_to(source).parts[0] in ['src','integrations'] or p.name in ['Cargo.toml','Cargo.lock'])):
  rel=str(p.relative_to(source)); data=p.read_bytes(); h.update(rel.encode()+b'\0'+data+b'\0')
  if p.relative_to(source).parts[0]=='src':core.update(rel.encode()+b'\0'+data+b'\0')
 digest=h.hexdigest();env=os.environ.copy();env['ROC_ABLATION_SOURCE_SHA256']=digest
 for name in ['src','integrations']:
  for p in (source/name).rglob('*.rs'):p.touch()
 with (folder/'clean.log').open('w') as log:subprocess.run(['cargo','clean','--release','-p','roc','--target-dir',str(cache)],cwd=source,stdout=log,stderr=subprocess.STDOUT,check=True)
 with (folder/'build.log').open('w') as log:r=subprocess.run(['cargo','build','--locked','--release','-p','roc-datafusion','--example','global_probe','--target-dir',str(cache)],cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
 if r.returncode:print((folder/'build.log').read_text()[-6000:]);raise SystemExit(r.returncode)
 log=(folder/'build.log').read_text();assert 'Compiling roc v' in log and 'Compiling roc-datafusion v' in log
 binary=folder/'benchmark';shutil.copy2(cache/'release/examples/global_probe',binary);binary.chmod(0o755)
 meta['variants'][label]={'local_source_commit':sha,'compiled_source_sha256':digest,'core_source_sha256':core.hexdigest(),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'binary_path':str(binary),'lock_sha256':hashlib.sha256((source/'Cargo.lock').read_bytes()).hexdigest()}
 print(label,'build complete',flush=True)
assert len({v['lock_sha256'] for v in meta['variants'].values()})==1
(root/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
