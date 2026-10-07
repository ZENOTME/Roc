import hashlib,json,os,shutil,subprocess,time,sys
from pathlib import Path
root=Path(__file__).resolve().parent
cache=Path('/Users/zenotme/Project/Roc/target')
meta=json.loads((root/'metadata.json').read_text())
for label,entry in meta['variants'].items():
    if len(sys.argv)>1 and label not in sys.argv[1:]:continue
    source=Path(entry['source_path']);folder=source.parent
    subprocess.run(['cargo','fmt','--all'],cwd=source,check=True)
    h=hashlib.sha256();core=hashlib.sha256()
    for p in sorted(p for p in source.rglob('*') if p.is_file() and (p.relative_to(source).parts[0] in ['src','integrations'] or p.name in ['Cargo.toml','Cargo.lock'])):
        rel=str(p.relative_to(source));data=p.read_bytes();h.update(rel.encode()+b'\0'+data+b'\0')
        if p.relative_to(source).parts[0]=='src':core.update(rel.encode()+b'\0'+data+b'\0')
    digest=h.hexdigest();env=os.environ.copy();env['ROC_ABLATION_SOURCE_SHA256']=digest
    with (folder/'clean.log').open('w') as log:subprocess.run(['cargo','clean','--release','-p','roc','--target-dir',str(cache)],cwd=source,stdout=log,stderr=subprocess.STDOUT,check=True)
    with (folder/'build.log').open('w') as log:
        result=subprocess.run(['cargo','build','--locked','--release','-p','roc-datafusion','--example','global_probe','--target-dir',str(cache)],cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
    if result.returncode:
        print((folder/'build.log').read_text()[-5000:],flush=True);raise SystemExit(result.returncode)
    text=(folder/'build.log').read_text();assert 'Compiling roc v' in text and 'Compiling roc-datafusion v' in text
    binary=folder/'benchmark';shutil.copy2(cache/'release/examples/global_probe',binary);binary.chmod(0o755)
    entry.update(compiled_source_sha256=digest,core_source_sha256=core.hexdigest(),binary_path=str(binary),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),lock_sha256=hashlib.sha256((source/'Cargo.lock').read_bytes()).hexdigest())
    print(label,'build complete',flush=True)
    (root/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
assert len({v['lock_sha256'] for v in meta['variants'].values()})==1
assert len({hashlib.sha256((Path(v['source_path'])/'integrations/datafusion/examples/global_probe.rs').read_bytes()).hexdigest() for v in meta['variants'].values()})==1
print('All builds finished. Safe to start timings.',flush=True)
