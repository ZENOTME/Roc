import hashlib,json,os,shutil,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
meta=json.loads((root/'metadata.json').read_text());source=Path(meta['source_path']);cache=Path('/Users/zenotme/Project/Roc/target')
h=hashlib.sha256();core=hashlib.sha256()
for p in sorted(p for p in source.rglob('*') if p.is_file()):
 rel=str(p.relative_to(source));data=p.read_bytes();h.update(rel.encode()+b'\0'+data+b'\0')
 if p.relative_to(source).parts[0]=='src':core.update(rel.encode()+b'\0'+data+b'\0')
meta['compiled_source_sha256']=h.hexdigest();meta['core_source_sha256']=core.hexdigest();meta['lock_sha256']=hashlib.sha256((source/'Cargo.lock').read_bytes()).hexdigest();env=os.environ.copy();env['ROC_ABLATION_SOURCE_SHA256']=h.hexdigest()
with (root/'clean.log').open('w') as log:subprocess.run(['cargo','clean','--release','-p','roc','--target-dir',str(cache)],cwd=source,stdout=log,stderr=subprocess.STDOUT,check=True)
with (root/'build.log').open('w') as log:subprocess.run(['cargo','build','--locked','--release','-p','roc-datafusion','--examples','--target-dir',str(cache)],cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
text=(root/'build.log').read_text();assert 'Compiling roc v' in text and 'Compiling roc-datafusion v' in text
meta['binaries']={}
for name in ['global_probe','parquet_compare']:
 dst=root/name;shutil.copy2(cache/'release/examples'/name,dst);dst.chmod(0o755);meta['binaries'][name]={'path':str(dst),'sha256':hashlib.sha256(dst.read_bytes()).hexdigest()}
(root/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
print('Prepared fresh binaries; verify actual merged main before timing.',flush=True)
