import hashlib,io,json,os,subprocess,tarfile
from pathlib import Path
root=Path(__file__).resolve().parent
repo=Path('/Users/zenotme/.codex/worktrees/datafusion-optimization-prs/Roc')
recorded=root/'prior-ledger.json'
ledger=json.loads(recorded.read_text() if recorded.exists() else Path('/Users/zenotme/Project/Roc/target/pr-series/core-only/agg-update-fns-restack.json').read_text())
recorded.write_text(json.dumps(ledger,indent=2)+'\n')
pr_snapshot=root/'prs-before.json'
if not pr_snapshot.exists():
    prs=json.loads(subprocess.check_output(['gh','api','repos/ZENOTME/Roc/pulls?state=open&per_page=100'],text=True))
    pr_snapshot.write_text(json.dumps([{'number':p['number'],'head':p['head']['sha'],'base':p['base']['ref'],'branch':p['head']['ref'],'body':p['body']}for p in prs],indent=2)+'\n')
by_number={p['number']:p for p in json.loads(pr_snapshot.read_text())}
for r in ledger['records']:assert by_number[r['number']]['head']==r['new_sha']
source=ledger['local_new'];records={r['number']:r['new_sha']for r in ledger['records']}
assert set(records)=={12,15,16}
metadata={'local_source':source,'main':ledger['main'],'core_heads':records,'variants':{}}
cache=Path('/Users/zenotme/Project/Roc/target')
archive=subprocess.check_output(['git','archive',source],cwd=repo)
for label in ['all','without12','without15','without16']:
    dest=root/label;dest.mkdir(exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(archive))as tar:tar.extractall(dest,filter='data')
    changes=[]
    def restore(path,sha):
        (dest/path).write_bytes(subprocess.check_output(['git','show',sha+':'+path],cwd=repo));changes.append({'path':path,'restored_from':sha})
    if label=='without12':restore('src/expr/scalar/kernels.rs',ledger['main'])
    if label=='without15':
        for path in ['src/exec/aggregate.rs','src/expr/agg/accumulator.rs','src/expr/agg/executor.rs']:restore(path,records[12])
        (dest/'tests/agg_optimized.rs').unlink();changes.append({'removed':'tests/agg_optimized.rs'})
    if label=='without16':
        for path in ['src/exec/filter.rs','src/operator/filter.rs']:restore(path,records[15])
        p=dest/'integrations/datafusion/src/converter.rs';s=p.read_text()
        start=s.index('                    let (projection, child) =\n')
        end=s.index('                    Ok(OperatorTreeNode::new(\n',start)
        s=s[:start]+'''                    let projection = self.bound_projection(
                        &expressions,
                        input.as_arrow(),
                        &project.schema,
                        None,
                    )?;
                    let child = self.node(&project.input).await?;
'''+s[end:]
        p.write_text(s);changes.append({'path':str(p.relative_to(dest)),'change':'Disable local projection/filter fusion and column index remapping; keep ordinary projection above full-schema filter.'})
    # Hash exactly the compiled production code and manifests, independent of cwd.
    paths=sorted([p for p in dest.rglob('*')if p.is_file() and (p.relative_to(dest).parts[0]in ['src','integrations'] or p.name in ['Cargo.toml','Cargo.lock'])])
    h=hashlib.sha256()
    for p in paths:h.update(str(p.relative_to(dest)).encode());h.update(b'\0');h.update(p.read_bytes());h.update(b'\0')
    digest=h.hexdigest()
    env=os.environ.copy();env['ROC_ABLATION_SOURCE_SHA256']=digest;env['ROC_ABLATION_BUILD_LABEL']=label
    for folder in ['src','integrations']:
        for p in (dest/folder).rglob('*.rs'):p.touch()
    # Cargo can reuse path-dependency artifacts across copied workspaces even
    # when source paths differ; clear only Roc artifacts, retaining DF/Arrow.
    with (dest/'clean.log').open('w') as log:
        subprocess.run(['cargo','clean','--release','-p','roc','--target-dir',str(cache)],cwd=dest,stdout=log,stderr=subprocess.STDOUT,check=True)
    cmd=['cargo','build','--locked','--release','-p','roc-datafusion','--example','parquet_compare','--target-dir',str(cache)]
    with (dest/'build.log').open('w')as log:r=subprocess.run(cmd,cwd=dest,env=env,stdout=log,stderr=subprocess.STDOUT)
    if r.returncode:print((dest/'build.log').read_text()[-7000:]);raise SystemExit(r.returncode)
    assert 'Compiling roc v' in (dest/'build.log').read_text()
    assert 'Compiling roc-datafusion v' in (dest/'build.log').read_text()
    binary=dest/'benchmark';binary.write_bytes((cache/'release/examples/parquet_compare').read_bytes());binary.chmod(0o755)
    metadata['variants'][label]={'changes':changes,'compiled_source_sha256':digest,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'lock_sha256':hashlib.sha256((dest/'Cargo.lock').read_bytes()).hexdigest()}
    (root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    print(label,'compiled',flush=True)
assert len({v['lock_sha256']for v in metadata['variants'].values()})==1
