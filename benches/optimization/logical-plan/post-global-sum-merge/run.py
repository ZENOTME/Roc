import hashlib,json,statistics,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
meta=json.loads((root/'metadata.json').read_text());assert meta['core_source_and_tests_match_merged_main']
data=Path(meta['data_directory']);source=Path(meta['source_path'])
def files():return [{'name':p.name,'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(data.glob('*.parquet'))]
inputs=files();assert len(inputs)==8
for name,v in meta['binaries'].items():assert hashlib.sha256(Path(v['path']).read_bytes()).hexdigest()==v['sha256']
(root/'global').mkdir(exist_ok=False);(root/'suite').mkdir(exist_ok=False)
for i in range(10):
 out=root/'global'/f'process-{i}.json'
 with (root/'global'/f'process-{i}.log').open('w') as log:subprocess.run([meta['binaries']['global_probe']['path'],str(data),str(out),'10'],cwd=source,stdout=log,stderr=subprocess.STDOUT,check=True)
 r=json.loads(out.read_text());assert r['compiled_source_sha256']==meta['compiled_source_sha256']
 for c in r['results']:
  assert c['input_rows']==4000000
  for x in c['paths']:assert all(len(x[k])==10 for k in ['datafusion_ms','roc_ms','roc_no_yield_ms'])
 print(f'global {i+1}/10: '+ '; '.join(f"{c['threads']}t "+' '.join(f"{x['storage']} DF={statistics.median(x['datafusion_ms']):.3f} Roc={statistics.median(x['roc_ms']):.3f}" for x in c['paths']) for c in r['results']),flush=True)
for i in range(5):
 out=root/'suite'/f'process-{i}'
 with (root/'suite'/f'process-{i}.log').open('w') as log:subprocess.run([meta['binaries']['parquet_compare']['path'],'--logical',str(data),str(out),'main-'+meta['merged_main_commit'][:12],'10'],cwd=source,stdout=log,stderr=subprocess.STDOUT,check=True)
 r=json.loads((out/'results.json').read_text());assert r['metadata']['compiled_source_sha256']==meta['compiled_source_sha256'];assert len(r['results'])==8
 for x in r['results']:assert len(x['datafusion_ms'])==len(x['roc_ms'])==10
 print(f'suite {i+1}/5: '+ '; '.join(f"{x['workload']} {x['threads']}t DF={statistics.median(x['datafusion_ms']):.3f} Roc={statistics.median(x['roc_ms']):.3f}" for x in r['results']),flush=True)
assert files()==inputs
meta['design']={'input_files':inputs,'global_processes':10,'suite_processes':5,'samples_per_case_engine':10,'warmups':2,'batch_rows':8192,'threads':[1,4],'global_cases':['Parquet SUM/COUNT','resident SUM/COUNT'],'suite_cases':['scan','filter/project','global aggregate','grouped aggregate'],'order':'Global probe rotates three engine orders inside samples; SQL suite alternates DataFusion-first/Roc-first. Processes run sequentially; no compilation during timing; no observations trimmed.','global_full_path_timings':1200,'global_kernel_timings':2200,'suite_full_path_timings':800,'validation':'Every global probe output and kernel result verified after timing. Suite performs exact schema and unordered-row checks for each case before warmups, and row-count checks in every timed sample.','scope':'SQL planning, logical optimization, native physical planning/conversion and graph construction excluded. Full execution, Parquet decoding where applicable, result collection and task cleanup included. Warm-cache local synthetic workload, not TPC-H.'}
(root/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
print('All timings complete; input file hashes unchanged.',flush=True)
