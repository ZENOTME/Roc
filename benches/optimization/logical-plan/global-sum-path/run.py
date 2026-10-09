import hashlib, itertools, json, math, random, statistics, subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
meta=json.loads((root/'metadata.json').read_text())
data=root.parent/'parquet-snappy/data-1791083916888251000'
files=[{'name':p.name,'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(data.glob('*.parquet'))]
labels=['main','count','sum','reuse']; orders=[]
rng=random.Random(2026100711)
for repeat in range(2):
 base=labels.copy();rng.shuffle(base)
 block=[base[i:]+base[:i] for i in range(4)];rng.shuffle(block);orders.extend(block)
runs={}
for replica,order in enumerate(orders):
 for label in order:
  v=meta['variants'][label]; binary=Path(v['binary_path']);assert hashlib.sha256(binary.read_bytes()).hexdigest()==v['binary_sha256']
  assert hashlib.sha256((root/label/'source/Cargo.lock').read_bytes()).hexdigest()==v['lock_sha256']
  out=root/label/f'process-{replica}.json'
  with (root/label/f'process-{replica}.log').open('w') as log:subprocess.run([str(binary),str(data),str(out),'10'],stdout=log,stderr=subprocess.STDOUT,check=True)
  r=json.loads(out.read_text());assert r['compiled_source_sha256']==v['compiled_source_sha256'];assert len(r['results'])==2
  for c in r['results']:
   assert c['input_rows']==4000000 and len(c['paths'])==2
   for x in c['paths']:assert all(len(x[k])==10 for k in ['datafusion_ms','roc_ms','roc_no_yield_ms'])
  runs[label,replica]=r
  print(f'{replica+1}/8 {label}: '+ '; '.join(f"{c['threads']}t "+ ' '.join(f"{x['storage']}:DF={statistics.median(x['datafusion_ms']):.3f},Roc={statistics.median(x['roc_ms']):.3f}" for x in c['paths']) for c in r['results']),flush=True)
assert files==[{'name':p.name,'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(data.glob('*.parquet'))]
summary={};boot=random.Random(2026100712)
def geo(x):return math.exp(statistics.mean(math.log(v) for v in x))
def interval(logs):
 xs=sorted((math.exp(statistics.mean(boot.choices(logs,k=len(logs))))-1)*100 for _ in range(10000));return [xs[249],xs[9749]]
for threads,storage in itertools.product([1,4],['p','m']):
 med={label:{engine:[] for engine in ['roc','datafusion','roc_no_yield']} for label in labels}
 for label in labels:
  for replica in range(8):
   c=next(c for c in runs[label,replica]['results'] if c['threads']==threads);x=next(x for x in c['paths'] if x['storage']==storage)
   for engine in med[label]:med[label][engine].append(statistics.median(x[engine+'_ms']))
 cells={}
 for i,label in enumerate(labels):
  old=labels[max(0,i-1)];roc=geo(med[label]['roc']);df=geo(med[label]['datafusion'])
  cells[label]={'roc_ms':roc,'datafusion_ms':df,'roc_no_yield_ms':geo(med[label]['roc_no_yield']),'roc_gap_percent':(roc/df-1)*100,'roc_vs_df_cluster_95_percent':interval([math.log(a/b) for a,b in zip(med[label]['roc'],med[label]['datafusion'])]),'previous_stage':old,'stage_elapsed_reduction_percent':(1-roc/geo(med[old]['roc']))*100,'stage_reduction_95_percent':[-v for v in reversed(interval([math.log(a/b) for a,b in zip(med[label]['roc'],med[old]['roc'])]))]}
 summary[f'{storage}/{threads}t']={'stages':cells,'process_medians_ms':med}
meta['design']={'data_directory':str(data),'input_files':files,'replicas_per_stage':8,'timed_samples_per_case_engine':10,'warmups':2,'process_orders':orders,'order':'Two randomized cyclic Latin squares; each stage appears twice at each process position; three engine orders rotate within each sample. No compilation during timing. No observations trimmed.','sql':'SELECT SUM(value) AS total, COUNT(value) AS count FROM p (Parquet) or m (resident same decoded column)','batch_rows':8192,'threads':[1,4],'timing':'Native collect or Roc execute through shutdown and output extraction; SQL/planning/conversion/Roc graph construction excluded; complete execution including Parquet decode included for p. Correctness/schema checked after each run, outside timers.','samples':{'full_path':3840,'kernel':6400},'statistics':'Geometric mean of eight per-process medians. 95% intervals resample the paired process-level log ratios (10000 resamples). Kernels include exact accumulator source in the example, compiled separately; full paths execute the core library.'}
(root/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n');(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:v['stages'] for k,v in summary.items()},indent=2),flush=True)
