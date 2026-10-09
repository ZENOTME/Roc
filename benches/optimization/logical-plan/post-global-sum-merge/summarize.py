import itertools,json,math,random,statistics
from pathlib import Path
root=Path(__file__).resolve().parent
rng=random.Random(2026100730)
def geo(v):return math.exp(statistics.mean(math.log(x) for x in v))
def summarize(roc,df):
 logs=[math.log(a/b) for a,b in zip(roc,df)];xs=sorted((math.exp(statistics.mean(rng.choices(logs,k=len(logs))))-1)*100 for _ in range(10000))
 return {'roc_ms':geo(roc),'datafusion_ms':geo(df),'roc_elapsed_change_percent':(geo(roc)/geo(df)-1)*100,'paired_process_95_percent':[xs[249],xs[9749]],'roc_process_medians_ms':roc,'df_process_medians_ms':df}
global_runs=[json.loads((root/'global'/f'process-{i}.json').read_text()) for i in range(10)]
suite_runs=[json.loads((root/'suite'/f'process-{i}/results.json').read_text()) for i in range(5)]
summary={'global':{},'suite':{}}
for threads,storage in itertools.product([1,4],['p','m']):
 roc=[];df=[]
 for r in global_runs:
  c=next(c for c in r['results'] if c['threads']==threads);x=next(x for x in c['paths'] if x['storage']==storage);roc.append(statistics.median(x['roc_ms']));df.append(statistics.median(x['datafusion_ms']))
 summary['global'][f'{storage}/{threads}t']=summarize(roc,df)
for workload,threads in itertools.product(['scan_only','filter_project','global_aggregate','grouped_aggregate'],[1,4]):
 roc=[];df=[]
 for r in suite_runs:
  x=next(x for x in r['results'] if x['workload']==workload and x['threads']==threads);roc.append(statistics.median(x['roc_ms']));df.append(statistics.median(x['datafusion_ms']))
 summary['suite'][f'{workload}/{threads}t']=summarize(roc,df)
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for group,cells in summary.items():
 print(group)
 for key,x in cells.items():print(key,'Roc',round(x['roc_ms'],4),'DF',round(x['datafusion_ms'],4),'elapsed change%',round(x['roc_elapsed_change_percent'],3),'95%',x['paired_process_95_percent'])
