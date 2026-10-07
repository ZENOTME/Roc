import csv,json,statistics,math
from pathlib import Path
from collections import defaultdict
root=Path(__file__).resolve().parent;data=defaultdict(lambda:defaultdict(lambda:defaultdict(dict)))
for p in range(3):
 rows=list(csv.DictReader((root/f'process-{p}.csv').open()));assert len(rows)==78*12*6,(p,len(rows))
 for r in rows:
  key=(int(r['rows']),r['nullable']=='true',r['case']);data[key][p][r['variant']][int(r['sample'])]=float(r['us_per_batch'])
 for key,processes in data.items():
  if p in processes:
   assert len(processes[p])==6
   assert all(len(s)==12 for s in processes[p].values())
comparisons={'actual_amendment':('before','after'),'full_control':('enum_dynamic','bound_static'),'binding_dynamic_direction':('enum_dynamic','bound_dynamic'),'binding_static_direction':('enum_static','bound_static'),'direction_enum_dispatch':('enum_dynamic','enum_static'),'direction_bound_dispatch':('bound_dynamic','bound_static')}
summary=[]
for (rows,nullable,case),processes in sorted(data.items()):
 medians={v:[statistics.median(processes[p][v].values()) for p in range(3)] for v in next(iter(processes.values()))}
 item={'rows':rows,'nullable':nullable,'case':case,'median_us':{v:statistics.median(values) for v,values in medians.items()},'process_median_us':medians,'comparisons':{}}
 for name,(a,b) in comparisons.items():
  changes=[100*(1-y/x) for x,y in zip(medians[a],medians[b])];paired=[100*(1-processes[p][b][s]/processes[p][a][s]) for p in range(3) for s in range(12)]
  item['comparisons'][name]={'elapsed_reduction_pct':statistics.median(changes),'process_min_pct':min(changes),'process_max_pct':max(changes),'process_reductions_pct':changes,'paired_round_median_pct':statistics.median(paired)}
 summary.append(item)
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
with (root/'summary.csv').open('w') as f:
 writer=csv.writer(f,lineterminator="\n");writer.writerow(['rows','nullable','case','comparison','before_us','after_us','elapsed_reduction_pct','process_min_pct','process_max_pct'])
 for item in summary:
  for name,(a,b) in comparisons.items():
   c=item['comparisons'][name];writer.writerow([item['rows'],item['nullable'],item['case'],name,item['median_us'][a],item['median_us'][b],c['elapsed_reduction_pct'],c['process_min_pct'],c['process_max_pct']])
print('All 16848 raw timing rows and paired correctness checks verified')
for rows in [128,8192,65536]:
 print('Rows:',rows)
 for item in summary:
  if item['rows']==rows and item['nullable']:
   c=item['comparisons']['actual_amendment'];d=item['comparisons']['binding_static_direction'];e=item['comparisons']['direction_bound_dispatch'];m=item['median_us']
   print(f"{item['case']:20s} {m['before']:.3f} -> {m['after']:.3f} us; actual {c['elapsed_reduction_pct']:+.2f}% [{c['process_min_pct']:+.2f}, {c['process_max_pct']:+.2f}]; binding {d['elapsed_reduction_pct']:+.2f}%; direction {e['elapsed_reduction_pct']:+.2f}%")
