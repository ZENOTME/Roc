import csv,json,math,random,statistics
from pathlib import Path
root=Path(__file__).resolve().parent;d=json.loads((root/'metadata.json').read_text());data={}
for label in d['source_commits']:
 for process in range(d['process_replicas']):
  rows=list(csv.DictReader((root/label/f'process-{process}.csv').open()));assert len(rows)==192
  for row in rows:
   key=(row['case'],int(row['batch_rows']),int(row['columns']));data.setdefault(key,{}).setdefault(label,{}).setdefault(process,[]).append(float(row['ns_per_run']))
rng=random.Random(81006);summary=[]
for key,variants in sorted(data.items()):
 entry={'case':key[0],'batch_rows':key[1],'columns':key[2],'milliseconds':{label:math.exp(statistics.mean(math.log(statistics.median(samples))for samples in processes.values()))/1e6 for label,processes in variants.items()}}
 for control in [x for x in d['source_commits'] if x!='pr8']:
  ratios=[math.log(statistics.median(variants['pr8'][i])/statistics.median(variants[control][i]))for i in range(d['process_replicas'])]
  boot=sorted(100*(1-math.exp(statistics.mean(rng.choices(ratios,k=len(ratios)))))for _ in range(10000))
  entry.setdefault('reductions',{})[control]={'percent':100*(1-math.exp(statistics.mean(ratios))),'ci95':[boot[249],boot[9749]],'per_process_percent':[100*(1-math.exp(x))for x in ratios]}
 summary.append(entry)
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
with (root/'summary.csv').open('w')as out:
 w=csv.writer(out,lineterminator="\n");w.writerow(['case','batch_rows','columns','main_ms','pr8_ms','full_pr_reduction_pct','ci95_low','ci95_high','context_reduction_pct','validation_reduction_pct','cast_reduction_pct','arg_vec_reduction_pct'])
 for r in summary:w.writerow([r['case'],r['batch_rows'],r['columns'],r['milliseconds']['main'],r['milliseconds']['pr8'],r['reductions']['main']['percent'],*r['reductions']['main']['ci95'],*[r['reductions'][k]['percent']for k in ['eager_context','validate_ids','arrow_cast','argument_vec']]])
for row in summary:
 if row['batch_rows']==2048:
  print(row['case'],row['columns'],'main/pr8 ms',*[round(row['milliseconds'][k],3)for k in ['main','pr8']], 'full',row['reductions']['main'],'context',row['reductions']['eager_context'])
print('Raw timed samples:',len(summary)*len(d['source_commits'])*d['process_replicas']*d['samples_per_process_case'])
