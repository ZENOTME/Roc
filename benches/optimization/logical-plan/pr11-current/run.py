import csv, json, random, statistics, subprocess, math
from pathlib import Path
root=Path(__file__).resolve().parent
rng=random.Random(1107)
rows=[]
for replica in range(5):
    labels=['main','pr11'];rng.shuffle(labels)
    for label in labels:
        args=[str(root/label/'benchmark')]+(['reverse']if replica%2 else [])
        output=subprocess.check_output(args,text=True)
        (root/label/f'process-{replica}.csv').write_text(output)
        for r in csv.DictReader(output.splitlines()):
            rows.append(dict(r,variant=label,replica=replica))
        print('Completed',replica,label,flush=True)
result={}
bootstrap=random.Random(110711)
for mode in ['ready','ready_channel','ready_sum','pending_once']:
    medians={label:[statistics.median(float(r['ns_per_batch'])for r in rows if r['variant']==label and r['replica']==i and r['mode']==mode)for i in range(5)]for label in ['main','pr11']}
    ratios=[math.log(medians['pr11'][i]/medians['main'][i])for i in range(5)]
    trials=sorted((1-math.exp(statistics.mean(bootstrap.choices(ratios,k=5))))*100 for _ in range(10000))
    old=statistics.geometric_mean(medians['main']);new=statistics.geometric_mean(medians['pr11'])
    result[mode]={'main_ns':old,'pr11_ns':new,'saved_ns':old-new,'reduction_percent':(1-new/old)*100,'paired_bootstrap_95_percent':[trials[249],trials[9749]],'process_medians_ns':medians}
(root/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
for mode,r in result.items():print(mode,json.dumps(r),flush=True)
