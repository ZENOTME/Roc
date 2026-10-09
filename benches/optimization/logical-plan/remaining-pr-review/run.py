import json,math,random,statistics,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent
data=Path('/Users/zenotme/Project/Roc/target/parquet-snappy/data-1791083916888251000')
assert len(list(data.glob('*.parquet')))==8
metadata=json.loads((root/'metadata.json').read_text())
metadata['design']={'replicas':5,'samples_per_case':7,'warmups':2,'threads':[1,4],'input':str(data),'comparison':'Remove one optimization at a time from the exact current local harness; retain all others. Without16 also disables local converter fusion/remapping. Timed full SQL execution over 4M deterministic Snappy Parquet rows, with planning outside timing. All four workloads, including unaffected controls, are measured.'}
(root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
rng=random.Random(121516)
all_runs=[]
for replica in range(5):
    labels=['all','without12','without15','without16'];rng.shuffle(labels)
    for label in labels:
        out=root/label/f'process-{replica}';out.mkdir(exist_ok=True)
        cmd=[str(root/label/'benchmark'),'--logical',str(data),str(out),label,'7']
        started=time.monotonic()
        with (out/'run.log').open('w')as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
        if r.returncode:print((out/'run.log').read_text()[-5000:]);raise SystemExit(r.returncode)
        report=json.loads((out/'results.json').read_text())
        assert report['metadata']['compiled_source_sha256']==metadata['variants'][label]['compiled_source_sha256']
        assert report['metadata']['executable_sha256']==metadata['variants'][label]['binary_sha256']
        files=[(f['path'],f['bytes'],f['sha256'])for f in report['input_files']]
        if all_runs:assert files==all_runs[0]['files']
        all_runs.append({'replica':replica,'variant':label,'files':files,'results':report['results']})
        print('Completed',replica,label,round(time.monotonic()-started,1),'seconds',flush=True)
        print('; '.join(f"{x['workload']}/{x['threads']}t={x['roc_median_ms']:.2f}ms"for x in report['results']),flush=True)
bootstrap=random.Random(1215161107)
summary={}
for number in [12,15,16]:
    cases={}
    for workload in ['scan_only','filter_project','global_aggregate','grouped_aggregate']:
        for threads in [1,4]:
            values={label:[]for label in ['all',f'without{number}']}
            for label in values:
                for replica in range(5):
                    run=next(x for x in all_runs if x['variant']==label and x['replica']==replica)
                    case=next(x for x in run['results']if x['workload']==workload and x['threads']==threads)
                    values[label].append(statistics.median(case['roc_ms']))
            logs=[math.log(values['all'][i]/values[f'without{number}'][i])for i in range(5)]
            intervals=sorted((1-math.exp(statistics.mean(bootstrap.choices(logs,k=5))))*100 for _ in range(10000))
            old=statistics.geometric_mean(values[f'without{number}']);new=statistics.geometric_mean(values['all'])
            cases[f'{workload}/{threads}t']={'without_ms':old,'with_ms':new,'saved_ms':old-new,'reduction_percent':(1-new/old)*100,'paired_bootstrap_95_percent':[intervals[249],intervals[9749]],'process_medians_ms':values}
    summary[str(number)]=cases
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
(root/'input-files.json').write_text(json.dumps(all_runs[0]['files'],indent=2)+'\n')
for number in [12,15,16]:
    affected='global_aggregate'if number==15 else'filter_project'
    for threads in [1,4]:print(number,f'{affected}/{threads}t',json.dumps(summary[str(number)][f'{affected}/{threads}t']),flush=True)
