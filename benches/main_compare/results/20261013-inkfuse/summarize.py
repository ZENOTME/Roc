import csv, json, math, statistics, hashlib
from pathlib import Path
P=Path(__file__).resolve().parent
median=statistics.median
summary={}
validation=[]
for label,folder in [('initial',P),('confirmation',P/'confirmation')]:
    rows=list(csv.DictReader((folder/'samples_sf1_t1_all.csv').open()))
    output=[]
    for q in dict.fromkeys(r['query'] for r in rows):
        entry={'query':q}
        for mode in ['interpreted','fused_prepared']:
            a=[r for r in rows if r['query']==q and r['mode']==mode and int(r['rep'])>=0]
            assert len(a)==7,(label,q,mode,len(a))
            entry[mode]={'samples':len(a),'execute_median_ms':median(float(r['elapsed_ms']) for r in a),'prepare_median_ms':median(float(r['prepare_ms']) for r in a),'execute_min_ms':min(float(r['elapsed_ms']) for r in a),'execute_max_ms':max(float(r['elapsed_ms']) for r in a),'end_to_end_median_ms':median(float(r['elapsed_ms'])+float(r['prepare_ms']) for r in a)}
        entry['zero_over_full']=entry['interpreted']['execute_median_ms']/entry['fused_prepared']['execute_median_ms']
        output.append(entry)
        pair=[]
        for mode in ['interpreted','fused_prepared']:
            data=list(csv.reader((folder/f'validation_sf1_t1_{q}_{mode}.csv').open()))
            pair.append(sorted(data[1:]))
        assert len(pair[0])==len(pair[1])
        for a,b in zip(*pair):
            assert len(a)==len(b)
            for x,y in zip(a,b):
                assert x==y or math.isclose(float(x),float(y),rel_tol=1e-8,abs_tol=1e-7),(q,x,y)
        validation.append({'run':label,'query':q,'rows':len(pair[0]),'mode_outputs_match':True})
    summary['tpch_'+label]=output
rows=list(csv.DictReader((P/'chain.csv').open()))
output=[]
for depth in [0,1,16,64]:
    entry={'additions':depth}
    for mode in ['interpreted','fused']:
        a=[r for r in rows if int(r['depth'])==depth and r['mode']==mode and int(r['rep'])>=0]
        assert len(a)==15
        entry[mode]={'samples':len(a),'execute_median_ms':median(float(r['elapsed_ms']) for r in a),'prepare_median_ms':median(float(r['prepare_ms']) for r in a),'execute_min_ms':min(float(r['elapsed_ms']) for r in a),'execute_max_ms':max(float(r['elapsed_ms']) for r in a)}
    entry['zero_over_full']=entry['interpreted']['execute_median_ms']/entry['fused']['execute_median_ms']
    output.append(entry)
summary['inkfuse_chain']=output
output=[]
for nullable in [True,False]:
    entry={'nullable':nullable}
    graph=(P/f'roc-graph-nullable-{nullable}.txt').read_text()
    entry['graph_counts']={op:graph.count(pattern) for op,pattern in [('Add','function: Add,'),('And','function: And,'),('RaiseIf','RaiseIf {')]}
    for mode in ['compile','zero-generated','zero-native']:
        runs=[json.loads(f.read_text()) for f in sorted(P.glob(f'roc-nullable-{nullable}-{mode}-r*.json'))]
        assert len(runs)==3
        meds=[r['cases'][0]['execute_median_ms'] for r in runs]
        entry[mode]={'process_replicas':3,'samples_per_replica':20,'median_of_process_medians_ms':median(meds),'process_medians_ms':meds}
    output.append(entry)
summary['roc_null_ablation']=output
summary['tpch_validation']=validation
(P/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
with (P/'comparison.csv').open('w') as f:
    w=csv.writer(f);w.writerow(['workload','zero_execute_ms','full_execute_ms','zero_over_full','full_prepare_ms'])
    for r in summary['tpch_confirmation']:
        w.writerow([r['query'],r['interpreted']['execute_median_ms'],r['fused_prepared']['execute_median_ms'],r['zero_over_full'],r['fused_prepared']['prepare_median_ms']])
    for r in summary['inkfuse_chain']:
        w.writerow([f"chain_{r['additions']}",r['interpreted']['execute_median_ms'],r['fused']['execute_median_ms'],r['zero_over_full'],r['fused']['prepare_median_ms']])
print((P/'comparison.csv').read_text())
print('All 16 TPC-H mode comparisons passed; chain validation: see run-chain.log.')
