import json,math,statistics,random
from pathlib import Path
root=Path(__file__).resolve().parent/'paired';p=root/'summary.json';r=json.loads(p.read_text());rng=random.Random(2026100714)
for k,v in r.items():
 med=v['process_medians_ms'];logs=[math.log((med['fused']['roc'][i]/med['fused']['datafusion'][i])/(med['reuse']['roc'][i]/med['reuse']['datafusion'][i])) for i in range(10)]
 est=(1-math.exp(statistics.mean(logs)))*100;boots=sorted((1-math.exp(statistics.mean(rng.choices(logs,k=10))))*100 for _ in range(10000))
 v['stages']['fused']['datafusion_control_change_percent']=(v['stages']['fused']['datafusion_ms']/v['stages']['reuse']['datafusion_ms']-1)*100
 v['stages']['fused']['relative_to_datafusion_control_reduction_percent']=est
 v['stages']['fused']['relative_to_control_reduction_95_percent']=[boots[249],boots[9749]]
p.write_text(json.dumps(r,indent=2)+'\n')
