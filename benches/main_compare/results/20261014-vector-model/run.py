import os,subprocess,json
from pathlib import Path
root=Path('/home/ze/benchmarks/roc-vector-model-20261014')
versions=[('main',Path('/home/ze/benchmarks/roc-modes-20261013/repo/target/mylinux-modes/main/build/release/roc-main-compare'),'main'),('before',root/'before','zero-generated'),('shape-and-buffers',root/'after','zero-generated')]
for rep in range(3):
 for label,binary,mode in versions[rep:]+versions[:rep]:
  name=f'{label}-t1-r{rep}'
  with (root/'results'/f'{name}.json').open('w') as out,(root/'results'/f'{name}.log').open('w') as err:
   subprocess.run(['taskset','-c','0',str(binary),'4000000','10','1',mode],stdout=out,stderr=err,check=True)
  data=json.loads((root/'results'/f'{name}.json').read_text())
  print(name,[(x['case'],round(x['execute_median_ms'],3)) for x in data['cases']],flush=True)
