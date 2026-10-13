import json,subprocess
from pathlib import Path
root=Path('/home/ze/benchmarks/roc-vector-model-20261014')
output=root/'results'/'confirmation';output.mkdir(exist_ok=True)
versions=[('main',Path('/home/ze/benchmarks/roc-modes-20261013/repo/target/mylinux-modes/main/build/release/roc-main-compare'),'main'),('before',root/'before','zero-generated'),('generated',root/'after','zero-generated'),('native',root/'after','zero-native'),('full',root/'after','compile')]
for rep in range(3):
 for threads in ([1,4] if rep%2==0 else [4,1]):
  for label,binary,mode in versions[rep:]+versions[:rep]:
   name=f'{label}-t{threads}-r{rep}'
   with (output/f'{name}.json').open('w') as out,(output/f'{name}.log').open('w') as err:
    subprocess.run(['taskset','-c','0' if threads==1 else '0,2,4,6',str(binary),'4000000','12',str(threads),mode],stdout=out,stderr=err,check=True)
   data=json.loads((output/f'{name}.json').read_text())
   print(name,[(x['case'],round(x['execute_median_ms'],3)) for x in data['cases']],flush=True)
