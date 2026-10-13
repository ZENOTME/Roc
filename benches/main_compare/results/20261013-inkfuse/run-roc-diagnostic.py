import os,subprocess,json
from pathlib import Path
root=Path('/home/ze/benchmarks/roc-llvm-20261013')
out=Path('/home/ze/inkfuse-bench-20261010/results-20261013')
binary=root/'harness/target/release/roc-inkfuse-diag'
for rep in range(3):
 for nullable in ([True,False] if rep%2==0 else [False,True]):
  for mode in (['compile','zero-generated','zero-native'] if rep%2==0 else ['zero-native','zero-generated','compile']):
   env=os.environ.copy();env['ROC_BENCH_CASES']='arithmetic_chain'
   if not nullable:env['ROC_NONNULL']='1'
   env['ROC_GRAPH_DUMP']=str(out/f'roc-graph-nullable-{nullable}.txt')
   label=f'roc-nullable-{nullable}-{mode}-r{rep}'
   with (out/f'{label}.json').open('w') as stdout,(out/f'{label}.log').open('w') as stderr:
    subprocess.run(['taskset','-c','0',str(binary),'4000000','20','1',mode],env=env,stdout=stdout,stderr=stderr,check=True)
   print(label,flush=True)
