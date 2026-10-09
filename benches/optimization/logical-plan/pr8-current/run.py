import csv,hashlib,json,os,platform,random,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent;d=json.loads((root/'metadata.json').read_text());labels=list(d['source_commits']);assert len(d['binary_sha256'])==len(labels);assert len(set(d['cargo_lock_sha256'].values()))==1
meta={'rustc':subprocess.check_output(['rustc','-Vv'],text=True).strip(),'platform':platform.platform(),'cpu':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip(),'recorded_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'rustflags':os.environ.get('RUSTFLAGS',''),'profile':'Cargo release defaults; opt-level=3; no LTO override','process_replicas':5,'seed':81006,'correctness':'Every workload compares every group output against a separate row-wise SUM/COUNT/AVG reference before timing.'};d.update(meta);(root/'metadata.json').write_text(json.dumps(d,indent=2)+'\n')
rng=random.Random(81006)
for process in range(5):
 order=labels.copy();rng.shuffle(order)
 for label in order:
  binary=root/label/'benchmark';assert hashlib.sha256(binary.read_bytes()).hexdigest()==d['binary_sha256'][label]
  start=time.monotonic()
  with (root/label/f'process-{process}.csv').open('w')as out,(root/label/f'process-{process}.log').open('w')as log:r=subprocess.run([str(binary),str(process),label],stdout=out,stderr=log)
  if r.returncode:print((root/label/f'process-{process}.log').read_text());raise SystemExit(r.returncode)
  assert len(list(csv.DictReader((root/label/f'process-{process}.csv').open())))==192
  print(f'{process} {label}: passed in {time.monotonic()-start:.1f}s',flush=True)
