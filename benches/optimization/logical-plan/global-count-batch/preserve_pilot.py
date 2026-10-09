import json, shutil
from pathlib import Path
root = Path(__file__).resolve().parent
pilot = root / 'pilot'
pilot.mkdir()
for name in ['main', 'count', 'summary.json', 'metadata.json', 'input-files.json']:
    shutil.move(root / name, pilot / name)
meta = json.loads((pilot / 'metadata.json').read_text())
meta.pop('sample_counts', None); meta.pop('full_correctness_checks', None); meta.pop('statistics', None)
meta['pilot_note'] = 'The initial complete five-pair run is preserved under pilot, including all samples and summary. Its last main process showed substantial slowdown in multiple unchanged workloads and native DataFusion controls (e.g. scan/4t ~61 ms versus ~31 ms normally, filter/4t DF ~117 ms versus ~29 ms). Therefore the entire five-pair experiment, rather than selected samples, is repeated once as the confirmation run. Pilot observations remain historical and are not included in the confirmation summary.'
(root / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
