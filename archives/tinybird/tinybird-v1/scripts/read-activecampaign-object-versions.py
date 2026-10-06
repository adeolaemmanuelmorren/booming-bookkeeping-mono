"""Read retained metadata for paths with repeated imports or overwritten objects."""
import json
from pathlib import Path
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

family = sys.argv[1] if len(sys.argv) == 2 else 'activecampaign'
if family not in {'activecampaign', 'stripe'}:
    raise SystemExit('Use activecampaign or stripe')
ROOT = Path(__file__).resolve().parents[1] / 'evidence' / f'{family}-history'
comparison = json.loads((ROOT / 'file-operation-comparison.json').read_text())
names = {item['name'] for source in comparison['sources'].values()
         for key in ['repeated_imports', 'current_generations_created_after_first_import'] for item in source[key]}

def read_metadata(task):
    name, flag = task
    uri = 'gs://booming-data/' + name
    process = subprocess.run([
        '/Users/adeola/google-cloud-sdk/bin/gcloud', 'storage', 'ls',
        uri, flag, '--json',
    ], text=True, capture_output=True)
    record = {'uri': uri, 'mode': flag, 'exit_code': process.returncode}
    if process.returncode == 0:
        record['metadata'] = json.loads(process.stdout)
    elif 'One or more URLs matched no objects.' in process.stderr:
        record['empty_listing'] = True
        record['metadata'] = []
    else:
        record['error'] = 'Metadata listing failed'
    return record

with ThreadPoolExecutor(max_workers=4) as pool:
    output = list(pool.map(read_metadata, [(name, flag) for name in sorted(names) for flag in ['--all-versions', '--soft-deleted']]))
path = ROOT / 'repeated-object-version-metadata.json'
path.write_text(json.dumps(output, indent=2) + '\n')
path.chmod(0o600)
for item in output:
    print(json.dumps({k: v for k, v in item.items() if k != 'metadata'} | {'objects': len(item.get('metadata', []))}))
