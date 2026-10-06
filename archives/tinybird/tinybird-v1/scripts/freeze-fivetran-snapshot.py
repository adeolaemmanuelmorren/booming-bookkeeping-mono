"""Save generation and checksum metadata for the completed fixed-time exports."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1] / 'restore/fivetran-snapshot'
plan_bytes = (ROOT / 'plan.json').read_bytes()
plan = json.loads(plan_bytes)
status = json.loads((ROOT / 'status-result.json').read_text())
if not status['complete'] or status['snapshot_at'] != plan['snapshot_at']:
    raise RuntimeError('Every fixed-time export must finish first')
credential = subprocess.run(['/Users/adeola/google-cloud-sdk/bin/gcloud', 'auth', 'print-access-token'], capture_output=True, text=True)
if credential.returncode:
    raise RuntimeError('GCP authentication is unavailable')
token = credential.stdout.strip()
stats = {row['source']: row for row in status['tables']}
tables = []
for table in plan['tables']:
    objects = []
    page = None
    while True:
        params = {'prefix': table['prefix'], 'maxResults': 1000,
                  'fields': 'items(bucket,name,generation,metageneration,size,md5Hash,crc32c,timeCreated,updated),nextPageToken'}
        if page:
            params['pageToken'] = page
        url = 'https://storage.googleapis.com/storage/v1/b/booming-data/o?' + urlencode(params)
        with urlopen(Request(url, headers={'Authorization': 'Bearer ' + token}), timeout=60) as response:
            result = json.load(response)
        objects.extend(result.get('items', []))
        page = result.get('nextPageToken')
        if not page:
            break
    if len(objects) != int(stats[table['source']]['export']['fileCount']):
        raise RuntimeError('GCS file count differs from the completed export job')
    if any(not row['name'].endswith('.parquet') or not row.get('md5Hash') or not row.get('crc32c') for row in objects):
        raise RuntimeError('Unexpected source snapshot object')
    tables.append({'source': table['source'], 'landing': table['landing'],
                   'rows': int(stats[table['source']]['export']['rowCount']),
                   'objects': sorted(objects, key=lambda row: row['name'])})
manifest = {'verified_at': datetime.now(timezone.utc).isoformat(), 'snapshot_at': plan['snapshot_at'],
            'plan_sha256': hashlib.sha256(plan_bytes).hexdigest(), 'tables': tables}
target = ROOT / 'manifest.json'
if target.exists():
    prior = json.loads(target.read_text())
    if prior['tables'] != tables or prior['plan_sha256'] != manifest['plan_sha256']:
        raise RuntimeError('Frozen snapshot objects changed')
else:
    target.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
print(json.dumps({'verified': True, 'snapshot_at': plan['snapshot_at'], 'tables': len(tables),
                  'objects': sum(len(row['objects']) for row in tables), 'rows': sum(row['rows'] for row in tables)}))
