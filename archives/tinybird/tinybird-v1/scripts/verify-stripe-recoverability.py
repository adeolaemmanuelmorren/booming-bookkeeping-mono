"""Verify import lineage against frozen source metadata before the authorized reset."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'evidence' / 'stripe-history'
manifest = json.loads((ROOT / 'manifest.json').read_text())
comparison = json.loads((ROOT / 'file-operation-comparison.json').read_text())
version_reads = json.loads((ROOT / 'repeated-object-version-metadata.json').read_text())
operations = json.loads((ROOT / 'operations-summary.json').read_text())
objects = {item['name']: item for source in manifest['sources'].values() for item in source['objects']}
versions = {}
problems = []
for read in version_reads:
    name = read['uri'].removeprefix('gs://booming-data/')
    if read.get('error'):
        problems.append({'reason': 'Metadata read failed', 'name': name})
    for entry in read.get('metadata', []):
        if entry.get('type') == 'cloud_object':
            versions.setdefault(name, []).append(entry['metadata'])

for name, rows in versions.items():
    if any(any(str(row[key]) != str(objects[name][key]) for key in ['md5Hash', 'crc32c', 'size']) for row in rows):
        problems.append({'reason': 'Retained object content changed', 'name': name})

sources = {}
for source, details in comparison['sources'].items():
    for key in ['imports_outside_manifest', 'file_row_count_mismatches']:
        if details[key]:
            problems.append({'reason': key, 'source': source})
    for item in details['current_generations_created_after_first_import']:
        if not any(row['timeCreated'].replace('T', ' ')[:19] <= item['first_import'] for row in versions.get(item['name'], [])):
            problems.append({'reason': 'Missing pre-import generation', 'name': item['name']})
    extra_rows = 0
    for name in details['objects_without_logged_import']:
        digest = hashlib.sha256(objects[name]['canonical_uri'].encode()).hexdigest()
        footer = json.loads((ROOT / 'footers' / source / f'{digest}.json').read_text())
        extra_rows += footer['row_count']
    if extra_rows:
        problems.append({'reason': 'Unimported files contain rows', 'source': source, 'rows': extra_rows})
    sources[source] = {'logged_rows': details['logged_rows'], 'unimported_empty_files': len(details['objects_without_logged_import']),
                       'duplicate_rows': details['extra_rows_explained_by_repeated_imports']}

for row in operations['data']:
    if row['event_type'] == 'create' and not int(row['affected_rows']):
        continue
    if row['event_type'] != 'append' or row['result'] != 'ok' or row['source_kind'] != 'gcs_http' or int(row['quarantine_rows']):
        problems.append({'reason': 'Source operation needs review', 'source': row['datasource_name']})

result = {'checked_at': datetime.now(timezone.utc).isoformat(), 'verified': not problems,
          'scope': 'Source-content recoverability from frozen objects, import receipts and retained versions',
          'sources': sources, 'retained_version_paths': len(versions), 'problems': problems}
path = ROOT / 'recoverability-verdict.json'
path.write_text(json.dumps(result, indent=2) + '\n')
path.chmod(0o600)
print(json.dumps({'verified': result['verified'], 'sources': len(sources), 'problems': problems}))
if problems:
    raise SystemExit(1)
