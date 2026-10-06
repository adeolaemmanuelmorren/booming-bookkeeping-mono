"""Compare existing import receipts with generation-pinned Parquet metadata."""
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

family = sys.argv[1] if len(sys.argv) == 2 else 'activecampaign'
if family not in {'activecampaign', 'stripe'}:
    raise SystemExit('Use activecampaign or stripe')
ROOT = Path(__file__).resolve().parents[1] / 'evidence' / f'{family}-history'
HISTORY = ROOT
manifest = json.loads((HISTORY / 'manifest.json').read_text())
operations = json.loads((ROOT / 'file-operations.json').read_text())
result = {'checked_at': datetime.now(timezone.utc).isoformat(), 'sources': {}}

for source in sorted(manifest['sources']):
    objects = {item['name']: item for item in manifest['sources'][source]['objects']}
    rows = [item for item in operations['data'] if item['datasource_name'] == source]
    seen = set()
    outside_manifest = []
    mismatches = []
    repeated_imports = []
    new_generations_after_import = []
    for row in rows:
        uri = urlsplit(row['source_path'])
        name = unquote(uri.path).removeprefix('/booming-data/')
        if uri.hostname != 'storage.googleapis.com' or name not in objects:
            outside_manifest.append(row)
            continue
        seen.add(name)
        obj = objects[name]
        key = hashlib.sha256(obj['canonical_uri'].encode()).hexdigest()
        footer = json.loads((HISTORY / 'footers' / source / f'{key}.json').read_text())
        expected_rows = footer['row_count'] * int(row['operations'])
        if int(row['rows']) != expected_rows or int(row['quarantine_rows']):
            mismatches.append({'name': name, 'expected': expected_rows, 'logged': row})
        created = datetime.fromisoformat(obj['timeCreated'])
        first_import = datetime.fromisoformat(row['first_operation']).replace(tzinfo=timezone.utc)
        if created > first_import:
            new_generations_after_import.append({'name': name, 'generation_created': obj['timeCreated'], 'first_import': row['first_operation']})
        if int(row['operations']) > 1:
            repeated_imports.append({
                'name': name,
                'generation': obj['generation'],
                'object_rows': footer['row_count'],
                'operations': int(row['operations']),
                'extra_rows': footer['row_count'] * (int(row['operations']) - 1),
                'first_import': row['first_operation'],
                'last_import': row['last_operation'],
            })
    result['sources'][source] = {
        'frozen_objects': len(objects),
        'logged_unique_object_paths': len(rows),
        'objects_without_logged_import': sorted(set(objects) - seen),
        'imports_outside_manifest': outside_manifest,
        'file_row_count_mismatches': mismatches,
        'current_generations_created_after_first_import': new_generations_after_import,
        'repeated_imports': repeated_imports,
        'extra_rows_explained_by_repeated_imports': sum(item['extra_rows'] for item in repeated_imports),
        'logged_rows': sum(int(item['rows']) for item in rows),
    }

out = ROOT / 'file-operation-comparison.json'
out.write_text(json.dumps(result, indent=2) + '\n')
out.chmod(0o600)
print(json.dumps({source: {key: len(value) if isinstance(value, list) else value for key,value in row.items()} for source,row in result['sources'].items()}))
