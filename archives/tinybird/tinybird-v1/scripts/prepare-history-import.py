"""Prepare a finite import plan and raw schemas. This does not copy or import data."""
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'restore'
SCHEMAS = ROOT.parent / 'tinybird-production' / 'datasources' / 'raw'
OUTPUT.mkdir(exist_ok=True)
(OUTPUT / 'datasources').mkdir(exist_ok=True)
plan = {'bucket': 'booming-data', 'destination_prefix': 'tinybird/v1-preserved/history/', 'sources': {}}

for family in ['history-recoverability', 'activecampaign-history', 'stripe-history']:
    evidence = ROOT / 'evidence' / family
    manifest_bytes = (evidence / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    if not manifest['complete']:
        raise RuntimeError('Source inventory is incomplete')
    for source, details in sorted(manifest['sources'].items()):
        if not re.fullmatch(r'raw_[a-z_]+', source):
            raise RuntimeError('Unexpected source name')
        candidates = [item for item in details['objects'] if item['uri'] == item['canonical_uri']]
        selected = []
        empty = []
        for item in candidates:
            key = hashlib.sha256(item['canonical_uri'].encode()).hexdigest()
            footer = json.loads((evidence / 'footers' / source / f'{key}.json').read_text())
            if not footer['complete']:
                raise RuntimeError('Source footer is incomplete')
            target = {**item, 'rows': footer['row_count'], 'destination': plan['destination_prefix'] + source + '/' + key + '.parquet'}
            (selected if footer['row_count'] else empty).append(target)
        # A wholly empty source keeps one native Parquet file as its zero-row input.
        if not selected and empty:
            selected = empty[:1]
        name = source.replace('raw_', 'v1_history_', 1)
        definition = (SCHEMAS / f'{source}.datasource').read_text()
        definition = re.sub(r'^DESCRIPTION >.*?(?=^SCHEMA >)', 'DESCRIPTION >\n    Preserved source records for the one-time V1 history restore.\n\n', definition, flags=re.M | re.S)
        definition = re.sub(r'^IMPORT_CONNECTION_NAME .*$', 'IMPORT_CONNECTION_NAME v1_history_gcs', definition, flags=re.M)
        definition = re.sub(r'^IMPORT_BUCKET_URI .*$', f"IMPORT_BUCKET_URI gs://{plan['bucket']}/{plan['destination_prefix']}{source}/*.parquet", definition, flags=re.M)
        definition = re.sub(r'^TOKEN .*\n?', '', definition, flags=re.M)
        (OUTPUT / 'datasources' / f'{name}.datasource').write_text(definition.rstrip() + '\n')
        plan['sources'][source] = {'family': family, 'landing_table': name, 'source_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
                                  'expected_physical_rows': sum(item['rows'] for item in selected), 'objects': selected}

content = json.dumps(plan, indent=2, sort_keys=True) + '\n'
(OUTPUT / 'history-import-plan.json').write_text(content)
(OUTPUT / 'connections').mkdir(exist_ok=True)
(OUTPUT / 'connections' / 'v1_history_gcs.connection').write_text(
    'TYPE gcs\nGCS_SERVICE_ACCOUNT_CREDENTIALS_JSON {{ tb_secret("BOOM_GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON", "") }}\n')
live_definition = (ROOT.parent / 'tinybird-production/datasources/live/jitsu_events_api_observations.datasource').read_text()
live_definition = 'DESCRIPTION >\n    Preserved Jitsu deliveries through the V1 buffer cutover.\n\n' + live_definition[live_definition.index('SCHEMA >'):]
live_definition += "\nIMPORT_CONNECTION_NAME v1_history_gcs\nIMPORT_BUCKET_URI gs://booming-data/tinybird/v1-preserved/live-jitsu*/bucket-*.ndjson.gz\nIMPORT_SCHEDULE '@on-demand'\nIMPORT_FORMAT ndjson\n"
(OUTPUT / 'datasources' / 'v1_history_live_jitsu.datasource').write_text(live_definition)
print(json.dumps({'sources': len(plan['sources']), 'objects': sum(len(item['objects']) for item in plan['sources'].values()),
                  'bytes': sum(obj['size'] for source in plan['sources'].values() for obj in source['objects']),
                  'rows': sum(item['expected_physical_rows'] for item in plan['sources'].values()),
                  'sha256': hashlib.sha256(content.encode()).hexdigest()}))
