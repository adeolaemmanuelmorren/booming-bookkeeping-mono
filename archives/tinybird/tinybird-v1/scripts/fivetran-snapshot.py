"""Freeze and export nine raw Fivetran tables. Only job metadata leaves GCP."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / 'restore' / 'fivetran-snapshot'
PLAN = DIRECTORY / 'plan.json'
PROJECT = 'able-folio-499722'
SOURCES = [
    'raw_activecampaign_contact', 'raw_activecampaign_contact_tag', 'raw_activecampaign_tags',
    'raw_stripe_charge', 'raw_stripe_customer', 'raw_stripe_payment_intent',
    'raw_stripe_kajabi_charge', 'raw_stripe_kajabi_customer', 'raw_stripe_kajabi_payment_intent',
]
MODE = sys.argv[1] if len(sys.argv) > 1 else 'prepare'
if MODE not in {'prepare', 'check', 'start', 'status'}:
    raise SystemExit('Use prepare, check, start, or status')
DIRECTORY.mkdir(parents=True, exist_ok=True)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def prepare():
    if PLAN.exists():
        return json.loads(PLAN.read_text())
    snapshot = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(second=0, microsecond=0)
    stamp = snapshot.strftime('%Y%m%dT%H%M%SZ')
    cutoff = snapshot.isoformat().replace('+00:00', 'Z')
    source_file = ROOT.parent / 'cloudflare-workers/bigquery-tinybird-sync/src/table-manifest.generated.ts'
    definitions = {}
    for line in source_file.read_text().splitlines():
        if line.strip().startswith('{"resourceName":'):
            row = json.loads(line.strip().rstrip(','))
            definitions[row['resourceName']] = row
    tables = []
    for name in SOURCES:
        definition = definitions[name]
        source = definition['source']
        if source['project'] != PROJECT or definition['exportKind'] != 'table':
            raise RuntimeError('Unexpected raw source contract')
        columns = []
        for column in definition['columns']:
            if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', column):
                raise RuntimeError('Unexpected source column')
            expression = f'`{column}`'
            if column in definition['jsonColumns']:
                expression = f'TO_JSON_STRING({expression}) AS `{column}`'
            elif column in definition['geographyColumns']:
                expression = f'ST_ASWKT({expression}) AS `{column}`'
            columns.append(expression)
        prefix = f'tinybird/v1-preserved/fivetran-snapshot/{stamp}/{name}/'
        uri = f'gs://booming-data/{prefix}part-*.parquet'
        query = (
            f"EXPORT DATA OPTIONS(uri='{uri}', format='PARQUET', overwrite=false) AS\n"
            + 'SELECT\n  ' + ',\n  '.join(columns)
            + f"\nFROM `{PROJECT}.{source['dataset']}.{source['table']}` FOR SYSTEM_TIME AS OF TIMESTAMP '{cutoff}'"
        )
        landing = 'v1_snapshot_' + name.removeprefix('raw_')
        template = (ROOT / 'restore/datasources' / ('v1_history_' + name.removeprefix('raw_') + '.datasource')).read_text()
        template = template.replace('Preserved source records for the one-time V1 history restore.', 'Fixed-time Fivetran source snapshot for V1 conversion facts.')
        template = re.sub(r'^IMPORT_BUCKET_URI .*$', 'IMPORT_BUCKET_URI ' + uri, template, flags=re.M)
        (ROOT / 'restore/datasources' / (landing + '.datasource')).write_text(template)
        tables.append({'source': name, 'landing': landing, 'uri': uri, 'prefix': prefix,
                       'job_id': f'tinybird_v1_snapshot_{stamp}_{name}', 'query': query,
                       'query_sha256': hashlib.sha256(query.encode()).hexdigest()})
    result = {'project': PROJECT, 'location': 'US', 'snapshot_at': cutoff, 'run_id': stamp, 'tables': tables}
    save(PLAN, result)
    return result


plan = prepare()
if plan['project'] != PROJECT or [row['source'] for row in plan['tables']] != SOURCES:
    raise RuntimeError('Unexpected frozen snapshot plan')
if MODE == 'prepare':
    print(json.dumps({'snapshot_at': plan['snapshot_at'], 'tables': len(plan['tables']), 'prepared': True}))
    raise SystemExit(0)

credential = subprocess.run(['/Users/adeola/google-cloud-sdk/bin/gcloud', 'auth', 'print-access-token'], capture_output=True, text=True)
if credential.returncode:
    raise RuntimeError('GCP authentication is unavailable')
token = credential.stdout.strip()


def request(path, body=None, missing=False):
    encoded = json.dumps(body).encode() if body is not None else None
    req = Request('https://bigquery.googleapis.com/bigquery/v2/projects/' + PROJECT + '/' + path,
                  data=encoded, headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urlopen(req, timeout=60) as response:
            return json.load(response)
    except HTTPError as error:
        if missing and error.code == 404:
            return None
        details = json.loads(error.read()).get('error', {})
        reasons = [row.get('reason') for row in details.get('errors', [])]
        raise RuntimeError(f'BigQuery metadata request failed with HTTP {error.code}: {reasons}') from None


results = []
for table in plan['tables']:
    if hashlib.sha256(table['query'].encode()).hexdigest() != table['query_sha256']:
        raise RuntimeError('Frozen query changed')
    configuration = {'query': {'query': table['query'], 'useLegacySql': False, 'maximumBytesBilled': str(5 * 1024 ** 3)}}
    if MODE == 'check':
        job = request('jobs', {'configuration': {**configuration, 'dryRun': True}, 'jobReference': {'projectId': PROJECT, 'location': 'US'}})
    else:
        job = request('jobs/' + quote(table['job_id']) + '?location=US', missing=True)
        if job is None and MODE == 'start':
            job = request('jobs', {'configuration': configuration,
                                  'jobReference': {'projectId': PROJECT, 'location': 'US', 'jobId': table['job_id']},
                                  'labels': {'purpose': 'tinybird_v1_source_snapshot'}})
        if job is None:
            results.append({'source': table['source'], 'status': 'NOT_STARTED'})
            continue
        if job.get('configuration', {}).get('query', {}).get('query') != table['query']:
            raise RuntimeError('Existing export job does not match frozen query')
    status = job.get('status', {})
    statistics = job.get('statistics', {})
    result = {'source': table['source'], 'job_id': table['job_id'], 'status': status.get('state'),
              'error_reason': status.get('errorResult', {}).get('reason'),
              'bytes_processed': statistics.get('totalBytesProcessed'),
              'export': statistics.get('query', {}).get('exportDataStatistics'),
              'started_at_ms': statistics.get('startTime'), 'ended_at_ms': statistics.get('endTime')}
    results.append(result)
    save(DIRECTORY / (table['source'] + '-' + MODE + '.json'), result)
    print(json.dumps(result), flush=True)
complete = MODE != 'check' and all(row['status'] == 'DONE' and not row.get('error_reason') and row.get('export', {}).get('rowCount') is not None for row in results)
save(DIRECTORY / (MODE + '-result.json'), {'checked_at': datetime.now(timezone.utc).isoformat(),
                                        'snapshot_at': plan['snapshot_at'], 'complete': complete, 'tables': results})
if any(row.get('error_reason') for row in results):
    raise SystemExit(1)
