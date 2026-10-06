"""Enable commit history on the nine approved raw source tables. Metadata only."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / 'restore' / 'fivetran-change-history'
PROJECT = 'able-folio-499722'
MODE = sys.argv[1] if len(sys.argv) > 1 else 'prepare'
if MODE not in {'prepare', 'enable', 'status', 'probe'}:
    raise SystemExit('Use prepare, enable, status, or probe')
DIRECTORY.mkdir(parents=True, exist_ok=True)

login = subprocess.run([
    '/Users/adeola/google-cloud-sdk/bin/gcloud', 'auth', 'print-access-token',
    '--project', PROJECT,
], capture_output=True, text=True, check=True)
token = login.stdout.strip()


def request(path, body=None, missing=False):
    req = Request('https://bigquery.googleapis.com/bigquery/v2/projects/' + PROJECT + '/' + path,
                  data=json.dumps(body).encode() if body is not None else None,
                  headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urlopen(req, timeout=60) as response:
            return json.load(response)
    except HTTPError as error:
        if missing and error.code == 404:
            return None
        details = json.loads(error.read()).get('error', {})
        message = details.get('message', '')[:500] if MODE == 'probe' else ''
        raise RuntimeError(f'BigQuery metadata request failed with HTTP {error.code}: {message}') from None


def save(path, value, exclusive=False):
    with path.open('x' if exclusive else 'w') as file:
        file.write(json.dumps(value, indent=2, sort_keys=True) + '\n')


def table_metadata(source):
    row = request('datasets/' + quote(source['dataset']) + '/tables/' + quote(source['table']))
    if row.get('type') != 'TABLE' or row.get('tableReference', {}).get('projectId') != PROJECT:
        raise RuntimeError('Source is not a regular table in the approved project')
    if row.get('streamingBuffer') or row.get('timePartitioning') or row.get('rangePartitioning'):
        raise RuntimeError('Source storage contract changed; review before enabling history')
    return {'creationTime': row['creationTime'], 'id': row['id']}


path = DIRECTORY / 'plan.json'
if not path.exists():
    if MODE != 'prepare':
        raise RuntimeError('Prepare and review the nine-table plan first')
    snapshot = json.loads((ROOT / 'restore/fivetran-snapshot/plan.json').read_text())
    names = {table['source'] for table in snapshot['tables']}
    definitions = {}
    for line in (ROOT / 'fivetran-transport/src/table-manifest.generated.ts').read_text().splitlines():
        if not line.strip().startswith('{"resourceName":'):
            continue
        row = json.loads(line.strip().rstrip(','))
        if row['resourceName'] in names:
            definitions[row['resourceName']] = row
    if len(names) != 9 or set(definitions) != names:
        raise RuntimeError('Expected the exact nine frozen conversion sources')
    tables = []
    for name in sorted(names):
        source = definitions[name]['source']
        if source['project'] != PROJECT:
            raise RuntimeError('Source project changed')
        tables.append({'name': name, 'source': source, **table_metadata(source)})
    query = '\n'.join(
        f"ALTER TABLE `{PROJECT}.{table['source']['dataset']}.{table['source']['table']}` "
        'SET OPTIONS(enable_change_history=TRUE);'
        for table in tables
    )
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    plan = {'project': PROJECT, 'location': 'US', 'snapshotAt': snapshot['snapshot_at'],
            'jobId': 'tinybird_v1_enable_change_history_' + stamp, 'tables': tables,
            'query': query, 'querySha256': hashlib.sha256(query.encode()).hexdigest()}
    save(path, plan, exclusive=True)

plan = json.loads(path.read_text())
if plan['project'] != PROJECT or len(plan['tables']) != 9:
    raise RuntimeError('Unexpected change-history plan')
if hashlib.sha256(plan['query'].encode()).hexdigest() != plan['querySha256']:
    raise RuntimeError('Planned query checksum changed')
for table in plan['tables']:
    actual = table_metadata(table['source'])
    if actual['id'] != table['id'] or actual['creationTime'] != table['creationTime']:
        raise RuntimeError('Source was recreated after preparation')

if MODE == 'prepare':
    print(json.dumps({'prepared': True, 'tables': 9, 'jobId': plan['jobId'], 'query': plan['query']}))
    raise SystemExit(0)

job = request('jobs/' + quote(plan['jobId']) + '?location=US', missing=True)
if job is None and MODE == 'enable':
    job = request('jobs', {
        'jobReference': {'projectId': PROJECT, 'location': 'US', 'jobId': plan['jobId']},
        'configuration': {'query': {'query': plan['query'], 'useLegacySql': False}},
        'labels': {'purpose': 'tinybird_v1_change_history'},
    })
if job is None:
    print(json.dumps({'state': 'NOT_STARTED'}))
    raise SystemExit(0)
if job.get('configuration', {}).get('query', {}).get('query') != plan['query']:
    raise RuntimeError('Existing job differs from the planned enablement')
status = job.get('status', {})
if status.get('errorResult'):
    raise RuntimeError('Change-history enablement failed; inspect job metadata before continuing')
result = {'checkedAt': datetime.now(timezone.utc).isoformat(), 'state': status.get('state'),
          'jobId': plan['jobId'], 'enabled': False}
if status.get('state') == 'DONE':
    clauses = []
    for table in plan['tables']:
        source = table['source']
        clauses.append(
            f"SELECT '{table['name']}' AS name, is_change_history_enabled FROM "
            f"`{PROJECT}.{source['dataset']}.INFORMATION_SCHEMA.TABLES` "
            f"WHERE table_name='{source['table']}'"
        )
    options = request('queries', {'query': '\nUNION ALL\n'.join(clauses),
                                 'useLegacySql': False, 'location': 'US', 'timeoutMs': 20000})
    rows = {row['f'][0]['v']: row['f'][1]['v'].lower() for row in options.get('rows', [])}
    if not options.get('jobComplete') or set(rows) != {row['name'] for row in plan['tables']} or set(rows.values()) != {'yes'}:
        raise RuntimeError('All nine change-history options must read back as enabled')
    completed_ms = int(job['statistics']['endTime'])
    result.update({'enabled': True,
                   'enabledThrough': datetime.fromtimestamp(completed_ms / 1000, timezone.utc).isoformat(),
                   'tables': plan['tables']})
save(DIRECTORY / 'status.json', result)
if MODE == 'probe':
    if not result['enabled']:
        raise RuntimeError('Finish enabling all nine tables before probing history')
    start = result['enabledThrough']
    end = datetime.now(timezone.utc).isoformat()
    clauses = []
    for table in plan['tables']:
        source = table['source']
        clauses.append(
            f"SELECT '{table['name']}' AS name, COUNT(*) AS changes FROM CHANGES("
            f"TABLE `{PROJECT}.{source['dataset']}.{source['table']}`, "
            f"TIMESTAMP '{start}', TIMESTAMP '{end}')"
        )
    probe = request('queries', {'query': '\nUNION ALL\n'.join(clauses),
                               'useLegacySql': False, 'location': 'US', 'timeoutMs': 20000,
                               'maximumBytesBilled': str(1024 ** 3)})
    if not probe.get('jobComplete') or len(probe.get('rows', [])) != 9:
        raise RuntimeError('Recent-history probe has not completed')
    result = {'checkedAt': end, 'start': start, 'end': end, 'recentHistoryReadable': True,
              'counts': {row['f'][0]['v']: int(row['f'][1]['v']) for row in probe['rows']},
              'totalBytesProcessed': probe.get('totalBytesProcessed')}
    save(DIRECTORY / 'recent-history-probe.json', result)
print(json.dumps(result))
