"""Copy generation-pinned source objects within GCS; never download payloads."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1] / 'restore'
args = argparse.ArgumentParser()
args.add_argument('--limit', type=int, default=0)
options = args.parse_args()
raw_plan = (ROOT / 'history-import-plan.json').read_bytes()
plan = json.loads(raw_plan)
if plan['bucket'] != 'booming-data' or plan['destination_prefix'] != 'tinybird/v1-preserved/history/':
    raise RuntimeError('Unexpected restore destination')
items = [item for source in plan['sources'].values() for item in source['objects']]
if options.limit:
    items = items[:options.limit]
receipts = ROOT / 'copy-receipts'
receipts.mkdir(exist_ok=True)
token_result = subprocess.run(['/Users/adeola/google-cloud-sdk/bin/gcloud', 'auth', 'print-access-token'], capture_output=True, text=True)
if token_result.returncode:
    raise RuntimeError('GCS authentication is unavailable')
token = token_result.stdout.strip()

def request(path, method='GET', body=None):
    req = Request('https://storage.googleapis.com/storage/v1/' + path, data=body,
                  headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}, method=method)
    for attempt in range(4):
        try:
            with urlopen(req, timeout=60) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 3:
                raise
        except (URLError, TimeoutError):
            if attempt == 3:
                raise RuntimeError('GCS metadata connection failed after retries') from None
        time.sleep(2 ** attempt)

def verify(item, result):
    if result.get('bucket') != 'booming-data' or result.get('name') != item['destination']:
        raise RuntimeError('Copied object is outside the selected restore destination')
    if any(str(result.get(key)) != str(item[key]) for key in ['size', 'md5Hash', 'crc32c']):
        raise RuntimeError('Preserved object does not match its source checksum')

def copy(item):
    if item['bucket'] != 'booming-data' or not item['name'].startswith('tinybird/') or not item['destination'].startswith(plan['destination_prefix']):
        raise RuntimeError('Object is outside the approved migration prefixes')
    key = hashlib.sha256(item['canonical_uri'].encode()).hexdigest()
    receipt_path = receipts / (key + '.json')
    destination_path = 'b/booming-data/o/' + quote(item['destination'], safe='')
    if receipt_path.exists():
        result = request(destination_path)
        verify(item, result)
        return False
    source_path = 'b/booming-data/o/' + quote(item['name'], safe='')
    params = {'sourceGeneration': item['generation'], 'ifGenerationMatch': '0'}
    for attempt in range(4):
        try:
            while True:
                result = request(source_path + '/rewriteTo/' + destination_path + '?' + urlencode(params), 'POST', b'{}')
                if result.get('done'):
                    result = result['resource']
                    break
                if not result.get('rewriteToken'):
                    raise RuntimeError('GCS copy returned no continuation')
                params['rewriteToken'] = result['rewriteToken']
            break
        except HTTPError as error:
            if error.code == 412:
                result = request(destination_path)
                break
            if error.code not in {429, 500, 502, 503, 504} or attempt == 3:
                raise RuntimeError(f'GCS preservation failed with HTTP {error.code}') from None
            time.sleep(2 ** attempt)
    verify(item, result)
    receipt = {'verified_at': datetime.now(timezone.utc).isoformat(), 'source': item['canonical_uri'],
               'destination': result, 'rows': item['rows']}
    temporary = receipt_path.with_suffix('.partial')
    temporary.write_text(json.dumps(receipt, indent=2) + '\n')
    temporary.chmod(0o600)
    temporary.replace(receipt_path)
    return True

complete = 0
created = 0
with ThreadPoolExecutor(max_workers=8) as pool:
    futures = [pool.submit(copy, item) for item in items]
    for future in as_completed(futures):
        created += int(future.result())
        complete += 1
        if complete % 250 == 0 or complete == len(items):
            print(json.dumps({'verified_objects': complete, 'selected_objects': len(items), 'created': created}), flush=True)
result = {'verified_at': datetime.now(timezone.utc).isoformat(), 'plan_sha256': hashlib.sha256(raw_plan).hexdigest(),
          'verified_objects': complete, 'total_plan_objects': sum(len(source['objects']) for source in plan['sources'].values()),
          'complete': complete == sum(len(source['objects']) for source in plan['sources'].values())}
(ROOT / 'preservation-result.json').write_text(json.dumps(result, indent=2) + '\n')
