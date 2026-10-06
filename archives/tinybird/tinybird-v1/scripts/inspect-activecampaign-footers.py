"""Inspect only generation-pinned Parquet footers, never event data pages."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import io
import json
import os
import struct
import subprocess
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pyarrow.parquet as pq
import importlib.util
from pathlib import Path
_spec = importlib.util.spec_from_file_location('ac_inventory', Path(__file__).with_name('freeze-activecampaign-history.py'))
_inventory = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_inventory)
ROOT, GCLOUD, save = _inventory.ROOT, _inventory.GCLOUD, _inventory.save

TIME_COLUMNS = {'_fivetran_synced', 'updated_timestamp', 'c_date', 'u_date', 'timestamp', 'received_at', 'sent_at', 'loaded_at', 'submitted_at', 'original_timestamp', 'uuid_ts', '__source_partition_time', 'source_partition_time'}


def read_range(item, token, first, last):
    query = urlencode({'alt': 'media', 'generation': item['generation']})
    url = f"https://storage.googleapis.com/storage/v1/b/{item['bucket']}/o/{quote(item['name'], safe='')}?{query}"
    request = Request(url, headers={'Authorization': f'Bearer {token}', 'Range': f'bytes={first}-{last}'})
    with urlopen(request, timeout=60) as response:
        expected_range = f"bytes {first}-{last}/{item['size']}"
        if response.status != 206 or response.headers.get('Content-Range') != expected_range:
            raise RuntimeError('Server did not honor the exact metadata-only byte range')
        data = response.read(last - first + 2)
    if len(data) != last - first + 1:
        raise RuntimeError('Metadata range byte count did not match')
    return data


def inspect(source, item, token):
    directory = ROOT / 'footers' / source
    directory.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(item['uri'].encode()).hexdigest()
    evidence_path = directory / f'{key}.json'
    if evidence_path.exists():
        evidence = json.loads(evidence_path.read_text())
        if evidence.get('complete') and evidence['uri'] == item['uri']:
            return evidence
    for attempt in range(4):
        try:
            trailer = read_range(item, token, item['size'] - 8, item['size'] - 1)
            metadata_size, magic = struct.unpack('<I4s', trailer)
            if magic != b'PAR1':
                raise RuntimeError('Object is not an unencrypted Parquet file')
            if metadata_size > 16 * 1024 * 1024 or metadata_size + 8 >= item['size']:
                raise RuntimeError('Unexpected Parquet metadata length')
            footer = read_range(item, token, item['size'] - 8 - metadata_size, item['size'] - 9)
            metadata = pq.read_metadata(io.BytesIO(b'PAR1' + footer + trailer))
            schema = metadata.schema.to_arrow_schema()
            times = {}
            for index in range(metadata.num_row_groups):
                group = metadata.row_group(index)
                for column_index in range(group.num_columns):
                    column = group.column(column_index)
                    if column.path_in_schema not in TIME_COLUMNS:
                        continue
                    statistics = column.statistics
                    if statistics is None:
                        continue
                    values = times.setdefault(column.path_in_schema, {'min': None, 'max': None, 'null_count': 0, 'row_groups_with_statistics': 0})
                    values['row_groups_with_statistics'] += 1
                    values['null_count'] += statistics.null_count or 0
                    if statistics.has_min_max:
                        minimum = str(statistics.min)
                        maximum = str(statistics.max)
                        values['min'] = minimum if values['min'] is None else min(values['min'], minimum)
                        values['max'] = maximum if values['max'] is None else max(values['max'], maximum)
            evidence = {
                'complete': True, 'uri': item['uri'], 'source': source,
                'row_count': metadata.num_rows, 'row_group_count': metadata.num_row_groups,
                'schema': str(schema), 'schema_sha256': hashlib.sha256(str(schema).encode()).hexdigest(),
                'column_types': {field.name: str(field.type) for field in schema},
                'timestamp_statistics': times, 'metadata_bytes_read': metadata_size + 8,
                'metadata_only': True, 'event_data_pages_read': False,
                'inspected_at': dt.datetime.now(dt.timezone.utc).isoformat(),
            }
            save(evidence_path, evidence)
            return evidence
        except Exception:
            if attempt == 3:
                raise RuntimeError(f'Footer inspection failed for {source} object {key}') from None
            time.sleep(attempt + 1)


def run():
    os.umask(0o077)
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--workers', type=int, default=8)
    arguments.add_argument('--limit-per-source', type=int)
    options = arguments.parse_args()
    manifest = json.loads((ROOT / 'manifest.json').read_text())
    if not manifest.get('complete'):
        raise RuntimeError('Inventory must be complete first')
    auth = subprocess.run([GCLOUD, 'auth', 'print-access-token'], capture_output=True, text=True)
    if auth.returncode:
        raise RuntimeError('Unable to obtain existing GCS credential')
    token = auth.stdout.strip()
    jobs = []
    for source, details in manifest['sources'].items():
        objects = [item for item in details['objects'] if item['uri'] == item['canonical_uri']]
        if options.limit_per_source:
            objects = objects[:options.limit_per_source]
        jobs.extend((source, item) for item in objects)
    completed = 0
    total_bytes = 0
    last_report = 0
    with ThreadPoolExecutor(max_workers=options.workers) as workers:
        pending = [workers.submit(inspect, source, item, token) for source, item in jobs]
        for future in as_completed(pending):
            result = future.result()
            completed += 1
            total_bytes += result['metadata_bytes_read']
            if time.monotonic() - last_report > 20 or completed == len(jobs):
                print(json.dumps({'inspected_footers': completed, 'total_footers': len(jobs), 'metadata_bytes': total_bytes}), flush=True)
                last_report = time.monotonic()


if __name__ == '__main__':
    run()
