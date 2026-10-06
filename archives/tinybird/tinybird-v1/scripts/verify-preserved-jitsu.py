"""Verify local payloads and optional GCS object metadata without downloading data."""
import base64
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
GCLOUD = '/Users/adeola/google-cloud-sdk/bin/gcloud'
mode = sys.argv[1] if len(sys.argv) == 2 else 'local'
if mode not in {'local', 'cloud'}:
    raise SystemExit('Use local or cloud')
evidence = {'checked_at': datetime.now(timezone.utc).isoformat(), 'mode': mode, 'backups': {}}
previous_cutoff = None

for name in ['live-jitsu', 'live-jitsu-tail']:
    directory = ROOT / 'backups' / name
    manifest = json.loads((directory / 'manifest.json').read_text())
    if not manifest['verified'] or len(manifest['buckets']) != manifest['bucketCount']:
        raise RuntimeError('Backup manifest is incomplete')
    if previous_cutoff and previous_cutoff != manifest['lower_cutoff']:
        raise RuntimeError('Backup cutoffs do not meet')
    previous_cutoff = manifest['cutoff']
    rows = 0
    objects = []
    if mode == 'cloud':
        uri = f'gs://booming-data/tinybird/v1-preserved/{name}/**'
        response = subprocess.run([GCLOUD, 'storage', 'ls', '--json', uri], capture_output=True, text=True)
        if response.returncode:
            raise RuntimeError('GCS metadata listing failed')
        objects = [entry['metadata'] for entry in json.loads(response.stdout) if entry.get('type') == 'cloud_object']
        remote = {entry['name'].split('/')[-1]: entry for entry in objects}
        expected = {'manifest.json'} | {item['filename'] for item in manifest['buckets'].values()}
        if set(remote) != expected:
            raise RuntimeError('Cloud backup object inventory differs from manifest')
        for filename in sorted(expected):
            digest = hashlib.md5()
            path = directory / filename
            with path.open('rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
            md5 = base64.b64encode(digest.digest()).decode()
            if md5 != remote[filename]['md5Hash'] or path.stat().st_size != int(remote[filename]['size']):
                raise RuntimeError('Cloud backup checksum or size differs')
        rows = manifest['rows']
    else:
        for bucket in manifest['buckets'].values():
            count = 0
            digest = hashlib.sha256()
            with gzip.open(directory / bucket['filename'], 'rb') as stream:
                for line in stream:
                    event = json.loads(line)
                    digest.update(line)
                    payload_hash = hashlib.sha256(event['fact_payload'].encode()).hexdigest()
                    if payload_hash != event['fact_payload_hash'].lower():
                        raise RuntimeError('Stored source payload hash differs from original')
                    timestamp = event['ingested_at']
                    if timestamp >= manifest['cutoff'] or timestamp < manifest.get('lower_cutoff', ''):
                        raise RuntimeError('Backup event is outside its cutoff interval')
                    count += 1
            if count != bucket['rows'] or digest.hexdigest() != bucket['sha256']:
                raise RuntimeError('Backup bucket hash or row count differs')
            rows += count
    if rows != manifest['rows']:
        raise RuntimeError('Backup total differs')
    evidence['backups'][name] = {'rows': rows, 'cutoff': manifest['cutoff'], 'verified': True}
    if mode == 'cloud':
        evidence['backups'][name]['objects'] = objects
    print(json.dumps({'backup': name, 'rows': rows, 'verified': True}), flush=True)

evidence['total_rows'] = sum(item['rows'] for item in evidence['backups'].values())
path = ROOT / 'evidence' / 'cutover' / f'preserved-jitsu-{mode}-verified.json'
path.write_text(json.dumps(evidence, indent=2) + '\n')
path.chmod(0o600)
