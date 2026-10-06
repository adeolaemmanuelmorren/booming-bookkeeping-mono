"""Freeze read-only GCS ActiveCampaign export metadata. Never reads or writes BigQuery."""
import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1] / 'evidence' / 'activecampaign-history'
SOURCES = Path('/Users/adeola/Boom Bookkeeping/tinybird-production/datasources/raw')
GCLOUD = '/Users/adeola/google-cloud-sdk/bin/gcloud'


def save(path, value):
    temporary = path.with_suffix('.partial')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def run():
    os.umask(0o077)
    ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path = ROOT / 'manifest.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
    else:
        manifest = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'sources': {}, 'complete': False}
    paths = sorted(SOURCES.glob('raw_activecampaign_*.datasource'))
    for path in paths:
        source = path.stem
        uri = re.search(r'^IMPORT_BUCKET_URI (gs://.+)$', path.read_text(), re.MULTILINE).group(1)
        listing_path = ROOT / f'{source}.objects.json'
        if not listing_path.exists():
            result = subprocess.run([GCLOUD, 'storage', 'ls', '--json', uri], capture_output=True, text=True)
            if result.returncode:
                raise RuntimeError(f'Object listing failed for {source}; exit status {result.returncode}')
            save(listing_path, json.loads(result.stdout))
        raw = json.loads(listing_path.read_text())
        objects = []
        by_content = {}
        for entry in raw:
            if entry.get('type') != 'cloud_object':
                continue
            meta = entry['metadata']
            if not meta['name'].endswith('.parquet'):
                continue
            for required in ['generation', 'md5Hash', 'crc32c', 'size']:
                if required not in meta:
                    raise RuntimeError(f'{source} has an object without {required}')
            item = {key: meta[key] for key in ['bucket', 'name', 'generation', 'metageneration', 'md5Hash', 'crc32c', 'size', 'timeCreated', 'updated']}
            item['size'] = int(item['size'])
            item['uri'] = f"gs://{meta['bucket']}/{meta['name']}#{meta['generation']}"
            content_key = f"{meta['md5Hash']}:{meta['size']}"
            item['content_key'] = content_key
            item['canonical_uri'] = by_content.setdefault(content_key, item['uri'])
            objects.append(item)
        canonical = [item for item in objects if item['uri'] == item['canonical_uri']]
        manifest['sources'][source] = {
            'import_uri': uri,
            'objects': objects,
            'object_count': len(objects),
            'total_bytes': sum(item['size'] for item in objects),
            'unique_content_count': len(canonical),
            'unique_content_bytes': sum(item['size'] for item in canonical),
            'listing_sha256': hashlib.sha256(listing_path.read_bytes()).hexdigest(),
        }
        save(manifest_path, manifest)
        print(json.dumps({'source': source, 'objects': len(objects), 'unique_content': len(canonical), 'unique_bytes': sum(item['size'] for item in canonical)}), flush=True)
    manifest['complete'] = len(manifest['sources']) == 3
    manifest['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
    save(manifest_path, manifest)
    if not manifest['complete']:
        raise RuntimeError('Expected all three ActiveCampaign source tables')


if __name__ == '__main__':
    run()
