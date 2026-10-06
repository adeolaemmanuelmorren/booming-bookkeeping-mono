"""Assemble only V1 fact tables and finite raw imports for deployment review."""
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path('/private/tmp/boom-v1-main-reset-project')
OUTPUT.mkdir(exist_ok=True)
for folder in ['datasources', 'connections']:
    target = OUTPUT / folder
    target.mkdir(exist_ok=True)
    files = list((ROOT / folder).glob('*')) + list((ROOT / 'restore' / folder).glob('*'))
    expected = {path.name for path in files if path.is_file()}
    for previous in target.iterdir():
        if previous.is_file() and previous.name not in expected:
            previous.unlink()
    for path in files:
        if path.is_file():
            shutil.copyfile(path, target / path.name)
            if path.suffix == '.datasource':
                # This credential stays on the Worker and supervised bootstrap job.
                # Native GCS imports keep their separate migration credential.
                grants = ['TOKEN v1_facts_runtime READ']
                if not path.stem.startswith(('v1_history_', 'v1_snapshot_', 'v1_fivetran_')):
                    grants.append('TOKEN v1_facts_runtime APPEND')
                existing = (target / path.name).read_text().splitlines()
                with (target / path.name).open('a') as output:
                    for grant in grants:
                        if grant not in existing:
                            output.write('\n' + grant + '\n')
(OUTPUT / 'tinybird.config.json').write_text(json.dumps({'dev_mode': 'manual', 'include': ['datasources', 'connections']}, indent=2) + '\n')
inventory = {str(path.relative_to(OUTPUT)): hashlib.sha256(path.read_bytes()).hexdigest()
             for path in sorted(OUTPUT.rglob('*')) if path.is_file()}
(ROOT / 'restore' / 'main-project-files.json').write_text(json.dumps(inventory, indent=2) + '\n')
print(json.dumps({'directory': str(OUTPUT), 'files': len(inventory), 'datasources': len(list((OUTPUT / 'datasources').glob('*.datasource')))}))
