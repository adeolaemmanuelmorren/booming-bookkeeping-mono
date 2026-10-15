"""Create an independent API key in Secret Manager without printing it."""
import os
from pathlib import Path
import secrets
import subprocess

project = 'able-folio-499722'
secret = 'boom-operations-api-key'
base = ['gcloud', '--project=' + project]
exists = subprocess.run(base + ['secrets', 'describe', secret], capture_output=True)
if exists.returncode:
    if b'NOT_FOUND' not in exists.stderr:
        raise SystemExit(exists.stderr.decode())
    subprocess.run(base + ['secrets', 'create', secret, '--replication-policy=automatic'], check=True)
    key = secrets.token_urlsafe(48)
    subprocess.run(base + ['secrets', 'versions', 'add', secret, '--data-file=-'], input=key.encode(), check=True)
else:
    key = subprocess.check_output(base + ['secrets', 'versions', 'access', 'latest', '--secret=' + secret]).decode().strip()

path = Path(__file__).resolve().parents[1] / '.env.credentials'
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, 'w') as file:
    file.write('OPERATIONS_API_KEY=' + key + '\n')
subprocess.run(base + ['secrets', 'add-iam-policy-binding', secret,
    '--member=serviceAccount:boom-operations-api@able-folio-499722.iam.gserviceaccount.com',
    '--role=roles/secretmanager.secretAccessor', '--format=none'], check=True)
print('API key saved in Secret Manager and ignored local .env.credentials, mode 0600.')
