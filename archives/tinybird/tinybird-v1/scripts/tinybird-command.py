"""Run the CLI with private environment auth and redact credentials from diagnostics."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
config = json.loads((root / 'tinybird-production/.tinyb').read_text())
if config['name'] != 'booming_bookkeeping' or config['host'] != 'https://api.us-east.tinybird.co':
    raise SystemExit('Unexpected Tinybird workspace')

env = dict(os.environ)
env.update(TB_TOKEN=config['token'], TB_HOST=config['host'], TB_CLI_TELEMETRY_OPTOUT='1', TB_VERSION_WARNING='0')
result = subprocess.run(['/Users/adeola/.local/bin/tb', *sys.argv[1:]], env=env, text=True, capture_output=True)
output = result.stdout + result.stderr
output = output.replace(config['token'], '[redacted]')
output = re.sub(r'(?i)(token=)[^&\s]+', r'\1[redacted]', output)
output = re.sub(r'(?i)(bearer\s+)[^\s]+', r'\1[redacted]', output)
print(output, end='')
raise SystemExit(result.returncode)
