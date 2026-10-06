import { existsSync, readdirSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

function tinybirdPythonPath() {
  if (process.env.TINYBIRD_PYTHONPATH) {
    return process.env.TINYBIRD_PYTHONPATH;
  }

  const libraryRoot = path.join(
    homedir(),
    '.local',
    'share',
    'uv',
    'tools',
    'tinybird',
    'lib',
  );

  if (!existsSync(libraryRoot)) {
    throw new Error(
      'Tinybird parser not found. Install the Tinybird CLI or set TINYBIRD_PYTHONPATH.',
    );
  }

  const pythonDirectories = readdirSync(libraryRoot)
    .filter((name) => name.startsWith('python'))
    .sort()
    .reverse();

  for (const directory of pythonDirectories) {
    const sitePackages = path.join(libraryRoot, directory, 'site-packages');
    if (existsSync(path.join(sitePackages, 'tinybird'))) {
      return sitePackages;
    }
  }

  throw new Error(
    'Tinybird parser not found. Install the Tinybird CLI or set TINYBIRD_PYTHONPATH.',
  );
}

const result = spawnSync('python3', ['scripts/parse-datafiles.py'], {
  cwd: process.cwd(),
  env: {
    ...process.env,
    PYTHONPATH: tinybirdPythonPath(),
  },
  encoding: 'utf8',
});

if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);

if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
