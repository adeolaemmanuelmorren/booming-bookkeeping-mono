import { readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { generatedFanInFiles } from './source-registry-files.mjs';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectRoot = join(scriptDirectory, '..');
const registryPath = join(projectRoot, 'project/source-registry.json');
const contractsPath = join(projectRoot, 'contracts/canonical-contracts.json');
const registry = JSON.parse(await readFile(registryPath, 'utf8'));
const canonicalContracts = JSON.parse(await readFile(contractsPath, 'utf8'));
const generatedFiles = generatedFanInFiles(registry, canonicalContracts);
const checkOnly = process.argv.includes('--check');
const changedFiles = [];

for (const [relativePath, expected] of generatedFiles) {
  const path = join(projectRoot, relativePath);
  let current = '';

  try {
    current = await readFile(path, 'utf8');
  } catch {
    // A missing generated file is reported below or created in render mode.
  }

  if (current === expected) continue;

  changedFiles.push(relativePath);
  if (!checkOnly) await writeFile(path, expected);
}

if (checkOnly && changedFiles.length > 0) {
  throw new Error(`Generated source fan-ins are stale: ${changedFiles.join(', ')}`);
}

const action = checkOnly ? 'Checked' : 'Rendered';
console.log(`${action} ${generatedFiles.size} source-registry fan-ins.`);
