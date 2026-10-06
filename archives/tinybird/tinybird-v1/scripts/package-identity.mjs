import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const [planPath, outputPath, proofPath = resolve(root, 'restore/native-import-verification.json')] = process.argv.slice(2);
if (!planPath || !outputPath) throw new Error('Usage: package-identity.mjs <plan.json> <new-directory> [native-import-proof.json]');
const { validateProductionIdentityPlan } = await import('../worker/identity/bootstrap-plan.ts');
const plan = JSON.parse(await readFile(planPath, 'utf8'));
const keys = ['baselineId', 'tenantId', 'startedAt', 'sourceSeal', 'inputs', 'rangeIds', 'maxSourceRows',
  'batchFacts', 'maxIdentifiers', 'maxComponentFacts', 'maxBatchBytes', 'nativeImportProofSha256'];
if (Object.keys(plan).some(key => !keys.includes(key))) throw new Error('Only identity configuration metadata may be packaged');
await validateProductionIdentityPlan(plan);
const proof = await readFile(proofPath);
if (proof.length > 2_000_000 || digest(proof) !== plan.nativeImportProofSha256) throw new Error('Native import proof differs from the identity plan');

const bundle = await build({ absWorkingDir: root, entryPoints: ['scripts/bootstrap-identity.ts'], bundle: true,
  write: false, metafile: true, format: 'esm', platform: 'node', target: 'node22', legalComments: 'none' });
const sources = [];
for (const input of Object.keys(bundle.metafile.inputs)) {
  const path = relative(root, resolve(root, input));
  if (!/^(scripts|worker)\/[a-zA-Z0-9_./-]+\.(ts|mjs)$/.test(path) || path.includes('..')) throw new Error('Unexpected identity build input');
  const bytes = await readFile(resolve(root, path));
  sources.push({ path, bytes: bytes.length, sha256: digest(bytes) });
}
const files = new Map([
  ['main.mjs', bundle.outputFiles[0].contents],
  ['config/identity-plan.json', Buffer.from(JSON.stringify(plan, null, 2) + '\n')],
  ['config/native-import-verification.json', proof],
  ['Dockerfile', Buffer.from('FROM node:22-bookworm-slim\nWORKDIR /app\nCOPY --chown=node:node main.mjs build-manifest.json ./\nCOPY --chown=node:node config ./config\nENV NODE_OPTIONS=--max-old-space-size=12288\nENV TINYBIRD_URL=https://api.us-east.tinybird.co\nUSER node\nCMD ["node", "main.mjs", "/app/config/identity-plan.json", "/app/config/native-import-verification.json", "/app/build-manifest.json"]\n')],
  ['.dockerignore', Buffer.from('**\n!Dockerfile\n!main.mjs\n!config/\n!config/identity-plan.json\n!config/native-import-verification.json\n!build-manifest.json\n')],
]);
const manifest = { entrypoint: 'main.mjs', snapshotAt: plan.startedAt, sourceSeal: plan.sourceSeal, sources,
  files: [...files].map(([path, bytes]) => ({ path, bytes: bytes.length, sha256: digest(bytes) })) };
files.set('build-manifest.json', Buffer.from(JSON.stringify(manifest, null, 2) + '\n'));
// A fresh directory and an explicit file list keep credentials and raw exports out of the upload.
const output = resolve(outputPath);
await mkdir(output, { mode: 0o700 });
for (const [name, bytes] of files) {
  await mkdir(dirname(resolve(output, name)), { recursive: true, mode: 0o700 });
  await writeFile(resolve(output, name), bytes, { flag: 'wx', mode: 0o600 });
}
console.log(JSON.stringify({ output, files: files.size, sourceFiles: sources.length, sourceSeal: plan.sourceSeal,
  bytes: [...files.values()].reduce((sum, bytes) => sum + bytes.length, 0) }));

function digest(bytes) { return createHash('sha256').update(bytes).digest('hex'); }
