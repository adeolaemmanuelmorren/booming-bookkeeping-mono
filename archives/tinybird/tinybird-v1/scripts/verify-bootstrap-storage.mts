import { tinybirdRequest } from './tinybird.mjs';

const { environments } = await (await tinybirdRequest('/v1/environments')).json();
const branch = environments.find((row: { name: string }) => row.name === 'v1_facts_validation');
if (branch?.id !== 'bfaead01-69a3-4215-a53b-02477bda5323' || branch?.main !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') {
  throw new Error('Wrong bootstrap validation branch');
}
process.env.TINYBIRD_URL = 'https://api.us-east.tinybird.co';
process.env.TINYBIRD_TOKEN = branch.token;
process.argv[2] = '--run';
await import('./bootstrap-smoke.ts');
