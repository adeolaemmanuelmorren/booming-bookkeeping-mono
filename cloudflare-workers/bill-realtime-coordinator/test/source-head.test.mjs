import test from 'node:test';
import assert from 'node:assert/strict';
import {build} from '../../../tinybird-v1/node_modules/esbuild/lib/main.js';
const bundle = await build({entryPoints:['cloudflare-workers/bill-realtime-coordinator/src/replacements.ts'],bundle:true,write:false,platform:'node',format:'esm'});
const {compactSourceHead,replacementFacts} = await import('data:text/javascript;base64,'+Buffer.from(bundle.outputFiles[0].text).toString('base64'));
test('compacted contact preserves retries, conflicts, and removed registration facts', async()=>{
 const original={source:'activecampaign',source_account:'default',scope_id:'contact:123',replacement_id:'r1',observation_sequence:1,observed_at:'2026-09-01T00:00:00Z',source_evidence:{large:'x'.repeat(20000)},rows:[{form_submission_id:'reg1',email:'test@example.com',phone:null,first_name:'Test',occurred_at:'2026-09-01T00:00:00Z'}]};
 const compact=await compactSourceHead(original);
 assert.ok(JSON.stringify(compact).length < JSON.stringify(original).length / 10);
 assert.deepEqual(await replacementFacts(original,compact),[]);
 await assert.rejects(replacementFacts({...original,rows:[]},compact),/Conflicting source version/);
 const removed={...original,observation_sequence:2,replacement_id:'r2',rows:[]};
 assert.deepEqual(await replacementFacts(removed,compact),await replacementFacts(removed,original));
 assert.equal(compact.rows[0].form_submission_id,'reg1');
});
