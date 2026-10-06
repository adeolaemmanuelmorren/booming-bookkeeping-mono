import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import {tinybirdConfig} from '../../scripts/tinybird.mjs';
import {generatedInputs,literalRows,settings} from './journey-query.mjs';

// Read-only simulation of insert-block aggregate states, not a persisted MV deployment.
const directory=new URL('./state-results/',import.meta.url);
await mkdir(directory,{recursive:true});
const config=await tinybirdConfig();
const evidence={checkedAt:new Date().toISOString(),synthetic:true,persisted:false,runs:[]};
const save=()=>writeFile(new URL('evidence.json',directory),JSON.stringify(evidence,null,2));
async function execute(name,sql) {
  await writeFile(new URL(`${name}.sql`,directory),sql);
  const started=performance.now();
  const response=await fetch(new URL('/v0/sql',config.host),{
    method:'POST',headers:{Authorization:`Bearer ${config.token}`},
    body:new URLSearchParams({q:`${sql}\nFORMAT JSON`}),signal:AbortSignal.timeout(30000),
  });
  if (!response.ok) {
    const error=(await response.text()).slice(0,2000);
    evidence.runs.push({name,failed:true,error,wallMs:Math.round(performance.now()-started)});
    await save();
    throw new Error(error);
  }
  const result=await response.json();
  const run={name,wallMs:Math.round(performance.now()-started),statistics:result.statistics,data:result.data};
  evidence.runs.push(run);
  await save();
  console.log(JSON.stringify({name,wallMs:run.wallMs,data:run.data}));
  return result.data;
}

const columns=['batch','touchpoint_id','profile_id','touch_time','session_time','source'];
function states(rows) {
  return `SELECT profile_id,argMinMerge(first_state) AS first FROM (
    SELECT profile_id,batch,argMinState(tuple(touchpoint_id,touch_time,session_time,source),
      tuple(session_time,touchpoint_id)) AS first_state FROM (${literalRows(rows,columns)})
    GROUP BY profile_id,batch) GROUP BY profile_id ORDER BY profile_id ${settings}`;
}
const original=[[1,1,1,9,9,'google'],[1,2,1,14,14,'meta'],[2,3,1,12,12,'organic']];
assert.deepEqual(await execute('separate-batches',states(original)),[{profile_id:1,first:[1,9,9,'google']}]);
assert.deepEqual(await execute('late-earlier-touch',states([...original,[3,4,1,7,7,'organic']])),
  [{profile_id:1,first:[4,7,7,'organic']}]);
assert.deepEqual(await execute('timestamp-ties-and-retry',states([
  [1,9,1,7,7,'meta'],[2,8,1,7,7,'google'],[3,8,1,7,7,'google']])),
  [{profile_id:1,first:[8,7,7,'google']}]);

// Appending a corrected source row cannot remove its previously aggregated minimum.
const correction=[3,1,1,15,15,'google'];
const stale=await execute('append-correction-stays-stale',states([...original,correction]));
assert.equal(stale[0].first[1],9);
const repaired=await execute('rebuild-from-current-facts',states([original[1],original[2],correction]));
assert.deepEqual(repaired,[{profile_id:1,first:[3,12,12,'organic']}]);

// Dataform orders by session start but checks eligibility using touch time.
// A lifetime minimum cannot answer every historical conversion in that case.
const mismatchedTimes=[[1,1,1,20,1,'google'],[1,2,1,10,5,'meta']];
const lifetime=await execute('lifetime-summary',states(mismatchedTimes));
const eligible=await execute('conversion-time-12-reference',states(mismatchedTimes.filter(row=>row[3]<=12)));
assert.equal(lifetime[0].first[0],1);
assert.equal(eligible[0].first[0],2);
evidence.fixtureChecksPassed=true;
evidence.correctionRequiresRepair=true;
evidence.lifetimeSummaryNotGenerallyEquivalent=true;
await save();

function resolve(source,mapping) {
  return `SELECT facts.*,identities.profile_id FROM (${source}) facts INNER JOIN (${mapping}) identities
    ON facts.identity_key=identities.identity_key`;
}
function stateReport(inputs) {
  const partial=`SELECT profile_id,intDiv(touchpoint_id,10000) AS batch,
    argMinState(tuple(touchpoint_id,touch_time,session_time,source,campaign_id,ad_id),
      tuple(session_time,touchpoint_id)) AS first_state
    FROM (${resolve(inputs.touches,inputs.mapping)}) GROUP BY profile_id,batch`;
  const merged=`SELECT profile_id,argMinMerge(first_state) AS first FROM (${partial}) GROUP BY profile_id`;
  const joined=`SELECT c.amount_cents,if(s.first.1>0 AND s.first.2<=c.conversion_time,s.first.4,'offline') AS source,
    if(source='offline',0,s.first.5) AS campaign_id,if(source='offline',0,s.first.6) AS ad_id
    FROM (${resolve(inputs.conversions,inputs.mapping)}) c LEFT JOIN (${merged}) s ON c.profile_id=s.profile_id`;
  return `SELECT source,campaign_id,ad_id,count() AS conversions_ft,sum(amount_cents) AS revenue_ft_cents
    FROM (${joined}) GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id ${settings}`;
}
function referenceReport(inputs) {
  const joined=`SELECT c.conversion_id,c.amount_cents,t.touchpoint_id,t.session_time,
    if(t.touchpoint_id=0,'offline',t.source) AS source,t.campaign_id,t.ad_id
    FROM (${resolve(inputs.conversions,inputs.mapping)}) c LEFT JOIN (${resolve(inputs.touches,inputs.mapping)}) t
      ON c.profile_id=t.profile_id AND t.touch_time<=c.conversion_time`;
  const ranked=`SELECT *,row_number() OVER (PARTITION BY conversion_id ORDER BY session_time,touchpoint_id) AS rank
    FROM (${joined})`;
  return `SELECT source,campaign_id,ad_id,count() AS conversions_ft,sum(amount_cents) AS revenue_ft_cents
    FROM (${ranked}) WHERE rank=1 GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id ${settings}`;
}

// These generated cases have matching touch/session timestamps, all conversions
// after the first touch, and a complete supplied identity map. First-touch only.
for (const [total,length,count] of [[1000000,20,3],[100000,200,300]]) {
  const input=generatedInputs(total,length,count);
  const name=`${total}-touches-${input.conversionCount}-conversions`;
  let reference;
  try { reference=await execute(`reference-${name}`,referenceReport(input)); }
  catch { /* Retain a failed reference and still test the compact summary. */ }
  const summary=await execute(`states-${name}`,stateReport(input));
  const expected=[{source:'direct',campaign_id:1,ad_id:1,conversions_ft:input.conversionCount,
    revenue_ft_cents:input.profiles*count/3*15000}];
  assert.deepEqual(summary,expected);
  if (reference) assert.deepEqual(summary,reference);
  evidence.runs.at(-1).expectedTotalsPassed=true;
  evidence.runs.at(-1).referenceMatched=Boolean(reference);
  await save();
}
