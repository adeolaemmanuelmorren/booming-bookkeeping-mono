import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import { tinybirdConfig } from '../../scripts/tinybird.mjs';

// Synthetic inputs only. No schema changes, customer exports, or graph computation.
// Identity mapping inputs represent an already published external graph version.
const output = new URL('./results/', import.meta.url);
await mkdir(output, { recursive: true });
const epoch = 1788246000; // September 1, 2026, midnight America/Los_Angeles.
const timestamp = day => epoch + day * 86400;
const settings = 'SETTINGS join_use_nulls=0, max_execution_time=20, max_memory_usage=536870912';
const evidence = { checkedAt: new Date().toISOString(), synthetic: true, runs: [] };
const config = await tinybirdConfig();

async function execute(name, sql) {
  await writeFile(new URL(`${name}.sql`, output), sql);
  const start = performance.now();
  const response = await fetch(new URL('/v0/sql',config.host), {
    method: 'POST', body: new URLSearchParams({ q: `${sql}\nFORMAT JSON` }),
    headers: { Authorization:`Bearer ${config.token}` }, signal:AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    // Query text and returned errors contain only this script's synthetic data.
    const message = (await response.text()).slice(0,2000);
    throw new Error(`HTTP ${response.status}: ${message}`);
  }
  const result = await response.json();
  const run = { name, wallMs: Math.round(performance.now() - start), statistics: result.statistics, data: result.data };
  evidence.runs.push(run);
  await writeFile(new URL('evidence.json', output), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(run));
  return result.data;
}

function literalRows(rows, columns) {
  const tuples = rows.map(row => `tuple(${row.map(value => typeof value === 'string' ? `'${value}'` : value).join(',')})`);
  return `SELECT ${columns.map((column, i) => `item.${i + 1} AS ${column}`).join(',')}
    FROM (SELECT arrayJoin([${tuples.join(',')}]) AS item)`;
}

function resolved(source, mapping) {
  return `SELECT facts.*, identities.profile_id FROM (${source}) facts
    INNER JOIN (${mapping}) identities ON facts.identity_key=identities.identity_key`;
}

function attribution(touches, conversions, mapping) {
  const joined = `SELECT c.conversion_id, c.amount_cents, t.touchpoint_id,
    if(t.touchpoint_id=0,c.conversion_time,t.session_time) AS click_time,
    if(t.touchpoint_id=0,'offline',t.source) AS source,
    t.campaign_id, t.ad_id,
    t.source='direct' AND t.medium='none' AS is_direct,
    t.source IN ('google','meta') AND t.medium IN ('cpc','paid','paid_social')
      AND t.campaign_id>0 AND t.ad_id>0 AS is_paid
    FROM (${resolved(conversions, mapping)}) c
    LEFT JOIN (${resolved(touches, mapping)}) t
      ON c.profile_id=t.profile_id AND t.touch_time<=c.conversion_time`;
  const ordered = `SELECT *, row_number() OVER
    (PARTITION BY conversion_id ORDER BY click_time,touchpoint_id) AS position FROM (${joined})`;
  const eligible = `SELECT *, NOT (is_direct AND position>1) AS eligible FROM (${ordered})`;
  const ranked = `SELECT *, countIf(eligible) OVER (PARTITION BY conversion_id) AS eligible_count,
    countIf(eligible) OVER (PARTITION BY conversion_id ORDER BY click_time,touchpoint_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS eligible_position FROM (${eligible})`;
  return `SELECT *, multiIf(NOT eligible,0.,eligible_count=1,1.,eligible_count=2,0.5,
    eligible_position=1 OR eligible_position=eligible_count,0.4,0.2/greatest(eligible_count-2,1)) AS mt,
    toFloat64(eligible AND eligible_position=1) AS ft,
    toFloat64(eligible AND eligible_position=eligible_count) AS lt FROM (${ranked})`;
}

// Different browser and conversion identifiers map to the same profile externally.
const mapping = literalRows(Array.from({ length: 6 }, (_, i) => [[100+i,i+1],[200+i,i+1]]).flat(),
  ['identity_key','profile_id']);
const touches = literalRows([
  [1,100,0,'direct','none',0,0], [2,100,11,'google','cpc',1,1],
  [3,100,12,'direct','none',0,0], [4,100,16,'meta','paid',2,2],
  [5,100,22,'google','cpc',3,3], [6,101,11,'google','cpc',1,1],
  [7,101,12,'meta','paid',2,2], [8,102,12,'google','cpc',1,1],
  [9,104,25,'google','cpc',1,1], [10,105,12,'google','cpc',1,1],
  [11,105,12,'meta','paid',2,2],
].map(([id,key,day,...rest]) => [id,key,timestamp(day),timestamp(day),...rest]),
['touchpoint_id','identity_key','touch_time','session_time','source','medium','campaign_id','ad_id']);
const conversions = literalRows([
  [1,200,20,10000], [2,201,16,8000], [3,202,20,4000], [4,203,13,1000],
  [5,204,14,2000], [6,205,13,6000], [7,202,21,0], [8,200,12,5000],
].map(([id,key,day,amount]) => [id,key,timestamp(day),amount]),
['conversion_id','identity_key','conversion_time','amount_cents']);
const fixture = attribution(touches, conversions, mapping);
const actual = await execute('fixture', `SELECT conversion_id,touchpoint_id,mt,ft,lt
  FROM (${fixture}) ORDER BY conversion_id,touchpoint_id ${settings}`);
const expected = [
  [1,1,.4,1,0], [1,2,.2,0,0], [1,3,0,0,0], [1,4,.4,0,1],
  [2,6,.5,1,0], [2,7,.5,0,1], [3,8,1,1,1], [4,0,1,1,1],
  [5,0,1,1,1], [6,10,.5,1,0], [6,11,.5,0,1], [7,8,1,1,1],
  [8,1,.5,1,0], [8,2,.5,0,1], [8,3,0,0,0],
].map(([conversion_id,touchpoint_id,mt,ft,lt]) => ({conversion_id,touchpoint_id,mt,ft,lt}));
assert.deepEqual(actual, expected);

const report = `SELECT source,campaign_id,ad_id,sum(amount_cents*mt) AS revenue_cents,
  sum(mt) AS conversions_mt FROM (${fixture})
  WHERE click_time>=${timestamp(10)} AND click_time<${timestamp(18)}
  GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id ${settings}`;
const reportRows = await execute('fixture-click-window', report);
assert.deepEqual(reportRows.map(row => [row.source,row.revenue_cents]),
  [['direct',0],['google',15500],['meta',11000],['offline',3000]]);

// Full outer merge at aggregate grain. Ad APIs are mocked, not fetched here.
const ads = [
  { source:'google',campaign_id:1,ad_id:1,spend_cents:1000 },
  { source:'meta',campaign_id:2,ad_id:2,spend_cents:2000 },
  { source:'google',campaign_id:99,ad_id:99,spend_cents:500 },
];
const key = row => JSON.stringify([row.source,row.campaign_id,row.ad_id]);
const merged = new Map(reportRows.map(row => [key(row), { ...row, spend_cents:0 }]));
for (const row of ads) merged.set(key(row), { revenue_cents:0, ...merged.get(key(row)), ...row });
assert.equal([...merged.values()].reduce((sum,row) => sum+row.spend_cents,0),3500);
assert.equal(merged.get(key(ads[2])).revenue_cents,0);
assert.equal([...merged.values()].reduce((sum,row) => sum+row.revenue_cents,0),29500);
evidence.fixturePassed = true;
evidence.mockSpendMergePassed = true;

function generated(total, journeyLength, conversionsPerProfile=3) {
  const profiles = total/journeyLength;
  assert.ok(Number.isInteger(profiles));
  const identity = `SELECT number+1 AS identity_key, intDiv(number,2)+1 AS profile_id FROM numbers(${profiles*2})`;
  const touch = `SELECT number+1 AS touchpoint_id,intDiv(number,${journeyLength})*2+1 AS identity_key,
    ${epoch}+toUInt64(number%${journeyLength})*3600 AS touch_time,touch_time AS session_time,
    if(number%${journeyLength} IN (0,7),'direct','google') AS source,
    if(source='direct','none','cpc') AS medium,number%4+1 AS campaign_id,number%4+1 AS ad_id
    FROM numbers(${total})`;
  const conversion = `SELECT number+1 AS conversion_id,intDiv(number,${conversionsPerProfile})*2+2 AS identity_key,
    ${epoch}+toUInt64((number%3+1)*${journeyLength}/3)*3600 AS conversion_time,
    multiIf(number%3=0,10000,number%3=1,5000,0) AS amount_cents FROM numbers(${profiles*conversionsPerProfile})`;
  const weighted = attribution(touch, conversion, identity);
  return { profiles, conversions:profiles*conversionsPerProfile, sql:`SELECT source,campaign_id,ad_id,
    sum(mt) AS conversions_mt,sum(ft) AS conversions_ft,sum(lt) AS conversions_lt,
    sum(amount_cents*mt) AS revenue_cents FROM (${weighted})
    GROUP BY source,campaign_id,ad_id ORDER BY source,campaign_id,ad_id ${settings}` };
}

// Run sequentially and stop on failure to bound load beside the historical job.
for (const [total,journeyLength] of [[10000,20],[100000,20],[1000000,20],[1000000,200]]) {
  const generatedCase = generated(total,journeyLength);
  const name = `scale-${total}-journey-${journeyLength}`;
  const rows = await execute(name,generatedCase.sql);
  const sums = rows.reduce((result,row) => ({mt:result.mt+row.conversions_mt,
    ft:result.ft+row.conversions_ft,lt:result.lt+row.conversions_lt,
    revenue:result.revenue+row.revenue_cents}), {mt:0,ft:0,lt:0,revenue:0});
  for (const model of ['mt','ft','lt']) assert.ok(Math.abs(sums[model]-generatedCase.conversions)<0.001);
  assert.ok(Math.abs(sums.revenue-generatedCase.profiles*15000)<0.1);
  evidence.runs.at(-1).input = { touchpoints:total,profiles:generatedCase.profiles,
    conversions:generatedCase.conversions,journeyLength };
  evidence.runs.at(-1).conservationPassed = true;
  await writeFile(new URL('evidence.json', output), JSON.stringify(evidence,null,2));
}

// A separate high-fanout case tests the risk hidden by total input row counts.
// 500 profiles each have 200 touchpoints and 300 conversion events.
const stress = generated(100000,200,300);
try {
  const rows = await execute('stress-repeat-conversions',stress.sql);
  const credited = rows.reduce((sum,row) => sum+row.conversions_mt,0);
  assert.ok(Math.abs(credited-stress.conversions)<0.01);
  evidence.stress = { passed:true, touchpoints:100000,conversions:stress.conversions,profiles:stress.profiles };
} catch (error) {
  evidence.stress = { passed:false,error:String(error),touchpoints:100000,
    conversions:stress.conversions,profiles:stress.profiles };
  console.log(JSON.stringify(evidence.stress));
}
await writeFile(new URL('evidence.json',output),JSON.stringify(evidence,null,2));
