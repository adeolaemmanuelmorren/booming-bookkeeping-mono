import assert from 'node:assert/strict';
import { mkdir,readFile,writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import { tinybirdConfig } from '../../scripts/tinybird.mjs';
import { epoch,generatedInputs,journeyQuery,literalRows } from './journey-query.mjs';

const directory = new URL('./journey-results/',import.meta.url);
await mkdir(directory,{recursive:true});
const config = await tinybirdConfig();
const evidence = { checkedAt:new Date().toISOString(),synthetic:true,runs:[] };
const save = () => writeFile(new URL('evidence.json',directory),JSON.stringify(evidence,null,2));

async function execute(name,sql) {
  await writeFile(new URL(`${name}.sql`,directory),sql);
  const start = performance.now();
  const response = await fetch(new URL('/v0/sql',config.host),{
    method:'POST',headers:{Authorization:`Bearer ${config.token}`},
    body:new URLSearchParams({q:`${sql}\nFORMAT JSON`}),signal:AbortSignal.timeout(30000),
  });
  if (!response.ok) {
    const error = (await response.text()).slice(0,2000);
    evidence.runs.push({name,failed:true,wallMs:Math.round(performance.now()-start),error});
    await save();
    throw new Error(error);
  }
  const result = await response.json();
  const run = {name,wallMs:Math.round(performance.now()-start),statistics:result.statistics,data:result.data};
  evidence.runs.push(run);
  await save();
  console.log(JSON.stringify({name,wallMs:run.wallMs,statistics:run.statistics}));
  return result.data;
}

const touchColumns = ['touchpoint_id','identity_key','touch_time','session_time','source','medium','campaign_id','ad_id'];
const conversionColumns = ['conversion_id','identity_key','conversion_time','amount_cents'];
const day = value => epoch+value*86400;
const mappings = Array.from({length:8},(_,i) => [[100+i,i+1],[200+i,i+1]]).flat();
const touches = [
  [1,100,0,0,'direct','none',0,0], [2,100,11,11,'google','cpc',1,1],
  [3,100,12,12,'direct','none',0,0], [4,100,16,16,'meta','paid',2,2],
  [5,100,22,22,'google','cpc',3,3], [6,101,11,11,'google','cpc',1,1],
  [7,101,12,12,'meta','paid',2,2], [8,102,12,12,'google','cpc',1,1],
  [9,104,25,25,'google','cpc',1,1], [10,105,12,12,'google','cpc',1,1],
  [11,105,12,12,'meta','paid',2,2],
  // Eligibility follows touch time, while ranking follows session start and ID.
  [12,106,14,10,'meta','paid',2,2], [13,106,12,11,'direct','none',0,0],
  [14,106,14,9,'google','cpc',1,1],
].map(([id,key,time,session,...rest]) => [id,key,day(time),day(session),...rest]);
const conversions = [
  [1,200,20,10000],[2,201,16,8000],[3,202,20,4000],[4,203,13,1000],
  [5,204,14,2000],[6,205,13,6000],[7,202,21,0],[8,200,12,5000],
  [9,203,19,500],[10,206,12,2000],[11,206,14,3000],[12,206,15,1500],
  [13,207,11,0],
].map(([id,key,time,amount]) => [id,key,day(time),amount]);

// Independent row-by-row oracle. No journey grouping or ASOF logic is reused.
function oracle(touchRows,conversionRows,mappingRows,{start=-Infinity,end=Infinity}={}) {
  const identities = new Map(mappingRows);
  const totals = new Map();
  for (const conversion of conversionRows) {
    const profile = identities.get(conversion[1]);
    const earlier = touchRows.filter(touch => identities.get(touch[1])===profile && touch[2]<=conversion[2]);
    earlier.sort((a,b) => a[3]-b[3] || a[0]-b[0]);
    if (!earlier.length) earlier.push([0,0,0,conversion[2],'offline','offline',0,0]);
    const included = earlier.filter((touch,i) => i===0 || touch[4]!=='direct' || touch[5]!=='none');
    for (const touch of earlier) {
      const rank = included.indexOf(touch);
      let mt=0,ft=0,lt=0;
      if (rank>=0) {
        ft=Number(rank===0); lt=Number(rank===included.length-1);
        if (included.length===1) mt=1;
        else if (included.length===2) mt=.5;
        else if (ft || lt) mt=.4;
        else mt=.2/(included.length-2);
      }
      if (touch[3]<start || touch[3]>=end) continue;
      const key = JSON.stringify([touch[3],touch[4],touch[6],touch[7]]);
      const row = totals.get(key) ?? {click_time:touch[3],source:touch[4],campaign_id:touch[6],ad_id:touch[7],
        conversions_mt:0,conversions_ft:0,conversions_lt:0,revenue_cents:0};
      row.conversions_mt+=mt; row.conversions_ft+=ft; row.conversions_lt+=lt;
      row.revenue_cents+=conversion[3]*mt;
      totals.set(key,row);
    }
  }
  return [...totals.values()];
}

function compare(actual,expected,byTime=false,tolerance=.01) {
  const key = row => JSON.stringify([...(byTime?[row.click_time]:[]),row.source,row.campaign_id,row.ad_id]);
  const wanted = new Map(expected.map(row => [key(row),row]));
  assert.equal(actual.length,wanted.size);
  for (const row of actual) {
    const reference = wanted.get(key(row));
    assert.ok(reference,`Unexpected group ${key(row)}`);
    for (const metric of ['conversions_mt','conversions_ft','conversions_lt','revenue_cents']) {
      assert.ok(Math.abs(row[metric]-reference[metric])<tolerance,`${key(row)} ${metric}: ${row[metric]} != ${reference[metric]}`);
    }
  }
}

for (const [name,options] of [['fixture-all',{byTime:true}],['fixture-window',{byTime:true,start:day(10),end:day(18)}]]) {
  const sql = journeyQuery(literalRows(touches,touchColumns),literalRows(conversions,conversionColumns),
    literalRows(mappings,['identity_key','profile_id']),options);
  const rows = await execute(name,sql);
  compare(rows,oracle(touches,conversions,mappings,options),true,1e-8);
  evidence.runs.at(-1).oraclePassed=true;
  await save();
}

// Rerun with an externally supplied identity merge and a late-arriving touch.
const mergedMappings = mappings.map(([key,profile]) => [key,profile===2?1:profile]);
const lateTouches = [...touches,[15,100,day(13),day(8),'google','cpc',4,4]];
const revised = await execute('fixture-late-touch-identity-merge',journeyQuery(literalRows(lateTouches,touchColumns),
  literalRows(conversions,conversionColumns),literalRows(mergedMappings,['identity_key','profile_id']),{byTime:true}));
compare(revised,oracle(lateTouches,conversions,mergedMappings),true,1e-8);
evidence.runs.at(-1).oraclePassed=true;
await save();

const baseline = JSON.parse(await readFile(new URL('./results/evidence.json',import.meta.url),'utf8'));
const scaleCases = process.argv.includes('--stress-only') ? [] : [[10000,20],[100000,20],[1000000,20],[1000000,200]];
for (const [total,length] of scaleCases) {
  const inputs = generatedInputs(total,length);
  const name = `scale-${total}-journey-${length}`;
  let rows;
  try {
    rows = await execute(name,journeyQuery(inputs.touches,inputs.conversions,inputs.mapping));
  } catch {
    continue;
  }
  compare(rows,baseline.runs.find(run => run.name===name).data);
  evidence.runs.at(-1).baselineMatch=true;
  await save();
}

const stress = generatedInputs(100000,200,300);
const stressRows = await execute('stress-repeat-conversions',journeyQuery(stress.touches,stress.conversions,stress.mapping));
// Same journey shapes as the 1M/200 baseline, with 1/10 the profiles and 100x conversions each.
const stressExpected = baseline.runs.find(run => run.name==='scale-1000000-journey-200').data.map(row => ({...row,
  conversions_mt:row.conversions_mt*10,conversions_ft:row.conversions_ft*10,
  conversions_lt:row.conversions_lt*10,revenue_cents:row.revenue_cents*10}));
compare(stressRows,stressExpected);
evidence.runs.at(-1).baselineMatch=true;
await save();

// Control: every conversion has a distinct boundary, so grouping cannot share work.
const unique = generatedInputs(100000,200,200,true);
try {
  const rows = await execute('stress-distinct-boundaries',journeyQuery(unique.touches,unique.conversions,unique.mapping));
  for (const metric of ['conversions_mt','conversions_ft','conversions_lt']) {
    assert.ok(Math.abs(rows.reduce((sum,row) => sum+row[metric],0)-unique.conversionCount)<.01);
  }
  evidence.runs.at(-1).conservationPassed=true;
} catch (error) {
  console.log(JSON.stringify({name:'stress-distinct-boundaries',failed:true,error:String(error)}));
}
await save();
