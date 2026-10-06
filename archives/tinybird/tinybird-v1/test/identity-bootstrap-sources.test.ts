import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { historicalStream, liveStream, fivetranStreams, SNAPSHOTS, type IdentityInputs } from '../worker/identity/bootstrap-sources.ts';
import { identityBootstrapPlan, validateProductionIdentityPlan, FROZEN_NATIVE_IMPORT_PROOF_SHA256 } from '../worker/identity/bootstrap-plan.ts';
import { IdentityBootstrapStore } from '../worker/identity/bootstrap-store.ts';
import { Tinybird, type JsonRecord } from '../worker/storage/tinybird.ts';
import { canonicalJson } from '../worker/storage/json.ts';

const T = '2026-09-05T22:28:00.000000Z';
const row = (value: JsonRecord) => ({ _fivetran_synced: T, ...value });
class SourceClient extends Tinybird {
  data: Record<string,JsonRecord[]>;
  queries: string[] = [];
  omitProjection = false;
  relatedDelay = false;
  constructor(data: Record<string,JsonRecord[]>) { super({TINYBIRD_URL:'https://api.us-east.tinybird.co',TINYBIRD_TOKEN:'fixture'}); this.data=Object.fromEntries(Object.entries(data).map(([table,rows])=>[table,rows.map(row=>('__tb_sort_1' in row)?row:{...row,__tb_sort_1:typeof row.id==='number'?row.id:String(row.id)})])); }
  async query<T>(sql: string): Promise<T[]> {
    this.queries.push(sql);
    const table = sql.match(/FROM (v1_[a-z0-9_]+)/)![1];
    const all = this.data[table] ?? [];
    const project = (rows: JsonRecord[], fields: string) => rows.map(row => fields.split(',').map(field => row[field.trim().split(' AS ')[0]] ?? null));
    if (sql.includes('AS coverage')) {
      const inner = sql.match(/\(SELECT (.+) FROM v1_\w+ WHERE __tb_sort_1 IN \((.+)\) ORDER BY __tb_sort_1 LIMIT/)!
      const ids = [...inner[2].matchAll(/'([^']*)'/g)].map(match=>match[1]);
      if (this.relatedDelay) { this.relatedDelay=false; return [{coverage:all.length-1,values:[]}] as T[]; }
      return [{coverage:all.length,values:project(all.filter(row=>ids.includes(String(row.id))),inner[1])}] as T[];
    }
    if (sql.includes('AS projected')) {
      const inner = sql.match(/\(SELECT DISTINCT (.+) FROM v1_\w+ WHERE (.+) ORDER BY (.+) LIMIT/)!
      if (typeof all[0]?.__tb_sort_1 === 'number' && /__tb_sort_1\s*(?:>|<=)\s*'/.test(inner[2])) throw new Error('Numeric source bound used a String literal');
      const rows = within(all,inner[2]);
      const projected = [...new Map(project(rows,inner[1]).map(value=>[canonicalJson(value),value])).values()];
      return [{physical:rows.length,projected:projected.length,values:this.omitProjection?projected.slice(1):projected}] as T[];
    }
    if (sql.includes('AS source_id')) {
      const match = sql.match(/SELECT DISTINCT (\w+) AS source_id FROM v1_\w+ WHERE (.+) ORDER BY \w+ LIMIT (\d+)/)!
      const values = [...new Set(within(all,match[2]).map(row=>row[match[1]]))].slice(0,Number(match[3]));
      return values.map(source_id=>({source_id})) as T[];
    }
    if (sql.startsWith('SELECT id,tags,')) return all.map(value=>({id:value.id,tags:value.tags,_fivetran_deleted:value._fivetran_deleted??false,_fivetran_synced:value._fivetran_synced})) as T[];
    throw new Error('Unexpected source-adapter query');
  }
}
function within(rows: JsonRecord[], bounds: string): JsonRecord[] {
  const conditions = [...bounds.matchAll(/(id|__tb_sort_1)\s*(<=|>)\s*(?:toInt64\()?'([^']*)'/g)];
  return rows.filter(row=>conditions.every(([,key,op,value])=>{
    const left=String(row[key]); const numeric=typeof row[key]==='number';
    return op==='>' ? numeric?Number(left)>Number(value):left>value : numeric?Number(left)<=Number(value):left<=value;
  })).sort((a,b)=> {const key='__tb_sort_1' in a?'__tb_sort_1':'id';return typeof a[key]==='number'?Number(a[key])-Number(b[key]):String(a[key]).localeCompare(String(b[key]));});
}

test('historical adapter reads compact identity fields, checks original ID and preserves null event time', async () => {
  const record={__tb_sort_1:'original',message_id:'original',anonymous_id:'visitor',user_id:' Person@Example.com ',email:'',loaded_at:null,timestamp:null,url:'not-needed'};
  const client=new SourceClient({v1_browser:[record,record]}); const store=new IdentityBootstrapStore(client,'test','fixture');
  const input={table:'v1_browser',source:'jitsu_data' as const,kind:'identify' as const,columns:Object.keys(record),expectedPhysicalRows:2,inputHash:'a'.repeat(64)};
  const pages=[];for await(const page of historicalStream(store,input,T,1000,20000).pages(null)) pages.push(page);
  assert.equal(pages.length,1);assert.equal(pages[0].physical,2);assert.equal(pages[0].rows.length,1);
  const fact=JSON.parse(pages[0].rows[0].fact_json);
  assert.equal(fact.observedAt,null);assert.equal(fact.sourceFactVersion,0);
  assert.deepEqual(fact.evidenceKeys,['anonymous_id:visitor','user_id:person@example.com']);
  assert.ok(!client.queries.some(sql=>sql.includes('url')));
  client.data.v1_browser=[{...record,__tb_sort_1:'different'}];
  await assert.rejects(async()=>{for await(const _ of historicalStream(store,input,T,1000,20000).pages(null)){}}, /physical key differs/);
});

test('a truncated projected range fails before a source cursor can advance', async () => {
  const client=new SourceClient({v1_browser:[{__tb_sort_1:'a',id:'a',anonymous_id:'v'}]});client.omitProjection=true;
  const store=new IdentityBootstrapStore(client,'test','fixture');
  await assert.rejects(async()=>{for await(const _ of historicalStream(store,{table:'v1_browser',source:'jitsu_data',kind:'page_view',columns:['id','anonymous_id'],expectedPhysicalRows:1,inputHash:'a'.repeat(64)},T,1000,20000).pages(null)){}}, /projection is incomplete/);
});

test('bulk source adapter applies AC primary suppression across pages and waits for complete related snapshots', async () => {
  const data: Record<string,JsonRecord[]> = Object.fromEntries(Object.keys(SNAPSHOTS).map(table=>[table,[]]));
  data.v1_snapshot_activecampaign_tags=[row({id:1,tags:'[KRC] Registered for Challenge'}),row({id:2,tags:'[KRC] Registered - Later'}),row({id:3,tags:'[CW] Registered for Webinar'})];
  data.v1_snapshot_activecampaign_contact_tag=[row({id:1,contact:42,tags:1}),row({id:2,contact:42,tags:2}),row({id:3,contact:42,tags:3})];
  data.v1_snapshot_activecampaign_contact=[row({id:42,email:'person@example.com'})];
  data.v1_snapshot_stripe_charge=[row({id:'ch',paid:true,status:'succeeded',customer_id:'cu',created:null})];
  data.v1_snapshot_stripe_customer=[row({id:'cu',email:'customer@example.com'})];
  const inputs:IdentityInputs={browser:[],live:{table:'v1_empty',cutoff:T,expectedPhysicalRows:0},fivetran:{snapshotAt:T,tables:Object.entries(data).map(([table,rows])=>({table,expectedPhysicalRows:rows.length}))}};
  const client=new SourceClient(data);const store=new IdentityBootstrapStore(client,'test','fixture');
  const streams=await fivetranStreams(store,inputs,1,100);
  client.relatedDelay=true;
  const facts=[];
  for(const stream of streams) for await(const page of stream.pages(null)) facts.push(...page.rows.map(row=>JSON.parse(row.fact_json)));
  assert.deepEqual(facts.map(f=>f.factKey),['stripe:ch','activecampaign:2','activecampaign:3']);
  assert.ok(facts[0].evidenceKeys.includes('email:customer@example.com'));
  assert.ok(client.queries.filter(sql=>sql.includes('AS coverage')).length>=4);
});

test('production plan pins all frozen browser/live/snapshot names, columns and physical counts', async () => {
  const inputs=JSON.parse(await readFile(new URL('../restore/identity-inputs.json',import.meta.url),'utf8')) as IdentityInputs;
  const config=await identityBootstrapPlan(inputs,'boom-identity-test-v1',FROZEN_NATIVE_IMPORT_PROOF_SHA256);
  assert.equal(config.inputs.browser.length,10);assert.equal(config.inputs.fivetran.tables.length,9);
  const reduced=structuredClone(config);reduced.inputs.fivetran.tables[0].expectedPhysicalRows--;
  await assert.rejects(validateProductionIdentityPlan(reduced), /frozen complete/);
  const missing=structuredClone(config);missing.inputs.live.expectedPhysicalRows=0;
  await assert.rejects(validateProductionIdentityPlan(missing), /frozen complete/);
});


test('old live producer sequences retain every delivery and ingestion attempt', async () => {
  const payload=JSON.stringify({messageId:'same-message',anonymousId:'visitor',userId:'person@example.com',timestamp:'2020-01-01T00:00:00Z'});
  const original={tenant_id:'test',producer_id:'producer',producer_sequence:7,message_id:'same-message',event_kind:'identify',
    delivery_event_id:'delivery-1',observed_at:T,ingested_at:'2026-09-05 21:00:00.000001',source_fact_version:1,source_deleted:0,fact_payload:payload};
  const records=[original,{...original,delivery_event_id:'delivery-2'},{...original,delivery_event_id:'delivery-2',ingested_at:'2026-09-05 21:00:00.000002'}];
  const sqls:string[]=[];
  const client={query:async (sql:string)=>{
    sqls.push(sql);
    if(sql.includes('AS projected')) {
      const fields=sql.match(/\(SELECT DISTINCT (.+) FROM v1_\w+ WHERE/)![1].split(',');
      return [{physical:3,projected:3,values:records.map(row=>fields.map(field=>(row as JsonRecord)[field]))}];
    }
    if(sql.includes(')>tuple(')) return [];
    return records.map(({producer_id,producer_sequence,delivery_event_id,ingested_at})=>({producer_id,producer_sequence,delivery_event_id,ingested_at}));
  }} as unknown as Tinybird;
  const pages=[];
  const store=new IdentityBootstrapStore(client,'test','fixture');
  for await(const page of liveStream(store,{table:'v1_history_live_jitsu',cutoff:T,expectedPhysicalRows:3},1000,20000).pages(null)) pages.push(page);
  assert.equal(pages.length,1);assert.equal(pages[0].physical,3);assert.equal(pages[0].rows.length,3);
  assert.equal(new Set(pages[0].rows.map(row=>row.origin_key)).size,3);
  assert.deepEqual(pages[0].cursor,['producer','7','delivery-2','2026-09-05 21:00:00.000002']);
  assert.ok(sqls.some(sql=>sql.includes('tuple(producer_id,producer_sequence,delivery_event_id,ingested_at)>tuple(')));
});
