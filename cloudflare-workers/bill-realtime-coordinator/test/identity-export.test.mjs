import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { build } from '../../../tinybird-v1/node_modules/esbuild/lib/main.js';
import { fileURLToPath } from 'node:url';
const bundled=await build({entryPoints:[fileURLToPath(new URL('../src/identity-export.ts',import.meta.url))],bundle:true,write:false,platform:'node',format:'esm'});
const {IdentityExport}=await import('data:text/javascript;base64,'+Buffer.from(bundled.outputFiles[0].contents).toString('base64'));

function context() {
  const db=new DatabaseSync(':memory:');
  db.exec('CREATE TABLE identity_state(state_kind TEXT,state_key TEXT,lookup_key TEXT,is_deleted INTEGER,payload TEXT)');
  const storage={sql:{exec(sql,...args){
    let rows=[];
    if (/^\s*(SELECT|PRAGMA)/i.test(sql)) rows=db.prepare(sql).all(...args);
    else if(args.length) db.prepare(sql).run(...args);
    else db.exec(sql);
    return {toArray:()=>rows,one:()=>{assert.equal(rows.length,1);return rows[0]},[Symbol.iterator]:()=>rows[Symbol.iterator]()};
  }},transactionSync(fn){db.exec('BEGIN');try{const value=fn();db.exec('COMMIT');return value}catch(e){db.exec('ROLLBACK');throw e}},async setAlarm(){}};
  return {db,ctx:{storage}};
}
function mockDestination(t) {
  const original=globalThis.fetch;
  const tables=new Map();
  let fail=false,corrupt=false,hiddenReads=0;
  globalThis.fetch=async(url,options)=>{
    const path=new URL(url);
    if(path.pathname==='/v0/events') {
      const name=path.searchParams.get('name');
      if(fail){fail=false;return new Response('',{status:503})}
      const rows=options.body.trim().split('\n').map(JSON.parse);
      tables.set(name,[...tables.get(name)??[],...rows]);
      return Response.json({successful_rows:rows.length,quarantined_rows:0});
    }
    if(hiddenReads>0){hiddenReads--;return Response.json({data:[]})}
    const sql=options.body.get('q');
    const table=sql.match(/FROM (\w+)/)[1];
    let rows=structuredClone(tables.get(table)??[]);
    if(table==='v1_identity_commits') {
      const version=Number(sql.match(/batch_version=(\d+)/)[1]);
      rows=rows.filter(row=>row.batch_version===version).map(row=>({row_count:row.row_count,content_hash:row.content_hash}));
    } else {
      const snapshot=sql.match(/snapshot_id='([^']+)'/)[1];
      rows=rows.filter(row=>row.snapshot_id===snapshot && sql.includes(row.row_hash));
      if(corrupt && rows.length)rows[0].source='corrupt';
    }
    return Response.json({data:[...new Map(rows.map(row=>[JSON.stringify(row),row])).values()]});
  };
  t.after(()=>{globalThis.fetch=original});
  return {tables,hideNextRead(){hiddenReads++},failNext(){fail=true},corrupt(){corrupt=true}};
}
function seed(db) {
  const mapping={identifierType:'email',identifierValue:'a@example.test',identifierKey:'email:a@example.test',profileId:'p1',firstSeenAt:'2026-09-01T00:00:00.000Z',lastSeenAt:'2026-09-01T00:00:00.000Z'};
  const profile={profileId:'p1',winnerIdentifierKey:mapping.identifierKey,memberIdentifierKeys:[mapping.identifierKey],historicalProfileIds:['old','p1'],firstSeenAt:mapping.firstSeenAt,lastSeenAt:mapping.lastSeenAt};
  for(const [kind,key,value] of [['mapping',mapping.identifierKey,mapping],['profile','p1',profile]]) db.prepare('INSERT INTO identity_state VALUES(?,?,?,?,?)').run(kind,kind+':'+key,key,0,JSON.stringify(value));
  return {mapping,profile};
}

test('copies existing state without changing it; retries before committing and preserves update order',async t=>{
  const {db,ctx}=context();t.after(()=>db.close());
  const {mapping}=seed(db);const before=db.prepare('SELECT * FROM identity_state').all();
  const destination=mockDestination(t);
  const exporter=new IdentityExport(ctx,{TINYBIRD_URL:'https://api.us-east.tinybird.co',TINYBIRD_TOKEN:'test'});exporter.initialize();
  await exporter.start(2408);
  ctx.storage.transactionSync(()=>exporter.enqueue({version:2409,batchId:'delta',createdAt:'2026-09-07T00:00:00.000Z'},[{kind:'identity',payload:{state_kind:'mapping',state_key:'m',lookup_key:mapping.identifierKey,is_deleted:1,payload_json:JSON.stringify(mapping)}}]));
  destination.failNext();await exporter.alarm();
  assert.equal(destination.tables.has('v1_identity_commits'),false);
  assert.equal(exporter.status().pending.batches,2);
  destination.hideNextRead();
  await exporter.alarm();assert.equal(exporter.status().exportedVersion,2408);
  await exporter.alarm();assert.equal(exporter.status().exportedVersion,2409);
  assert.equal(exporter.status().pending.batches,0);
  assert.ok(destination.tables.get('identifiers').some(row=>row.action==='removed'));
  assert.ok(destination.tables.get('profile_redirects').some(row=>row.old_profile_id==='old'&&row.new_profile_id==='p1'));
  assert.deepEqual(db.prepare('SELECT * FROM identity_state').all(),before);
});
test('refuses to commit corrupt delivery even when the stored hash field matches',async t=>{
  const {db,ctx}=context();t.after(()=>db.close());seed(db);
  const destination=mockDestination(t);destination.corrupt();
  const exporter=new IdentityExport(ctx,{TINYBIRD_URL:'https://api.us-east.tinybird.co',TINYBIRD_TOKEN:'test'});exporter.initialize();
  await exporter.start(2408);await exporter.alarm();
  assert.equal(destination.tables.has('v1_identity_commits'),false);
  assert.equal(exporter.status().pending.batches,1);
});
