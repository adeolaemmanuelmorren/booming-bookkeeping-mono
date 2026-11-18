import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {handler,validate} from '../src/app.js';

test('only authenticated fixed warehouse operations reach the sink', async()=>{
 let calls=0;
 const token='a'.repeat(64);
 const server=createServer(handler({token,warehouse:async()=>{calls++;return {windowEnd:null};}}));
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const base=`http://127.0.0.1:${server.address().port}`;
 try{
  const body=JSON.stringify({account:'stripe'});
  assert.equal((await fetch(base+'/window',{method:'POST',body})).status,401);
  assert.equal((await fetch(base+'/query',{method:'POST',headers:{Authorization:`Bearer ${token}`},body})).status,404);
  assert.equal((await fetch(base+'/window',{method:'POST',headers:{Authorization:`Bearer ${token}`},body:JSON.stringify({account:'other'})})).status,400);
  assert.equal((await fetch(base+'/window',{method:'POST',headers:{Authorization:`Bearer ${token}`},body})).status,200);
  assert.equal(calls,1);
 }finally{await new Promise(resolve=>server.close(resolve));}
});

test('batches reject unexpected streams, mismatched IDs, secrets and future timestamps',()=>{
 const batch={account:'stripe',batchId:'fixture:0',observedAt:new Date().toISOString(),records:[{stream:'charges',id:'ch_1',payload:{id:'ch_1'}}]};
 assert.equal(validate('/records',batch),true);
 assert.equal(validate('/records',{...batch,records:[{...batch.records[0],stream:'arbitrary'}]}),false);
 assert.equal(validate('/records',{...batch,records:[{...batch.records[0],payload:{id:'ch_2'}}]}),false);
 assert.equal(validate('/records',{...batch,records:[{...batch.records[0],payload:{id:'ch_1',client_secret:'secret'}}]}),false);
 assert.equal(validate('/records',{...batch,observedAt:'2099-01-01'}),false);
 assert.equal(validate('/complete',{account:'stripe',workflowId:'run',start:10,end:9}),false);
});
