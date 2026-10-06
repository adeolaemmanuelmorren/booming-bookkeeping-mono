import { performance } from 'node:perf_hooks';
import { IdentityComponentIndex, buildIdentitySeedBatch } from '../worker/bootstrap/identity.ts';
import { hash } from '../worker/bootstrap/hash.ts';
import type { PendingIdentityFact } from '../worker/identity/engine.ts';
const memory=()=>{(globalThis as unknown as {gc?:()=>void}).gc?.();const m=process.memoryUsage();return {heapMiB:Math.round(m.heapUsed/1048576),rssMiB:Math.round(m.rss/1048576)};};
const before=memory();const started=performance.now();const payload='{"first_name":null,"last_name":null}';const payloadHash=await hash(payload);
const facts:PendingIdentityFact[]=Array.from({length:100000},(_,i)=>({eventId:'e'+i,producerId:'fixture',factKind:'browser',factKey:String(i).padStart(6,'0'),sourcePriority:2,sourceFactVersion:1,
  observedAt:null,ingestedAt:'2026-09-05T22:28:00.000000Z',factDeleted:false,factPayload:payload,factPayloadHash:payloadHash,evidenceKeys:['anonymous_id:v'+i,'email:person'+i+'@example.invalid']}));
const sourceMemory=memory();const graphStart=performance.now();const index=new IdentityComponentIndex(1000000);for(const fact of facts)index.addSelectedFact(fact);
const summary=index.seal(facts.length);const graphMs=Math.round(performance.now()-graphStart);const graphMemory=memory();
const batchStart=performance.now();const components=await Promise.all(facts.slice(0,10000).map(async fact=>({componentKey:index.componentKey(fact),sourceSeal:'fixture',facts:[fact],expectedFactCount:1,expectedFactHash:await hash([fact])})));
const result=await buildIdentitySeedBatch({tenantId:'fixture',batchId:'fixture',sourceSeal:'fixture',committedAt:'2026-09-05T22:28:00.000000Z',components},index);
console.log(JSON.stringify({synthetic:true,inputFacts:facts.length,graph:summary,graphMs,totalPreparationMs:Math.round(performance.now()-started),batchFacts:10000,batchRows:result.rows.length,batchMs:Math.round(performance.now()-batchStart),memory:{before,source:sourceMemory,graph:graphMemory,afterBatch:memory()}}));
