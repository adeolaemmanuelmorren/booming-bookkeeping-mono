import { canonicalJson, compareStrings } from '../sessions/session-engine.ts';
import { hash } from './hash.ts';

export interface BootstrapPlan {
  runId: string;
  tenantId: string;
  inputManifestHash: string;
  sourceSeal: string;
  inputPartitions: string[];
  expectedSelectedPageHeads: number;
  expectedVisitors: number;
  expectedIdentityFacts: number;
  expectedIdentityComponents: number;
  firstSessionSequence: number;
}

interface Receipt {
  sourceSeal: string;
  contentHash: string;
  verifiedContentHash: string;
}

export interface SourceReceipt extends Receipt {
  partition: string;
  lastCursor: string;
  eof: true;
}

export interface SessionReceipt extends Receipt {
  sequence: number;
  firstVisitor: string;
  lastVisitor: string;
  visitorCount: number;
  pageHeadCount: number;
  sessionCount: number;
}

export interface IdentityReceipt extends Receipt {
  batchId: string;
  firstComponent: string;
  lastComponent: string;
  componentCount: number;
  factCount: number;
}

/** Readers and live writers activate only after this final manifest is durably committed. */
export async function sealBootstrap(
  plan: BootstrapPlan,
  receipts: { sources: SourceReceipt[]; sessions: SessionReceipt[]; identity: IdentityReceipt[]; pageHeadStore: Receipt },
): Promise<{
  runId: string; tenantId: string; sourceSeal: string; inputManifestHash: string;
  finalSessionSequence: number; identityVersion: 1; visitorRevision: '1';
  counts: { visitors: number; pageHeads: number; sessions: number; identityFacts: number; identityComponents: number };
  receiptsHash: string;
}> {
  const sources = unique(receipts.sources, row => row.partition).sort((a, b) => compareStrings(a.partition, b.partition));
  const sessions = unique(receipts.sessions, row => String(row.sequence)).sort((a, b) => a.sequence - b.sequence);
  const identity = unique(receipts.identity, row => row.batchId).sort((a, b) => compareStrings(a.firstComponent, b.firstComponent));
  for (const receipt of [...sources, ...sessions, ...identity, receipts.pageHeadStore]) {
    if (receipt.sourceSeal !== plan.sourceSeal || !/^[a-f0-9]{64}$/.test(receipt.contentHash)
        || receipt.verifiedContentHash !== receipt.contentHash) throw new Error('Bootstrap output has not verified at the sealed input');
  }
  if (sources.some(row => row.eof !== true) || canonicalJson(sources.map(row => row.partition)) !== canonicalJson([...plan.inputPartitions].sort(compareStrings))) {
    throw new Error('Bootstrap has incomplete source partitions');
  }
  for (let index = 0; index < sessions.length; index++) {
    const row = sessions[index];
    if (row.sequence !== plan.firstSessionSequence + index) throw new Error('Bootstrap session sequence has a gap');
    if (compareStrings(row.firstVisitor, row.lastVisitor) > 0 || (index && compareStrings(sessions[index - 1].lastVisitor, row.firstVisitor) >= 0)) {
      throw new Error('Bootstrap visitor ranges overlap or are out of order');
    }
  }
  for (let index = 0; index < identity.length; index++) {
    const row = identity[index];
    if (compareStrings(row.firstComponent, row.lastComponent) > 0 || (index && compareStrings(identity[index - 1].lastComponent, row.firstComponent) >= 0)) {
      throw new Error('Bootstrap identity component ranges overlap');
    }
  }
  const counts = {
    visitors: sum(sessions.map(row => row.visitorCount)),
    pageHeads: sum(sessions.map(row => row.pageHeadCount)),
    sessions: sum(sessions.map(row => row.sessionCount)),
    identityFacts: sum(identity.map(row => row.factCount)),
    identityComponents: sum(identity.map(row => row.componentCount)),
  };
  if (counts.visitors !== plan.expectedVisitors || counts.pageHeads !== plan.expectedSelectedPageHeads
      || counts.identityFacts !== plan.expectedIdentityFacts || counts.identityComponents !== plan.expectedIdentityComponents) {
    throw new Error('Bootstrap output totals do not cover every selected source head');
  }
  return {
    runId: plan.runId, tenantId: plan.tenantId, sourceSeal: plan.sourceSeal, inputManifestHash: plan.inputManifestHash,
    finalSessionSequence: sessions.at(-1)?.sequence ?? plan.firstSessionSequence - 1,
    identityVersion: 1, visitorRevision: '1', counts,
    receiptsHash: await hash({ sources, sessions, identity, pageHeadStore: receipts.pageHeadStore }),
  };
}

function unique<T>(rows: T[], key: (row: T) => string): T[] {
  const found = new Map<string, T>();
  for (const row of rows) {
    const prior = found.get(key(row));
    if (prior && canonicalJson(prior) !== canonicalJson(row)) throw new Error('Conflicting bootstrap receipt retry');
    found.set(key(row), row);
  }
  return [...found.values()];
}

function sum(values: number[]): number {
  if (values.some(value => !Number.isSafeInteger(value) || value < 0)) throw new Error('Invalid bootstrap count');
  const total = values.reduce((a, b) => a + b, 0);
  if (!Number.isSafeInteger(total)) throw new Error('Bootstrap count exceeds integer precision');
  return total;
}
