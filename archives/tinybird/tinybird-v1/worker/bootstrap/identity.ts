import { runIdentityEngine, type PendingIdentityFact, type IdentityEngineResult } from '../identity/engine.ts';
import { canonicalJson, compareStrings } from '../sessions/session-engine.ts';
import { hash } from './hash.ts';

/** One-time compact graph index. Payloads stay in the sealed source store. */
export class IdentityComponentIndex {
  private keys = new Map<string, number>();
  private parents: number[] = [];
  private sizes: number[] = [];
  private labels: string[] = [];
  private anchorFactCounts: number[] = [];
  private componentCounts = new Map<string, number>();
  private facts = 0;
  private previousFact: PendingIdentityFact | null = null;
  private sealed = false;
  private maxIdentifiers: number;

  constructor(maxIdentifiers: number) {
    if (!Number.isSafeInteger(maxIdentifiers) || maxIdentifiers < 1) throw new Error('Identifier memory limit is required');
    this.maxIdentifiers = maxIdentifiers;
  }

  /** Input contains only the globally selected current fact for each kind/key. */
  addSelectedFact(fact: PendingIdentityFact): void {
    if (this.sealed) throw new Error('Identity component index is sealed');
    if (this.previousFact !== null && compareFacts(fact, this.previousFact) <= 0) {
      throw new Error('Selected identity facts must arrive once in key order');
    }
    this.previousFact = fact;
    this.facts++;
    const ids = (fact.factDeleted ? [] : validKeys(fact)).map(key => this.add(key));
    if (!ids.length) {
      this.componentCounts.set(`empty:${canonicalJson([fact.factKind, fact.factKey])}`, 1);
      return;
    }
    this.anchorFactCounts[ids[0]]++;
    for (const id of ids.slice(1)) this.union(ids[0], id);
  }

  seal(expectedFacts: number): { facts: number; identifiers: number; components: number } {
    if (this.sealed) throw new Error('Identity index is already sealed');
    if (this.facts !== expectedFacts) throw new Error('Identity index scan did not include every selected fact');
    for (let id = 0; id < this.parents.length; id++) {
      const key = `identifier:${this.labels[this.find(id)]}`;
      this.componentCounts.set(key, (this.componentCounts.get(key) ?? 0) + this.anchorFactCounts[id]);
    }
    this.sealed = true;
    return { facts: this.facts, identifiers: this.keys.size, components: this.componentCounts.size };
  }

  expectedComponentFacts(key: string): number {
    if (!this.sealed) throw new Error('Identity index must be sealed');
    const count = this.componentCounts.get(key);
    if (count === undefined) throw new Error('Unknown sealed identity component');
    return count;
  }

  components():{key:string;count:number}[]{
    if(!this.sealed) throw new Error('Identity component index is not sealed');
    return [...this.componentCounts].sort(([a],[b])=>compareStrings(a,b)).map(([key,count])=>({key,count}));
  }

  componentKey(fact: PendingIdentityFact): string {
    if (!this.sealed) throw new Error('Identity index must be complete before assigning components');
    const keys = fact.factDeleted ? [] : validKeys(fact);
    if (!keys.length) return `empty:${canonicalJson([fact.factKind, fact.factKey])}`;
    const roots = keys.map(key => {
      const id = this.keys.get(key);
      if (id === undefined) throw new Error('Fact contains an identifier outside the sealed component index');
      return this.find(id);
    });
    if (roots.some(root => root !== roots[0])) throw new Error('Fact crosses sealed identity components');
    return `identifier:${this.labels[roots[0]]}`;
  }

  *assignments(): Generator<{ identifierKey: string; componentKey: string }> {
    if (!this.sealed) throw new Error('Identity index must be sealed');
    for (const [key, id] of this.keys) yield { identifierKey: key, componentKey: `identifier:${this.labels[this.find(id)]}` };
  }

  private add(key: string): number {
    const existing = this.keys.get(key);
    if (existing !== undefined) return existing;
    if (this.keys.size >= this.maxIdentifiers) throw new Error('Identity index needs a larger bootstrap process; no identifiers were discarded');
    const id = this.parents.length;
    this.keys.set(key, id);
    this.parents.push(id);
    this.sizes.push(1);
    this.labels.push(key);
    this.anchorFactCounts.push(0);
    return id;
  }

  private find(id: number): number {
    let root = id;
    while (this.parents[root] !== root) root = this.parents[root];
    while (this.parents[id] !== id) {
      const next = this.parents[id];
      this.parents[id] = root;
      id = next;
    }
    return root;
  }

  private union(left: number, right: number): void {
    let first = this.find(left);
    let second = this.find(right);
    if (first === second) return;
    if (this.sizes[first] < this.sizes[second]) [first, second] = [second, first];
    this.parents[second] = first;
    this.sizes[first] += this.sizes[second];
    if (compareStrings(this.labels[second], this.labels[first]) < 0) this.labels[first] = this.labels[second];
  }
}

export interface CompleteIdentityComponent {
  componentKey: string;
  sourceSeal: string;
  facts: PendingIdentityFact[];
  expectedFactCount: number;
  expectedFactHash: string;
}

/** Several disjoint complete components may share one bulk publication batch. */
export async function buildIdentitySeedBatch(
  input: { tenantId: string; batchId: string; sourceSeal: string; committedAt: string; components: CompleteIdentityComponent[] },
  index: IdentityComponentIndex,
): Promise<IdentityEngineResult> {
  const facts: PendingIdentityFact[] = [];
  const componentKeys = new Set<string>();
  const factKeys = new Set<string>();
  for (const component of input.components) {
    if (component.sourceSeal !== input.sourceSeal) throw new Error('Identity component belongs to another source seal');
    if (componentKeys.has(component.componentKey)) throw new Error('Duplicate identity component');
    componentKeys.add(component.componentKey);
    const sorted = [...component.facts].sort(compareFacts);
    if (component.expectedFactCount !== index.expectedComponentFacts(component.componentKey)) {
      throw new Error('Identity component omits facts from the complete graph index');
    }
    if (sorted.length !== component.expectedFactCount || await hash(sorted) !== component.expectedFactHash) {
      throw new Error('Identity component read is incomplete or changed');
    }
    for (const fact of sorted) {
      if (index.componentKey(fact) !== component.componentKey) throw new Error('Identity fact is in the wrong component');
      const key = canonicalJson([fact.factKind, fact.factKey]);
      if (factKeys.has(key)) throw new Error('Duplicate selected identity fact');
      factKeys.add(key);
      facts.push(fact);
    }
  }
  return runIdentityEngine({
    tenantId: input.tenantId, batchVersion: 1, batchId: input.batchId,
    committedAt: input.committedAt, pendingFacts: facts,
    currentFacts: [], currentMappings: [], currentProfiles: [],
    checkpointIngestedAt: '', checkpointEventId: '',
  });
}

function validKeys(fact: PendingIdentityFact): string[] {
  return [...new Set(fact.evidenceKeys.filter(key => key.indexOf(':') > 0))].sort(compareStrings);
}

function compareFacts(left: PendingIdentityFact, right: PendingIdentityFact): number {
  return compareStrings(left.factKind, right.factKind) || compareStrings(left.factKey, right.factKey);
}
