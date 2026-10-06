import {
  affectedIdentifierKeys, changedIdentityFacts, runIdentityEngine,
  type CurrentIdentityFact, type CurrentIdentityMapping, type CurrentIdentityProfile,
  type IdentityEngineResult, type PendingIdentityFact,
} from './engine.ts';

export interface IdentityEvidence { identifierKey: string; factKey: string }

/** Every read in one computation must use the same committed version. */
export interface IdentityStateReader {
  facts(keys: string[]): Promise<CurrentIdentityFact[]>;
  mappings(keys: string[]): Promise<CurrentIdentityMapping[]>;
  profiles(ids: string[]): Promise<CurrentIdentityProfile[]>;
  evidence(keys: string[]): Promise<IdentityEvidence[]>;
}

export interface IdentityBatch {
  tenantId: string;
  version: number;
  id: string;
  committedAt: string;
  facts: PendingIdentityFact[];
}

/** Load complete affected components before changing any graph edges. */
export async function computeIdentityBatch(batch: IdentityBatch, state: IdentityStateReader): Promise<IdentityEngineResult> {
  const priorInputs = await state.facts(unique(batch.facts.map(fact => fact.factKey)));
  const changed = changedIdentityFacts(batch.facts, priorInputs);
  const initialKeys = affectedIdentifierKeys(changed, priorInputs);
  const initialMappings = await state.mappings(initialKeys);
  const profiles = await state.profiles(unique(initialMappings.map(mapping => mapping.profileId)));
  const keys = unique([...initialKeys, ...profiles.flatMap(profile => profile.memberIdentifierKeys)]);
  const mappings = await state.mappings(keys);
  const evidence = await state.evidence(keys);
  const facts = await state.facts(unique([...changed.map(fact => fact.factKey), ...evidence.map(row => row.factKey)]));

  // A broken reverse index must stop publication instead of silently splitting people.
  const factKeys = new Set(facts.map(fact => fact.factKey));
  for (const row of evidence) {
    if (!factKeys.has(row.factKey)) throw new Error('Identity reverse evidence points to a missing fact');
  }
  const knownKeys = new Set(keys);
  for (const fact of facts) {
    if (fact.factDeleted || fact.isDeleted) continue;
    if (fact.evidenceKeys.some(key => !knownKeys.has(key))) {
      throw new Error('Identity component read is incomplete');
    }
  }

  return runIdentityEngine({
    tenantId: batch.tenantId, batchVersion: batch.version, batchId: batch.id,
    // Only changed inputs belong to these components. Replaying an unchanged fact
    // after discarding its source head would otherwise resurrect old evidence.
    committedAt: batch.committedAt, pendingFacts: changed,
    currentFacts: facts, currentMappings: mappings, currentProfiles: profiles,
    checkpointIngestedAt: '', checkpointEventId: '',
  });
}

function unique(values: string[]): string[] { return [...new Set(values)].sort(); }
