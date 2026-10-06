// Pure identity semantics retained from the correctness reference. No export or reporting coordinator.
import { md5Hex } from "../sessions/md5.ts";

const ZERO_HASH = "0".repeat(64);

export interface PendingIdentityFact {
  eventId: string;
  producerId: string;
  observedAt: string | null;
  ingestedAt: string;
  factKind: string;
  factKey: string;
  sourcePriority?: number;
  sourceFactVersion: number;
  factDeleted: boolean;
  factPayloadHash: string;
  factPayload: string;
  evidenceKeys: string[];
}

export interface CurrentIdentityFact {
  producerId: string;
  factKind: string;
  factKey: string;
  sourcePriority?: number;
  sourceFactVersion: number;
  factDeleted: boolean;
  factObservedAt: string | null;
  factPayloadHash: string;
  evidenceKeys: string[];
  firstName: string;
  lastName: string;
  isDeleted: boolean;
}

export interface CurrentIdentityMapping {
  identifierType: string;
  identifierValue: string;
  identifierKey: string;
  profileId: string;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
}

export interface CurrentIdentityProfile {
  profileId: string;
  profileKey: string;
  winnerIdentifierKey: string;
  memberIdentifierKeys: string[];
  historicalProfileIds: string[];
  firstName: string;
  lastName: string;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
}

export interface IdentityEngineInput {
  tenantId: string;
  batchVersion: number;
  batchId: string;
  committedAt: string;
  pendingFacts: PendingIdentityFact[];
  currentFacts: CurrentIdentityFact[];
  currentMappings: CurrentIdentityMapping[];
  currentProfiles: CurrentIdentityProfile[];
  checkpointIngestedAt: string;
  checkpointEventId: string;
}

export interface IdentityJournalRow {
  tenant_id: string;
  state_kind: string;
  state_key: string;
  lookup_key: string;
  sub_key: string;
  batch_version: number;
  batch_id: string;
  committed_at: string;
  is_deleted: number;
  row_hash: string;
  identifier_type: string;
  identifier_value: string;
  identifier_key: string;
  profile_id: string;
  profile_key: string;
  winner_identifier_key: string;
  member_identifier_keys: string[];
  anonymous_ids: string[];
  user_ids: string[];
  emails: string[];
  phones: string[];
  historical_profile_ids: string[];
  first_name: string;
  last_name: string;
  first_seen_at: string | null;
  last_seen_at: string | null;
  fact_kind: string;
  fact_key: string;
  source_priority: number;
  source_fact_version: number;
  fact_deleted: number;
  fact_observed_at: string | null;
  fact_payload_hash: string;
  fact_payload: string;
  evidence_keys: string[];
  producer_id: string;
  checkpoint_sequence: number;
  checkpoint_ingested_at: string;
  checkpoint_event_id: string;
  prior_profile_ids: string[];
  dirty_profile_ids: string[];
  input_event_count: number;
  input_hash: string;
  output_row_count: number;
  output_hash: string;
}

export interface IdentityEngineResult {
  inputHash: string;
  changedFacts: PendingIdentityFact[];
  rows: IdentityJournalRow[];
  manifest: IdentityJournalRow;
  outputHash: string;
  diagnostics?: Record<string, number>;
}

interface LiveFact {
  producerId: string;
  factKind: string;
  factKey: string;
  sourceFactVersion: number;
  factObservedAt: string | null;
  factPayloadHash: string;
  factPayload: string;
  evidenceKeys: string[];
  firstName: string;
  lastName: string;
}

interface IdentifierObservation {
  identifierType: string;
  identifierValue: string;
  identifierKey: string;
  winnerPriority: number;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
  hasNullObservation: boolean;
  firstName: string;
  lastName: string;
  firstNameFactAt: string | null;
  firstNameFactKey: string;
  lastNameFactAt: string | null;
  lastNameFactKey: string;
}

interface RecomputedProfile extends CurrentIdentityProfile {
  anonymousIds: string[];
  userIds: string[];
  emails: string[];
  phones: string[];
  memberObservations: IdentifierObservation[];
}

export async function runIdentityEngine(
  input: IdentityEngineInput,
): Promise<IdentityEngineResult> {
  const pendingFacts = latestPendingFacts(input.pendingFacts);
  const inputHash = await sha256Hex(
    [...input.pendingFacts].map((fact) => fact.eventId).sort().join("|"),
  );
  const currentFacts = mapByFact(input.currentFacts);
  const changedFacts = pendingFacts.filter((fact) => shouldApplyFact(
    fact,
    currentFacts.get(factMapKey(fact)),
  ));

  const liveFacts = overlayLiveFacts(input.currentFacts, changedFacts);
  const observations = buildIdentifierObservations(liveFacts);
  const components = buildComponents(liveFacts, observations);
  const profiles = buildProfiles(components, input.currentMappings, input.currentProfiles);

  const rows = await buildJournalRows(input, inputHash, changedFacts, profiles);
  const distinctRows = distinctJournalRows(rows);
  const outputHash = await outputRowsHash(distinctRows);
  const manifest = await buildManifestRow(input, inputHash, distinctRows.length, outputHash);

  return {
    inputHash,
    changedFacts,
    rows: distinctRows,
    manifest,
    outputHash,
  };
}

export function affectedIdentifierKeys(
  changedFacts: PendingIdentityFact[],
  currentFacts: CurrentIdentityFact[],
): string[] {
  const currentByKey = mapByFact(currentFacts);
  const keys = new Set<string>();

  for (const fact of changedFacts) {
    const stable = currentByKey.get(factMapKey(fact));
    if (stable && !stable.isDeleted && !stable.factDeleted) {
      addValidIdentifiers(keys, stable.evidenceKeys);
    }
    if (!fact.factDeleted) addValidIdentifiers(keys, fact.evidenceKeys);
  }

  return [...keys].sort();
}

export function changedIdentityFacts(
  pendingFacts: PendingIdentityFact[],
  currentFacts: CurrentIdentityFact[],
): PendingIdentityFact[] {
  const currentByKey = mapByFact(currentFacts);
  return latestPendingFacts(pendingFacts).filter((fact) => shouldApplyFact(
    fact,
    currentByKey.get(factMapKey(fact)),
  ));
}

function latestPendingFacts(facts: PendingIdentityFact[]): PendingIdentityFact[] {
  const latest = new Map<string, PendingIdentityFact>();

  for (const fact of facts) {
    if (!Number.isSafeInteger(fact.sourceFactVersion) || fact.sourceFactVersion < 0) {
      throw new Error("Identity source version must be a safe nonnegative integer");
    }
    const key = factMapKey(fact);
    const current = latest.get(key);
    sourcePriority(fact);
    if (current && compareSourceVersions(current, fact) === 0) {
      assertMatchingVersion(fact, current);
    }
    if (!current || comparePendingFacts(fact, current) > 0) latest.set(key, fact);
  }

  return [...latest.values()].sort((left, right) => factMapKey(left).localeCompare(factMapKey(right)));
}

function comparePendingFacts(left: PendingIdentityFact, right: PendingIdentityFact): number {
  const versionOrder = compareSourceVersions(left, right);
  if (versionOrder !== 0) return versionOrder;
  if (left.ingestedAt !== right.ingestedAt) return left.ingestedAt.localeCompare(right.ingestedAt);
  return left.eventId.localeCompare(right.eventId);
}

function shouldApplyFact(
  incoming: PendingIdentityFact,
  stable: CurrentIdentityFact | undefined,
): boolean {
  if (!stable) return true;
  const versionOrder = compareSourceVersions(incoming, stable);
  if (versionOrder !== 0) return versionOrder > 0;
  assertMatchingVersion(incoming, stable);
  return false;
}

function assertMatchingVersion(
  left: PendingIdentityFact,
  right: PendingIdentityFact | CurrentIdentityFact,
): void {
  const rightObservedAt = "observedAt" in right ? right.observedAt : right.factObservedAt;
  const sameEvidence = JSON.stringify(validIdentifiers(left.evidenceKeys)) === JSON.stringify(validIdentifiers(right.evidenceKeys));
  if (left.factDeleted === right.factDeleted
      && left.factPayloadHash === right.factPayloadHash
      && left.observedAt === rightObservedAt
      && sameEvidence) return;
  throw new Error(`Conflicting identity source revision for ${left.factKind}:${left.factKey}`);
}

function sourcePriority(fact: { sourcePriority?: number }): number {
  const priority = fact.sourcePriority ?? 0;
  if (!Number.isSafeInteger(priority) || priority < 0) throw new Error("Identity source priority must be a safe nonnegative integer");
  return priority;
}

function compareSourceVersions(
  left: { sourcePriority?: number; sourceFactVersion: number },
  right: { sourcePriority?: number; sourceFactVersion: number },
): number {
  const priorityOrder = sourcePriority(left) - sourcePriority(right);
  if (priorityOrder !== 0) return priorityOrder;
  return left.sourceFactVersion - right.sourceFactVersion;
}

function overlayLiveFacts(
  currentFacts: CurrentIdentityFact[],
  changedFacts: PendingIdentityFact[],
): LiveFact[] {
  const facts = new Map<string, LiveFact>();

  for (const current of currentFacts) {
    if (current.isDeleted || current.factDeleted) continue;
    facts.set(factMapKey(current), liveFactFromCurrent(current));
  }

  for (const changed of changedFacts) {
    const key = factMapKey(changed);
    if (changed.factDeleted) {
      facts.delete(key);
      continue;
    }
    facts.set(key, liveFactFromPending(changed));
  }

  return [...facts.values()].sort((left, right) => factMapKey(left).localeCompare(factMapKey(right)));
}

function liveFactFromCurrent(fact: CurrentIdentityFact): LiveFact {
  return {
    producerId: fact.producerId,
    factKind: fact.factKind,
    factKey: fact.factKey,
    sourceFactVersion: fact.sourceFactVersion,
    factObservedAt: fact.factObservedAt,
    factPayloadHash: fact.factPayloadHash,
    factPayload: "",
    evidenceKeys: validIdentifiers(fact.evidenceKeys),
    firstName: fact.firstName,
    lastName: fact.lastName,
  };
}

function liveFactFromPending(fact: PendingIdentityFact): LiveFact {
  const names = namesFromPayload(fact.factPayload);
  return {
    producerId: fact.producerId,
    factKind: fact.factKind,
    factKey: fact.factKey,
    sourceFactVersion: fact.sourceFactVersion,
    factObservedAt: fact.observedAt,
    factPayloadHash: fact.factPayloadHash,
    factPayload: fact.factPayload,
    evidenceKeys: validIdentifiers(fact.evidenceKeys),
    firstName: names.firstName,
    lastName: names.lastName,
  };
}

function buildIdentifierObservations(facts: LiveFact[]): Map<string, IdentifierObservation> {
  const observations = new Map<string, IdentifierObservation>();

  for (const fact of facts) {
    for (const identifierKey of fact.evidenceKeys) {
      const current = observations.get(identifierKey);
      const parts = splitIdentifierKey(identifierKey);
      const next = current ?? {
        identifierType: parts.type,
        identifierValue: parts.value,
        identifierKey,
        winnerPriority: winnerPriority(parts.type),
        firstSeenAt: fact.factObservedAt,
        lastSeenAt: fact.factObservedAt,
        hasNullObservation: fact.factObservedAt === null,
        firstName: "",
        lastName: "",
        firstNameFactAt: null,
        firstNameFactKey: "",
        lastNameFactAt: null,
        lastNameFactKey: "",
      };

      next.firstSeenAt = minText(next.firstSeenAt, fact.factObservedAt);
      next.lastSeenAt = maxText(next.lastSeenAt, fact.factObservedAt);
      next.hasNullObservation ||= fact.factObservedAt === null;

      if (fact.firstName && nameFactWins(fact, next, "firstName")) {
        next.firstName = fact.firstName;
        next.firstNameFactAt = fact.factObservedAt;
        next.firstNameFactKey = fact.factKey;
      }
      if (fact.lastName && nameFactWins(fact, next, "lastName")) {
        next.lastName = fact.lastName;
        next.lastNameFactAt = fact.factObservedAt;
        next.lastNameFactKey = fact.factKey;
      }

      observations.set(identifierKey, next);
    }
  }

  return observations;
}

function nameFactWins(
  fact: LiveFact,
  observation: IdentifierObservation,
  field: "firstName" | "lastName",
): boolean {
  if (observation[field] === "") return true;
  const priorAt = observation[`${field}FactAt`];
  const priorKey = observation[`${field}FactKey`];
  if (fact.factObservedAt !== priorAt) return compareNullableTimes(fact.factObservedAt, priorAt) > 0;
  return fact.factKey > priorKey;
}

function buildComponents(
  facts: LiveFact[],
  observations: Map<string, IdentifierObservation>,
): IdentifierObservation[][] {
  const set = new DisjointSet();

  for (const fact of facts) {
    const identifiers = fact.evidenceKeys;
    if (identifiers.length === 0) continue;
    for (const identifier of identifiers) set.add(identifier);
    for (const identifier of identifiers.slice(1)) set.union(identifiers[0], identifier);
  }

  const membersByRoot = new Map<string, IdentifierObservation[]>();
  for (const [identifierKey, observation] of observations) {
    const root = set.find(identifierKey);
    const members = membersByRoot.get(root) ?? [];
    members.push(observation);
    membersByRoot.set(root, members);
  }

  return [...membersByRoot.values()]
    .map((members) => members.sort((left, right) => left.identifierKey.localeCompare(right.identifierKey)))
    .sort((left, right) => left[0].identifierKey.localeCompare(right[0].identifierKey));
}

function buildProfiles(
  components: IdentifierObservation[][],
  mappings: CurrentIdentityMapping[],
  profiles: CurrentIdentityProfile[],
): RecomputedProfile[] {
  const mappingByIdentifier = new Map(mappings.map((row) => [row.identifierKey, row]));
  const profileById = new Map(profiles.map((row) => [row.profileId, row]));

  return components.map((members) => {
    const winner = [...members].sort(compareWinner)[0];
    const profileId = md5Hex(winner.identifierKey);
    const historicalIds = new Set<string>([profileId]);

    for (const member of members) {
      const priorId = mappingByIdentifier.get(member.identifierKey)?.profileId;
      if (!priorId) continue;
      historicalIds.add(priorId);
      for (const historicalId of profileById.get(priorId)?.historicalProfileIds ?? []) {
        historicalIds.add(historicalId);
      }
    }

    return {
      profileId,
      profileKey: winner.identifierValue,
      winnerIdentifierKey: winner.identifierKey,
      memberIdentifierKeys: members.map((member) => member.identifierKey).sort(),
      anonymousIds: valuesOfType(members, "anonymous_id"),
      userIds: valuesOfType(members, "user_id"),
      emails: valuesOfType(members, "email"),
      phones: valuesOfType(members, "phone"),
      memberObservations: members,
      historicalProfileIds: [...historicalIds].sort(),
      firstName: latestProfileName(members, "firstName"),
      lastName: latestProfileName(members, "lastName"),
      firstSeenAt: members.reduce<string | null>((earliest, member) => minText(earliest, member.firstSeenAt), null),
      lastSeenAt: members.reduce<string | null>((latest, member) => maxText(latest, member.lastSeenAt), null),
    };
  }).sort((left, right) => left.profileId.localeCompare(right.profileId));
}

async function buildJournalRows(
  input: IdentityEngineInput,
  inputHash: string,
  changedFacts: PendingIdentityFact[],
  profiles: RecomputedProfile[],
): Promise<IdentityJournalRow[]> {
  const rows: IdentityJournalRow[] = [];
  const profileByMember = new Map<string, RecomputedProfile>();
  for (const profile of profiles) {
    for (const member of profile.memberIdentifierKeys) profileByMember.set(member, profile);
  }

  for (const fact of changedFacts) rows.push(await factRow(input, inputHash, fact));
  rows.push(...await evidenceRows(input, inputHash, changedFacts));

  for (const [identifierKey, profile] of profileByMember) {
    rows.push(await mappingRow(input, inputHash, identifierKey, profile));
  }
  for (const mapping of input.currentMappings) {
    if (profileByMember.has(mapping.identifierKey)) continue;
    rows.push(await mappingTombstoneRow(input, inputHash, mapping));
  }

  for (const profile of profiles) rows.push(await profileRow(input, inputHash, profile));
  const currentProfileIds = new Set(profiles.map((profile) => profile.profileId));
  for (const profile of input.currentProfiles) {
    if (currentProfileIds.has(profile.profileId)) continue;
    rows.push(await profileTombstoneRow(input, inputHash, profile));
  }

  return rows;
}

async function factRow(
  input: IdentityEngineInput,
  inputHash: string,
  fact: PendingIdentityFact,
): Promise<IdentityJournalRow> {
  const row = baseRow(input, inputHash);
  row.state_kind = "fact";
  row.state_key = `fact:${fact.factKey}`;
  row.lookup_key = fact.factKey;
  row.is_deleted = Number(fact.factDeleted);
  row.row_hash = await sha256Hex(
    `fact:${fact.factKind}:${fact.factKey}:${sourcePriority(fact)}:${fact.sourceFactVersion}:${fact.factPayloadHash}:${input.batchVersion}`,
  );
  row.fact_kind = fact.factKind;
  row.fact_key = fact.factKey;
  row.source_priority = sourcePriority(fact);
  row.source_fact_version = fact.sourceFactVersion;
  row.fact_deleted = Number(fact.factDeleted);
  row.fact_observed_at = fact.observedAt;
  row.fact_payload_hash = fact.factPayloadHash;
  row.fact_payload = fact.factPayload;
  row.evidence_keys = [...fact.evidenceKeys];
  row.producer_id = fact.producerId;
  return row;
}

async function evidenceRows(
  input: IdentityEngineInput,
  inputHash: string,
  changedFacts: PendingIdentityFact[],
): Promise<IdentityJournalRow[]> {
  const stableByKey = mapByFact(input.currentFacts);
  const rows: IdentityJournalRow[] = [];

  for (const fact of changedFacts) {
    const stable = stableByKey.get(factMapKey(fact));
    const oldKeys = new Set(
      stable && !stable.isDeleted && !stable.factDeleted
        ? validIdentifiers(stable.evidenceKeys)
        : [],
    );
    const newKeys = new Set(fact.factDeleted ? [] : validIdentifiers(fact.evidenceKeys));

    for (const identifierKey of newKeys) {
      rows.push(await evidenceRow(input, inputHash, fact, identifierKey, false));
    }
    for (const identifierKey of oldKeys) {
      if (newKeys.has(identifierKey)) continue;
      rows.push(await evidenceRow(input, inputHash, fact, identifierKey, true, stable));
    }
  }

  return rows;
}

async function evidenceRow(
  input: IdentityEngineInput,
  inputHash: string,
  fact: PendingIdentityFact,
  identifierKey: string,
  deleted: boolean,
  stable?: CurrentIdentityFact,
): Promise<IdentityJournalRow> {
  const row = baseRow(input, inputHash);
  const identifier = splitIdentifierKey(identifierKey);
  row.state_kind = "evidence";
  row.state_key = `evidence:${utf8Length(identifierKey)}:${identifierKey}:${fact.factKey}`;
  row.lookup_key = identifierKey;
  row.sub_key = fact.factKey;
  row.is_deleted = Number(deleted);
  row.row_hash = await sha256Hex(
    `evidence:${identifierKey}:${fact.factKey}:${Number(deleted)}:${input.batchVersion}`,
  );
  row.identifier_type = identifier.type;
  row.identifier_value = identifier.value;
  row.identifier_key = identifierKey;
  row.first_seen_at = stable ? stable.factObservedAt : fact.observedAt;
  row.last_seen_at = row.first_seen_at;
  row.fact_kind = fact.factKind;
  row.fact_key = fact.factKey;
  row.source_priority = sourcePriority(fact);
  row.fact_deleted = Number(deleted);
  row.fact_observed_at = row.first_seen_at;
  row.evidence_keys = [identifierKey];
  return row;
}

async function mappingRow(
  input: IdentityEngineInput,
  inputHash: string,
  identifierKey: string,
  profile: RecomputedProfile,
): Promise<IdentityJournalRow> {
  const identifier = splitIdentifierKey(identifierKey);
  const observation = profileObservation(profile, identifierKey);
  const row = baseRow(input, inputHash);
  row.state_kind = "mapping";
  row.state_key = identityStateKey("mapping", identifierKey);
  row.lookup_key = identifierKey;
  row.row_hash = await sha256Hex(
    `mapping:${identifierKey}:${profile.profileId}:0:${input.batchVersion}`,
  );
  row.identifier_type = identifier.type;
  row.identifier_value = identifier.value;
  row.identifier_key = identifierKey;
  row.profile_id = profile.profileId;
  row.first_seen_at = observation.firstSeenAt;
  row.last_seen_at = observation.lastSeenAt;
  row.prior_profile_ids = [profile.profileId];
  return row;
}

async function mappingTombstoneRow(
  input: IdentityEngineInput,
  inputHash: string,
  mapping: CurrentIdentityMapping,
): Promise<IdentityJournalRow> {
  const row = baseRow(input, inputHash);
  row.state_kind = "mapping";
  row.state_key = identityStateKey("mapping", mapping.identifierKey);
  row.lookup_key = mapping.identifierKey;
  row.is_deleted = 1;
  row.row_hash = await sha256Hex(
    `mapping:${mapping.identifierKey}:${mapping.profileId}:1:${input.batchVersion}`,
  );
  row.identifier_type = mapping.identifierType;
  row.identifier_value = mapping.identifierValue;
  row.identifier_key = mapping.identifierKey;
  row.profile_id = mapping.profileId;
  row.first_seen_at = mapping.firstSeenAt;
  row.last_seen_at = mapping.lastSeenAt;
  row.prior_profile_ids = [mapping.profileId];
  return row;
}

async function profileRow(
  input: IdentityEngineInput,
  inputHash: string,
  profile: RecomputedProfile,
): Promise<IdentityJournalRow> {
  const row = baseRow(input, inputHash);
  row.state_kind = "profile";
  row.state_key = identityStateKey("profile", profile.profileId);
  row.lookup_key = profile.profileId;
  row.row_hash = await sha256Hex(
    `profile:${profile.profileId}:${profile.memberIdentifierKeys.join("|")}:${input.batchVersion}`,
  );
  applyProfile(row, profile);
  row.prior_profile_ids = [...profile.historicalProfileIds];
  row.dirty_profile_ids = [profile.profileId];
  return row;
}

async function profileTombstoneRow(
  input: IdentityEngineInput,
  inputHash: string,
  profile: CurrentIdentityProfile,
): Promise<IdentityJournalRow> {
  const row = baseRow(input, inputHash);
  row.state_kind = "profile";
  row.state_key = identityStateKey("profile", profile.profileId);
  row.lookup_key = profile.profileId;
  row.is_deleted = 1;
  row.row_hash = await sha256Hex(`delete:profile:${profile.profileId}:${input.batchVersion}`);
  row.profile_id = profile.profileId;
  row.profile_key = profile.profileKey;
  row.winner_identifier_key = profile.winnerIdentifierKey;
  row.member_identifier_keys = [...profile.memberIdentifierKeys];
  row.historical_profile_ids = [...profile.historicalProfileIds];
  row.first_seen_at = profile.firstSeenAt;
  row.last_seen_at = profile.lastSeenAt;
  row.prior_profile_ids = [profile.profileId];
  return row;
}

async function buildManifestRow(
  input: IdentityEngineInput,
  inputHash: string,
  outputRowCount: number,
  outputHash: string,
): Promise<IdentityJournalRow> {
  const row = baseRow(input, inputHash);
  row.state_kind = "batch_manifest";
  row.state_key = `batch_manifest:${input.batchId}`;
  row.lookup_key = input.batchId;
  row.row_hash = await sha256Hex(`${input.batchId}:${inputHash}:${outputHash}`);
  row.output_row_count = outputRowCount;
  row.output_hash = outputHash;
  return row;
}

function baseRow(input: IdentityEngineInput, inputHash: string): IdentityJournalRow {
  return {
    tenant_id: input.tenantId,
    state_kind: "",
    state_key: "",
    lookup_key: "",
    sub_key: "",
    batch_version: input.batchVersion,
    batch_id: input.batchId,
    committed_at: input.committedAt,
    is_deleted: 0,
    row_hash: ZERO_HASH,
    identifier_type: "",
    identifier_value: "",
    identifier_key: "",
    profile_id: "",
    profile_key: "",
    winner_identifier_key: "",
    member_identifier_keys: [],
    anonymous_ids: [],
    user_ids: [],
    emails: [],
    phones: [],
    historical_profile_ids: [],
    first_name: "",
    last_name: "",
    first_seen_at: null,
    last_seen_at: null,
    fact_kind: "",
    fact_key: "",
    source_priority: 0,
    source_fact_version: 0,
    fact_deleted: 0,
    fact_observed_at: null,
    fact_payload_hash: ZERO_HASH,
    fact_payload: "",
    evidence_keys: [],
    producer_id: "identity_compactor",
    checkpoint_sequence: 0,
    checkpoint_ingested_at: input.checkpointIngestedAt,
    checkpoint_event_id: input.checkpointEventId,
    prior_profile_ids: [],
    dirty_profile_ids: [],
    input_event_count: input.pendingFacts.length,
    input_hash: inputHash,
    output_row_count: 0,
    output_hash: ZERO_HASH,
  };
}

function applyProfile(row: IdentityJournalRow, profile: RecomputedProfile): void {
  row.profile_id = profile.profileId;
  row.profile_key = profile.profileKey;
  row.winner_identifier_key = profile.winnerIdentifierKey;
  row.member_identifier_keys = [...profile.memberIdentifierKeys];
  row.anonymous_ids = [...profile.anonymousIds];
  row.user_ids = [...profile.userIds];
  row.emails = [...profile.emails];
  row.phones = [...profile.phones];
  row.historical_profile_ids = [...profile.historicalProfileIds];
  row.first_name = profile.firstName;
  row.last_name = profile.lastName;
  row.first_seen_at = profile.firstSeenAt;
  row.last_seen_at = profile.lastSeenAt;
}

function profileObservation(
  profile: RecomputedProfile,
  identifierKey: string,
): { firstSeenAt: string | null; lastSeenAt: string | null } {
  const observation = profile.memberObservations.find(
    (member) => member.identifierKey === identifierKey,
  );
  if (!observation) throw new Error(`Profile member is missing: ${identifierKey}`);
  return {
    firstSeenAt: observation.firstSeenAt,
    lastSeenAt: observation.lastSeenAt,
  };
}

function compareWinner(left: IdentifierObservation, right: IdentifierObservation): number {
  if (left.winnerPriority !== right.winnerPriority) {
    return left.winnerPriority - right.winnerPriority;
  }
  const leftTime = left.hasNullObservation ? null : left.firstSeenAt;
  const rightTime = right.hasNullObservation ? null : right.firstSeenAt;
  const timeOrder = compareNullableTimes(leftTime, rightTime);
  if (timeOrder !== 0) return timeOrder;
  // GoogleSQL orders strings by Unicode code point, not machine locale.
  return compareIdentifierValues(left.identifierValue, right.identifierValue);
}

function latestProfileName(
  members: IdentifierObservation[],
  field: "firstName" | "lastName",
): string {
  const factAt = `${field}FactAt` as const;
  const factKey = `${field}FactKey` as const;
  return [...members]
    .filter((member) => member[field] !== "")
    .sort((left, right) => {
      if (left[factAt] !== right[factAt]) {
        return compareNullableTimes(right[factAt], left[factAt]);
      }
      return right[factKey].localeCompare(left[factKey]);
    })[0]?.[field] ?? "";
}

function valuesOfType(members: IdentifierObservation[], type: string): string[] {
  return [...new Set(
    members.filter((member) => member.identifierType === type).map((member) => member.identifierValue),
  )].sort();
}

function winnerPriority(type: string): number {
  if (type === "email") return 1;
  if (type === "canonical_email") return 2;
  if (type === "user_id") return 3;
  if (type === "anonymous_id") return 4;
  return 5;
}

function namesFromPayload(payload: string): { firstName: string; lastName: string } {
  if (!payload) return { firstName: "", lastName: "" };
  try {
    const value = JSON.parse(payload) as Record<string, unknown>;
    return {
      firstName: typeof value.first_name === "string" ? value.first_name : "",
      lastName: typeof value.last_name === "string" ? value.last_name : "",
    };
  } catch {
    return { firstName: "", lastName: "" };
  }
}

function distinctJournalRows(rows: IdentityJournalRow[]): IdentityJournalRow[] {
  const distinct = new Map<string, IdentityJournalRow>();
  for (const row of rows) {
    const key = [row.state_kind, row.lookup_key, row.sub_key, row.row_hash].join("\u0000");
    if (!distinct.has(key)) distinct.set(key, row);
  }
  return [...distinct.values()].sort(compareJournalRows);
}

function compareJournalRows(left: IdentityJournalRow, right: IdentityJournalRow): number {
  const leftKey = `${left.state_kind}:${left.lookup_key}:${left.sub_key}:${left.row_hash}`;
  const rightKey = `${right.state_kind}:${right.lookup_key}:${right.sub_key}:${right.row_hash}`;
  return leftKey.localeCompare(rightKey);
}

async function outputRowsHash(rows: IdentityJournalRow[]): Promise<string> {
  const values = rows.map((row) => (
    `${row.state_kind}:${row.lookup_key}:${row.sub_key}:${row.row_hash}`
  )).sort();
  return sha256Hex(values.join("|"));
}

function mapByFact<T extends { factKind: string; factKey: string }>(facts: T[]): Map<string, T> {
  return new Map(facts.map((fact) => [factMapKey(fact), fact]));
}

function factMapKey(fact: { factKind: string; factKey: string }): string {
  return `${fact.factKind}\u0000${fact.factKey}`;
}

function identityStateKey(kind: string, value: string): string {
  return `${kind}:${utf8Length(value)}:${value}`;
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).length;
}

function validIdentifiers(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.indexOf(":") > 0))].sort();
}

function addValidIdentifiers(target: Set<string>, values: string[]): void {
  for (const value of validIdentifiers(values)) target.add(value);
}

function splitIdentifierKey(key: string): { type: string; value: string } {
  const separator = key.indexOf(":");
  if (separator <= 0) throw new Error(`Invalid identity identifier key: ${key}`);
  return { type: key.slice(0, separator), value: key.slice(separator + 1) };
}

function minText(left: string | null, right: string | null): string | null {
  if (left === null) return right;
  if (right === null) return left;
  return left <= right ? left : right;
}

function maxText(left: string | null, right: string | null): string | null {
  if (left === null) return right;
  if (right === null) return left;
  return left >= right ? left : right;
}

/** GoogleSQL ascending timestamp order puts NULL before every known time. */
function compareNullableTimes(left: string | null, right: string | null): number {
  if (left === right) return 0;
  if (left === null) return -1;
  if (right === null) return 1;
  return left < right ? -1 : 1;
}

function compareIdentifierValues(left: string, right: string): number {
  const leftPoints = Array.from(left, (character) => character.codePointAt(0)!);
  const rightPoints = Array.from(right, (character) => character.codePointAt(0)!);
  for (let index = 0; index < Math.min(leftPoints.length, rightPoints.length); index++) {
    if (leftPoints[index] !== rightPoints[index]) return leftPoints[index] - rightPoints[index];
  }
  return leftPoints.length - rightPoints.length;
}

async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

class DisjointSet {
  private readonly parent = new Map<string, string>();

  add(value: string): void {
    if (!this.parent.has(value)) this.parent.set(value, value);
  }

  find(value: string): string {
    const parent = this.parent.get(value);
    if (!parent) throw new Error(`Identity node is missing: ${value}`);
    if (parent === value) return value;
    const root = this.find(parent);
    this.parent.set(value, root);
    return root;
  }

  union(left: string, right: string): void {
    const leftRoot = this.find(left);
    const rightRoot = this.find(right);
    if (leftRoot === rightRoot) return;
    const [winner, loser] = [leftRoot, rightRoot].sort();
    this.parent.set(loser, winner);
  }
}
