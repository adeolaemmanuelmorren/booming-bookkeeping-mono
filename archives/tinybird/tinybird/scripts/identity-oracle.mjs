import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const tinybirdDirectory = join(scriptDirectory, '..');

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function md5Upper(value) {
  return createHash('md5').update(value).digest('hex').toUpperCase();
}

async function readNdjson(relativePath) {
  const contents = await readFile(join(tinybirdDirectory, relativePath), 'utf8');

  return contents
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function readProjectFile(relativePath) {
  return readFile(join(tinybirdDirectory, relativePath), 'utf8');
}

function canonicalEmail(email) {
  const normalized = email.trim().toLowerCase();
  if (!normalized) return '';

  const parts = normalized.split('@');
  if (!['gmail.com', 'googlemail.com'].includes(parts[1])) return normalized;

  const localPart = parts[0].replace(/\+.*/, '').replaceAll('.', '');
  return `${localPart}@gmail.com`;
}

function normalizedPhone(phone) {
  const digits = phone.replace(/[^0-9]/g, '');
  const national = digits.length === 11 && digits.startsWith('1')
    ? digits.slice(1)
    : digits;

  if (national.length !== 10) return '';
  if (!/^[2-9][0-9]{2}[2-9][0-9]{6}$/.test(national)) return '';
  if (national.slice(3, 6) === '555') return '';
  if (/(0000000|1111111|2222222|3333333|4444444|5555555|6666666|7777777|8888888|9999999)$/.test(national)) return '';
  if (/^(0123456789|1234567890|234567890[0-9]|9876543210)$/.test(national)) return '';

  return `+1${national}`;
}

function normalizedIdentifiers(observation) {
  const anonymousId = observation.anonymous_id.trim();
  const userId = observation.user_id.trim().toLowerCase();
  const email = observation.email.trim().toLowerCase();
  const canonical = canonicalEmail(observation.email);
  const phone = normalizedPhone(observation.phone);

  const candidates = [
    ['anonymous_id', anonymousId, 4, true],
    ['user_id', userId, 3, true],
    ['email', email, 1, true],
    ['canonical_email', canonical, 2, false],
    ['phone', phone, 5, true],
  ];

  return candidates
    .filter(([, value]) => value)
    .map(([type, value, priority, isPublic]) => ({
      type,
      value,
      key: `${type}:${value}`,
      priority,
      isPublic,
      observedAt: observation.observed_at,
    }));
}

class DisjointSet {
  constructor() {
    this.parent = new Map();
  }

  add(value) {
    if (!this.parent.has(value)) this.parent.set(value, value);
  }

  find(value) {
    const parent = this.parent.get(value);
    if (parent === value) return value;

    const root = this.find(parent);
    this.parent.set(value, root);
    return root;
  }

  union(left, right) {
    const leftRoot = this.find(left);
    const rightRoot = this.find(right);
    if (leftRoot === rightRoot) return;

    const [winner, loser] = [leftRoot, rightRoot].sort();
    this.parent.set(loser, winner);
  }
}

function buildProfiles(observations) {
  const identifiersByKey = new Map();
  const disjointSet = new DisjointSet();

  for (const observation of observations) {
    const identifiers = normalizedIdentifiers(observation);
    for (const identifier of identifiers) {
      disjointSet.add(identifier.key);

      const current = identifiersByKey.get(identifier.key);
      if (!current || identifier.observedAt < current.observedAt) {
        identifiersByKey.set(identifier.key, identifier);
      }
    }

    const anchor = identifiers[0]?.key;
    for (const identifier of identifiers.slice(1)) {
      disjointSet.union(anchor, identifier.key);
    }
  }

  const membersByRoot = new Map();
  for (const identifier of identifiersByKey.values()) {
    const root = disjointSet.find(identifier.key);
    const members = membersByRoot.get(root) ?? [];
    members.push(identifier);
    membersByRoot.set(root, members);
  }

  const profiles = [];
  const mapping = new Map();

  for (const members of membersByRoot.values()) {
    members.sort((left, right) => {
      if (left.priority !== right.priority) return left.priority - right.priority;
      if (left.observedAt !== right.observedAt) return left.observedAt.localeCompare(right.observedAt);
      return left.value.localeCompare(right.value);
    });

    const winner = members[0];
    const profileId = md5Upper(winner.key);
    const profile = {
      profileId,
      winnerKey: winner.key,
      memberKeys: members.map((member) => member.key).sort(),
    };

    profiles.push(profile);
    for (const member of members) mapping.set(member.key, profileId);
  }

  profiles.sort((left, right) => left.profileId.localeCompare(right.profileId));
  return { profiles, mapping };
}

function validateDeliveries(deliveries) {
  const sequenceVariants = new Map();
  const eventVariants = new Map();

  for (const delivery of deliveries) {
    const sequenceKey = `${delivery.producer_id}:${delivery.producer_sequence}`;
    const sequenceSet = sequenceVariants.get(sequenceKey) ?? new Set();
    sequenceSet.add(`${delivery.event_id}:${delivery.payload_hash}`);
    sequenceVariants.set(sequenceKey, sequenceSet);

    const eventKey = `${delivery.producer_id}:${delivery.event_id}`;
    const eventSet = eventVariants.get(eventKey) ?? new Set();
    eventSet.add(`${delivery.producer_sequence}:${delivery.payload_hash}`);
    eventVariants.set(eventKey, eventSet);
  }

  return deliveries.filter((delivery, index) => {
    const sequenceKey = `${delivery.producer_id}:${delivery.producer_sequence}`;
    const eventKey = `${delivery.producer_id}:${delivery.event_id}`;
    const firstExactReplay = deliveries.findIndex((candidate) => (
      candidate.producer_id === delivery.producer_id
      && candidate.producer_sequence === delivery.producer_sequence
      && candidate.event_id === delivery.event_id
      && candidate.payload_hash === delivery.payload_hash
    )) === index;

    return sequenceVariants.get(sequenceKey).size === 1
      && eventVariants.get(eventKey).size === 1
      && firstExactReplay;
  });
}

function contiguousHigh(checkpoint, sequences) {
  const sorted = [...new Set(sequences)].sort((left, right) => left - right);

  for (let rank = 0; rank < sorted.length; rank += 1) {
    const expected = checkpoint + rank + 1;
    if (sorted[rank] !== expected) return checkpoint + rank;
  }

  return sorted.at(-1) ?? checkpoint;
}

function internalRecordKey(publicId, eventClass, sourceSystem) {
  return publicId ?? `__null_partition__:${eventClass}:${sourceSystem}`;
}

function selectFactWinner(stable, pending) {
  const sameVersion = pending.filter((fact) => fact.version === stable.version);
  const sameVersionFingerprints = new Set(
    sameVersion.map((fact) => `${fact.deleted}:${fact.payloadHash}`),
  );

  if (sameVersionFingerprints.size > 1) return { status: 'rejected_conflict' };
  if (sameVersion.some((fact) => fact.payloadHash !== stable.payloadHash || fact.deleted !== stable.deleted)) {
    return { status: 'rejected_conflict' };
  }

  const candidates = [stable, ...pending];
  candidates.sort((left, right) => right.version - left.version);
  const winner = candidates[0];

  return {
    status: winner === stable ? 'stable_unchanged' : 'pending_wins',
    winner,
  };
}

function parseTimestampMicros(timestamp) {
  const match = timestamp.match(/^(.*T\d{2}:\d{2}:\d{2})(?:\.([0-9]{1,6}))?Z$/);
  assert(match, `Unsupported fixture timestamp: ${timestamp}`);

  const wholeSecondMillis = Date.parse(`${match[1]}Z`);
  const fractionalMicros = BigInt((match[2] ?? '').padEnd(6, '0'));
  return (BigInt(wholeSecondMillis) * 1000n) + fractionalMicros;
}

function sessionize(pages) {
  const pagesByVisitor = new Map();

  for (const page of pages) {
    const visitorPages = pagesByVisitor.get(page.visitor_key) ?? [];
    visitorPages.push(page);
    pagesByVisitor.set(page.visitor_key, visitorPages);
  }

  const result = [];
  const thresholdMicros = 1_860_000_000n;

  for (const [visitorKey, visitorPages] of pagesByVisitor.entries()) {
    visitorPages.sort((left, right) => {
      const timeDifference = parseTimestampMicros(left.timestamp) - parseTimestampMicros(right.timestamp);
      if (timeDifference !== 0n) return timeDifference < 0n ? -1 : 1;
      return left.page_view_id.localeCompare(right.page_view_id);
    });

    let sessionNumber = 0;
    let previousMicros;

    for (const page of visitorPages) {
      const currentMicros = parseTimestampMicros(page.timestamp);
      if (previousMicros === undefined || currentMicros - previousMicros >= thresholdMicros) {
        sessionNumber += 1;
      }

      result.push({
        ...page,
        sessionNumber,
        sessionId: md5Upper(`${visitorKey}|${sessionNumber}`),
      });
      previousMicros = currentMicros;
    }
  }

  return result;
}

function resolveProfile(session, mappings) {
  return mappings.get(`anonymous_id:${session.anonymous_id}`)
    ?? mappings.get(`user_id:${session.user_id.toLowerCase()}`)
    ?? mappings.get(`email:${session.user_id.toLowerCase()}`)
    ?? '';
}

function profileTokenResolutions(currentProfiles) {
  const targetsByToken = new Map();

  for (const profile of currentProfiles) {
    const tokens = new Set([...profile.historicalProfileIds, profile.priorProfileId]);

    for (const token of tokens) {
      const targets = targetsByToken.get(token) ?? new Set();
      for (const target of profile.targetProfileIds) targets.add(target);
      targetsByToken.set(token, targets);
    }
  }

  return new Map([...targetsByToken].map(([token, targets]) => [token, {
    resolvedProfileId: targets.size === 1 ? [...targets][0] : '',
    targetCount: targets.size,
  }]));
}

async function main() {
  const observations = await readNdjson('fixtures/identity/observations.ndjson');
  const deliveries = await readNdjson('fixtures/identity/deliveries.ndjson');
  const baselinePages = await readNdjson('fixtures/sessions/pages_baseline.ndjson');
  const latePages = await readNdjson('fixtures/sessions/pages_late_arrival.ndjson');

  const withBridge = buildProfiles(observations);
  const withoutBridge = buildProfiles(observations.filter((row) => !row.optional_bridge));

  const gmailAliasKey = 'email:boom.identity.test+promo@googlemail.com';
  const gmailCanonicalKey = 'email:boomidentitytest@gmail.com';
  const separateKey = 'email:separate.component@example.test';

  assert(
    withBridge.mapping.get(gmailAliasKey) === withBridge.mapping.get(gmailCanonicalKey),
    'Canonical Gmail aliases must connect.',
  );
  assert(
    withBridge.mapping.get(gmailAliasKey) === withBridge.mapping.get(separateKey),
    'The optional bridge must merge both components.',
  );
  assert(
    withoutBridge.mapping.get(gmailAliasKey) !== withoutBridge.mapping.get(separateKey),
    'Removing the bridge must split the prior component.',
  );

  const winningProfileId = md5Upper(gmailAliasKey);
  assert(
    withoutBridge.mapping.get(gmailAliasKey) === winningProfileId,
    'Profile IDs must be uppercase MD5 of the deterministic winning identifier key.',
  );
  assert(
    withBridge.mapping.has('phone:+12122345678'),
    'A valid NANP phone must enter identity.',
  );
  assert(!withBridge.mapping.has('phone:+12025550123'), 'A 555 exchange must be rejected.');
  assert(!withBridge.mapping.has('phone:+12122222222'), 'A repeated suffix must be rejected.');
  assert(!withBridge.mapping.has('phone:+11234567890'), 'A sequential number must be rejected.');
  assert(
    [...withBridge.mapping.keys()].every((key) => !key.startsWith('fbclid:') && !key.startsWith('gclid:')),
    'Click IDs must never enter identity.',
  );

  const validDeliveries = validateDeliveries(deliveries);
  assert(validDeliveries.filter((row) => row.event_id === 'event-replay').length === 1, 'Exact replay must deduplicate.');
  assert(validDeliveries.every((row) => row.producer_id !== 'fixture-conflict'), 'Conflicting sequence must reject.');
  assert(validDeliveries.every((row) => row.producer_id !== 'fixture-reused-id'), 'Reused event ID must reject.');
  assert(
    contiguousHigh(41, deliveries.filter((row) => row.producer_id === 'fixture-producer').map((row) => row.producer_sequence)) === 42,
    'Checkpoint 41 with sequences 42 and 44 must stop at 42.',
  );
  assert(
    internalRecordKey(null, 'page_view', 'boom_domains') === '__null_partition__:page_view:boom_domains',
    'A retained NULL-ID partition must receive a deterministic internal key.',
  );
  assert(
    internalRecordKey('public-id', 'page_view', 'boom_domains') === 'public-id',
    'A public source ID must remain unchanged.',
  );

  const stableFact = { version: 8, deleted: 0, payloadHash: 'stable-v8' };
  assert(
    selectFactWinner(stableFact, [{ version: 6, deleted: 0, payloadHash: 'late-v6' }]).status === 'stable_unchanged',
    'Late fact v6 must not replace stable v8.',
  );
  assert(
    selectFactWinner(stableFact, [{ version: 9, deleted: 1, payloadHash: 'deleted-v9' }]).status === 'pending_wins',
    'A newer tombstone must replace stable fact state.',
  );
  assert(
    selectFactWinner(stableFact, [{ version: 8, deleted: 0, payloadHash: 'conflicting-v8' }]).status === 'rejected_conflict',
    'Same-version fact hash conflicts must reject.',
  );

  const baselineSessions = sessionize(baselinePages);
  const lateSessions = sessionize([...baselinePages, ...latePages]);
  const baselineByPage = new Map(baselineSessions.map((page) => [page.page_view_id, page]));
  const lateByPage = new Map(lateSessions.map((page) => [page.page_view_id, page]));

  assert(
    baselineByPage.get('page-a1').sessionId === baselineByPage.get('page-a2').sessionId,
    'A 30:59 gap must stay in the same session.',
  );
  assert(
    baselineByPage.get('page-a2').sessionId !== baselineByPage.get('page-a3').sessionId,
    'A 31:00 gap must start a new session.',
  );
  assert(
    baselineByPage.get('page-b1').sessionId === baselineByPage.get('page-b2').sessionId,
    'A non-minute-aligned 30:59.999 gap must stay in the same session.',
  );
  assert(
    baselineByPage.get('page-b2').sessionId !== baselineByPage.get('page-b3').sessionId,
    'A non-minute-aligned exact 31:00 gap must start a new session.',
  );
  assert(
    baselineByPage.get('page-a3').sessionId !== lateByPage.get('page-a3').sessionId,
    'A late page must trigger full-history resessionization and change the later session ID.',
  );

  const precedenceMappings = new Map([
    ['anonymous_id:anon-priority', 'PROFILE-ANONYMOUS'],
    ['user_id:user-priority', 'PROFILE-USER'],
    ['email:user-priority', 'PROFILE-EMAIL'],
  ]);
  assert(
    resolveProfile({ anonymous_id: 'anon-priority', user_id: 'user-priority' }, precedenceMappings) === 'PROFILE-ANONYMOUS',
    'Session identity must resolve anonymous ID before user ID and email.',
  );

  const generationNAliases = profileTokenResolutions([{
    priorProfileId: 'PROFILE-B',
    historicalProfileIds: ['PROFILE-A', 'PROFILE-B'],
    targetProfileIds: ['PROFILE-B'],
  }]);
  const generationNPlusOneAliases = profileTokenResolutions([{
    priorProfileId: 'PROFILE-B',
    historicalProfileIds: ['PROFILE-A', 'PROFILE-B'],
    targetProfileIds: ['PROFILE-C'],
  }]);

  assert(
    generationNPlusOneAliases.get('PROFILE-A').resolvedProfileId === 'PROFILE-C'
      && generationNPlusOneAliases.get('PROFILE-B').resolvedProfileId === 'PROFILE-C',
    'A to B to C aliases must flatten directly to C.',
  );
  assert(
    generationNPlusOneAliases.get('PROFILE-A').resolvedProfileId === 'PROFILE-C',
    'A manual link to historical profile A must resolve to current profile C.',
  );
  assert(
    generationNAliases.get('PROFILE-A').resolvedProfileId === 'PROFILE-B',
    'A generation-N alias must remain pinned after generation N+1 changes.',
  );

  const shadowMergeAliases = profileTokenResolutions([{
    priorProfileId: 'PROFILE-B',
    historicalProfileIds: ['PROFILE-A', 'PROFILE-B'],
    targetProfileIds: ['PROFILE-C'],
  }]);
  assert(
    shadowMergeAliases.get('PROFILE-A').resolvedProfileId === 'PROFILE-C',
    'A shadow merge must publish the same flattened alias as normal compaction.',
  );

  const shadowSplitAliases = profileTokenResolutions([{
    priorProfileId: 'PROFILE-A',
    historicalProfileIds: ['PROFILE-A'],
    targetProfileIds: ['PROFILE-A', 'PROFILE-D'],
  }]);
  assert(
    shadowSplitAliases.get('PROFILE-A').targetCount === 2
      && shadowSplitAliases.get('PROFILE-A').resolvedProfileId === '',
    'A split with an ambiguous manual profile token must block publication.',
  );

  const shadowStateSchema = await readProjectFile(
    'datasources/state/identity_shadow_state_versions.datasource',
  );
  const shadowRequestSchema = await readProjectFile(
    'datasources/state/identity_shadow_requests.datasource',
  );
  const shadowWorkSchema = await readProjectFile(
    'datasources/state/identity_shadow_work_versions.datasource',
  );
  assert(
    shadowRequestSchema.includes('`source_generation_id` String')
      && shadowRequestSchema.includes('`source_manifest_version` UInt64')
      && shadowRequestSchema.includes('`source_manifest_digest` FixedString(64)')
      && shadowRequestSchema.includes('`connection_inventory_digest` FixedString(64)')
      && shadowRequestSchema.includes('`cutoff_set_digest` FixedString(64)')
      && shadowRequestSchema.includes('`request_hash` FixedString(64)')
      && shadowStateSchema.includes('`source_generation_id` String')
      && shadowStateSchema.includes('`source_manifest_version` UInt64')
      && shadowStateSchema.includes('`source_manifest_digest` FixedString(64)')
      && shadowStateSchema.includes('`connection_inventory_digest` FixedString(64)')
      && shadowStateSchema.includes('`cutoff_set_digest` FixedString(64)')
      && shadowStateSchema.includes('`request_hash` FixedString(64)')
      && shadowStateSchema.includes(
        'ENGINE_SORTING_KEY "tenant_id, shadow_generation, source_generation_id, request_hash, state_kind, state_key"',
      )
      && shadowWorkSchema.includes(
        'ENGINE_SORTING_KEY "tenant_id, shadow_generation, source_generation_id, request_hash, record_kind, record_key, iteration"',
      ),
    'Persisted shadow rows must carry their exact source generation and request hash.',
  );

  const shadowBuild = await readProjectFile('pipes/identity/shadow_identity_union.pipe');
  assert(
    shadowBuild.includes('FROM current_source_connection_cutoffs')
      && shadowBuild.includes('source_generation_cutoff_readiness')
      && shadowBuild.includes('status.source_generation_id = tupleElement(request.current_request, 1)')
      && shadowBuild.includes('status.request_hash = tupleElement(request.current_request, 6)')
      && shadowBuild.includes('tupleElement(status.current_status, 10) = cutoff_readiness.cutoff_set_digest')
      && shadowBuild.includes('integrity.source_generation_id')
      && shadowBuild.includes('integrity.source_manifest_version')
      && shadowBuild.includes('integrity.cutoff_set_digest')
      && shadowBuild.includes('integrity.request_hash'),
    'Shadow construction must use complete generic cutoffs and persist exact request provenance.',
  );

  const shadowPublish = await readProjectFile('copies/identity_shadow_publish.pipe');
  assert(
    shadowPublish.includes('NODE current_shadow_request_versions')
      && shadowPublish.includes('completed.source_generation_id = source_generation.source_generation_id')
      && shadowPublish.includes('completed.source_generation_id = tupleElement(request.current_request, 1)')
      && shadowPublish.includes('completed.source_manifest_version = tupleElement(request.current_request, 2)')
      && shadowPublish.includes('completed.cutoff_set_digest = tupleElement(request.current_request, 5)')
      && shadowPublish.includes('completed.request_hash = tupleElement(request.current_request, 6)')
      && shadowPublish.includes('source.source_generation_id = selected.source_generation_id')
      && shadowPublish.includes('source.request_hash = selected.request_hash'),
    'Shadow publication must reject stale source generations and superseded shadow requests.',
  );
  assert(
    shadowPublish.match(/'source_generation_id', source_generation_id/g)?.length === 2
      && shadowPublish.match(/'request_hash', request_hash/g)?.length === 2,
    'Both published shadow audit rows must record source and request provenance.',
  );

  const correctionActivation = await readProjectFile('copies/identity_activate_candidate.pipe');
  assert(
    correctionActivation.includes("shadow_publish.state_kind = 'shadow_publish_audit'")
      && correctionActivation.includes("JSONExtractString(\n          shadow_publish.fact_payload,\n          'source_generation_id'")
      && correctionActivation.includes("JSONExtractString(\n          shadow_publish.fact_payload,\n          'cutoff_set_digest'")
      && correctionActivation.includes("JSONExtractString(\n          identity_audit.fact_payload,\n          'source_generation_id'"),
    'Correction activation must consume a shadow batch published for the approved source generation.',
  );

  console.log(JSON.stringify({
    status: 'ok',
    checks: {
      identityProfilesWithBridge: withBridge.profiles.length,
      identityProfilesWithoutBridge: withoutBridge.profiles.length,
      validDeliveries: validDeliveries.length,
      baselineSessions: new Set(baselineSessions.map((page) => page.sessionId)).size,
      sessionsAfterLatePage: new Set(lateSessions.map((page) => page.sessionId)).size,
    },
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack ?? error.message);
  process.exitCode = 1;
});
