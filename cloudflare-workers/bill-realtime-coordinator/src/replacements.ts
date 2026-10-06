import { conversionIdentityFacts } from "../../../tinybird-v1/worker/conversions/identity.ts";
import type { SourceReplacement as ConversionReplacement } from "../../../tinybird-v1/worker/conversions/publication.ts";
import type { NormalizedBrowserEvent } from "../../../tinybird-v1/worker/browser/normalize.ts";
import type { PendingIdentityFact } from "../../../tinybird-v1/worker/identity/engine.ts";
import { canonicalJson, sha256 } from "../../../tinybird-v1/worker/storage/json.ts";

export type BrowserReplacement = Omit<ConversionReplacement, "source" | "source_account"> & {
  source: "browser"; source_account: "default"; browser: NormalizedBrowserEvent;
};
export type HistoryReplacement = Omit<ConversionReplacement, "source" | "source_account"> & {
  source: "history"; source_account: "default"; facts: PendingIdentityFact[]; baseline_id: string;
};
export type SourceReplacement = ConversionReplacement | BrowserReplacement | HistoryReplacement;

export function fingerprint(value: SourceReplacement): string {
  if (value.source !== "browser") return canonicalJson(value);
  const source = value.browser.source;
  return canonicalJson([scopeKey(value), source.source_priority, source.source_revision,
    source.original_payload_hash, source.source_deleted]);
}

export function compareVersion(a: SourceReplacement, b: SourceReplacement): number {
  if (a.source === "browser" && b.source === "browser") {
    const priority = a.browser.source.source_priority - b.browser.source.source_priority;
    if (priority) return priority;
  }
  return a.observation_sequence - b.observation_sequence;
}

async function identityFacts(value: SourceReplacement) {
  if ("storedIdentityFacts" in value) return value.storedIdentityFacts as PendingIdentityFact[];
  if (value.source === "history") return value.facts;
  if (value.source === "browser") return value.browser.identity ? [value.browser.identity] : [];
  return conversionIdentityFacts(value);
}

export function scopeKey(value: SourceReplacement): string {
  return JSON.stringify([value.source, value.source_account, value.scope_id]);
}

export function validateReplacement(value: SourceReplacement): void {
  const validSource = value.source === "stripe" && ["main", "kajabi"].includes(value.source_account)
    || value.source === "activecampaign" && value.source_account === "default"
    || value.source === "browser" && value.source_account === "default" && value.browser?.source?.tenant_id === "boom"
    || value.source === "history" && value.source_account === "default" && /^b_[a-f0-9]{24}$/.test(value.baseline_id)
      && Array.isArray(value.facts) && value.facts.every(fact =>
        fact.sourcePriority === 0 && Number.isSafeInteger(fact.sourceFactVersion) && fact.sourceFactVersion >= 0 ||
        fact.sourcePriority === 1 && fact.sourceFactVersion > 0 && fact.factDeleted);
  if (!validSource || !value.scope_id || !value.replacement_id || !Array.isArray(value.rows)) {
    throw new Error("Invalid source replacement.");
  }
  if (!Number.isSafeInteger(value.observation_sequence) || value.observation_sequence < 0 ||
      !Number.isFinite(Date.parse(value.observed_at))) throw new Error("Invalid source version.");
  if (canonicalJson(value).length > 500_000) throw new Error("Source replacement exceeds the size limit.");
}

export async function replacementFacts(next: SourceReplacement, previous: SourceReplacement | null) {
  if (previous && scopeKey(next) !== scopeKey(previous)) throw new Error("Source scopes differ.");
  if (previous && compareVersion(next, previous) < 0) return [];
  if (previous && compareVersion(next, previous) === 0) {
    if (await sha256(fingerprint(next)) !== ("storedFingerprint" in previous ? previous.storedFingerprint : await sha256(fingerprint(previous)))) throw new Error("Conflicting source version.");
    return [];
  }
  const facts = await identityFacts(next);
  if (!previous) return facts;
  const currentKeys = new Set(facts.map((fact) => fact.factKey));
  const oldFacts = await identityFacts(previous);
  // A complete contact replacement may have zero registrations. Explicitly
  // retract evidence from registrations that disappeared from its scope.
  for (const old of oldFacts) {
    if (currentKeys.has(old.factKey)) continue;
    facts.push({ ...old, eventId: `${next.replacement_id}:removed:${old.factKey}`,
      ingestedAt: next.observed_at, sourceFactVersion: next.observation_sequence,
      factDeleted: true, evidenceKeys: [] });
  }
  return facts;
}

/** Source heads only need revision checks and prior identity facts, not full report payloads. */
export async function compactSourceHead(value: SourceReplacement): Promise<SourceReplacement> {
  if ("storedIdentityFacts" in value) return value;
  if (value.source === "history") throw new Error("Historical batches are not source heads");
  return {
    source: value.source, source_account: value.source_account, scope_id: value.scope_id,
    replacement_id: value.replacement_id, observation_sequence: value.observation_sequence,
    observed_at: value.observed_at,
    rows: value.source === "activecampaign" ? value.rows.map(row => ({ form_submission_id: row.form_submission_id })) : [],
    ...(value.source === "browser" ? { browser: { source: { source_priority: value.browser.source.source_priority } } } : {}),
    storedFingerprint: await sha256(fingerprint(value)),
    storedIdentityFacts: await identityFacts(value),
  } as unknown as SourceReplacement;
}
