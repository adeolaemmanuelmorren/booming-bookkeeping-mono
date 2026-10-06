import { canonicalJson } from "../../worker/storage/json.ts";
import { attributeProfile, metricCount } from "./engine.mjs";

// Models current normalized source heads and an externally published identity map.
// It does not decide which identities belong together.
export class CurrentInputs {
  heads = new Map();
  identities = new Map();
  byIdentity = new Map();
  byProfile = new Map();
  identityVersion = 0;
  identityReceipt = "";
  profile(kind, fact) {
    return (
      this.identities.get(fact.identityKey) ?? `unmapped:${kind}:${fact.id}`
    );
  }
  add(index, key, value) {
    if (!index.has(key)) index.set(key, new Set());
    index.get(key).add(value);
  }
  remove(index, key, value) {
    index.get(key)?.delete(value);
    if (index.get(key)?.size === 0) index.delete(key);
  }
  applyFacts(changes) {
    const checked = new Map(this.heads);
    for (const change of changes) {
      if (
        !["touch", "conversion"].includes(change.kind) ||
        !change.fact.id ||
        !Number.isSafeInteger(change.version) ||
        change.version < 1
      )
        throw new Error("Invalid fact");
      if (
        change.kind === "conversion" &&
        (change.fact.metrics.length !== metricCount ||
          change.fact.metrics.some((value) => !Number.isFinite(value)))
      )
        throw new Error("Invalid metrics");
      const key = `${change.kind}:${change.fact.id}`;
      const prior = checked.get(key);
      if (
        prior &&
        prior.version === change.version &&
        canonicalJson(prior) !== canonicalJson(change)
      )
        throw new Error("Conflicting source version");
      if (!prior || change.version > prior.version) checked.set(key, change);
    }
    const affected = new Set();
    for (const [key, change] of checked) {
      const prior = this.heads.get(key);
      if (prior === change) continue;
      if (prior && !prior.deleted) {
        const profile = this.profile(prior.kind, prior.fact);
        affected.add(profile);
        this.remove(this.byProfile, profile, key);
        this.remove(this.byIdentity, prior.fact.identityKey, key);
      }
      this.heads.set(key, change);
      if (change.deleted) continue;
      const profile = this.profile(change.kind, change.fact);
      affected.add(profile);
      this.add(this.byProfile, profile, key);
      this.add(this.byIdentity, change.fact.identityKey, key);
    }
    return affected;
  }
  applyIdentity(changes, version = this.identityVersion + 1) {
    if (!Number.isSafeInteger(version) || version < 1)
      throw new Error("Invalid identity version");
    if (version < this.identityVersion) return new Set();
    const receipt = canonicalJson(
      [...changes].sort(([a], [b]) => a.localeCompare(b)),
    );
    if (version === this.identityVersion) {
      if (receipt !== this.identityReceipt)
        throw new Error("Conflicting identity version");
      return new Set();
    }
    const affected = new Set();
    for (const [identity, profile] of changes) {
      const keys = [...(this.byIdentity.get(identity) ?? [])];
      for (const key of keys) {
        const head = this.heads.get(key),
          old = this.profile(head.kind, head.fact);
        affected.add(old);
        this.remove(this.byProfile, old, key);
      }
      if (profile === null) this.identities.delete(identity);
      else this.identities.set(identity, profile);
      for (const key of keys) {
        const head = this.heads.get(key),
          next = this.profile(head.kind, head.fact);
        affected.add(next);
        this.add(this.byProfile, next, key);
      }
    }
    this.identityVersion = version;
    this.identityReceipt = receipt;
    return affected;
  }
  inputs(profile) {
    const touches = [],
      conversions = [];
    for (const key of this.byProfile.get(profile) ?? []) {
      const head = this.heads.get(key);
      (head.kind === "touch" ? touches : conversions).push(head.fact);
    }
    return { touches, conversions };
  }
  rebuild(profiles) {
    return new Map(
      [...profiles].map((profile) => {
        const input = this.inputs(profile);
        return [profile, attributeProfile(input.touches, input.conversions)];
      }),
    );
  }
}
