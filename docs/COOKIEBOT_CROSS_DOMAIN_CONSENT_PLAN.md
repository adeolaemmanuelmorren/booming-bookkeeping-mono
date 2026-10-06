# Cookiebot Cross-Domain Consent Implementation Plan

## Status

- Cookiebot behavior spike: complete.
- Cookiebot GTM-template spike: complete.
- Phases 1–3: implemented and tested locally on July 28, 2026.
- Phase 4 real-domain validation: not started.
- Deployment: not started.

## Goal

When a visitor makes an explicit Cookiebot choice on one Boom domain, carry
that choice to the other Boom domains:

```text
thebookkeepingchallenge.com
keyboardrichchallenge.com
keyboardrich.com
boomingbookkeeping.com
```

The transferred choice must initialize Cookiebot on the destination without
showing another notice.

An unanswered notice is not consent. If the visitor has made no choice, there
is nothing to transfer, so the notice appears on each root they visit.

## Scope

This implementation includes:

- Shared consent storage in the existing Cloudflare Worker.
- Two consent API endpoints.
- A small browser bootstrap.
- The existing Cookiebot GTM template, fired by
  `cookiebot_bootstrap_ready`.
- Consent status attached to every Jitsu event.
- Cross-domain and failure-path tests.

Cookiebot account content, legal wording, cookie scans, the Cookie Declaration,
and the detailed consent configuration of unrelated GTM tags are managed
outside this repository and are not part of this implementation plan.

## Fixed Decisions

| Area | Decision |
| --- | --- |
| Production Cookiebot group | `e20b853d-f8c4-4311-a9be-b15edf42a59f` |
| GTM container | `GTM-5HGQQHN8` |
| Cookiebot loading | Use the existing Cookiebot GTM template |
| Cookiebot trigger | `cookiebot_bootstrap_ready` |
| GTM timing | GTM loads immediately |
| Jitsu timing | Jitsu never waits for Cookiebot or the consent lookup |
| Jitsu delivery | No event is delayed or dropped because consent is denied or unknown |
| Event data | Attach the consent status known when each Jitsu event is received |
| Cross-domain identity | Reuse the existing `ajs_aid` / `an_aid` handoff |
| Consent in URLs | Never place consent values in a URL |
| Central storage | 32 SQLite Durable Object shards |
| Central key | HMAC of the normalized anonymous ID; do not store the raw ID |
| Fast path | Signed `bb_consent_state` cookie |
| API | Only `/consent/bootstrap` and `/consent/state` |
| Native Cookiebot cross-domain sharing | Disabled |
| Affirmative-choice lifetime | 12 months |
| Opt-out lifetime | Honor until the visitor explicitly changes it, while they remain recognizable |
| Already-open pages | Learn about changes on their next load or reload; no polling |
| Policy version | Start with `v1` |

Use only the four apex domains in Cookiebot. The `www` form of each site should
redirect to its matching apex domain; it does not need a separate Cookiebot
registration.

## What the Spikes Proved

The spikes confirmed all of the Cookiebot behavior that this design depends on:

- `submitCustomConsent(preferences, statistics, marketing)` accepts both
  opt-ins and opt-outs before the notice renders.
- The imported choice is saved in Cookiebot's first-party state.
- The notice does not appear after an imported explicit choice.
- The saved choice survives reload.
- An unanswered notice stores no choice and reappears after reload.
- The US notice includes the “Do Not Sell or Share” control.
- The GTM container can load immediately while the Cookiebot template remains
  idle.
- Pushing `cookiebot_bootstrap_ready` causes the template to load Cookiebot
  with the correct production group.
- Automatic opt-in and automatic opt-out both work through that GTM-template
  flow and survive reload without showing the notice.

The spikes did **not** test the complete cross-domain path. That test requires
the shared Worker service described below.

## Runtime Flow

### 1. Start normal page services

GTM and Jitsu initialize immediately.

Jitsu events continue normally. They do not wait for Cookiebot. If consent has
not been resolved when an event arrives, that event receives
`responseStatus = unknown`.

### 2. Resolve shared consent

The browser bootstrap:

1. Reads matching `ajs_aid` / `an_aid` values from the URL when present.
2. Calls the current root's first-party `sg.*` host.
3. Sends the handoff identity to `POST /consent/bootstrap`.
4. Uses a short timeout. A timeout must not block the page, GTM, or Jitsu.

The existing hosts are:

```text
sg.thebookkeepingchallenge.com
sg.keyboardrichchallenge.com
sg.keyboardrich.com
sg.boomingbookkeeping.com
```

### 3. Prepare Cookiebot

Before loading Cookiebot, the bootstrap registers the Cookiebot event handlers.

If `/consent/bootstrap` returns an explicit choice, the bootstrap keeps the
three category values ready for `CookiebotOnDialogInit`.

If it returns no record, times out, or fails, there is no imported choice.

### 4. Load Cookiebot through GTM

The bootstrap always pushes:

```js
window.dataLayer.push({
  event: "cookiebot_bootstrap_ready",
});
```

The existing Cookiebot GTM tag then loads Cookiebot exactly once.

- Explicit shared choice: call `submitCustomConsent()` during
  `CookiebotOnDialogInit`; Cookiebot saves it and shows no notice.
- No shared choice: do not call `submitCustomConsent()`; Cookiebot behaves
  normally and shows the notice when applicable.
- Active GPC: Cookiebot's applicable GPC result must not be overridden by a
  transferred opt-in.

### 5. Save later changes

When Cookiebot records a new opt-in, opt-out, granular change, withdrawal, or
applicable GPC result:

1. Read the final Cookiebot category values.
2. Send the explicit result to `POST /consent/state`.
3. Increment the central revision.
4. Refresh the signed `bb_consent_state` cookie.

An imported choice must not create another revision merely because it was
applied on a destination.

Withdrawal is stored as an explicit all-false opt-out:

```text
preferences = false
statistics  = false
marketing   = false
```

It does not erase the answer and does not force the notice to reappear.

## Worker Storage

### Central record

Create a central record only for an explicit choice:

```text
subjectKey
revision
preferences
statistics
marketing
responseType
gpcApplied
policyVersion
sourceDomain
updatedAt
expiresAt
```

`responseType` is one of:

```text
opted_in
opted_out
custom
gpc
```

Do not create a central record for an unanswered notice.

Derive `subjectKey` with HMAC-SHA-256 from the normalized anonymous ID. Store
only the HMAC value, never the raw `ajs_aid` or `an_aid`.

Select one of 32 `ConsentShard` Durable Objects from the HMAC prefix. Store all
states, including opt-outs, in the selected subject row.

### Conflicting identities

If a URL handoff ID and the existing local identity refer to different
subjects, read both records and keep the most restrictive result.

A denial in either record remains denied. This prevents a later cross-domain
arrival from accidentally undoing an earlier opt-out.

### Signed first-party cookie

Set one compact, signed, HttpOnly cookie on each root:

```text
bb_consent_state
```

It contains:

```text
subjectKey
preferences
statistics
marketing
responseStatus
revision
policyVersion
expiresAt
```

Use `Secure`, `HttpOnly`, `SameSite=Lax`, and the matching root-domain scope.
The HMAC subject key binds the cookie to the anonymous identity without placing
the raw anonymous ID in the cookie.

The Worker verifies the signature before using the cookie:

- Valid cookie: use it without a database lookup.
- Missing, expired, or invalid cookie: look up the central record once and
  refresh the cookie.
- No usable cookie or central record: use `responseStatus = unknown`.

## API

Add only these routes to the existing reverse-proxy Worker:

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/consent/bootstrap` | Resolve the existing identity, return the newest explicit choice or no record, and refresh the signed cookie |
| `POST` | `/consent/state` | Store an explicit choice or change, increment its revision, and refresh the signed cookie |

Both routes must:

- Accept only a matching Boom root or its first-party subdomains through that
  root's `sg.*` host.
- Validate the anonymous ID using the existing Jitsu identity rules.
- Return `Cache-Control: no-store`.
- Never log raw anonymous IDs, HMAC keys, signed cookie values, or consent
  records.

Do not add separate read, withdrawal, GPC, merge, or audit endpoints.

## Jitsu Events

Every Jitsu event continues to the first-party warehouse.

Attach:

```text
context.consent.preferences
context.consent.statistics
context.consent.marketing
context.consent.responseStatus
context.consent.revision
context.consent.policyVersion
```

Rules:

- Do not wait for the consent bootstrap.
- Do not drop pageviews or other events in the Worker.
- Before consent is known, attach `unknown` with null category values.
- When the signed cookie or central record supplies an explicit choice,
  overwrite untrusted incoming consent fields with the authoritative values.
- Once Cookiebot has resolved the normal unanswered-notice state, later
  browser events may carry that current category snapshot with
  `responseStatus = unanswered`.
- Do not attach the HMAC key, raw anonymous ID, signed cookie, or complete
  central record to an event.

This plan adds consent context to events. It does not make the Worker discard
first-party warehouse events. Downstream systems can use the attached consent
context when deciding whether data may leave the first-party warehouse.

## Failure Behavior

| Failure | Behavior |
| --- | --- |
| No handoff identity or no shared record | Load Cookiebot normally |
| Invalid handoff identity | Ignore it and load Cookiebot normally |
| Bootstrap timeout or API failure | Push `cookiebot_bootstrap_ready`, load Cookiebot normally, and keep Jitsu running |
| Cookiebot unavailable | Keep Jitsu running with `unknown` until a status becomes available |
| Missing or invalid signed cookie on an event | Perform the central lookup; use `unknown` if it cannot resolve |
| Newer central revision | Apply it on the next page load or reload |
| Expired affirmative choice | Treat it as no record and show the normal notice |
| Old policy version | Do not reuse an old affirmative grant; continue honoring its denials |
| Existing opt-out | Never expire it into an opt-in |

No failure may fabricate an opt-in or erase an opt-out.

## Implementation Order

### Phase 0 — Complete

- Direct Cookiebot behavior spike.
- Cookiebot GTM-template timing and saved-state spike.

### Phase 1 — Worker storage and API — Complete locally

- Add the 32-shard `ConsentShard` storage.
- Add HMAC subject-key derivation.
- Add `/consent/bootstrap`.
- Add `/consent/state`.
- Add signed `bb_consent_state` cookie handling.
- Add the most-restrictive identity merge.

### Phase 2 — Jitsu consent context — Complete locally

- Attach the best consent state known when every event arrives.
- Use the signed cookie fast path.
- Fall back to one central lookup when the cookie is missing, expired, or
  invalid.
- Preserve every event for the first-party warehouse.

### Phase 3 — Browser bootstrap — Complete locally

- Resolve the existing cross-domain identity.
- Call `/consent/bootstrap` with a short timeout.
- Register Cookiebot event handlers.
- Push `cookiebot_bootstrap_ready`.
- Import only explicit choices during `CookiebotOnDialogInit`.
- Send later explicit changes to `/consent/state`.

### Phase 4 — End-to-end validation — Not started

Test the real Worker path between two different apex domains, then repeat the
critical checks across all four:

- Unanswered notice appears on both roots.
- Explicit opt-in transfers with no destination notice.
- Explicit opt-out transfers with no destination notice.
- Granular category choices transfer exactly.
- GPC is not overridden by a transferred opt-in.
- Destination reload uses its saved Cookiebot state without a notice.
- A changed choice is applied on another root's next load or reload.
- Jitsu continues under every state and every event has consent context.
- A bootstrap failure leaves Cookiebot functional and Jitsu running.
- Each `www` address redirects to its corresponding apex address.

### Phase 5 — Rollout — Not started

- Keep the existing CMP in place until the Worker and browser tests pass.
- Publish the Cookiebot rollout in a controlled release.
- Remove the previous CMP only after Cookiebot is confirmed on all four roots.

## Definition of Done

- An explicit choice made on one root is recovered on another root through the
  existing anonymous-ID handoff.
- Cookiebot applies the transferred categories before displaying its notice.
- The destination saves its own Cookiebot state.
- An unanswered notice is never treated as acceptance.
- An opt-out is never replaced by an opt-in because of expiry, failure, or an
  identity conflict.
- Every Jitsu event reaches the first-party warehouse with the consent status
  known at the time it was received.
- The Worker does not perform a database lookup when the signed consent cookie
  is valid.
- The complete cross-domain test passes on all four production roots.
