# ClickFunnels tracking snippet

One script tag, pasted into every ClickFunnels Classic and Kajabi page in the funnels. It loads Jitsu through the first-party Worker for the current domain, captures lead and checkout submissions, carries the anonymous ID across our four root domains, and fires `Order Completed` only after the Worker has seen the Stripe charge.

This file is the documentation for how the snippet behaves and why. The code in `src/` is the exact behavior.

## Layout

```text
clickfunnels/
  src/        ES modules. Edit here. index.js is the entry.
  dist/       Build output, gitignored. Never edit.
  scripts/    build-snippets.mjs, the esbuild bundler.
  reference/  custom-code.html, the paste-ready script tag.
```

## Build and publish

From the repo root.

```sh
npm run build:clickfunnels
npm run publish:clickfunnels -- --dry-run
npm run publish:clickfunnels -- --bucket assets
npm run publish:clickfunnels:versioned -- --bucket assets
```

Build bundles `src/index.js` into `dist/combined.min.js` and `dist/combined.min.html`. Publish rebuilds, then uploads the `.js` to R2 key `cf-sh-seg` and the `.html` to `cf-sh-seg-html`. The versioned variant appends a content hash to the key and sets a one-year immutable cache header. The publish script lives in `../cloudflare-workers/reverse-proxy/scripts/`.

Four hostnames serve the same R2 bucket. Use the one matching the funnel's root domain.

```html
<script src="https://assets.thebookkeepingchallenge.com/cf-sh-seg"></script>
<script src="https://assets.keyboardrichchallenge.com/cf-sh-seg"></script>
<script src="https://assets.keyboardrich.com/cf-sh-seg"></script>
<script src="https://assets.boomingbookkeeping.com/cf-sh-seg"></script>
```

## How submissions are captured

We listen to the form's `formdata` event, not `submit` and not click.

ClickFunnels finishes many submissions by calling native `form.submit()`, which skips the `submit` event entirely. Click fires too early, before validation or payment. `formdata` fires when the browser serializes the payload, including for native `form.submit()`, so it is the last moment we can read the finalized fields without touching the submission. Kajabi's checkout fires several ordinary `submit` events during Stripe setup and then calls native `form.submit()` once, so the same listener covers it.

`forms.js` binds the listener to every form on the page and to forms added later. It skips forms posting to `*.facebook.com`. The Meta pixel sometimes uses a form as its transport, and we were tracking those as leads.

On each `formdata`, the form is either a checkout or a lead. A checkout is the Kajabi form by id or class, or any form with `purchase[...]` fields plus either a selected product or a Stripe script on the page. For a checkout we send `Identify` and register a purchase attempt with the Worker. No `Order Completed` yet. For anything else we send `Identify` and `Form Submitted`.

### Dedup on `Form Submitted`

ClickFunnels can serialize the same form twice in quick succession. Two guards handle it. `submission-burst.js` drops an identical fingerprint from the same form element inside one second. `forms.js` also remembers every `event_id` it has sent on the current page and refuses to send the same one again.

### Form to registration type mapping

`registration-forms.js` is the list. Right now it is two entries.

| ActiveCampaign form ID | `registration_type` | `content_name` |
| --- | --- | --- |
| `20` | `krc` | Keyboard Rich Challenge Registration |
| `15` | `webinar` | Booming Bookkeeping Webinar Registration |

The form ID comes from `data-active-campaign-form-id`, the hidden `f` or `u` input, or the `_form_<id>_` element id, in that order. A form that matches nothing gets `registration_type = "general"` and empty `lead_source` and `content_name`.

This matters because the event ID includes the type:

```text
form_submission_<sha256(lowercase(email) | registration_type | YYYY-MM-DD Pacific)>
```

The same formula runs server-side against ActiveCampaign tags in `../dataform/includes/business_rules.js`, so a browser lead and its ActiveCampaign tag deduplicate at the ad platform. Pacific date because it had the smallest midnight-straddle risk when we measured. One person registering twice on the same day is the same event. Registering for KRC and the webinar on the same day is two events, which is why the type is in the hash. If you change the formula, change both sides in the same commit.

## Purchases

`Order Completed` fires only for a confirmed Stripe charge. We do not trust the browser submission as a purchase.

Why. The form payload has no order ID and no PaymentIntent, only a `pm_` PaymentMethod in `purchase[stripe_customer_token]`. We spent July trying to hash email, product, PaymentMethod, and date into a deterministic ID that Stripe could rebuild. It worked for most cases, but product names had to be normalized identically on both sides, multi-product orders and one-click upsells needed their own rules, and there was still a measurable midnight collision window. Polling the Worker for the real charge replaced all of it. The event ID is now just `purchase_<charge_id>`, which the warehouse already has.

### The flow

1. On a checkout `formdata`, `purchase-confirmation.js` POSTs `/v1/purchase-attempts` with `anonymous_id`, lowercased email, the `pm_` ID, and `submitted_at`. If any of those is missing it does nothing. PayPal checkouts never register because there is no `pm_`.
2. The Worker stores the attempt in a Durable Object keyed by `anonymous_id` and watches Stripe for a matching successful charge.
3. The browser polls `/v1/purchase-confirmations` on a `0, 1.5, 3, 5, 8, 13, 21` second schedule, honoring any `retry_after_ms` the Worker returns. It stops when a charge comes back or the Worker says nothing is pending.
4. Each returned `ch_` ID fires one `Order Completed` with `event_id = purchase_<charge_id>`, `is_payment_confirmed = true`, `completion_basis = stripe_charge_confirmed`, and Stripe's product, value, and customer fields. A charge ID the page has already fired is ignored. The Worker also never returns the same charge twice.

### When polling starts

Two route lists in `purchase-confirmation.js` decide this. Nothing else polls.

`POLL_ON_LOAD_ROUTES` are pages a customer lands on after paying. Polling starts on page load. These are the thank-you, confirmation, OTO, and receipt pages. A route gets added here only after the warehouse shows confirmed charges reaching it within a few seconds of the charge. The July 27 audit in `../outputs/PURCHASE_THANK_YOU_PAGE_AUDIT.md` is the record of how each current route qualified. Checkout entry pages like `free-1`, `vipfc-2`, `upgrade-1`, and `yes-*` do not belong here even though they show up in purchase journeys.

`POLL_AFTER_SUBMIT_ROUTES` are checkouts with no reliable next page. `keyboardrich.com/yes-1` and the Kajabi checkout are the main ones. Most buyers there never navigate anywhere within ten minutes. Polling starts right after the attempt registers, on the checkout page itself.

Wildcards match one path segment, so `learn.boomingbookkeeping.com/offers/*/checkout` covers any offer.

### One-click upsells

The OTO form submits only `purchase[product_id]`, `purchase[stripe_customer_id]`, and `upsell=1`. No email, no `pm_`. So on every non-upsell checkout submission, `checkout-context.js` saves email, name, phone, and the `pm_` ID to `sessionStorage` for two hours. When `upsell=1` arrives, `forms.js` fills the attempt from that context so the Worker can match the OTO charge.

## Anonymous ID across domains

Visitors move between `thebookkeepingchallenge.com`, `keyboardrichchallenge.com`, `keyboardrich.com`, `boomingbookkeeping.com`, and `learn.boomingbookkeeping.com`. Cookies do not cross root domains, so without help every hop creates a new anonymous ID and the journey breaks. Two things stop that.

### Link decoration

`links.js` appends `ajs_aid=<id>` and `an_aid=<id>` to any link pointing at another one of our roots, on load and as links are added. `identity-handoff.js` reads those params on the destination before Jitsu initializes and adopts the ID. If both params are present and disagree, it strips both and adopts neither, on the theory that a conflicting handoff is worse than a fresh ID.

### ActiveCampaign hidden field and redirect

This one is the odd one and the reason for most of `active-campaign.js`.

The KRC registration popup on `thebookkeepingchallenge.com` is an ActiveCampaign form. It POSTs to ActiveCampaign's `proc.php`, and ActiveCampaign issues the redirect to `keyboardrichchallenge.com/vipfc-1`. Our JavaScript never gets to touch that redirect URL. Link decoration cannot help because there is no link. It is a server-side 302 from a third party to a different root domain. Without intervention every registrant arrives on the VIP page as a stranger.

The workaround is to make ActiveCampaign carry the ID for us. There is a hidden custom contact field on the form, `field[39]`, labeled Segment Anonymous ID in ActiveCampaign. The snippet fills it with the current anonymous ID once Jitsu is fully ready. ActiveCampaign stores it on the contact, and the form's redirect URL in ActiveCampaign is configured as

```text
https://keyboardrichchallenge.com/vipfc-1?ajs_aid=%SEGMENT_ANON_FIELD_TAG%
```

so the 302 `Location` contains the real ID and `identity-handoff.js` picks it up on arrival. ActiveCampaign also appends its own `vgo_ee` token and later strips it with `history.replaceState`. It leaves `ajs_aid` alone.

Things that have bitten us here:

- The snippet fills the field only after Jitsu initializes. Before the July 26 fix it could fill from a pre-init `__eventn_id` cookie and hand off the wrong ID.
- `hydrateActiveCampaignForms` runs on load, on a delayed schedule, on DOM mutations that add forms, and from a patched `HTMLFormElement.prototype.submit`. The last one exists because ClickFunnels calls native `submit()` and the field has to be current at that instant.
- ClickFunnels ships Garlic, a localStorage autosave for form fields. Garlic stores every ClickFunnels custom-type input under one shared `input.custom_type` key. So a value we wrote into `segment_anonymous_id` could come back on the next page inside the `hpcheck` honeypot, and inside ActiveCampaign's `field[31]` honeypot too. ActiveCampaign then rejected real registrations as bots. The fix purges Garlic's custom-type keys and clears any known tracking ID out of both honeypots. It also stopped dispatching synthetic input events, since those are what made Garlic persist the value in the first place. Real bot values in the honeypots stay put.

The default field name is `field[39]` and the default custom type is `segment_anonymous_id`. A page can add more before the script tag:

```html
<script>
  window.BOOM_CLICKFUNNELS_ACTIVE_CAMPAIGN_ANONYMOUS_ID_FIELDS = ["field[39]", "field[52]"];
  window.BOOM_CLICKFUNNELS_ANONYMOUS_ID_CUSTOM_TYPES = ["segment_anonymous_id"];
</script>
```

The snippet never creates these fields. They have to exist in the ActiveCampaign form HTML already.

## Identify

`identity.js` sends `Identify` when an email field blurs with a valid value and again from the `formdata` payload. The second one covers autofill and people who submit without ever leaving the field. It suppresses repeat identifies of the same value on the same input. `phone.js` normalizes phone numbers to E.164 for Google enhanced conversions.

## Side channels

`datalayer.js` mirrors every track and identify to `window.dataLayer` for GTM. `Form Submitted` carries `ga4_event = generate_lead` with `ga4_properties.lead_source`. `Order Completed` carries `ga4_event = purchase` with a GA4 `ecommerce` object whose `transaction_id` is the raw charge ID.

`attribution.js` and `attr-tracking.js` copy UTMs, click IDs, and attribution cookies onto every event. The full field lists are in `config.js`. When they change, the snippet sends an `attr` track event and pings `/route/ck` so the Worker can set its server-side attribution cookie.

`consent.js` reads Cookiebot, calls the Worker's `/consent/bootstrap` and `/consent/state`, and sets `X-Boom-Consent` on tracking requests.

`kajabi-purchase-diagnostic.js` wraps Kajabi's own data layer on `learn.boomingbookkeeping.com` and forwards its purchase pushes as a `Kajabi Data Layer Purchase` event. Diagnostic only.

## Worker endpoints the snippet calls

All on `sg.<root domain>`.

```text
GET  /p.js
POST /api/s/page, /api/s/track, /api/s/identify
POST /route/ck
POST /consent/bootstrap, /consent/state
POST /v1/purchase-attempts, /v1/purchase-confirmations
```

## Rules

- Worker routing, Stripe lookups, and Durable Object state belong in `../cloudflare-workers/reverse-proxy`.
- Change `event-ids.js` or `registration-forms.js` together with `../dataform/includes/business_rules.js`.
- A new thank-you page goes in `POLL_ON_LOAD_ROUTES` after the warehouse shows charges landing there, not before.
- A new registration form needs a `registration-forms.js` entry, the Dataform tag mapping, and a GTM trigger.
- Do not submit real checkout forms while testing. Lead forms and DOM inspection are fine. `window.BoomClickFunnels.getSelectedCheckoutProducts()` shows what the checkout parser sees.
- Rebuild before publishing.
