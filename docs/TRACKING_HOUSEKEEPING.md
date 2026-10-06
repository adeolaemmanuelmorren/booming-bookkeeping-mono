# Tracking Housekeeping

Last updated: July 27, 2026

## Snapshot

- Browser events are sent through Jitsu and pushed to `dataLayer`.
- Server form conversions come from ActiveCampaign tags.
- Server purchase conversions come from Stripe and Kajabi Stripe through Dataform.
- Browser and server use the same deterministic IDs so ad platforms can deduplicate matching conversions.
- Web GTM container: `GTM-5HGQQHN8`.
- The current web GTM workspace changes have **not** been published by Codex.

## Key Conversions

| Conversion | Browser event | Server source | Browser `event_id` | Server `event_id` | Server GTM Google Ads trigger |
| --- | --- | --- | --- | --- | --- |
| KRC registration | `Form Submitted` → `generate_lead` | ActiveCampaign registration tag | `form_submission_<sha256(email\|krc\|Pacific date)>` | Same formula | `event_name = generate_lead` and `lead_source = krc` |
| Webinar registration | `Form Submitted` → `generate_lead` | ActiveCampaign registration tag | `form_submission_<sha256(email\|webinar\|Pacific date)>` | Same formula | `event_name = generate_lead` and `lead_source = webinar` |
| Keyboard Rich Book | `Order Completed` → `purchase` | Main Stripe | `purchase_<charge_id>` | `purchase_<charge_id>` | `event_name = purchase` and `items.0.item_id` contains `keyboard rich book` |
| Challenge VIP | `Order Completed` → `purchase` | Main Stripe | `purchase_<charge_id>` | `purchase_<charge_id>` | `event_name = purchase` and `items.0.item_id` contains `vip` |
| Mentorship | `Order Completed` → `purchase` | Main Stripe | `purchase_<charge_id>` | `purchase_<charge_id>` | `event_name = purchase` and `items.0.item_id` matches `(deposit\|one-time payment\|payment plan\|3 payments\|installment)` |
| Kajabi mentorship | `Order Completed` → `purchase` | Kajabi Stripe | `purchase_<charge_id>` | `purchase_<charge_id>` | `event_name = purchase` and `items.0.item_id` equals `booming bookkeeping mentorship program` |

For purchases, GA4 `transaction_id` is the raw Stripe Charge ID. Renewals and later payment-plan collections remain payments but are excluded from new-order conversions.

Purchase events expose the same Meta product fields on both paths:

- Browser: `properties.fb_content_ids` and `properties.fb_contents`
- SEGRETL: `fb_content_ids` and `fb_contents`

`fb_content_ids` mirrors `content_ids`. `fb_contents` mirrors `contents` and
contains `id`, `quantity`, and `item_price`.

## Form Mapping

| Registration | ActiveCampaign form | Browser mapping | Server tag mapping |
| --- | ---: | --- | --- |
| KRC | `20` | `registration_type = krc`, `lead_source = krc` | Primary prefixes: `[KRC] Registered for Challenge -` or `[KRC] Registered -`; fallback: exact `[KRC] Registered for Challenge` only when no primary tag exists |
| Webinar | `15` | `registration_type = webinar`, `lead_source = webinar` | Exact `[CW] Registered for Webinar` |

Browser mapping: [`clickfunnels/src/registration-forms.js`](clickfunnels/src/registration-forms.js)  
Server mapping: [`dataform/includes/business_rules.js`](dataform/includes/business_rules.js)

When adding a registration form, update both mappings. The browser identifies a form from `data-registration-type`, the ActiveCampaign `f`/`u` field, or the `_form_<id>_` element ID.

## Purchase Mapping

| `product_case` | SEGRETL content-ID match | Browser/server alignment |
| --- | --- | --- |
| `book` | Contains `keyboard rich book` or `top tax loopholes` | Stripe product line names become normalized `content_ids` and GA4 items |
| `vip` | Contains `vip` or `keyboard rich challenge` | Trigger from normalized VIP/Challenge item names |
| `mentorship` | Main Stripe and contains `booming bookkeeping mentorship program`, or the exact `$4,997` `booming bookkeeping installment` checkout product | Smaller custom installments and later subscription collections remain excluded |
| `kajabi` | Kajabi Stripe and contains `booming bookkeeping mentorship program` | Only the initial subscription payment is a new order |

SEGRETL builds `product_id`, `content_ids`, and product names directly from
Stripe invoice lines, Checkout lines, `metadata.products`, or a marked charge
description—in the same priority used by the browser proxy. The four
`product_case` values exist only to separate the four GTM conversion types.

Product resolution: [`dataform/definitions/intermediate/stripe/int_stripe_browser_product_resolution.sqlx`](dataform/definitions/intermediate/stripe/int_stripe_browser_product_resolution.sqlx)  
GTM cases: [`dataform/definitions/output/segment/segretl_repeatable_conversions.sqlx`](dataform/definitions/output/segment/segretl_repeatable_conversions.sqlx)

## Checkout and Confirmation Routes

Two route arrays prevent polling Stripe from every page:

- `POLL_ON_LOAD_ROUTES`: known post-purchase destinations. Poll when the customer lands there.
- `POLL_AFTER_SUBMIT_ROUTES`: checkouts without a reliable redirect. Poll immediately after checkout submission.

```text
POLL_ON_LOAD_ROUTES
boomingbookkeeping.com/confirmation-1
boomingbookkeeping.com/go-1
keyboardrichchallenge.com/vipconfirmation-1
keyboardrichchallenge.com/vipsteps-1
keyboardrichchallenge.com/vip-thanks-1
keyboardrichchallenge.com/vipsuccess-1
keyboardrich.com/oto-1-page-1
keyboardrich.com/oto-2-page-1
keyboardrich.com/receipt-1
keyboardrich.com/free-2

POLL_AFTER_SUBMIT_ROUTES
keyboardrich.com/yes-1
keyboardrich.com/yes-2
learn.boomingbookkeeping.com/offers/*/checkout
```

Current source of truth: [`clickfunnels/src/purchase-confirmation.js`](clickfunnels/src/purchase-confirmation.js).

The browser registers a short-lived attempt using `anonymous_id`, email, PaymentMethod ID and submission time. The Worker confirms Stripe success and returns the Charge ID. Each confirmed charge fires once.

## GTM and GA4 Contract

| Browser event | `ga4_event_type` | GA4 event | Data object |
| --- | --- | --- | --- |
| `Form Submitted` | `standard_event` | `generate_lead` | `ga4_properties: { lead_source }` |
| `Order Completed` | `ecommerce` | `purchase` | `ecommerce: { transaction_id, value, currency, items }` |

GTM setup:

1. The Google tag uses `{{Server Container URL}}`.
2. GA4 event tags use `{{dlp - ga4_event}}` as the event name.
3. Ecommerce events read the `ecommerce` object from the data layer.
4. Standard/custom events read `ga4_properties`.
5. Both lead and purchase GA4 tags pass an event parameter named `user_data`.
6. `user_data` uses the User-Provided Data variable built from top-level `traits`: email, `phone_number`, and address.
7. Server-side Google Ads conversion tags use `event_name`, `lead_source`, and `items.0.item_id` for their triggers.

A separate Google Ads User-provided Data Event tag is unnecessary when `user_data` is present on the conversion event itself.

Browser GA4 source: [`clickfunnels/src/config.js`](clickfunnels/src/config.js) and [`clickfunnels/src/datalayer.js`](clickfunnels/src/datalayer.js).

## Change Checklist

- New form: update both form mappings and its GTM trigger.
- New product in an existing GTM case: no warehouse mapping change is needed.
- New GTM conversion case: add one `product_case` condition and its GTM trigger.
- New checkout/thank-you page: update the appropriate polling route array.
- Validate in GTM Preview, raw Jitsu events, and the client/server marts before publishing.
