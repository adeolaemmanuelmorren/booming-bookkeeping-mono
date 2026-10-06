# ClickFunnels and Kajabi Client-Side Conversion Capture

## The Short Version

- ClickFunnels leads and orders are captured from the form's finalized `formdata` event.
- Kajabi orders use the same finalized `formdata` approach as ClickFunnels.
- Both paths send an `Identify` event before the conversion event.
- Leads become `Form Submitted` events in Jitsu.
- Checkout submissions become `Order Completed` events in Jitsu.
- These are browser submission signals. Stripe remains the source of truth for a successful payment.
- Jitsu receives events through the tracking helper. Jitsu does not automatically listen to `dataLayer.push()`.

| Conversion | Jitsu event |
| --- | --- |
| Challenge registration and other leads | `Form Submitted` |
| VIP, mentorship deposit, final payment, book, and one-click-upsell checkouts | `Order Completed` |
| Kajabi initial checkout | `Order Completed` |
| DocuSign completion | Not captured by this browser helper |

## ClickFunnels: What We Tried and What Works

### What did not work reliably

**A normal `submit` listener:** ClickFunnels sometimes finishes by calling the form's native `form.submit()` method. That bypasses the normal browser `submit` event.

**A click listener:** A click happens too early. The customer can click without completing the form or without the payment being accepted.

**Stripe browser events:** ClickFunnels owns the Stripe integration. Our script does not receive a dependable Stripe payment-success event that contains the complete order and customer data.

**The thank-you page:** It does not reliably contain every field needed to reconstruct the purchase. One-click upsells can also move through several pages, so there may not be one final thank-you page that every purchase reaches.

### What works

We listen passively to the ClickFunnels form's `formdata` event.

This event fires when ClickFunnels builds the final form payload, including when ClickFunnels uses native `form.submit()`. At that point we can read the email, name, phone, product, payment-method token, and other submitted values that are available.

ClickFunnels places the Stripe PaymentMethod ID in `purchase[stripe_customer_token]`. Despite the field name, the observed values are normal Stripe `pm_...` IDs. In the live matched purchases checked on July 22, 2026, this value matched `stripe.charge.payment_method_id` exactly.

The listener does not cancel, delay, or replace the form submission. It does not call Stripe or interfere with the payment flow.

For every ClickFunnels submission:

1. Read the finalized form data.
2. Send `Identify` with the available email, name, and phone.
3. If it is a payment form, send `Order Completed`.
4. Otherwise, send `Form Submitted`.

`submitted_form_data` contains every non-empty submitted value as one JSON string. Useful fields are also promoted to normal event properties where possible.

ClickFunnels can build the same form data more than once. A short browser-side burst guard suppresses identical repeated tracking events from the same form. It does not suppress `Identify` and does not change the actual form submission.

## Multi-Product ClickFunnels Orders

ClickFunnels submits every selected product ID in `purchase[product_ids][]`. The book checkout currently uses:

| ClickFunnels product ID | Product | Price |
| --- | --- | ---: |
| `4458112` | Keyboard Rich Book (Plus FREE eBook version) | $1.00 |
| `4463617` | Domestic Shipping | $6.95 |
| `4470788` | Keyboard Rich Audiobook | $29.00 |

The browser helper uses the submitted product-ID list as the authoritative selection and ordering. It resolves each ID to the matching product name and price, removes duplicate or incomplete DOM representations of the same ID, joins the selected product names with `, `, and sums their prices.

Examples:

```text
Keyboard Rich Book (Plus FREE eBook version), Domestic Shipping
value = 7.95
```

```text
Keyboard Rich Book (Plus FREE eBook version), Domestic Shipping, Keyboard Rich Audiobook
value = 36.95
```

The combined product name is also the product component of the purchase event ID. This matches Stripe's `metadata.products` string. When this calculation was tested against the three synchronized book purchases from July 23, 2026, the product string, value, and reconstructed event ID matched Stripe for all three.

## One-Click Upsells

ClickFunnels one-click-upsell forms do not repeat the original checkout identity or PaymentMethod ID. The observed OTO form submits only:

```text
purchase[product_id] = 5100527
purchase[stripe_customer_id] = cus_...
upsell = 1
```

The preceding checkout does contain the customer's email, name, phone, and Stripe PaymentMethod ID. The helper therefore stores only that required checkout context in same-tab `sessionStorage` for up to two hours.

Known OTO products are defined in one registry:

| ClickFunnels product ID | Product | Value |
| --- | --- | ---: |
| `5100527` | 5-Day Keyboard Rich Challenge VIP Ticket - July 27th | $47 |

When a known OTO is submitted and fresh checkout context exists, the helper:

1. resolves the product from the registry;
2. carries forward the missing email, name, phone, and PaymentMethod ID;
3. keeps the OTO form's own `submitted_form_data` unchanged;
4. sends the resolved product, value, identity, and `payment_method_id` on `Order Completed`;
5. constructs a new event ID from the OTO product, not from the preceding product.

The new OTO event ID therefore cannot equal the preceding book event ID. Adding another OTO requires adding its ClickFunnels product ID, canonical Stripe product name, value, and currency to `clickfunnels/src/one-click-upsells.js`; the capture logic does not need to change.

If a known OTO is submitted without fresh carried context, the helper still records the raw submission but cannot create the deterministic composite event ID.

## What the Browser Events Mean

| Jitsu event | Meaning |
| --- | --- |
| `Form Submitted` | A ClickFunnels lead form was submitted. This includes challenge registration. |
| `Order Completed` | A ClickFunnels or Kajabi checkout form was submitted. It is a strong purchase-attempt signal, but it is not processor confirmation. |

Payment events retain these fields so the distinction stays clear:

```text
completion_basis = checkout_form_submission
is_payment_confirmed = false
payment_status = submitted_unconfirmed
```

Stripe webhooks or Stripe warehouse data provide the confirmed server-side payment.

## Form Submission Event ID

`Form Submitted` uses a deterministic event ID that the browser and ActiveCampaign data can both recreate:

```text
email = lowercase(trim(email))
submission_date = YYYY-MM-DD in America/Los_Angeles

event_id = form_submission_<lowercase_sha256_hex(email|submission_date)>
```

Repeated submissions from the same email on the same Pacific date receive the same ID. A submission without an email does not receive an event ID.

In the 2,595 browser and ActiveCampaign registration pairs checked on July 23, 2026, every pair had the same Pacific date. ActiveCampaign added the registration tag an average of 8.6 seconds after Jitsu recorded the form submission.

## ClickFunnels Purchase Event ID

### Why we cannot use an order ID or PaymentIntent ID

The ClickFunnels order does not exist yet when the browser submits the form.

The current browser payload reliably contains a Stripe PaymentMethod ID, but it does not reliably contain a PaymentIntent ID. The thank-you page and one-click-upsell flow do not solve this reliably.

We therefore need an ID that both the browser and the Stripe server data can reconstruct from fields they share.

### The agreed key

The logical purchase key is:

```text
normalized email
+ normalized product
+ Stripe payment_method_id
+ Pacific purchase date
```

The final event ID should be a SHA-256 hash of that exact joined value, prefixed with `purchase_`.

Use `America/Los_Angeles` for the date. This handles daylight-saving time correctly.

### Important product-field detail

We originally proposed using the ClickFunnels `product_id`. The browser has it, but the Stripe dataset does not. Across approximately 58,700 successful ClickFunnels charges checked, Stripe had no ClickFunnels product ID. It stores the product name in `metadata.products`.

For the current browser/server key, **normalized product therefore means the normalized product name**. Both sides must decode HTML entities, lowercase it, trim it, and collapse repeated whitespace before hashing it. HTML decoding matters because ClickFunnels currently sends `Q&amp;A` while Stripe stores `Q&A`.

If a future server source supplies the same ClickFunnels product ID as the browser, both sides can switch to that ID together. The browser and server must never use different product fields for the same event ID.

### Exact normalization

```text
email = lowercase(trim(email))
product = lowercase(trim_and_collapse_whitespace(decode_html_entities(product_name)))
payment_method_id = trim(payment_method_id)
purchase_date = YYYY-MM-DD in America/Los_Angeles

event_id = purchase_<lowercase_sha256_hex(email|product|payment_method_id|purchase_date)>
```

`Form Submitted` events do not receive this purchase event ID. They use the form-submission event ID described above.

### Why the date is included

The date allows a person to buy the same product again on another day without being treated as the original purchase.

Any date boundary creates a small risk: the browser timestamp could fall before midnight while the Stripe timestamp falls after midnight. Pacific time produced the smallest measured risk of the Eastern and Pacific cutoffs tested.

## Measured Collision Risk

We checked one year of live successful ClickFunnels Stripe charges.

### Midnight-boundary risk

- 71 of 58,704 live payments occurred during the five minutes before midnight Pacific.
- That is **0.1209%**, or about **1 in 827 payments**.
- This is a conservative ceiling, not the expected duplicate rate. A payment only fails to deduplicate if the browser and server timestamps actually land on opposite sides of midnight.
- The browser/server examples we matched were only two to three seconds apart, so the real risk should be much lower.

### Same-key collision risk

Among 52,526 live charges with all required fields, only two repeated the same email, product, payment method, and Pacific date:

1. Two $47 charges ten seconds apart; the second was fully refunded.
2. Two $94 charges 11 minutes and 35 seconds apart; neither was refunded, so this one remains ambiguous.

If the ambiguous charge was a legitimate second purchase, the observed false-merge risk was **0.0019%**, or about **1 in 52,526 payments**. It may have been an accidental duplicate, in which case the observed false-merge count was zero.

The combined conservative exposure is below **0.123%**. The realistic rate should be considerably lower.

## Kajabi Purchase Capture

The Kajabi checkout form is `new_checkout_offer`. Kajabi first fires ordinary `submit` events for validation and Stripe setup. Those are too early and can occur more than once. After Stripe populates the hidden payment fields, Kajabi calls native `form.submit()` for the finalized submission.

The helper therefore ignores Kajabi's ordinary `submit` events and listens for finalized `formdata`. It then:

1. Sends `Identify` from the submitted customer fields.
2. Reads the Kajabi offer ID from the checkout page.
3. Reads the product name and displayed price from the checkout page.
4. Sends `Order Completed` to Jitsu.

For the current offer, the captured values are:

```text
product_id = 2149808496
product_name = Booming Bookkeeping Mentorship Program
value = 199
currency = USD
```

Like ClickFunnels, this browser event means the checkout form was submitted. The paid Stripe invoice is the confirmation.

### Kajabi verification on July 22, 2026

We compared live Jitsu events from July 19–22, 2026 with the separate `stripe_kajabi` dataset. Only paid first subscription invoices were counted. Renewal invoices were excluded.

- Stripe contained 117 paid first subscription invoices for $199 during the Jitsu observation window.
- 115 of the 117 payments had a Jitsu `Order Completed` event with the same email.
- Every one of those 115 matches occurred within 13 seconds of the Stripe charge.
- First-payment coverage by exact email was therefore **98.3%**.
- All 208 Kajabi Jitsu events contained email, product ID, product name, $199 value, and USD currency correctly.

The historical raw event count is **not** accurate as a purchase count because the earlier implementation listened to ordinary `submit`:

- Jitsu received 208 Kajabi `Order Completed` events from 120 email addresses.
- The browser can send several submit events for the same checkout. One Stripe payment had as many as seven nearby Jitsu events.
- 32 browser events had no paid first invoice with the same email within one hour. These are checkout attempts, failed payments, or unmatched email changes—not confirmed purchases.
- Two Stripe first payments did not have an exact-email Jitsu match. A browser event occurred nearby for each, but under a different email, so they cannot be safely declared matches.

**Conclusion:** Kajabi is capturing the correct customer and offer information, and it covers almost all first payments. However, raw Kajabi `Order Completed` events must not be counted as confirmed purchases without server confirmation and deduplication.

## Current Status

- ClickFunnels `formdata` capture is live and working.
- The corrected Kajabi `formdata` capture is live.
- The browser now reads ClickFunnels' `purchase[stripe_customer_token]` as the PaymentMethod ID and creates the composite purchase `event_id` synchronously before sending the event.
- Multi-product ClickFunnels orders use the complete selected product set for their product name, total value, and event ID.
- Registered one-click upsells use their own product plus short-lived identity and PaymentMethod context from the preceding checkout.
- The latest browser implementation was published on July 23, 2026 HST. The server-side Meta pipeline must reconstruct the same formula from Stripe before browser/server deduplication is enabled.
- A PaymentIntent ID remains a fallback when a checkout supplies one and the composite fields are unavailable.
- DocuSign completion is outside this browser capture and requires its own server-side source.

## Relevant Files

```text
clickfunnels/src/forms.js
clickfunnels/src/order-products.js
clickfunnels/src/one-click-upsells.js
clickfunnels/src/checkout-context.js
clickfunnels/src/event-ids.js
clickfunnels/src/identity.js
clickfunnels/src/jitsu-loader.js
clickfunnels/src/submission-burst.js
```

The daily monitoring procedure is in [`CLICKFUNNELS_DAILY_CONVERSION_AUDIT_CHECKLIST.md`](CLICKFUNNELS_DAILY_CONVERSION_AUDIT_CHECKLIST.md).
