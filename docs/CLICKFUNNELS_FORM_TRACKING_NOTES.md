# ClickFunnels Form Tracking Notes

## Deployment Changelog

### 2026-07-26 19:35 HST

- ActiveCampaign and ClickFunnels anonymous-ID field hydration now waits until Jitsu is fully initialized.
- This prevents a pre-initialization `__eventn_id` cookie value from being written into form fields.
- Form submission capture, purchase tracking, and cross-domain tracking were not changed.
- Published to `assets/cf-sh-seg` and verified byte-for-byte on all four asset domains.

## Goal

Track ClickFunnels Classic lead forms and checkout pages without requiring manually added data attributes.

The helper should work with:

- ClickFunnels popup forms
- hidden ClickFunnels `#cfAR` forms
- embedded ActiveCampaign forms
- checkout pages with `purchase[...]` fields
- Kajabi checkout forms with `checkout_offer[...]` fields

## Form Detection Rules

ClickFunnels Classic often keeps the visible fields outside the hidden form that is actually submitted.

For that reason, the helper reads both:

- real `<form>` elements, including `#cfAR` and ActiveCampaign forms
- visible `.elInput` controls created by ClickFunnels popups

The fallback event context is:

- `document.title`
- `window.location.pathname`
- `window.location.href`

## ActiveCampaign Field Hydration Only

The helper does not inject generic hidden fields into every form.

It only fills configured ActiveCampaign custom fields that already exist on the form, currently:

```text
field[39]
```

The helper does not create this ActiveCampaign field. The field must already be present in the ActiveCampaign form HTML.

That field receives:

```js
analytics.user().anonymousId()
```

## ActiveCampaign Anonymous ID Field

ActiveCampaign must receive the Segment anonymous ID before it can place that value into the native form redirect URL.

After the hidden ActiveCampaign field is added to the form, copy the generated field name. It should look like:

```text
field[39]
```

The helper already includes `field[39]` as the default. If another ActiveCampaign anonymous ID field is added later, configure the helper with that field name before the bundled helper loads:

```html
<script>
  window.BOOM_CLICKFUNNELS_ACTIVE_CAMPAIGN_ANONYMOUS_ID_FIELDS = ["field[39]", "field[52]"];
</script>
```

## Events

Regular lead forms emit:

```text
Form Submitted
```

Checkout/payment forms do not emit `Form Submitted`.

Checkout submissions emit:

```text
Order Completed
```

ClickFunnels events are emitted from the form's passive `formdata` event. Unlike the ordinary `submit` event, `formdata` still fires when ClickFunnels calls native `form.submit()`. The listener reads the finalized payload without cancelling, changing, or delaying the form submission.

Kajabi keeps its existing working submit-event path.

Checkout properties explicitly record:

```text
completion_basis = checkout_form_submission
is_payment_confirmed = false
payment_status = submitted_unconfirmed
```

A checkout-form submission does not prove that the payment processor accepted the payment; server-side Stripe remains the authoritative source for confirmed payments.

Kajabi's `new_checkout_offer` form keeps its existing submit-event path and still emits `Order Completed`; it does not fall through to `Form Submitted`.

## Product Detection

Checkout product metadata is read from selected ClickFunnels purchase inputs:

- `purchase[product_id]`
- `purchase[product_ids][]`
- checked inputs with `data-product-name`

The helper stores:

- product id
- product name
- price
- currency
- page URL/path/title

For Kajabi, it reads the offer ID from the checkout page class and reads the product name and displayed price from the checkout markup.

## Identity

Valid email values are identified on email-field blur/change and again from the finalized `formdata` payload. The formdata-time call covers autofill and users who submit without leaving the email field. Kajabi retains its existing submit-time identify call. Repeated identification of the same value on the same input is suppressed; the final submission deliberately emits an identify event with the complete available email, phone, and name traits.

Safe payment reconciliation identifiers are retained in checkout events, including Stripe PaymentIntent/PaymentMethod IDs and PayPal payer/payment/order IDs. Credentials and authorization material such as card data, authorization tokens, client secrets, passwords, and nonces are excluded.

## Live Asset

The shared bundle is published to the existing Cloudflare R2 object `assets/cf-sh-seg` and is served on the Boom domain at:

```text
https://assets.boomingbookkeeping.com/cf-sh-seg
```

The current product extraction can be inspected in the browser console:

```js
window.BoomClickFunnels.getSelectedCheckoutProducts()
```

## Mutation Observers

The form observer only reacts when newly-added DOM nodes are forms or contain forms.

The identify observer only reacts when newly-added DOM nodes are email/phone inputs or contain email/phone inputs.

The helper does not observe page-wide `class`, `style`, or `value` attribute changes.

## Browser QA Rules

Allowed:

- open regular form preview pages
- open popup forms
- submit regular test lead forms when needed
- inspect checkout DOM
- inspect selected checkout product metadata

Not allowed:

- submit checkout/payment forms during browser testing

## ActiveCampaign Redirect Decision

Use ActiveCampaign native redirect:

```text
https://keyboardrichchallenge.com/vipfc-1?ajs_aid=%SEGMENT_ANON_FIELD_TAG%
```
