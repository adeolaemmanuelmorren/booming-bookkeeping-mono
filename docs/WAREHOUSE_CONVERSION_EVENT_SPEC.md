# Warehouse Conversion Event Specification

## Conversion Events

| Business conversion | Platform event |
| --- | --- |
| Challenge Registered | `CompleteRegistration` |
| Structured Keyboard Rich Challenge Purchased | `Purchase` |
| VIP Upgrade Purchased — normal checkout | `Purchase` |
| VIP Upgrade Purchased — OTO | `Purchase` |
| Top Tax Loopholes OTO Purchased | `Purchase` |
| Mentorship Deposit Paid | `Purchase` |
| Final Payment Completed — one-time balance | `Purchase` |
| Mentorship Payment Plan Started — initial charge only | `Purchase` |
| Book Purchased | `Purchase` |
| Kajabi Initial Purchase — first payment only | `Purchase` |

Do not send later Kajabi renewals or later mentorship payment-plan installments as new purchase conversions.

## Challenge Registration Event ID

Required warehouse fields:

- Registration email
- Registration timestamp

Normalization:

```text
email = lowercase(trim(email))
registration_date = YYYY-MM-DD in America/Los_Angeles
```

Construction:

```text
event_id =
  "form_submission_" +
  lowercase_sha256_hex(email + "|" + registration_date)
```

Example input:

```text
person@example.com|2026-07-23
```

Content parameters:

```text
content_name = "Keyboard Rich Challenge Registration"
```

No `content_ids` are necessary because this is a registration, not a product purchase.

## Purchase Event ID

Required warehouse fields:

- Customer email
- Complete Stripe product-name string
- Stripe PaymentMethod ID
- Successful payment timestamp

Stripe fields will generally be:

```text
email
metadata.products
payment_method_id
created timestamp
```

Normalization:

```text
email =
  lowercase(trim(email))

product =
  decode HTML entities
  lowercase
  trim
  collapse repeated whitespace

payment_method_id =
  trim(payment_method_id)

purchase_date =
  YYYY-MM-DD in America/Los_Angeles
```

Construction:

```text
event_id =
  "purchase_" +
  lowercase_sha256_hex(
    email + "|" +
    product + "|" +
    payment_method_id + "|" +
    purchase_date
  )
```

The product component is the complete combined product-name string, preserving product order.

Example:

```text
person@example.com|
keyboard rich book (plus free ebook version), domestic shipping, keyboard rich audiobook|
pm_123456|
2026-07-23
```

If the composite fields are unavailable but a PaymentIntent ID exists:

```text
event_id = "purchase_" + payment_intent_id
```

If neither method can produce an ID, do not invent one.

## Purchase Content Parameters

Construct these fields for every purchase:

```text
content_ids
content_name
content_type
value
currency
```

### `content_ids`

An array containing each individual normalized product name:

```text
content_ids = [
  normalized individual product name,
  normalized individual product name
]
```

Use the same product normalization as the event ID:

```text
decode HTML entities
lowercase
trim
collapse repeated whitespace
```

Do not use ClickFunnels product IDs or Stripe product IDs.

### `content_name`

The original human-readable complete product string:

```text
content_name = metadata.products
```

For multi-product orders, preserve the complete combined name and product order.

### Other fields

```text
content_type = "product"
value = successful Stripe amount converted to dollars
currency = uppercase Stripe currency
```

Example:

```text
content_ids = [
  "keyboard rich book (plus free ebook version)",
  "domestic shipping",
  "keyboard rich audiobook"
]

content_name =
  "Keyboard Rich Book (Plus FREE eBook version), Domestic Shipping, Keyboard Rich Audiobook"

content_type = "product"
value = 36.95
currency = "USD"
```

## Expected Content Examples

| Conversion | Expected `content_ids` |
| --- | --- |
| VIP Upgrade — normal | Individual normalized VIP product name from Stripe |
| VIP Upgrade — OTO | Normalized OTO VIP product name from Stripe |
| Top Tax Loopholes OTO | `["top tax loopholes for bookkeeping business owners"]` |
| Mentorship Deposit | `["booming bookkeeping mentorship program deposit"]` |
| Final Payment | `["booming bookkeeping mentorship program"]` |
| Payment-plan initial charge | `["booming bookkeeping mentorship program (3 payments of $1,997)"]` |
| Book Purchase | Array of the selected normalized book, shipping and audiobook names |
| Kajabi Initial Purchase | `["booming bookkeeping mentorship program"]` |

## Important Notes

- Browser/server deduplication uses the same platform event name and `event_id`.
- `content_ids` do not participate in deduplication.
- The warehouse must use the complete combined product string for the purchase event ID.
- The warehouse must use the individual product names for the `content_ids` array.
- Preserve product ordering.
- Use Pacific time through the `America/Los_Angeles` timezone—not a fixed UTC offset.
- Kajabi initial purchases and ClickFunnels final payments currently share the product name `Booming Bookkeeping Mentorship Program`. Product name alone cannot distinguish those two conversion types. Retain the warehouse source if they need separate reporting or custom-conversion filters.
