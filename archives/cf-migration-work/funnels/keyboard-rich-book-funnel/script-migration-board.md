# Script Migration Board - Keyboard Rich Book Funnel

Use this as the working checklist for moving page-level scripts into GTM.

Status values:

- `not_started`
- `gtm_created`
- `preview_validated`
- `published`
- `removed_from_page`

Standard dataLayer snippets are not listed below. This board only tracks scripts that need a migration decision.

Classes:

- `tracking`: GTM migration candidate. Ads, pixels, affiliate, conversion tracking, ActiveCampaign, GTM snippets, and tracking dataLayer pushes.
- `page_function`: Usually keep page-level. Form validation, page behavior, UI changes, CF Pro Tools, video behavior, and supporting page libraries.
- `third_party_widget`: Review manually. Trustpilot, Proof, chat widgets, badges, embeds.

## Page Status

| Page | Active/Inactive |
|---|---|
| 01 - Keyboard Rich Order |  |
| 02 - Keyboard Rich OTO 1 |  |
| 03 - Keyboard Rich OTO 2 |  |
| 04 - Receipt |  |
| 05 - Free Training |  |
| 06 - Challenge Downsell |  |
| 07 - Bonus Free Ticket |  |
| 08 - Bonus Dream Life Calculator |  |
| 09 - Bonus Bookkeeping Made Easy |  |
| 10 - Bonus QBO Account |  |
| 11 - Bonus Getting Clients |  |
| 12 - Bonus Maura Case Study |  |
| 13 - Bonus Student Success Stories |  |
| 14 - Challenge Application |  |
| 15 - Breakout Session |  |
| 16 - Terms Conditions |  |
| 17 - Privacy Policy |  |
| 18 - Challenge Order |  |
| 19 - Challenge Order Confirmation |  |
| 20 - New Upsell Page |  |
| 21 - Zoom Background |  |
| 22 - Challenge Order No Timer |  |
| 23 - Reviews |  |

---

## Funnel-Level Scripts

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF shared segmentation script | tracking | Loads `https://assets.keyboardrich.com/cf-sh-seg`. | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MZVSK7R`. | not_started |  |
| Google Ads base tag | tracking | Loads `gtag.js` and configures `AW-577876231`. | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel and initializes `223840571612758`. | not_started |  |
| Meta Pixel noscript fallback | tracking | Facebook noscript PageView image pixel for `223840571612758`. | not_started |  |
| Boom tracking universal script | tracking | Loads `https://t.boomingbookkeeping.com/v1/lst/universal-script` with `tag=!clicked`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| GTM noscript iframe | tracking | Loads `GTM-MZVSK7R` noscript iframe. | not_started |  |

---

## 01 - Keyboard Rich Order

Versions: A and B use the same scripts.

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| CF Pro Tools Keyboard Rich Order Summary 1 | page_function | Loads order summary add-on `98d66f4b...`. | not_started |  |
| CF Pro Tools Keyboard Rich International Shipping 1 | page_function | Loads international shipping add-on `0d11436d...`. | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number in `intl-phone`. | not_started |  |

---

## 02 - Keyboard Rich OTO 1

Versions: A and B use the same scripts.

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads book purchase conversion | tracking | Fires `AW-577876231/ET_eCN2q3ZkYEIfixpMC`. | not_started |  |
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 03 - Keyboard Rich OTO 2

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 04 - Receipt

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 05 - Free Training

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 06 - Challenge Downsell

Versions: A and B use the same scripts.

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 07 - Bonus Free Ticket

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 08 - Bonus Dream Life Calculator

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 09 - Bonus Bookkeeping Made Easy

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 10 - Bonus QBO Account

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 11 - Bonus Getting Clients

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 12 - Bonus Maura Case Study

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 13 - Bonus Student Success Stories

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 14 - Challenge Application

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 15 - Breakout Session

Versions: A and B use the same scripts.

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 16 - Terms Conditions

No migrated scripts found beyond the dataLayer marker.

---

## 17 - Privacy Policy

No migrated scripts found beyond the dataLayer marker.

---

## 18 - Challenge Order

Versions: A and B use the same scripts.

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Meta Pixel base PageView | tracking | Loads Meta pixel and initializes `992152782782277` and `223840571612758`. | not_started |  |
| Meta Pixel noscript fallback | tracking | Two Facebook noscript PageView image pixels. | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MSSDW42T`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| GTM noscript iframe | tracking | Loads `GTM-MSSDW42T` noscript iframe. | not_started |  |
| BBB 997 dataLayer/CAPI prep | tracking | Hashes checkout fields and pushes `bbb_997` purchase payload to dataLayer. | not_started |  |

---

## 19 - Challenge Order Confirmation

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 20 - New Upsell Page

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads registration conversion | tracking | Fires `AW-577876231/hAcOCP_Y_vIYEIfixpMC`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 21 - Zoom Background

No migrated scripts found beyond the dataLayer marker.

---

## 22 - Challenge Order No Timer

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 23 - Reviews

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | not_started |  |
| Book review JSON-LD schema | page_function | Adds Book, AggregateRating, and Review structured data. | not_started |  |
