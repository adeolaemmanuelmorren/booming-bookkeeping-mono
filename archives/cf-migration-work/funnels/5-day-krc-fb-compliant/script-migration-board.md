# Script Migration Board - 5-Day KRC FB Compliant

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
| 01 - KRC Reg FB Compliant |  |
| 02 - KRC Reg Taboola |  |
| 03 - Terms |  |
| 04 - Privacy |  |

---

## Funnel-Level Scripts

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF shared segmentation script | tracking | Loads `https://assets.thebookkeepingchallenge.com/cf-sh-seg`. | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MZVSK7R`. | not_started |  |
| Boom tracking universal script | tracking | Loads `https://t.boomingbookkeeping.com/v1/lst/universal-script` with `tag=!clicked`. | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel and initializes `223840571612758`. | not_started |  |
| Meta Pixel noscript fallback | tracking | Facebook noscript PageView image pixel for `223840571612758`. | not_started |  |
| Google Ads base tag | tracking | Loads `gtag.js` and configures `AW-577876231`. | not_started |  |
| Proof widget | third_party_widget | Loads Proof pixel `JIWAWvECnSV7YOgov4fwHVdU47C2`. | not_started |  |
| VWO SmartCode | third_party_widget | Loads Visual Website Optimizer account `1099351`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| GTM noscript iframe | tracking | Loads `GTM-MZVSK7R` noscript iframe. | not_started |  |

---

## 01 - KRC Reg FB Compliant

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Everflow SDK | tracking | Loads `https://www.undjsk912is.com/scripts/sdk/everflow.js`. | A only | not_started |  |
| Everflow click tracking | tracking | Runs `EF.click(...)` using URL params like `oid`, `affid`, `sub1`, `uid`, and `_ef_transaction_id`. | A only | not_started |  |
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | A/B | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel. Version A initializes `992152782782277` and `223840571612758`; version B initializes only `992152782782277`. | A/B different | not_started |  |
| Meta Pixel noscript fallback | tracking | Version A has two Facebook noscript PageView image pixels; version B has one. | A/B different | not_started |  |
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | A/B | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | A/B | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number in `intl-phone`. | A/B | not_started |  |
| Complete Free Registration Pixel/Stape event | tracking | Hashes user fields, fires `fbq("trackCustom", "Complete Free Registration")`, and posts the event to `https://events.thebookkeepingchallenge.com`. | A/B | not_started |  |

---

## 02 - KRC Reg Taboola

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Everflow SDK | tracking | Loads `https://www.undjsk912is.com/scripts/sdk/everflow.js`. | A only | not_started |  |
| Everflow click tracking | tracking | Runs `EF.click(...)` using URL params like `oid`, `affid`, `sub1`, `uid`, and `_ef_transaction_id`. | A only | not_started |  |
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | A/B | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel. Version A initializes `992152782782277` and `223840571612758`; version B initializes only `992152782782277`. | A/B different | not_started |  |
| Meta Pixel noscript fallback | tracking | Version A has two Facebook noscript PageView image pixels; version B has one. | A/B different | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MSSDW42T`. | A/B | not_started |  |
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | A/B | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | A/B | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number in `intl-phone`. | A/B | not_started |  |
| GTM noscript iframe | tracking | Loads `GTM-MSSDW42T` noscript iframe. | A/B | not_started |  |
| Complete Free Registration Pixel/Stape event | tracking | Hashes user fields, fires `fbq("trackCustom", "Complete Free Registration")`, and posts the event to `https://events.thebookkeepingchallenge.com`. Version A also pushes `free_registration` to dataLayer. | A/B different | not_started |  |

---

## 03 - Terms

No migrated scripts found beyond the dataLayer marker.

---

## 04 - Privacy

No migrated scripts found beyond the dataLayer marker.
