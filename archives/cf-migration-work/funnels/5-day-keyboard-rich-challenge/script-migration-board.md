# Script Migration Board - 5-Day Keyboard Rich Challenge

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
| 01 - Challenge Registration - Bill |  |
| 02 - VIP Free Challenge |  |
| 03 - VIP Thank You |  |
| 04 - Regular Thank You Free Challenge |  |
| 05 - VIP Upgrade |  |
| 06 - VIP Upgrade A |  |
| 07 - VIP Upgrade A Thank You |  |
| 08 - VIP Upgrade B |  |
| 09 - VIP Upgrade B Thank You |  |
| 10 - Replays A |  |
| 11 - Replays B |  |
| 12 - Breakout Replay |  |
| 13 - Welcome Party Replay |  |
| 14 - $47 Checkout |  |
| 15 - VIP/Platinum OTO |  |
| 16 - Regular Thank You |  |
| 17 - Platinum Upgrade |  |
| 18 - Sales Page |  |
| 19 - Evergreen Replays |  |
| 20 - Sales Thank You |  |
| 21 - Waiting List Registration |  |
| 22 - Waitlist Thank You |  |
| 23 - Dream Life Calculator |  |
| 24 - Terms |  |
| 25 - Privacy |  |
| 26 - NEW Registration Page |  |
| 27 - VIP Thank You |  |
| 28 - Challenge Review Page |  |
| 29 - DoorDash VIP |  |

---

## 01 - Challenge Registration - Bill

Versions: A and B use the same scripts.

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Everflow SDK | tracking | Loads `https://www.undjsk912is.com/scripts/sdk/everflow.js`. | not_started |  |
| Everflow click tracking | tracking | Runs `EF.click(...)` using URL params like `oid`, `affid`, `sub1`, `uid`, and `_ef_transaction_id`. | not_started |  |
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | not_started |  |
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | not_started |  |
| Phone validation custom logic | page_function | Validates phone field, adds `intl-phone`, disables submit on invalid phone. | not_started |  |

---

## 02 - VIP Free Challenge

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | A/B | not_started |  |
| Google Ads free registration conversion | tracking | Fires `AW-577876231/H4-bCJjm24MaEIfixpMC`. | A/B | not_started |  |
| Meta CompleteRegistration | tracking | Fires `fbq("track", "CompleteRegistration")` with `content_type: krc-free`. | A/B | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel and initializes `992152782782277` and `223840571612758`. | A/B | not_started |  |
| Meta Pixel noscript fallback | tracking | Two Facebook noscript PageView image pixels. | A/B | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MSSDW42T`. | A/B | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | A/B | not_started |  |
| CF Pro Tools Show Only These Products VIP FC | page_function | Product visibility add-on `655b6e1e...`. | A only | not_started |  |
| CF Pro Tools Show Only These Products VIPFC-2 | page_function | Product visibility add-on `a3f6b2af...`. | B only | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number. | A/B | not_started |  |
| GTM noscript iframe | tracking | Loads `GTM-MSSDW42T` noscript iframe. Version B has it twice. | A/B | not_started |  |
| VIP paid 47 dataLayer/CAPI prep | tracking | Hashes user data and pushes `bt_krc_vip_paid_47`. | A/B | not_started |  |

---

## 03 - VIP Thank You

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Google Ads VIP purchase conversion | tracking | Fires `AW-577876231/RUk3CIaCgvMYEIfixpMC`. Version B adds value, currency, and generated transaction ID. | A/B different | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MSSDW42T`. | A/B | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | A/B | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| GTM noscript iframe | tracking | Loads `GTM-MSSDW42T` noscript iframe. | A/B | not_started |  |

---

## 04 - Regular Thank You Free Challenge

Versions: A and B use the same scripts.

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 05 - VIP Upgrade

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel and initializes `992152782782277` and `223840571612758`. | not_started |  |
| Meta Pixel noscript fallback | tracking | Two Facebook noscript PageView image pixels. | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MSSDW42T`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| CF Pro Tools Show Only These Products 1 | page_function | Product visibility add-on `426262ae...`. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number. | not_started |  |
| GTM noscript iframe | tracking | Loads `GTM-MSSDW42T` noscript iframe. | not_started |  |
| VIP paid 47 dataLayer/CAPI prep | tracking | Hashes user data and pushes `bt_krc_vip_paid_47`. | not_started |  |

---

## 06 - VIP Upgrade A

Same script stack as VIP Upgrade, except there is no CF Pro Tools product filter add-on in the body. Use the same classes: tracking for Meta/GTM/ActiveCampaign/dataLayer scripts, page_function for phone validation and Vimeo behavior.

Status: not_started

---

## 07 - VIP Upgrade A Thank You

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads VIP purchase conversion | tracking | Fires `AW-577876231/RUk3CIaCgvMYEIfixpMC`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 08 - VIP Upgrade B

Same script stack as VIP Upgrade A. Use the same classes: tracking for Meta/GTM/ActiveCampaign/dataLayer scripts, page_function for phone validation and Vimeo behavior.

Status: not_started

---

## 09 - VIP Upgrade B Thank You

Same script stack as VIP Upgrade A Thank You. Use the same classes: tracking for Google Ads and ActiveCampaign, page_function for Vimeo behavior.

Status: not_started

---

## 10 - Replays A

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A only | not_started |  |

Version B has no migrated scripts beyond the dataLayer marker.

---

## 11 - Replays B

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A only | not_started |  |

Version B has no migrated scripts beyond the dataLayer marker.

---

## 12 - Breakout Replay

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 13 - Welcome Party Replay

No migrated scripts found beyond the dataLayer marker.

---

## 14 - $47 Checkout

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | A/B | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| CF Pro Tools Show Only These Products - Split Test A | page_function | Product visibility add-on `42ae17ce...`. | A only | not_started |  |
| CF Pro Tools Show Only These Products - Split Test B | page_function | Product visibility add-on `e037fa9d...`. | B only | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number. | A/B | not_started |  |

---

## 15 - VIP/Platinum OTO

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Google Ads registration conversion | tracking | Fires `AW-577876231/hAcOCP_Y_vIYEIfixpMC`. | A/B | not_started |  |
| Everflow SDK | tracking | Loads `https://www.undjsk912is.com/scripts/sdk/everflow.js`. | A/B | not_started |  |
| Everflow conversion offer 24520 | tracking | Runs `EF.conversion({ offer_id: 24520 })`. | A/B | not_started |  |
| Meta Purchase event | tracking | Fires `fbq("track", "Purchase")` with `content_type: krc-paid`, value 47 USD. | B only | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | A/B | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |

---

## 16 - Regular Thank You

Versions: A and B use the same scripts.

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 17 - Platinum Upgrade

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number. | not_started |  |

---

## 18 - Sales Page

No migrated scripts found beyond the dataLayer marker.

---

## 19 - Evergreen Replays

No migrated scripts found beyond the dataLayer marker.

---

## 20 - Sales Thank You

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 21 - Waiting List Registration

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 22 - Waitlist Thank You

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 23 - Dream Life Calculator

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 24 - Terms

No migrated scripts found beyond the dataLayer marker.

---

## 25 - Privacy

No migrated scripts found beyond the dataLayer marker.

---

## 26 - NEW Registration Page

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | not_started |  |

---

## 27 - VIP Thank You

Versions: A and B use the same scripts.

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads VIP purchase conversion | tracking | Fires `AW-577876231/RUk3CIaCgvMYEIfixpMC`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 28 - Challenge Review Page

No migrated scripts found beyond the dataLayer marker.

---

## 29 - DoorDash VIP

Same script stack as VIP Upgrade A. Use the same classes: tracking for Meta/GTM/ActiveCampaign/dataLayer scripts, page_function for phone validation and Vimeo behavior.

Status: not_started
