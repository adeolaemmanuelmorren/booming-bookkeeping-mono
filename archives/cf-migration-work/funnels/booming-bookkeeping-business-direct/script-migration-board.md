# Script Migration Board - Booming Bookkeeping Business - Direct

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

## Funnel-Level Scripts

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Booming Bookkeeping segment script | page_function | Loads `https://assets.boomingbookkeeping.com/cf-sh-seg`. | not_started |  |
| Google Tag Manager web container | tracking | Loads `GTM-MZVSK7R`. | not_started |  |
| Google Ads global tag | tracking | Loads/configures `AW-577876231`. | not_started |  |
| Booming Bookkeeping universal tracking script | tracking | Loads `https://t.boomingbookkeeping.com/v1/lst/universal-script...` with the current page URL. | not_started |  |
| Meta Pixel base PageView | tracking | Loads Meta pixel, initializes `223840571612758`, and fires PageView. | not_started |  |
| Meta Pixel noscript fallback | tracking | Facebook noscript PageView image pixel. | not_started |  |
| Deadline Funnel embed | third_party_widget | Loads Deadline Funnel unified countdown/urgency script. | not_started |  |
| Pinterest base tag | tracking | Loads Pinterest tag `2614114035627` and fires `pintrk("page")`. | not_started |  |
| Pinterest noscript fallback | tracking | Pinterest noscript init image pixel. | not_started |  |
| Proof widget | third_party_widget | Loads Proof social proof widget account `JIWAWvECnSV7YOgov4fwHVdU47C2`. | not_started |  |
| Microsoft Ads UET | tracking | Loads Bing/Microsoft Ads UET tag `137010094`. | not_started |  |
| TikTok Pixel | tracking | Loads TikTok pixel `C9LH2OJC77UEMUC6P74G` and fires page view. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| GTM noscript iframe | tracking | Loads `GTM-MZVSK7R` noscript iframe. | not_started |  |

---

## Page Status

| Page | Active/Inactive |
|---|---|
| 01 - Training Registration |  |
| 02 - HT Webinar Page |  |
| 03 - HT Application |  |
| 04 - HT Calendar |  |
| 05 - HT Homework |  |
| 06 - HT Sorry |  |
| 07 - HT Order |  |
| 08 - HT Challenge Order |  |
| 09 - HT Order Confirmation |  |
| 10 - Training Registration MMO |  |
| 11 - Training Broadcast |  |
| 12 - Training Broadcast MMO |  |
| 13 - Sales Page |  |
| 14 - Order |  |
| 15 - Challenge Order Confirmation |  |
| 16 - Replay |  |
| 17 - Replay MMO |  |
| 18 - Objection 1 |  |
| 19 - Objection 2 |  |
| 20 - Behind The Scenes |  |
| 21 - FAQ |  |
| 22 - Get Over Fears |  |
| 23 - Expired |  |
| 24 - Waitlist Thanks |  |
| 25 - Sales Page Retarget FB Friendly |  |
| 26 - Application |  |
| 27 - Mastermind Deposit |  |
| 28 - Schedule Call |  |
| 29 - Affiliate Access |  |
| 30 - Affiliate Area |  |
| 31 - Sales Page |  |
| 32 - Order |  |
| 33 - Order Confirmation |  |
| 34 - Bootcamp Registration |  |
| 35 - Bootcamp Confirmation |  |
| 36 - Sales Page Black Friday |  |
| 37 - Black Friday HT |  |
| 38 - Order Black Friday |  |
| 39 - PayPal Buttons |  |
| 40 - Ann Case Study |  |
| 41 - Maura Case Study |  |
| 42 - Pro Training Registration |  |
| 43 - Pro Training Broadcast |  |
| 44 - Pro Sales Page |  |
| 45 - Pro Order |  |
| 46 - Pro Order Confirmation |  |
| 47 - Pro Replay |  |
| 48 - Pro Ann Case Study |  |
| 49 - Order Proof |  |
| 50 - CB Order |  |
| 51 - KRC Calendar |  |
| 52 - Subscription No Timer |  |
| 53 - HT Order Payment Plans |  |
| 54 - Challenge HT No Timer |  |
| 55 - Challenge Subscription |  |
| 56 - Challenge HT Sales Page |  |
| 57 - Terms |  |
| 58 - Privacy Policy |  |

---

## 01 - Training Registration

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | B only | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| Phone validation custom logic | page_function | Validates phone field and stores formatted number. | B only | not_started |  |

---

## 02 - HT Webinar Page

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Pinterest lead event | tracking | Fires `pintrk("track", "lead")`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 03 - HT Application

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 04 - HT Calendar

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 05 - HT Homework

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 06 - HT Sorry

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 07 - HT Order

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 08 - HT Challenge Order

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |
| CF Pro Tools Grouped Products 1 | page_function | Loads grouped product display add-on `720e5510...`. | A only | not_started |  |

---

## 09 - HT Order Confirmation

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Pinterest checkout event | tracking | Fires `pintrk("track", "checkout")`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 10 - Training Registration MMO

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| VWO experiment loader | tracking | Loads Visual Website Optimizer account `1099351`. | not_started |  |
| Everflow SDK | tracking | Loads `https://www.undjsk912is.com/scripts/sdk/everflow.js`. | not_started |  |
| Everflow click tracking | tracking | Runs `EF.click(...)` using URL params like `oid`, `affid`, `sub1`, `uid`, and `_ef_transaction_id`. | not_started |  |
| Phone validation dependency | page_function | Loads intl-tel-input CSS and JS. | not_started |  |
| Trustpilot TrustBox widget | third_party_widget | Loads Trustpilot widget bootstrap. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| PowerScripts loader | page_function | Loads BOAA PowerScripts for page behavior. | not_started |  |
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | not_started |  |
| Phone validation custom logic | page_function | Validates phone field, adds `intl-phone`, and disables submit on invalid phone. | not_started |  |

---

## 11 - Training Broadcast

Versions: A and B use the same scripts.

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 12 - Training Broadcast MMO

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 13 - Sales Page

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 14 - Order

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 15 - Challenge Order Confirmation

Header scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Google Ads challenge checkout conversion | tracking | Fires `AW-577876231/SkudCIT-9OQBEIfixpMC`. | A only | not_started |  |
| Pinterest checkout event | tracking | Fires `pintrk("track", "checkout")`. | A only | not_started |  |
| Google Ads challenge order conversion | tracking | Fires `AW-577876231/Pc2YCIiBsvIYEIfixpMC`. | B only | not_started |  |

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | A/B | not_started |  |

---

## 16 - Replay

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 17 - Replay MMO

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 18 - Objection 1

No migrated scripts found beyond the dataLayer marker.

---

## 19 - Objection 2

No migrated scripts found beyond the dataLayer marker.

---

## 20 - Behind The Scenes

No migrated scripts found beyond the dataLayer marker.

---

## 21 - FAQ

No migrated scripts found beyond the dataLayer marker.

---

## 22 - Get Over Fears

No migrated scripts found beyond the dataLayer marker.

---

## 23 - Expired

No migrated scripts found beyond the dataLayer marker.

---

## 24 - Waitlist Thanks

No migrated scripts found beyond the dataLayer marker.

---

## 25 - Sales Page Retarget FB Friendly

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 26 - Application

No migrated scripts found beyond the dataLayer marker.

---

## 27 - Mastermind Deposit

No migrated scripts found beyond the dataLayer marker.

---

## 28 - Schedule Call

No migrated scripts found beyond the dataLayer marker.

---

## 29 - Affiliate Access

No migrated scripts found beyond the dataLayer marker.

---

## 30 - Affiliate Area

No migrated scripts found beyond the dataLayer marker.

---

## 31 - Sales Page

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 32 - Order

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 33 - Order Confirmation

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 34 - Bootcamp Registration

No migrated scripts found beyond the dataLayer marker.

---

## 35 - Bootcamp Confirmation

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads bootcamp conversion | tracking | Fires `AW-625074777/Ag5ZCK7n69QBENnEh6oC`. | not_started |  |
| Google Ads bootcamp conversion alternate | tracking | Fires `AW-625074777/KF-4CMrWzNQBENnEh6oC`. | not_started |  |
| Google Ads global tag | tracking | Loads/configures `AW-577876231`. | not_started |  |
| Google Ads challenge checkout conversion | tracking | Fires `AW-577876231/SkudCIT-9OQBEIfixpMC`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 36 - Sales Page Black Friday

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Unslider testimonial carousel | page_function | Loads Unslider CSS/JS and CF Pro Tools `cfUnslider` helper. | not_started |  |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | not_started |  |

---

## 37 - Black Friday HT

Same script stack as Sales Page Black Friday. Use the same classes: page_function for Unslider and FAQ behavior.

Status: not_started

---

## 38 - Order Black Friday

Versions: A and B use the same scripts.

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 39 - PayPal Buttons

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| PayPal SDK | third_party_widget | Loads PayPal SDK button factory script. | not_started |  |
| PayPal button render logic | third_party_widget | Renders PayPal button for `Booming Bookkeeping Business Training`, amount 1997 USD. | not_started |  |

---

## 40 - Ann Case Study

No migrated scripts found beyond the dataLayer marker.

---

## 41 - Maura Case Study

No migrated scripts found beyond the dataLayer marker.

---

## 42 - Pro Training Registration

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 43 - Pro Training Broadcast

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads pro training broadcast conversion | tracking | Fires `AW-577876231/ASGlCL_Q2OQBEIfixpMC`. | not_started |  |
| Pinterest lead event | tracking | Fires `pintrk("track", "lead")`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |
| CF Pro Tools Vimeo Autoplay Unmute Restart 1 | page_function | Loads Vimeo autoplay/unmute/restart add-on. | not_started |  |

---

## 44 - Pro Sales Page

No migrated scripts found beyond the dataLayer marker.

---

## 45 - Pro Order

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 46 - Pro Order Confirmation

Header scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| Google Ads challenge checkout conversion | tracking | Fires `AW-577876231/SkudCIT-9OQBEIfixpMC`. | not_started |  |
| Pinterest checkout event | tracking | Fires `pintrk("track", "checkout")`. | not_started |  |

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 47 - Pro Replay

No migrated scripts found beyond the dataLayer marker.

---

## 48 - Pro Ann Case Study

No migrated scripts found beyond the dataLayer marker.

---

## 49 - Order Proof

Same script stack as Training Registration MMO. Use the same classes: tracking for VWO/Everflow/ActiveCampaign, page_function for PowerScripts/FAQ/phone validation, third_party_widget for Trustpilot.

Status: not_started

---

## 50 - CB Order

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 51 - KRC Calendar

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 52 - Subscription No Timer

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Unslider testimonial carousel | page_function | Loads Unslider CSS/JS and CF Pro Tools `cfUnslider` helper. | A only | not_started | LEAVE ALONE |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | A only | not_started | LEAVE ALONE |

---

## 53 - HT Order Payment Plans

Body scripts:

| Script | Class | What it does | Status | Action |
|---|---|---|---|---|
| ActiveCampaign site tracking | tracking | Loads ActiveCampaign diffuser and runs `vgo("process")`. | not_started |  |

---

## 54 - Challenge HT No Timer

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Unslider testimonial carousel | page_function | Loads Unslider CSS/JS and CF Pro Tools `cfUnslider` helper. | A only | not_started | LEAVE ALONE |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | A only | not_started | LEAVE ALONE |

---

## 55 - Challenge Subscription

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Unslider testimonial carousel | page_function | Loads Unslider CSS/JS and CF Pro Tools `cfUnslider` helper. | A only | not_started | LEAVE ALONE |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | A only | not_started | LEAVE ALONE |

---

## 56 - Challenge HT Sales Page

Versions: A and B use the same scripts.

Body scripts:

| Script | Class | What it does | Version | Status | Action |
|---|---|---|---|---|---|
| Unslider testimonial carousel | page_function | Loads Unslider CSS/JS and CF Pro Tools `cfUnslider` helper. | A/B | not_started | LEAVE ALONE |
| FAQ accordion behavior | page_function | Adds expand/collapse behavior to FAQ blocks. | A/B | not_started | LEAVE ALONE |

---

## 57 - Terms

No migrated scripts found beyond the dataLayer marker.

---

## 58 - Privacy Policy

No migrated scripts found beyond the dataLayer marker.
