# Completed Funnel Script Review

Scope reviewed:

- `funnels/keyboard-rich-book-funnel`
- `funnels/5-day-keyboard-rich-challenge`
- `funnels/booming-bookkeeping-business-direct`
- `funnels/5-day-krc-fb-compliant`

This review is only about:

- page-level scripts that directly duplicate a funnel top-level script
- page-level scripts that look like they should live at funnel top-level

It does not classify conversion tags or page behavior scripts.

## Keyboard Rich Book Funnel

Top-level file reviewed:

- `funnels/keyboard-rich-book-funnel/top-level-scripts.html`

Top-level scripts currently present:

- `assets.keyboardrich.com/cf-sh-seg`
- GTM container `GTM-MZVSK7R`
- Google Ads base tag `AW-577876231`
- Meta Pixel base/PageView for pixel `223840571612758`
- Booming Bookkeeping universal script
- GTM noscript for `GTM-MZVSK7R`

### Direct Duplicates Of Top-Level Scripts

| Script duplicated at page level | Found in |
| --- | --- |
| Meta Pixel base/PageView for main pixel `223840571612758` | `18-challenge-order/version-a/header.html` |
| Meta Pixel base/PageView for main pixel `223840571612758` | `18-challenge-order/version-b/header.html` |

Notes:

- These two files also initialize test pixel `992152782782277`, which is not in the top-level file.
- I did not find page-level duplicates of `cf-sh-seg`, `GTM-MZVSK7R`, Google Ads base `AW-577876231`, the Booming universal script, or the `GTM-MZVSK7R` noscript block.

### Page-Level Scripts That Look Like Top-Level Scripts

| Script | Found in | Why it looks top-level |
| --- | --- | --- |
| ActiveCampaign diffuser / `vgo("process")` | 24 body files | This is a site/funnel tracking initializer repeated across many pages. |
| Meta test pixel base/PageView `992152782782277` | `18-challenge-order/version-a/header.html`, `18-challenge-order/version-b/header.html` | This is an ad pixel initialization/PageView script. If the test pixel is supposed to run funnel-wide, it belongs top-level. |
| GTM container `GTM-MSSDW42T` and matching noscript | `18-challenge-order/version-a/header.html`, `18-challenge-order/version-a/body.html`, `18-challenge-order/version-b/header.html`, `18-challenge-order/version-b/body.html` | This is a container initialization script. If this container is intended for the funnel, it belongs top-level. |

## 5-Day Keyboard Rich Challenge

This funnel has extracted page-level scripts, but I did not find a root `top-level-scripts.html` file in:

- `funnels/5-day-keyboard-rich-challenge`

Because there is no top-level file, I could not check for direct duplicates against a top-level source of truth.

### Page-Level Scripts That Look Like Top-Level Scripts

| Script | Found in | Why it looks top-level |
| --- | --- | --- |
| ActiveCampaign diffuser / `vgo("process")` | 31 body files | This is a site/funnel tracking initializer repeated across many pages. |
| Meta Pixel base/PageView for main pixel `223840571612758` | 6 header files | This is an ad pixel initialization/PageView script repeated across several pages. |
| Meta test pixel base/PageView `992152782782277` | 6 header files | This is an ad pixel initialization/PageView script repeated across several pages. |
| GTM container `GTM-MSSDW42T` and matching noscript | 8 page variants | This is a container initialization script repeated across multiple pages. |

## Booming Bookkeeping Business - Direct

Top-level file reviewed:

- `funnels/booming-bookkeeping-business-direct/top-level-scripts.html`

Top-level scripts currently present:

- `assets.boomingbookkeeping.com/cf-sh-seg`
- GTM container `GTM-MZVSK7R`
- Google Ads base tag `AW-577876231`
- Booming Bookkeeping universal script
- Meta Pixel base/PageView for pixel `223840571612758`
- Deadline Funnel unified script
- Pinterest tag/PageView for tag `2614114035627`
- Proof pixel
- Microsoft UET tag `137010094`
- TikTok pixel `C9LH2OJC77UEMUC6P74G`
- GTM noscript for `GTM-MZVSK7R`

### Direct Duplicates Of Top-Level Scripts

| Script duplicated at page level | Found in |
| --- | --- |
| Google Ads base tag `AW-577876231` (`gtag.js` include and `gtag("config", "AW-577876231")`) | `35-bootcamp-confirmation/version-a/header.html` |

Notes:

- I did not find page-level duplicates of `cf-sh-seg`, `GTM-MZVSK7R`, the Booming universal script, Meta Pixel `223840571612758`, Deadline Funnel, Pinterest base/PageView, Proof, Microsoft UET, TikTok, or the `GTM-MZVSK7R` noscript block.
- `35-bootcamp-confirmation/version-a/header.html` also has Google Ads conversion event snippets. Those are page events, but the base `gtag.js` setup duplicates the funnel top-level setup.

### Page-Level Scripts That Look Like Top-Level Scripts

| Script | Found in | Why it looks top-level |
| --- | --- | --- |
| ActiveCampaign diffuser / `vgo("process")` | 36 body files | This is a site/funnel tracking initializer repeated across many pages and is not present in the top-level file. |
| VWO Async SmartCode account `1099351` | 13 header files | This is an optimization/testing platform initializer repeated across the same offer sequence pages. If VWO should run on those pages consistently, it is a top-level candidate. |
| `ps.boaa.it/powerscripts.js` loader | 13 body files | This is a repeated script loader that derives the page ID dynamically. It may be centralizable if every target page has the expected `page-id` value available. |

### Additional Notes

- The 13 page variants with VWO also include a second `funnel_step_ready` block labeled `Funnel: 5-Day Keyboard Rich Challenge`. That is page metadata, not a top-level script, but it looks migrated from another funnel and should be reviewed separately.
- Pinterest `pintrk("track", ...)` snippets appear in 5 header files. I did not classify them as top-level because they are page-specific event snippets and the Pinterest base/PageView already exists at top-level.
- Everflow SDK / `EF.click` snippets appear in the same 13 header files as VWO. I did not classify them as top-level because they are affiliate click-attribution snippets tied to URL parameters.

## 5-Day KRC FB Compliant

Top-level file reviewed:

- `funnels/5-day-krc-fb-compliant/top-level-scripts.html`

Top-level scripts currently present:

- `assets.thebookkeepingchallenge.com/cf-sh-seg`
- GTM container `GTM-MZVSK7R`
- Booming Bookkeeping universal script
- Meta Pixel base/PageView for pixel `223840571612758`
- Google Ads base tag `AW-577876231`
- Proof pixel
- VWO Async SmartCode account `1099351`
- GTM noscript for `GTM-MZVSK7R`
- `ps.boaa.it/powerscripts.js` loader

### Direct Duplicates Of Top-Level Scripts

| Script duplicated at page level | Found in |
| --- | --- |
| Meta Pixel base/PageView for main pixel `223840571612758` | `01-krc-reg-fb-compliant/version-a/header.html` |
| Meta Pixel base/PageView for main pixel `223840571612758` | `02-krc-reg-taboola/version-a/header.html` |

Notes:

- These two files also initialize test pixel `992152782782277`, which is not in the top-level file.
- I did not find page-level duplicates of `cf-sh-seg`, `GTM-MZVSK7R`, the Booming universal script, Google Ads base `AW-577876231`, Proof, VWO, `ps.boaa.it/powerscripts.js`, or the `GTM-MZVSK7R` noscript block.

### Page-Level Scripts That Look Like Top-Level Scripts

| Script | Found in | Why it looks top-level |
| --- | --- | --- |
| ActiveCampaign diffuser / `vgo("process")` | 4 body files | This is a site/funnel tracking initializer repeated across all registration page variants and is not present in the top-level file. |
| Meta test pixel base/PageView `992152782782277` | 4 header files | This is an ad pixel initialization/PageView script repeated across all registration page variants. If the test pixel should run funnel-wide, it belongs top-level. |
| GTM container `GTM-MSSDW42T` and matching noscript | `02-krc-reg-taboola/version-a/header.html`, `02-krc-reg-taboola/version-a/body.html`, `02-krc-reg-taboola/version-b/header.html`, `02-krc-reg-taboola/version-b/body.html` | This is a container initialization script. If this Taboola/testing container is intended for the funnel or the whole Taboola step, it belongs at the highest shared level available. |

### Additional Notes

- Everflow SDK / `EF.click` appears in `01-krc-reg-fb-compliant/version-a/header.html` and `02-krc-reg-taboola/version-a/header.html`. I did not classify it as top-level because it is affiliate click-attribution tied to URL parameters.
- Phone validation and Trustpilot widget scripts appear on the registration page headers. I did not classify them as top-level because they support page UI/forms rather than funnel-wide tracking.
- The custom `fbq("trackCustom", "Complete Free Registration", ...)` handlers in the registration bodies look page-specific and are not direct duplicates of top-level scripts.

## Not Top-Level Based On This Review

These were found in page files, but they do not look like funnel top-level scripts:

- Google Ads conversion event snippets
- Everflow conversion/click snippets
- Pinterest conversion/event snippets
- Meta/custom registration event handlers
- Page-specific `funnel_step_ready` dataLayer snippets
- CF Pro Tools add-ons
- Vimeo behavior scripts
- FAQ/accordion DOM scripts
- Phone validation scripts
- Trustpilot widget scripts
- Order-form hashing/dataLayer helper scripts
