# ActiveCampaign Native Segment Cross-Domain Handoff

## Goal

Preserve the same Segment `anonymousId` when a visitor submits the ClickFunnels Classic popup form on:

```text
https://thebookkeepingchallenge.com
```

and is redirected to:

```text
https://keyboardrichchallenge.com/vipfc-1
```

The intended final destination URL should include Segment's querystring parameter:

```text
https://keyboardrichchallenge.com/vipfc-1?ajs_aid=<segment-anonymous-id>
```

ActiveCampaign may also add `vgo_ee`. That is fine. ActiveCampaign's own tracking script removes only `vgo_ee` after the page loads. In testing, `ajs_aid` stayed in the URL.

## Recommended First Approach

Use ActiveCampaign's native form redirect URL with a custom hidden field.

## Current Rollout Plan

### 1. Deploy the Worker First

Deploy the Worker with dedicated tracking subdomains for each root domain:

```text
https://sg.thebookkeepingchallenge.com
https://sg.keyboardrichchallenge.com
https://sg.keyboardrich.com
https://sg.boomingbookkeeping.com
```

The Worker allows CORS and redirect handoff for the root domains and any of their subdomains:

```text
thebookkeepingchallenge.com
*.thebookkeepingchallenge.com
keyboardrichchallenge.com
*.keyboardrichchallenge.com
keyboardrich.com
*.keyboardrich.com
boomingbookkeeping.com
*.boomingbookkeeping.com
```

Keep the existing ClickFunnels and ActiveCampaign behavior unchanged during this step.

After deploy, confirm the Segment proxy endpoints are reachable:

```text
https://sg.thebookkeepingchallenge.com/route/ck
https://sg.keyboardrichchallenge.com/route/ck
https://sg.keyboardrich.com/route/ck
https://sg.boomingbookkeeping.com/route/ck
```

### 2. Install Segment on the Sites

Load Segment through the matching first-party proxy for each root domain:

```text
https://sg.thebookkeepingchallenge.com/route/ajs/...
https://sg.keyboardrichchallenge.com/route/ajs/...
https://sg.keyboardrich.com/route/ajs/...
https://sg.boomingbookkeeping.com/route/ajs/...
```

Do not change the form redirect behavior yet.

### 3. Manually Test `ajs_aid` on the Destination

Before changing ActiveCampaign or ClickFunnels, manually open the destination page with a test anonymous ID:

```text
https://keyboardrichchallenge.com/vipfc-1?ajs_aid=manual-test-anonymous-id
```

Confirm Segment initializes with that same anonymous ID on the destination page:

```text
analytics.user().anonymousId()
```

Expected result:

```text
manual-test-anonymous-id
```

Only continue if this works.

### 3A. Configure R2 Asset Domains

Use one R2 bucket for the ClickFunnels helper script assets.

Recommended bucket:

```text
assets
```

Connect the same bucket to custom domains for each root domain where the script may be hosted:

```text
assets.thebookkeepingchallenge.com
assets.keyboardrichchallenge.com
assets.keyboardrich.com
assets.boomingbookkeeping.com
```

Each domain should point to the same R2 bucket. The object key stays the same, but the browser can load it through the first-party asset host for the current root domain.

Example same object, different first-party hostnames:

```text
https://assets.thebookkeepingchallenge.com/cf-sh-seg
https://assets.keyboardrichchallenge.com/cf-sh-seg
https://assets.keyboardrich.com/cf-sh-seg
https://assets.boomingbookkeeping.com/cf-sh-seg
```

Publish the ClickFunnels helper once:

```sh
npm run publish:clickfunnels -- --bucket assets
```

Use `--public-url` only to control what URL is printed after publishing:

```sh
npm run publish:clickfunnels -- --bucket assets --public-url=https://assets.keyboardrich.com
```

### 4. Remove the Native ClickFunnels Redirect Last

Only after the ActiveCampaign native redirect is verified should the ClickFunnels-native redirect be removed or disabled.

This avoids changing too many moving parts at once.

Final intended submit flow:

```text
ClickFunnels popup form
  -> ActiveCampaign proc.php
  -> keyboardrichchallenge.com/vipfc?ajs_aid=<id>&vgo_ee=<token>
  -> keyboardrichchallenge.com/vipfc-1?ajs_aid=<id>&vgo_ee=<token>
  -> ActiveCampaign strips vgo_ee
  -> final visible URL keeps ajs_aid
```

## ActiveCampaign Setup

### 1. Create a Custom Contact Field

In ActiveCampaign, create a custom contact field:

```text
Segment Anonymous ID
```

Recommended field type:

```text
Text input
```

After creating it, find its personalization tag. It will look similar to:

```text
%SEGMENT_ANON_ID%
```

Use the actual tag ActiveCampaign gives you.

Reference: ActiveCampaign supports custom contact fields and makes them available as personalization tags.

https://help.activecampaign.com/hc/en-us/articles/221433307-Custom-contact-field-overview

### 2. Add the Field to the ActiveCampaign Form as Hidden

Edit the ActiveCampaign form used by the ClickFunnels popup.

Add the custom field:

```text
Segment Anonymous ID
```

Set it as a hidden field.

Reference: ActiveCampaign hidden fields can pass information into the contact record when the form is submitted.

https://help.activecampaign.com/hc/en-us/articles/115000856864-How-do-I-add-a-hidden-field-to-my-form

### 3. Make ClickFunnels Submit the Segment Anonymous ID

The ActiveCampaign form submission must include the hidden field value.

The value should be the current visitor's Segment anonymous ID.

Expected submitted value:

```text
field[%ACTIVE_CAMPAIGN_FIELD_ID%]=<segment-anonymous-id>
```

or whatever field name ActiveCampaign assigns to the custom hidden field in the generated form HTML.

Important: ActiveCampaign can only put this value into the redirect URL if the value was submitted into ActiveCampaign first.

### 4. Set ActiveCampaign's Form Redirect URL

In the ActiveCampaign form settings, set the submit action to open a URL.

Use this redirect URL:

```text
https://keyboardrichchallenge.com/vipfc?ajs_aid=%SEGMENT_ANON_ID%
```

Replace `%SEGMENT_ANON_ID%` with the exact personalization tag ActiveCampaign gives the custom field.

ActiveCampaign documents this pattern for redirecting/pre-filling with query parameters such as:

```text
?email=%EMAIL%
?firstname=%FIRSTNAME%&email=%EMAIL%
```

References:

https://help.activecampaign.com/hc/en-us/articles/115001559664--Video-How-do-I-create-a-multi-page-form

https://help.activecampaign.com/hc/en-us/articles/360020748159-How-to-pre-fill-fields-on-your-ActiveCampaign-form

## Expected Redirect Chain

Expected behavior after form submit:

```text
POST https://boomingbookkeeping.activehosted.com/proc.php
  -> 302 https://keyboardrichchallenge.com/vipfc?ajs_aid=<id>&vgo_ee=<activecampaign-token>
  -> 302 https://keyboardrichchallenge.com/vipfc-1?ajs_aid=<id>&vgo_ee=<activecampaign-token>
  -> page loads
  -> ActiveCampaign removes vgo_ee from visible URL
  -> final visible URL keeps ajs_aid
```

Expected final visible URL:

```text
https://keyboardrichchallenge.com/vipfc-1?ajs_aid=<id>
```

## Test Checklist

### 1. Submit a Test Lead

Use a new test email.

Example:

```text
ac-segment-test+001@example.com
```

### 2. Inspect the ActiveCampaign POST

In browser DevTools, find:

```text
https://boomingbookkeeping.activehosted.com/proc.php
```

Confirm the POST payload includes the Segment anonymous ID custom field.

If the hidden field is missing or blank, fix the ClickFunnels form before testing the redirect.

### 3. Inspect the ActiveCampaign 302 Response

Check the response header:

```text
Location
```

It should include:

```text
ajs_aid=<segment-anonymous-id>
```

It may also include:

```text
vgo_ee=<activecampaign-token>
```

That is normal.

### 4. Inspect the Final Destination URL

After the page loads, the final visible URL should still include:

```text
ajs_aid=<segment-anonymous-id>
```

If `vgo_ee` disappears, that is expected. ActiveCampaign's tracking script removes it.

### 5. Confirm Segment Uses the ID

On the destination page, confirm Segment initializes with the same anonymous ID.

Expected result:

```text
analytics.user().anonymousId() === <segment-anonymous-id-from-source-domain>
```

## Failure Cases

### Failure: ActiveCampaign Redirect Shows the Literal Tag

Bad redirect:

```text
https://keyboardrichchallenge.com/vipfc?ajs_aid=%SEGMENT_ANON_ID%
```

Meaning: ActiveCampaign did not substitute the custom field in the redirect URL.

Fixes:

1. Confirm the personalization tag is exactly correct.
2. Confirm the custom field exists on the contact.
3. Confirm the form submission includes a non-empty value for the field.

### Failure: ActiveCampaign Redirect Has Blank `ajs_aid`

Bad redirect:

```text
https://keyboardrichchallenge.com/vipfc?ajs_aid=
```

Meaning: ActiveCampaign substituted the field, but the field value was blank.

Fix ClickFunnels or the hidden field injection so the anonymous ID is submitted into ActiveCampaign.

### Failure: Final URL Loses `ajs_aid`

This should not happen based on the live test.

If it does happen, inspect which script calls:

```text
history.replaceState
```

The known ActiveCampaign cleanup removes only `vgo_ee`, not `ajs_aid`.

## Decision Rule

Use ActiveCampaign native redirect only. The redirect is correct when:

```text
ActiveCampaign 302 Location contains ajs_aid=<real-id>
```
