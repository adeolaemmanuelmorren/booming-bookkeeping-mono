Nice speaking with you today, Adeola. I wanted to include the below information (some of which we discussed on the call) to give you as much context as possible.

Here's an overview of the funnel so you have an idea of how people flow through it and the conversions we track.

We drive paid traffic to: **thebookkeepingchallenge.com**. That's where people register for a free 5-day challenge. We run the challenge every week. Once they register, the next page in the funnel is **keyboardrichchallenge.com/vipfc** (note it is a different domain...long story!). On that page, they can upgrade to VIP status during the challenge for $47 that gives them some bonuses. If they upgrade, they go to a thank you page at: **keyboardrichchallenge.com/vipsteps**. If they do not upgrade, they go to a thank you page at: **keyboardrichchallenge.com/nextstepsfc**.

If they don't upgrade their first time registering, prior to the challenge starting (and through Tuesday of their challenge week), we encourage them through emails to upgrade to VIP. Since we run this every week, we need to have 2 different VIP upgrade pages that we alternate every other week (and we can't send them to the VIP upgrade page above as that one will reset to the next week's challenge during this process).

VIP Upgrade A: **keyboardrichchallenge.com/upgrade**

VIP Upgrade A Thank You Page: **keyboardrichchallenge.com/vipconfirmation**

VIP Upgrade B: **keyboardrichchallenge.com/vipupgrade**

VIP Upgrade B Thank You Page: **keyboardrichchallenge.com/vipsuccess**

There is another VIP Upgrade page that exists and is used (again...long story) that folks can upgrade on Sunday-Tuesday of their challenge week. It is: **keyboardrichchallenge.com/vip**. The thank you page for that is: **keyboardrichchallenge.com/vip-thanks**.

The above came to be from the evolution of a live monthly challenge to an automated challenge that runs every week. Things are running smoothly and it is working very well.

On Thursday of the challenge, we offer folks the opportunity to join our full mentorship program. We direct them to: **keyboardrich.com/yes** (note the new domain). On that page they make a deposit of $997. Once that payment is processed, they are redirected to a Docusign where the electronically sign the agreement. Once that is signed, they are automatically redirected to a page to make the balance of their payment: **boomingbookkeeping.com/go** (note the new domain).

As you can imagine, the mentorship program purchase is the most valuable event for us, followed by the VIP upgrade. The initial registration is an important step but since it is free, it is not nearly as important as the other 2 and we do not optimize for that.

The funnel pages are built with Clickfunnels and we use Stripe for checkout. Our CRM is ActiveCampaign. We currently use HYROS to look at ad performance. We do not have GA4 setup. We do have GTM but the tags on both workspaces aren't being used so it might make sense to start from scratch. There is also a GTM that was created by our ads agency on their own account so I don't have much visibility there. But I believe they are using that to incorporate **Stape.io** into the mix for CAPI. Not sure if they are doing the same with Google conversions api but it doesn't look like it. I would like to take ownership of that.

Let me know what else you need from me regarding information, access, etc.

've given access to segment and google.A few notes on the summary...One thing I should have mentioned that we may want to work in here is that once the "cart has closed" on Sunday night to purchase the mentorship program at the $4997 or 3 payments of $1997 price, on Monday and Tuesday we offer a subscription option for the same program...$199 per month. That purchase is made on our Kajabi site (https://learn.boomingbookkeeping.com/offers/v3WtGzPH/checkout) and it uses a separate Stripe account.You mention HYROS only linked to t.boomingbookkeeping.com. The HYROS pixel is installed on all domains in the journey.There is a test pixel (BBB test) that was setup by the current agency and is actually owned by that agency. Not sure if that is even being used. BBB Pixel is ours and we should be using that. Please remember that Core Setup is on for that pixel and I don't see how to turn it off or if it is even possible. I believe it is limiting some of the data that can be passed back, including any part of a URL after the domain name (which is bad for custom conversions that use URL rules) and I'm told even content values. Would like to get Core Setup turned off (my rep hasn't been very helpful with that) but if not, hopefully the work you're doing will work around that.An attempt had been made to setup Google enhanced conversions but I don't believe that is working properly.How do we handle folks who make payments for the mentorship program in non-standard ways? It doesn't happen often but maybe a few times per week. For example, some folks reach out to us to just pay the full $4997 at once. Some split the $4997 in 2. Some split the $997 between multiple credit cards. Things like that.That is all I have after reviewing the doc. Let me know if you need anything else from me.

Adeola Morren  [3:14 PM]

Perfect received the access to both!Regarding Hyros; yep its installed on all domains, but currently it’s only proxied through that one domain, which mean that the first party domain tracking benefits of hyros only apply through that main domain. I should’ve clarified that better.Ah ok regarding the kajabi that’s good to know.  Could you clarify how that transition process works?So let’s say I’ a potential customer I filled out the challenge, I didnt purchase in time Sunday strikes. How do I arrive at that Kajabi checkout page?Noted regarding the BBB test pixel.Regarding core set up; I just checked and I don’t think there is an easy way to disable it. It looks like meta has automatically applied this because it can be classified as a sensitive industry (financial services)**_However,_** i was already planning to work around this and we won’t be limited by this in the new set up.Yes the enhanced conversions will be properly set up 🙂Regarding non standard ways; how do these folks make the payment? Do you send them a direct stripe invoice? If we could make sure that we send those with the same email address or phone number, we can easily integrate that with the system.Would that work?

Bill Von Fumetti  [7:06 PM]

Regarding the subscription...if you went through the challenge and didn't purchase the lifetime access option ($4997 or 3 monthly payments of $1997), then you get one email on Monday and 2 emails on Tuesday (following your challenge week) promoting the subscription option. Those emails direct people to: https://www.boomingbookkeeping.com/monthly. That is a Clickfunnels page. When they click a the "Click here to join now" button on that page, they go to the subscription signup page on Kajabi mentioned above: https://learn.boomingbookkeeping.com/offers/v3WtGzPH/checkoutRegarding non-standard payments for the $4997...we usually send them what's called a "payment link" in stripe that has an amount field that they can enter manually and pay.You mention using the same email address...it should be noted that for the most part, folks use the same email address for free registration as well as purchases like VIP or BBB (that's what I call Booming Bookkeeping Business mentorship program purchases) purchases. But we get plenty of folks that use one address to sign up for the free challenge (perhaps they aren't sure if it is legit or spam) and then another email address when they upgrade to VIP or purchase the mentorship program. So hopefully we can account for that. Let me know if there's anything we can try to do on our end to keep things tight. We don't have control over VIP or BBB purchases through our normal flow (hopefully there's something you can do there), but for non-standard BBB purchases like I described, we can try to keep tracking intact if you let us know how best to do that.

boomingbookkeeping.com

Join Booming Bookkeeping Business

Together, we will start and grow your bookkeeping business that provides the income and freedom you want!

Adeola Morren  [8:53 AM]

**@Bill Von Fumetti** Got your notes and that all makes sense.

1. People sign up for challenge with Email A, they make the purchases with the same email. This scenario will be fully covered by the tracking set up.
2. People sign up for challenge with Email A , they make purchase with Email B on one of the pages hosted on your funnels. This scenario will also be fully accounted for, because all of it happens on the browser on domains that we can control.
3. A person signs up for the challenge with Email A. Then one of your staff manually **sends out a stripe payment link.** If the person does not use the same email address on this purchase, it’s technically impossible to know that this person is the same as the person that signed up for the challenge. However, stripe does allow you to automatically prefill the email address through the link that you sent to the person. So would we be able to have your staff do this? I can show you how to do this. I assume that you would indeed have the challenge email address of the person you would be sending the stripe link to, or would this **_not_** be the case?

Lastly inside of the Docusign we have discussed tracking that with this system as well. Currently how does this process work of sending out those docusign contracts? Is this fully automated through Zapier, or does your staff currently manually send those out?

Bill Von Fumetti  [10:08 AM]

Got it on 1 & 2. Regarding #3...yes we can do that (as we have the email address) but can you show us the best way to do it?

[10:12 AM]

For Docusign, when the user makes the $997 deposit on keyboardrich.com/yes, on submit there is a redirect to a docusign powerform which is essentially a URL that loads the contract ready to be signed in the browser. The powerform address is: https://na4.docusign.net/Member/PowerFormSigning.aspx?PowerFormId=98b8e908-8050-40e7-9a3e-f70855b0e63d&env=na4&acct=836602eb-79b0-4cd2-94aa-37b04ec7d656&v=2.The settings on that powerform redirect back to boomingbookkeeping.com/go when it is signed so they can make the balance of their payment.

Adeola Morren  [2:15 PM]

**@Bill Von Fumetti** could I get access to the settings page in Kajabi? Just need to place our segment tracking script there or I can also give you the specific script if it helps

Bill Von Fumetti  [2:41 PM]

Do you want to send me the script and let me know where to put it?

Adeola Morren  [2:46 PM]

Yep here it is:

```
<script src="https://assets.boomingbookkeeping.com/cf-sh-seg"></script>
```

And this link shows where to place it:https://help.kajabi.com/articles/sales/checkout/how-to-add-a-javascript-tracking-code-to-your-checkout-pages

Kajabi

Add a JavaScript tracking code to checkout pages - Kajabi

Add tracking codes like Google Analytics to checkout pages to monitor customer behavior.

Bill Von Fumetti  [5:47 PM]

**@Adeola Morren** This has been done. Let me know if you see it or if there are any issues with it!

For the field, I can create that and add it to the form. That form is integrated in a lot of different spots so I want to be careful with it. Just let me know what you'd like me to name it. And it can be hidden, correct?

On that note, you've probably seen that one can register for the challenge in 3 funnels...

the first funnel step in the "5-Day Keyboard Rich Challenge" funnel. This is [keyboardrichchallenge.com](http://keyboardrichchallenge.com). Only organic traffic here. We run the challenge every week so to do that, we use both the control and variation on that funnel step NOT for split testing. One has the current week's dates on it to sign up for this week's challenge, and the other has next week's. Every Monday at around 10am PT we slide the slider to direct 100% of the traffic to next week's challenge. And then we update the dates to the week after next on the one that was registering for this week's challenge. So these pages are identical except for the dates shown. We also change the tag added in the form above at around 10am PT as well so folks get sent into the right automation. Nothing you need to do here just wanted you to know we use both the control and variation on alternating weeks.
KRC - FB Compliant - This is [thebookkeepingchallenge.com](http://thebookkeepingchallenge.com) where we drive paid traffic. The first step is Meta & Google and the second funnel step is Taboola (since they have different requirements on buttons, etc). Like above, we use both the control and variation of both steps and swap back and forth alternating weeks.
Keyboard Rich Book Funnel - This is [keyboardrich.com](http://keyboardrich.com). No paid traffic at the moment. But once they order the book on the 1st step, the second step is an upsell page to get a VIP ticket to the challenge. So this is a free registration PLUS VIP sale. Again, we use both the control and variation on this second funnel step.

Hi [**@Adeola Morren**](https://fortunemastersgroup.slack.com/team/UTKBH54MT)...just got back from out of town so I'll have to handle cloudflare and the AC field tomorrow. There is one other AC form in use but it doesn't redirect users to a different domain. It is in the Booming Bookkeeping Business - Direct funnel ([boomingbookkeeping.com](http://boomingbookkeeping.com) domain). We don't run paid traffic to this funnel but it does get organic traffic. It is a free webinar that promotes the challenge. So while it doesn't get much traffic, folks who watch the webinar and then join the challenge do jump from [boomingbookkeeping.com](http://boomingbookkeeping.com) to the challenge opt-in page at [keyboardrichchallenge.com](http://keyboardrichchallenge.com). Let me know if we need to add the segment field to this form.

ctually, they click a button and then go to the challenge registration page on [keyboardrichchallenge.com](http://keyboardrichchallenge.com) to register again so on second thought, I don't think there's anything needed there.

But for the book funnel ([keyboardrich.com](http://keyboardrich.com))...step 1 - they order the book. Then the next page in the funnel is a 1-click upsell for a VIP ticket to the challenge. So they don't enter their information again or fill out the ActiveCampaign embedded form. Maybe we need something there?

is one more message he sent so update the list with pages where we collect customer info?
