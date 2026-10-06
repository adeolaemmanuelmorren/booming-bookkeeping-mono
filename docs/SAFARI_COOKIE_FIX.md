# Safari cookie fix + SSL watchdog — quick context

**Paste this into any Claude session to restore full context.**

## What we changed
Safari (ITP) was capping our tracking cookies (`__eventn_id` etc., set by the `sg.*` Cloudflare Worker) to **7 days**, because the apex (ClickFunnels' IPs) and `sg.*` (our Cloudflare zone's IPs) resolved to different infrastructure. Fix: flipped the apex DNS records to **Cloudflare-proxied (orange cloud)** — thebookkeepingchallenge.com + keyboardrichchallenge.com on 2026-07-18, keyboardrich.com + boomingbookkeeping.com by 2026-08-11. All four apexes now share their `sg.*` host's Cloudflare IPs → Safari accepts the full ~5-year cookie lifetime. This works via Cloudflare **orange-to-orange (O2O)**: our zone in front of ClickFunnels' Cloudflare-for-SaaS.

## SSL renewal risk — RESOLVED
The one risk of proxying was ClickFunnels' ~90-day cert auto-renewal (HTTP validation) failing behind the proxy. Two things closed it:

1. **Empirical proof:** keyboardrichchallenge.com renewed successfully **while proxied** on 2026-08-02 (new cert valid to Oct 31, 2026), before any special records existed.
2. **DCV delegation (2026-08-05 → 08-10):** ClickFunnels support (Vikram/Axel) provided `_acme-challenge.<domain>` CNAMEs → `<domain>.497bf50d70872bbf.dcv.cloudflare.com` for all four domains; all four are added and verified resolving (2026-08-11). Cert validation now happens via DNS, independent of the proxy, and ClickFunnels confirmed the records can stay proxied. (Note: their first email had a `__acme-challenge` double-underscore typo; the correct single-underscore form is what's live.)

Cert state as of 2026-08-11: TBC → Sep 15, boomingbookkeeping → Sep 19, keyboardrich → Oct 5, keyboardrichchallenge → Oct 31. Next natural renewal ~Aug 16 (TBC).

## The watchdog
Daily cloud routine ("SSL renewal watchdog — ClickFunnels domains", 9am HST) checks all 4 apexes + all 4 `sg.*` hosts: site up, TLS valid, **days remaining on each cert**. Pushes a notification ONLY if a site is down, TLS fails, or a cert drops under **21 days** without renewing. Manage at claude.ai/code/routines.

**Known gotchas:** (a) the routine's cloud environment must have network access to these domains — if its egress allowlist blocks them the routine pushes a "watchdog can't run" alert instead of health data; fix in the environment's network settings on claude.ai. (b) Plain `curl` gets a bot-protection **403** from the apexes; checks must send a browser User-Agent (the routine prompt includes one).

## If it alerts on a real cert problem
In Cloudflare DNS, flip the affected apex back to **DNS-only (grey cloud)** — site restores in ~1 minute — and contact ClickFunnels support. Then investigate before re-proxying.
