import { ApiError, integer } from "./errors.js";

export function createMetaProvider({ accessToken, accountId = "10151088695453802", version = "v25.0", fetchImpl = fetch }) {
  async function get(path, params = {}) {
    if (!accessToken) throw new ApiError(503, "Meta access token is not configured.");
    const url = new URL(`https://graph.facebook.com/${version}/${path}`);
    for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
    const response = await fetchImpl(url, {
      headers: { Authorization: `Bearer ${accessToken}` }, signal: AbortSignal.timeout(25_000),
    });
    const body = await response.json();
    if (!response.ok || body.error) throw new ApiError(502, `Meta request failed with HTTP ${response.status}, code ${body.error?.code ?? "unknown"}.`);
    // Meta's next URLs can embed the access token. Return only an opaque cursor.
    return { ...body, paging: body.paging ? { cursors: body.paging.cursors, hasMore: !!body.paging.next } : undefined };
  }
  const account = () => get(`act_${accountId}`, { fields: "id,name,account_status,currency,timezone_name,disable_reason" });
  return {
    account,
    async insights(params) {
      const today = new Intl.DateTimeFormat("en-CA", { timeZone: "America/Los_Angeles" }).format(new Date());
      const since = params.get("since") ?? today;
      const until = params.get("until") ?? since;
      const validDate = date => /^\d{4}-\d{2}-\d{2}$/.test(date) && Number.isFinite(Date.parse(date)) &&
        new Date(date).toISOString().slice(0, 10) === date;
      if (!validDate(since) || !validDate(until)) throw new ApiError(400, "Use valid YYYY-MM-DD dates.");
      const days = (Date.parse(until) - Date.parse(since)) / 86_400_000;
      if (days < 0 || days > 30) throw new ApiError(400, "Date range must contain 1 to 31 days.");
      const level = params.get("level") ?? "account";
      if (!["account", "campaign", "adset", "ad"].includes(level)) throw new ApiError(400, "Invalid insights level.");
      const fields = ["account_id", "account_name", "impressions", "clicks", "spend", "reach", "actions", "action_values"];
      if (level !== "account") fields.push("campaign_id", "campaign_name");
      if (["adset", "ad"].includes(level)) fields.push("adset_id", "adset_name");
      if (level === "ad") fields.push("ad_id", "ad_name");
      return get(`act_${accountId}/insights`, {
        fields: fields.join(","), level, time_increment: "1",
        time_range: JSON.stringify({ since, until }), limit: integer(params.get("limit"), 50),
        ...(params.get("after") ? { after: params.get("after") } : {}),
      });
    },
    async status() {
      const data = await account();
      return { status: data.account_status === 1 ? "ok" : "degraded", account: data,
        note: "Token and account access verified. Insights reads verify delivery metrics." };
    },
  };
}
