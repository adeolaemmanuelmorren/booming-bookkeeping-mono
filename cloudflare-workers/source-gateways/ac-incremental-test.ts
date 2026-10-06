// Fixed diagnostic only: never accepts customer IDs, tags, or arbitrary API paths.
export async function testContactTagIncremental(baseUrl: string, token: string) {
  const api = async (path: string, method = "GET", body?: unknown) => {
    const response = await fetch(`${baseUrl.replace(/\/$/, "")}/api/3${path}`, {
      method,
      headers: { "Api-Token": token, "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) {
      const detail = await response.json().catch(() => ({})) as { message?: string; errors?: { title?: string; detail?: string }[] };
      const reason = detail.message ?? detail.errors?.map(error => error.title ?? error.detail ?? "").join("; ") ?? "";
      throw new Error(`ActiveCampaign ${method} ${path.split("?")[0]}: ${response.status} ${reason.slice(0, 300)}`);
    }
    if (response.status === 204) return {};
    const text = await response.text();
    return text ? JSON.parse(text) : {};
  };
  const pause = () => new Promise(resolve => setTimeout(resolve, 2500));
  const runId = crypto.randomUUID();
  let contactId: string | undefined;
  let tagId: string | undefined;
  const result: Record<string, unknown> = { runId, checks: [], cleanup: {} };
  try {
    const created = await api("/contacts", "POST", { contact: {
      email: `codex-incremental-${runId}@example.invalid`, firstName: "Temporary API diagnostic",
    } });
    contactId = String(created.contact.id);
    const tag = await api("/tags", "POST", { tag: { tag: `codex-incremental-${runId}`, tagType: "contact", description: "Temporary incremental sync diagnostic" } });
    tagId = String(tag.tag.id);
    const inspect = async (cutoff: string) => {
      const direct = await api(`/contacts/${contactId}`);
      const parameters = new URLSearchParams({ "ids[]": contactId!, "filters[updated_after]": cutoff, include: "contactTags", limit: "100" });
      const filtered = await api(`/contacts?${parameters}`);
      const tags = await api(`/contacts/${contactId}/contactTags`);
      return { udate: direct.contact.udate, includedByFilter: filtered.contacts?.some((c: { id: string }) => String(c.id) === contactId) ?? false,
        testTagPresent: tags.contactTags?.some((t: { tag: string }) => String(t.tag) === tagId) ?? false };
    };
    await pause();
    const addCutoff = new Date().toISOString();
    const beforeAdd = await inspect(addCutoff);
    const assignment = await api("/contactTags", "POST", { contactTag: { contact: contactId, tag: tagId } });
    await pause();
    const afterAdd = await inspect(addCutoff);
    await pause();
    const removeCutoff = new Date().toISOString();
    const beforeRemove = await inspect(removeCutoff);
    await api(`/contactTags/${assignment.contactTag.id}`, "DELETE");
    await pause();
    const afterRemove = await inspect(removeCutoff);
    await pause();
    const editCutoff = new Date().toISOString();
    await api(`/contacts/${contactId}`, "PUT", { contact: { firstName: "Temporary API diagnostic updated" } });
    await pause();
    const afterEdit = await inspect(editCutoff);
    result.checks = { beforeAdd, afterAdd, beforeRemove, afterRemove, afterEdit };
  } catch (error) {
    result.error = error instanceof Error ? error.message : "Diagnostic failed";
  } finally {
    const cleanup: Record<string, unknown> = {};
    for (const [kind, id] of [["contacts", contactId], ["tags", tagId]]) {
      if (!id) continue;
      try { await api(`/${kind}/${id}`, "DELETE"); cleanup[kind!] = "deleted"; }
      catch { cleanup[kind!] = { status: "failed", id }; }
    }
    result.cleanup = cleanup;
  }
  return result;
}
