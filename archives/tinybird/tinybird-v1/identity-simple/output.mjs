export async function appendOutput(connection, table, rows, fetcher = fetch) {
  if (!["identifiers", "profiles", "profile_redirects"].includes(table))
    throw new Error("Unexpected identity output table");
  if (connection.host !== "https://api.us-east.tinybird.co")
    throw new Error("Wrong output region");
  const body = rows.map((row) => JSON.stringify(row)).join("\n") + "\n";
  if (Buffer.byteLength(body) > 1100000)
    throw new Error("Identity output batch is too large");
  const response = await fetcher(
    `${connection.host}/v0/events?name=${table}&wait=true`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${connection.token}`,
        "Content-Type": "application/x-ndjson",
      },
      body,
      signal: AbortSignal.timeout(60000),
      redirect: "manual",
    },
  );
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Identity output HTTP ${response.status}`);
  }
  const result = await response.json();
  if (
    result.successful_rows !== rows.length ||
    result.quarantined_rows !== 0 ||
    result.error
  )
    throw new Error("Identity output was not fully accepted");
}
