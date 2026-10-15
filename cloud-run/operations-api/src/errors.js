export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export function integer(value, fallback, max = 200) {
  if (value === null || value === undefined) return fallback;
  if (!/^\d+$/.test(String(value))) throw new ApiError(400, "Expected a positive integer.");
  const number = Number(value);
  if (number < 1 || number > max) throw new ApiError(400, `Value must be between 1 and ${max}.`);
  return number;
}

export function identifier(value) {
  if (!/^[a-zA-Z0-9_-]{1,200}$/.test(value)) throw new ApiError(400, "Invalid resource ID.");
  return value;
}

// Upstream configuration and logs can contain credentials. Always redact before returning them.
export function redact(value, secrets = []) {
  if (Array.isArray(value)) return value.map(item => redact(item, secrets));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [
      key,
      !/^(next)?pageToken$/i.test(key) &&
      /secret|token|password|authorization|private.?key|write.?key|credentials|connection.?string/i.test(key)
        ? "[redacted]" : redact(item, secrets),
    ]));
  }
  if (typeof value !== "string") return value;
  let result = value;
  for (const secret of secrets.filter(Boolean)) result = result.split(secret).join("[redacted]");
  return result.replace(/([?&](?:access_token|api_key|key|token)=)[^&\s"']+/gi, "$1[redacted]");
}
