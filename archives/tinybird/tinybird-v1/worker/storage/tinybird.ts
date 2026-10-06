export interface TinybirdEnv {
  TINYBIRD_URL: string;
  TINYBIRD_TOKEN: string;
}

export type JsonRecord = Record<string, unknown>;
type Fetcher = typeof fetch;

export interface TinybirdOptions {
  requestTimeoutMs?: number;
  appendMaxBytes?: number;
  appendConcurrency?: number;
}

/** Bounded requests, redacted failures, and explicit rejection of quarantined rows. */
export class Tinybird {
  private env: TinybirdEnv;
  private fetcher: Fetcher;
  private options: Required<TinybirdOptions>;

  constructor(env: TinybirdEnv, fetcher: Fetcher = (input, init) => fetch(input, init), options: TinybirdOptions = {}) {
    this.env = env;
    this.fetcher = fetcher;
    this.options = { requestTimeoutMs: options.requestTimeoutMs ?? 20_000, appendMaxBytes: options.appendMaxBytes ?? 900_000,
      appendConcurrency: options.appendConcurrency ?? 1 };
    if (!Number.isSafeInteger(this.options.requestTimeoutMs) || this.options.requestTimeoutMs < 1 || this.options.requestTimeoutMs > 60_000
      || !Number.isSafeInteger(this.options.appendMaxBytes) || this.options.appendMaxBytes < 1 || this.options.appendMaxBytes > 8_000_000
      || !Number.isSafeInteger(this.options.appendConcurrency) || this.options.appendConcurrency < 1 || this.options.appendConcurrency > 4) {
      throw new Error('Invalid Tinybird transport limits');
    }
  }

  async query<T>(sql: string): Promise<T[]> {
    const response = await this.request('/v0/sql', {
      method: 'POST',
      body: new URLSearchParams({ q: `${sql}\nFORMAT JSON` }),
    });
    const result = await response.json() as { data?: T[]; error?: string };
    if (!Array.isArray(result.data) || result.error) throw new Error('Invalid Tinybird query response');
    return result.data;
  }

  async append(table: string, rows: readonly JsonRecord[]): Promise<void> {
    if (!/^v1_[a-z0-9_]+$/.test(table) && !['identifiers', 'profiles', 'profile_redirects'].includes(table)) throw new Error('Unexpected V1 table name');
    const send = async (body: string) => {
      const response = await this.request(`/v0/events?name=${table}&wait=true`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-ndjson' },
        body,
      });
      const result = await response.json() as { successful_rows?: number; quarantined_rows?: number; error?: string };
      const expected = body.split('\n').length - 1;
      if (result.error || result.quarantined_rows !== 0 || result.successful_rows !== expected) {
        throw new Error('Tinybird did not accept every publication row');
      }
    };
    let pending: Promise<void>[] = [];
    const drain = async () => {
      const results = await Promise.allSettled(pending);
      pending = [];
      const failure = results.find(result => result.status === 'rejected');
      if (failure?.status === 'rejected') throw failure.reason;
    };
    try {
      for (const body of ndjsonBatches(rows, this.options.appendMaxBytes)) {
        pending.push(send(body));
        if (pending.length === this.options.appendConcurrency) await drain();
      }
      await drain();
    } catch (error) {
      await Promise.allSettled(pending);
      throw error;
    }
  }

  private async request(path: string, options: RequestInit): Promise<Response> {
    if (this.env.TINYBIRD_URL !== 'https://api.us-east.tinybird.co') {
      throw new Error('Unexpected Tinybird region');
    }
    const response = await this.fetcher(new URL(path, this.env.TINYBIRD_URL), {
      ...options,
      headers: { ...options.headers, Authorization: `Bearer ${this.env.TINYBIRD_TOKEN}` },
      signal: AbortSignal.timeout(this.options.requestTimeoutMs),
      redirect: 'manual',
    });
    if (response.ok) return response;
    await response.body?.cancel();
    throw new Error(`Tinybird request failed with HTTP ${response.status}`);
  }
}

export function sqlString(value: string): string {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}

export function sqlStrings(values: readonly string[]): string {
  if (!values.length) throw new Error('Empty key list');
  return values.map(sqlString).join(',');
}

/** Tinybird's JSON parser requires a JSON number for UInt64 columns. */
export function uint64Number(value: string | number): number {
  if (typeof value === 'string' && !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error('Invalid unsigned publication integer');
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new Error('Publication integer exceeds exact JSON number range');
  }
  return number;
}

/** A successful ingest can precede query visibility on another replica. */
export async function waitForReadback<T>(
  read: () => Promise<T>,
  isComplete: (value: T) => boolean,
  delays = [0, 200, 500, 1_000, 2_000, 4_000, 8_000, 8_000],
): Promise<T> {
  for (const delay of delays) {
    if (delay) await new Promise(resolve => setTimeout(resolve, delay));
    const value = await read();
    if (isComplete(value)) return value;
  }
  throw new Error('Tinybird publication is not yet fully visible');
}

export function* ndjsonBatches(rows: readonly JsonRecord[], maxBytes = 900_000): Generator<string> {
  const encoder = new TextEncoder();
  let body = '';
  let bytes = 0;
  for (const row of rows) {
    const line = JSON.stringify(row) + '\n';
    const size = encoder.encode(line).byteLength;
    if (size > maxBytes) throw new Error('A publication row exceeds the upload limit');
    if (bytes + size > maxBytes) {
      yield body;
      body = '';
      bytes = 0;
    }
    body += line;
    bytes += size;
  }
  if (body) yield body;
}
