type ChunkTable = "browser_outbox_chunks" | "prepared_session_chunks";

// A UTF-16 code unit needs at most 3 UTF-8 bytes. Every row stays below 96 KiB.
const CHUNK_CODE_UNITS = 32_768;

/** Call inside a storage transaction so replacing all chunks is atomic. */
export function writeChunkedJson(storage: DurableObjectStorage, table: ChunkTable, owner: string, value: unknown): void {
  const text = JSON.stringify(value);
  storage.sql.exec(`DELETE FROM ${table} WHERE owner_key = ?`, owner);
  let offset = 0;
  let index = 0;
  while (offset < text.length) {
    let end = Math.min(text.length, offset + CHUNK_CODE_UNITS);
    const last = text.charCodeAt(end - 1);
    if (end < text.length && last >= 0xd800 && last <= 0xdbff) end--;
    storage.sql.exec(`INSERT INTO ${table} VALUES (?, ?, ?)`, owner, index++, text.slice(offset, end));
    offset = end;
  }
}

export function readChunkedJson<T>(storage: DurableObjectStorage, table: ChunkTable, owner: string): T {
  const rows = storage.sql.exec<{ chunk_index: number; payload: string }>(
    `SELECT chunk_index, payload FROM ${table} WHERE owner_key = ? ORDER BY chunk_index`, owner,
  ).toArray();
  if (!rows.length || rows.some((row, index) => row.chunk_index !== index)) throw new Error("Stored JSON chunks are incomplete");
  return JSON.parse(rows.map(row => row.payload).join("")) as T;
}
