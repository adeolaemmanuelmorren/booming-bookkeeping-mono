const MAX_BATCH_ID_LENGTH = 200;

export function recoveredIdentityBatchId(
  currentBatchId: string,
  nowMs: number,
): string {
  const suffix = `_retry${nowMs}`;
  const prefixLength = MAX_BATCH_ID_LENGTH - suffix.length;
  return `${currentBatchId.slice(0, prefixLength)}${suffix}`;
}
