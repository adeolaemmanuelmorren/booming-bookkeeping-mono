// Explicitly limit the remote fetcher's access to source state operations.
export const FETCHER_STORE_METHODS = [
  "getCursor", "setCursor", "putInboxIfAbsent", "listPendingInbox", "saveInboxProgress",
  "markInboxOutboxed", "markInboxProcessed", "nextSequence", "putOutboxIfAbsent",
  "listPendingOutbox", "commitPublishedOutbox", "getState", "getPublishedRows",
] as const;
