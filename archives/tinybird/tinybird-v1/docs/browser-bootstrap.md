# Browser bootstrap runner

The one-time Node runner restores browser facts from finite Tinybird raw landing tables, builds complete visitor sessions in bulk, initializes complete identity components, and seals a baseline for lazy live hydration. No BigQuery/Dataform runtime, attribution, ads, local raw files, or visitor Durable Objects are involved.

Run `scripts/bootstrap.ts` in the approved US East job. Start with four CPUs, 16 GiB process memory, and an 8 GiB Node heap. This is a sizing recommendation; the full historical run has not measured peak heap yet. The largest measured visitor has 1,213 page views. The process retains compact source references or identity keys during their respective phases, never the whole raw history. Those indexes can require several GiB for millions of keys.

## Configuration and integration

`BootstrapConfig` in `worker/bootstrap/executor.ts` is the complete metadata contract. `scripts/bootstrap-config.ts` builds it from the frozen history summary, object manifest, and explicit settings. Settings choose the actual landing table and physical original-ID column for every source, workspace identity, tenant, baseline ID, fixed start timestamp, region, import inventory mode, and memory/batch limits. The optional live input pins its table, ingestion cutoff, and physical row count.

The runner requires `BOOTSTRAP_CONFIG_PATH`, `BOOTSTRAP_RUNNER_REGION`, `BOOTSTRAP_SINGLE_TASK=1`, `TINYBIRD_URL`, and `TINYBIRD_TOKEN`. Credentials remain in memory. It verifies `/v1/workspace` against the configured ID and name before writing. `Dockerfile.bootstrap` supplies Node 22 and starts the same entrypoint. Keep the exact configuration on retries. One supervised task owns a baseline; checkpoints are not a distributed lock.

Integrate `worker/bootstrap/`, the eight bootstrap datasource schemas, the three bootstrap scripts, `Dockerfile.bootstrap`, and the bootstrap tests/helpers. Keep current main versions of browser/session/identity dependencies. Identity storage needs only the small publication-readback pagination patch and Set lookup; do not replace unrelated main changes. Existing `TinybirdSessionPublisher.publishSnapshots`, `TinybirdBrowserGroupPublisher`, and `IdentityStorage.publish` own the actual session/group/identity wire formats.

The history proof is in `tinybird-v1/evidence/history-recoverability`. Native landing inputs must match the chosen generation/checksum inventory before this runner starts. A wildcard is not an immutable manifest. Provider histories are separate from this browser bootstrap; their direct ingestion merges through the live identity engine.

## Work and retry cost

Raw input paging first chooses at most `pageSize + 1` original IDs using the physical first sorting key. The wide row/hash query then reads only that ID range, ordered by the same physical key, source revision, and full raw-row hash. It preserves all raw schema fields, exact microseconds, all five browser kinds, missing event times, future browser clocks, source priority, and tombstones. One initial cardinality query per source checks both physical rows and distinct raw rows. A short raw scan resets its cursor and replays the same immutable normalized facts.

Before publishing selected heads, the runner scans the complete source input into compact key/hash references and checks its count against the verified upstream receipt. A short scan discards those references and retries. Payloads are then fetched by bounded known keys and exact hashes. Logical selection follows the same rule. Partial source visibility cannot publish an older head or lower-priority source as the final choice.

Before session publication, a complete page-head pass builds a compact visitor count/hash index. Its total must equal the selected page and visitor totals. Each subsequent batch fetches those known visitors and verifies its complete heads against the independent index. Session IDs and session behavior come from the existing session library. There are bulk writes per chunk, not per-visitor calls or DO creation.

Identity first builds the complete compact component index. It drives payload reads from its known component keys and expected fact counts. Components are never split. Fetch batches normally contain at most 2,000 facts or 100 components; one larger complete component stays intact and is bounded by configuration. The exact existing identity engine creates the rows.

No normal stage page computes a full-history scalar count. For comparison, counting all 3.17 million page heads on every 500-row page would visit roughly 20.1 billion rows; doing that for nine million identity rows would visit 162 billion. This implementation makes a few linear passes and uses same-query coverage only for bounded visitor/component/key sets. The 200-visitor batch usually contains a few hundred heads. A large component increases only its own bounded read cost. Failed compact-index attempts repeat one linear pass. Verified output checkpoints avoid republishing completed source/session/identity work after a process restart.

## Authenticated absence

The seal includes `membership: MembershipSeal` from `worker/bootstrap/membership.ts`. It authenticates 64 descriptor-page hashes for source keys and 64 for visitor keys. Each index has 16,384 buckets. A descriptor authenticates the count and hash of a complete bucket list. A zero-count bucket can prove absence directly. For nonempty buckets, the reader must fetch and verify the entire list before deciding whether a key is absent.

Runtime lookups read descriptor pages and bucket lists in batches. At most 16 bucket lists are retained at once, each capped at 128 KB. Source lookups accept 200 keys; member lookups internally split the router's 500-key read plans into 200-key proof batches. An absent proof row waits or fails. An absent member/head cannot mean a new key merely because a seal was visible. Returned head/session payloads also require the saved per-key count/hash and exact publication content.

Membership construction reads only key metadata. It verifies a complete key index before publishing bucket descriptors. The final seal is written after all source, session, identity, and membership proofs verify. Bootstrap history and proof tables have no TTL. BrowserRouter starts above `finalSessionSequence`; identity starts above `identityVersion = 1`. Old seals without membership proofs are rejected.

## Validation and remaining checks

Twenty-four local tests cover the concrete HTTP/NDJSON transport, every phase, ambiguous source/checkpoint/member/session/identity acknowledgements, source precedence, exact microseconds, null-time pages, whole-graph identity parity, short replica scans, missing proof rows, false-negative prevention, 200-key negative lookups, numeric cursor order, and schema field/numeric wire parity. Strict TypeScript checking passes.

Root completed the earlier real validation-branch smoke with 5 page heads, 4 visitors, 4 sessions, 9 identity facts, and 3 components, including the deliberate lost session-commit acknowledgement and restart. The updated membership protocol and bounded coverage SQL still need the second real branch smoke. `scripts/bootstrap-smoke.ts --prepare` writes only synthetic schema/config files. `--run` is fenced to the existing validation branch, uses baseline v2 by default, and may reuse its historical synthetic raw tables. It tenant-scopes the live seed so v1 evidence remains intact.

The real wide historical source-range query also needs a read-only row/byte/memory statistics check before the full job. The small synthetic fixture has only eleven raw columns. No new cloud call, deployment, source download, or production mutation ran from this isolated implementation task.
