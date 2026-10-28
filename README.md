# Murmur — ordered DataShards, MVCC and planned transactions

A bounded Cloudflare Workers/Durable Objects row-database POC, developed with **celld**. It implements YDB-inspired ordered tables, secondary-index backing tables, shard-local transaction validation, durable coordinator intent and mediator plan/visibility tracking. It is not a YDB port or a complete YDB implementation.

## Run

```sh
npm ci
npm run setup:celld # pinned celld v0.6.3
npm test
npm run test:coordinator-pool # isolated virtual-alarm fairness and scoped persistence
npm run test:e2e    # isolated real celld, including SIGKILL/restart recovery
npm run test:composite # tuple routing/indexes and crash recovery
npm run test:online-index # resumable concurrent backfill and crash recovery
npm run test:query-pagination # bounded cursors, ACK windows and lifecycle
npm run test:storage # chunked recovery, 1000-version reads, selective DML and GC
npm run setup:moto && npm run test:fleet # isolated two-node S3-emulator failover
npm run dev        # 127.0.0.1:9876
# Another terminal:
node scripts/demo.mjs
```

The local runtime requires **celld ≥0.6.3**. [Upstream release notes](https://github.com/denoland/celld/releases/tag/v0.6.3) describe a fail-closed ownership-loss fix absent in 0.6.2. Earlier 0.6.2 test results are baseline evidence, not qualification of the new pin.

**V7 is intentionally incompatible:** use a fresh checkout/state directory. There are no version-migration or compatibility shims; existing `.celld/dev` data is not deleted automatically. The test harness uses temporary state. `PORT` changes the local server/test port; `BASE_URL` changes standalone test targets.

```sh
curl http://127.0.0.1:9876/query -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"setup-1",
  "sql":"CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL); INSERT INTO accounts (id,balance) VALUES (-10,20.5),(2,100),(10,200); CREATE INDEX balance_idx ON accounts(balance)"
}'
curl http://127.0.0.1:9876/query -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"read-1",
  "sql":"SELECT * FROM accounts WHERE id > 0 AND id <= 10 ORDER BY id"
}'
```

Each `/query` is one transaction. Replaying an identical `idempotencyKey`, SQL and explicit snapshot returns the original result; changed input under the same key returns 409. Wide transactions may return **503 `PLAN_PENDING`** with `outcome:"PENDING"` and a transaction ID after a bounded progress slice. This is not a commit receipt. Retry the **same request epoch, key, SQL and parameters** until the final 200 result; changed payloads return 409 even while pending. Other retryable 503s can also mean an unknown outcome. Durable peer/alarm work continues independently; only the final receipt confirms commit. Foreground wide-transaction slices limit participant delivery, ReadSet sending and finalization separately, while background drains remain bounded.

`POST /snapshot` with `{}` returns the safe snapshot, allocated plan step, delivery frontier and unresolved plans. Read-only `/query` requests may supply `"snapshot": N` for a retained historical snapshot. `POST /recover` with `{}` helps a bounded page of active coordinator recovery queues; pass its `nextCursor` to continue. Each idempotency key has an independent durable transaction context, routed deterministically to one of 16 Coordinator service actors. Contexts have separate execution queues and storage namespaces; a stalled remote transaction does not serialize other admitted contexts. Physical KV operations and storage transactions share a short local queue, never held across actor RPCs, to avoid overlapping savepoints/input gates. Bounded virtual alarms recover abandoned work. Default reads use an allocated plan snapshot and wait/recover only unresolved writes intersecting their keys, ranges or routing metadata. Unrelated plan holes do not block fresh reads or completed writes.

## Bounded query pages

`POST /cursor` with `{ "action":"start", "sql":"SELECT * FROM accounts ORDER BY id", "idempotencyKey":"pages-1", "pageSize":64, "ttlMs":60000 }` returns an `id` and signed `token`. Request `action:"page"` with that ID/token; follow `nextToken` until `done`. Each page has a fixed snapshot, bounded actor work and output bytes, so short or empty nonterminal pages are valid. Primary or ready-index column ordering supports ASC/DESC; unsupported streaming sorts are rejected explicitly.

Tokens bind query parameters, order, snapshot and table/index generation. Retrying an unacknowledged token returns its exact cached page. `action:"ack", throughSeq:N` releases consumed pages through N; older tokens then return **410 PAGE_EXPIRED**, never a different page. The default replay window is eight pages/4 MiB (`CURSOR_MAX_CACHE_PAGES/BYTES`); ACK allows a query to continue beyond that window. `action:"renew", ttlMs:60000` extends the same still-live snapshot lease; it never revives an expired lease or changes query context. Repeated explicit renewals are separate extensions, while page retries remain exactly replayable. `action:"close"` releases the query lease. Each lease extension is at most five minutes; abandoned clients expire without renewal. Each active page separately pins its snapshot so GC cannot interrupt an admitted read. Expired/closed cursors return **410**.

## Retained snapshots and compaction

`POST /snapshot` with `{ "action":"lease", "id":"my-unique-lease", "snapshot":123, "ttlMs":60000 }` retains a snapshot. Use `action:"renew"` or `action:"release"` with that ID; released IDs cannot be rebound. Queries may send `snapshotLease` alongside `snapshot`. Expired/collected snapshots return **410**, never silently newer data.

`POST /maintenance` with `{ "retainSteps":1000 }` runs one bounded shard compaction batch. Repeat with the returned `continuation` until `done:true`. The floor cannot pass admitted operations or live external leases. This endpoint removes obsolete history: run it only on the database you intend to maintain. Tests use generated, isolated state. Existing state directories are never deleted or migrated by setup.

For ongoing operation, run `npm run maintenance` as an operator-owned process. It automatically performs bounded history and retired-epoch collection batches. Configure `BASE_URL`, `API_TOKEN`, `RETAIN_STEPS` (default 1000), `INTERVAL_MS` (30000), `WORK_BATCHES` (8), and `CHECKPOINT_FILE` for durable continuation; `-- --once` runs one cycle. Optional `ROTATE_INTERVAL_MS` enables epoch rollover with `KEEP_EPOCHS` (2). **Rotation is off by default:** clients must explicitly follow current request epochs before enabling it. Busy epochs are never forcibly retired. Start this process only for the database whose history you intend to collect. The tested plateau is a count of logical KV records in the actor model, not a plateau of celld LTX/object-store bytes or SQLite file sizes. Provider log retention and physical space reclamation are separate; diagnostic states are not pruned to improve measurements.

## Replay epochs

Queries accept `requestEpoch` (initial default **0**, never silently the current epoch). `/snapshot` reports `currentEpoch`; `action:"rotateEpoch"` opens the next epoch. New clients then explicitly supply it. `/maintenance` with `action:"retireEpochs", before:N` refuses retirement while older operations, incomplete plans or live leases remain. Once retired, old/missing epochs return **410 REQUEST_EXPIRED** even if their idempotency receipt has been removed.

Use `/maintenance` with `action:"collectEpochs"` and returned cursors to collect retired coordinator receipts and protocol records, then run ordinary shard maintenance for retired markers. Idempotency is scoped by `(requestEpoch,idempotencyKey)`; changed SQL or parameters within that identity is rejected. DROP INDEX retires its backing generation; recreating the same SQL name uses a new physical identity.

## One DataShard implementation

**Base tables and secondary indexes use the same `DataShard` class and `SHARDS` binding.** They differ only in table/partition identity and row schema. Catalog records also use versioned DataShard rows; the other actors are 16 physical `Coordinator` services with independent per-request contexts, a short `PlanAllocator` admission/order service and multiple `PlanBucket` mediators. There is no whole-table lock-manager actor.

DataShards append `(typed key, plan step) → row/tombstone` versions and version routing metadata. A snapshot selects the latest version at or below its step. Delete tombstones, previous rows and retired tablets remain readable while their snapshots are retained, so leased historical reads survive updates, splits and merges. Ordinary commits persist **only changed base/index rows**, not replacement tablet snapshots.

## Transaction flow

1. A coordinator pins an allocated snapshot, executes with read-your-writes, and records point/range dependencies including empty predicates, unique prefixes and index-to-base reads.
2. Every participant durably stores its complete immutable **proposal** (body, dependencies and participant names). This is not a COMMIT vote. The coordinator persists a planning obligation before requesting a step.
3. The allocator anchors each participant identity and proposal through the actual named DataShard, then atomically assigns `(transaction, step, bucket, manifest)`. Assignment races an irreversible unplanned-abort fence. Durable registration outboxes repair an assigned-but-unregistered bucket.
4. At its fixed step, each shard validates applied versions newer than its snapshot and every earlier unresolved conflicting planned write. Complete proposal registration before allocation makes earlier dependencies discoverable; authoritative allocator lookups distinguish a future proposal from a missing bucket record. Disjoint keys can proceed concurrently.
5. Each participant atomically freezes an immutable signed COMMIT/ABORT ReadSet and peer outbox. Inbox persistence precedes ACK. Bounded sends run outside actor critical sections. Any participant can submit an authenticated all-COMMIT or any-ABORT certificate; the bucket records one irreversible outcome. Participants append MVCC deltas/tombstones and issue signed application ACKs.
6. Participant/bucket alarms can finish assigned data outcomes without the originating coordinator. Its durable response receipt still supplies idempotent SQL-result replay. Success requires every own participant applied, not unrelated plans. Allocated snapshots wait only on intersecting unfinished writes; the resolved prefix remains a conservative GC bound.

Identity signatures bind protocol domain/version, transaction, request epoch, plan step, manifest, participant and outcome. Replay fences and epoch admission reject delayed messages after collection. Network calls never run inside the allocator's local serialized mutation section or participant peer-delivery section.

**Isolation:** default transactions are serializable in plan order under this conservative validation protocol. Newer applied versions and earlier planned write footprints intersecting read/write predicates cause abort/retry, preventing lost updates, write skew and phantoms. Common-step MVCC and relevant-dependency barriers prevent mixed base/index/catalog visibility. Explicit retained snapshots intentionally read historical state.

## Ordered tables and indexes

Base tablets own contiguous **`[lower, upper)` typed primary-key ranges**; null catalog endpoints mean infinity. Splits choose an actual key by estimated UTF-8 row-byte weight; any adjacent small ranges can merge. Defaults: split above 4,096 bytes, merge at/below 1,024, a configurable maximum of 256 tablets per base/index table (up to 4,096 descriptors per map). One oversized row remains unsplit. Each tablet has its own routing epoch: only replaced actors change metadata. Routing uses binary search plus the intersecting range walk. Topology changes validate catalog/source dependencies; ordinary row transactions retain precise schema and row predicates, not table locks. Old snapshot catalog mappings and source versions remain readable after retirement.

Physical keys preserve INTEGER/REAL numeric order, BOOLEAN order, NULL-first order and **UTF-8 byte TEXT order**. Signed zero is canonicalized; nonfinite numbers and malformed surrogate text are rejected. Encoded physical keys are limited to 1,800 bytes. Single-statement SELECT `=`, `>`, `>=`, `<`, `<=` and bounded AND prune both tablet routing and local version-key scans. Contradictory ranges issue no data reads. Plans report visited tablets, visible rows and physical versions inspected. Compatible primary/index ordering with LIMIT consumes forward/reverse pages and applies residual predicates before stopping; unrelated ORDER BY still uses explicit bounded sorting.

`CREATE [UNIQUE] INDEX name ON table(column)` creates an ordered backing table keyed by **(indexed value, base PK)**; the duplicate PK component is omitted when indexing the PK itself. Indexes have independent ordered tablet maps. Prefix bounds span every PK suffix, including duplicate indexed values split across tablets. Index-to-base point reads are batched by base tablet. NULL entries are stored, but SQL NULL comparisons do not match; uniqueness permits repeated NULL. Index changes, builds and topology changes share the transaction outcome with base rows.

## Online index jobs

`POST /schema` with `{ "action":"start", "sql":"CREATE INDEX by_balance ON accounts(balance)", "idempotencyKey":"build-1", "requestEpoch":0 }` starts a resumable job. A 202 response contains `job.id`; use `action:"status"`, `"resume"` or `"cancel"` with that ID. Alarms resume durable work after restart. READY returns 200; failed/cancelled jobs expose their terminal state explicitly.

A building index is write-maintained but not selected by query planning. The backfill enumerates keys from the exact index-install commit snapshot, re-reads current rows, and validates base-point dependencies before index writes. Durable stage receipts precede cursor advancement. Cancellation/failure retires only that physical generation. BUILDING does not yet enforce uniqueness. At EOF, VALIDATING activates unique-prefix guards and pins a complete index snapshot; ordered duplicate validation retains group state across pages. READY requires successful validation. Duplicates present at that snapshot conservatively fail the build.

Backfill splits affected index tablets incrementally before READY; untouched actors retain their epochs. Cancellation and `/schema` DROP INDEX retire captured generations in bounded batches after atomic logical catalog removal. Cleanup settles at most one batch per resume: default 4 retiring actors (`SCHEMA_CLEANUP_TABLETS`, 1–16) and 32 KiB of retirement metadata (`SCHEMA_CLEANUP_BYTES`, 1–64 KiB); the atomic catalog mutation retains its separate transaction budget. Multi-statement SQL DROP remains atomic and subject to transaction budgets. `/schema` also accepts DROP TABLE; recreated names use a new physical table generation, while leased old snapshots retain the old generation.

## Deliberate simplifications and bounds

The peer ReadSet flow is YDB-inspired, not a complete YDB port: full-proposal registration plus authoritative allocation probes replaces YDB's ordered PlanStep delivery/dependency streams. Preallocated plan batches would invalidate that proof without additional delivery barriers. One short global allocator remains an ordering/admission dependency; bucket execution and peer completion are distributed. Validation conservatively aborts rather than building YDB's full dependency graph. The resolved prefix may delay GC behind holes, but does not block unrelated read/write visibility.

Ordinary mutations fetch only predicate candidates, overlay their own writes and probe global unique prefixes. Split/merge materializes affected tablets only; untouched tablets retain their existing epochs. Atomic multi-statement DDL still uses bounded snapshot materialization. Standalone online index jobs use a durable paged backfill instead. Point MVCC lookup seeks directly to the latest version at/below the snapshot; paged head indexes avoid traversing obsolete history in scans and validation. MVCC history is compacted incrementally behind a durable retention floor. Active operations pin their snapshots before reads; external snapshot leases retain historical snapshots. Every key retains its floor anchor (including tombstones), plus newer versions. Request epochs permit safe retirement of receipts and protocol fences: old epochs are rejected before coordinator lookup, even after their state is collected.

SQL: inline or ordered composite `PRIMARY KEY(a,b,...)`; CREATE TABLE/INDEX, DROP TABLE/INDEX; explicit-column multi-row INSERT/full-row UPSERT; SELECT; UPDATE literals/column references/numeric +/-; DELETE; AND comparisons, one ORDER BY column, LIMIT, same-request BEGIN/COMMIT. Types INTEGER (safe JS integer), REAL, TEXT, BOOLEAN. Named `:parameter` literals bind from a scalar `params` object without SQL substitution. No joins, aggregates, ALTER, CDC or topics.

Coordinator services admit at most 4 concurrent contexts by default (configurable up to 8), 16 queued calls, 4 calls per context and 8 MiB of admitted request payload per service. Excess admission returns retryable backpressure; active contexts are never evicted. Pooling bounds coordinator-cell growth but does not fix storage-provider cold recovery of a large shard set. This is a fresh incompatible service layout; no existing state is migrated or deleted.

Limits: 32 statements, 8 touched tables, 64 participants, 64 active proposals per shard, 64 KiB body, 32,768 SQL characters, 8 MiB read work and 2 MiB durable transaction/response budgets. Larger durable records use SHA-256-verified 48 KiB chunks, atomically published and reclaimed; no storage value needs to exceed the provider limit. These budgets are explicit backpressure, not total table-size limits. No production replication, consensus, backup or availability claim is made. Local durability is only as strong as celld's filesystem.

Public access requires `API_TOKEN` and bearer authorization. Keep `ALLOW_FAULTS=false` outside tests; fault/pause/debug controls are gated. No command here deploys infrastructure. With test faults enabled only, `after-first-commit` retains a durable partial-apply fence and returns its stage proof; the SIGKILL harness verifies that proof before killing. Recovery explicitly retries the same identity without the fault (or calls `/recover`) to release the injected obstruction. Alarms alone do not release this test fence.

## YDB mapping and tests

Reference revision `533b58f85b1f332e14088d9c5a597d6e9c6dd18d`: SchemeShard typed partition endpoints and split/merge; `scheme_board/cache.cpp::FillRangePartitioning`; `table_index.cpp::CalcTableImplDescription`; typed cells/prefix borders; DataShard persistent lock validation and versioned reads; coordinator persistent plan manifests and mediator delivery acknowledgments. The role mappings above distinguish implemented behavior from the omitted YDB machinery.

Historical checkpoint validation includes the unit/protocol suite; 34 baseline celld HTTP cases and five SIGKILL/restarts; chunked transactions/results, 1000-version seeks, >2000-row selective writes and bounded snapshot GC; and two-process celld failover against a pinned local Moto S3 emulator. The fleet harness checks conditional-write rejection and range reads before database failover. Emulator success is not production object-store qualification. CI has independent blocking stock-celld compatibility and explicitly experimental prefix-patch qualification lanes, with identical test assertions and request budgets. The experimental lane builds pinned upstream source plus the reviewed patch and runs its equivalence harness; stock failures remain visible and blocking. Both retain logs and runtime identities without deployment credentials. The initial stock CI run passed units/pool smoke but failed the loaded split/index recovery budget; later suites were skipped. No deployment was performed.

**Open runtime qualification gate:** on the preserved pre-pool checkpoint, a cold restart after 500 sequential updates exceeded the explicit 10-second first-query budget on official celld 0.6.2 and 0.6.3 with LocalStore. A one-node Moto/S3 control passed split recovery in 7.934 seconds but timed out on merge recovery at 10.011 seconds. Small-state recovery passed. Pooling alone also failed this gate. An [optional, pinned-source experimental LocalStore patch](patches/celld-prefix/README.md) passed controlled same-profile and fresh-workload comparisons; it is not the default or an official release. The experimental qualification below is separate; the official-runtime loaded-restart gate remains open.

Tests include physical range pruning and typed ordering; simultaneous same-table disjoint transactions; write-skew, empty-range phantom and unique races; invisible-version conflicts; old base/index snapshots and routing; lost plan/apply acknowledgments; alarm-only recovery; and real SIGKILL/restart at prepare, durable planning obligation, partial apply, split and merge. Storage-double tests supplement, not replace, actual celld execution.

### Frozen local qualification (2026-10-28)

Commit `65fdd781a75a16c4b1842900774eff5d0c71eb65` passed from a complete immutable archive, sequentially, using the **experimental** celld binary SHA-256 `ab7874dd7583a4df3cf2ba1f1e93ddefc1bc139d4e8b38a7220217c14740d004`:

| Command | Seconds | Result |
| --- | ---: | --- |
| `npm test` | 12 | 314 passed |
| `npm run test:coordinator-pool` | 5 | alarm/storage isolation passed |
| `npm run test:e2e` | 128 | HTTP invariants + five SIGKILL recoveries passed |
| `npm run test:storage` | 168 | large rows/history + restart passed |
| `npm run test:retention` | 3 | snapshot/replay GC + restart passed |
| `npm run test:composite` | 15 | typed composite/index + restart passed |
| `npm run test:online-index` | 157 | 4,096-row build, wide DROP + restart passed |
| `npm run test:query-pagination` | 38 | all five groups passed |
| `npm run test:fleet` | 22 | two-node Moto failover passed |

Unchanged finite request budgets were retained. Logs distinguish 110 expected `PLAN_PENDING` responses from 18 coordinator-admission backpressure retries, three confirmed allocator-cell overload retries (honoring `Retry-After: 1`), and 18 owner-unreachable retries after intentional fleet SIGKILL. Fleet recovery took 9.58 seconds. The 51-participant synchronous index seed took 12.355 seconds across five same-key calls (two pending, two overloads); acceptance was never reported as commit. No transport-timeout retry was hidden. Official Wrangler dry-run passed (263.89 KiB bundle); no deployment or hosted qualification was performed.

### Reproducible measurements

`npm run benchmark -- --iterations 40 --concurrency 4` measures a running local instance using generated tables, disjoint/contended point writes, verified final values and same-key retry accounting. It reports latency percentiles, not a throughput guarantee. `npm run benchmark:protocol` separately instruments actual protocol classes in a memory-backed actor model and reports every actor RPC; those counts are not hosted latency measurements. Coordination amplification and contention retries remain material costs. Run benchmarks separately from recovery suites to avoid conflating local resource contention with correctness.

Observed separately on that experimental runtime: 16 point writes per scenario at concurrency 4 had disjoint p50/p95 952/1,273 ms and contended 2,043/6,565 ms, with final sums verified and no HTTP retries. These small local measurements are not throughput, hosted-cost, or production-availability guarantees.
