# Murmur — ordered DataShards, MVCC and planned transactions

A bounded Cloudflare Workers/Durable Objects row-database POC, developed with **celld**. It implements YDB-inspired ordered tables, secondary-index backing tables, shard-local transaction validation, durable coordinator intent and mediator plan/visibility tracking. It is not a YDB port or a complete YDB implementation.

## Run

```sh
npm ci
npm run setup:celld
npm test
npm run test:e2e    # isolated real celld, including SIGKILL/restart recovery
npm run dev        # 127.0.0.1:9876
# Another terminal:
node scripts/demo.mjs
```

**V4 is intentionally incompatible:** use a fresh checkout/state directory. There are no version-migration or compatibility shims; existing `.celld/dev` data is not deleted automatically. The test harness uses temporary state. `PORT` changes the local server/test port; `BASE_URL` changes standalone test targets.

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

Each `/query` is one transaction. Replaying an identical `idempotencyKey`, SQL and explicit snapshot returns the original result; changed input under the same key returns 409. A retryable 503 may mean an unknown outcome: retry the **same key and SQL**, without a test fault.

`POST /snapshot` with `{}` returns the safe snapshot, allocated plan step, delivery frontier and unresolved plans. Read-only `/query` requests may supply `"snapshot": N` for a retained historical snapshot. `POST /recover` with `{}` helps all coordinator recovery queues. Durable alarms also recover abandoned work. Default reads do not wait for unresolved transaction holes; they see the last safe committed snapshot.

## One DataShard implementation

**Base tables and secondary indexes use the same `DataShard` class and `SHARDS` binding.** They differ only in table/partition identity and row schema. Catalog records also use versioned DataShard rows; the other actors are four transaction `Coordinator`s and one `Mediator`. There is no whole-table lock-manager actor.

DataShards append `(typed key, plan step) → row/tombstone` versions and version routing metadata. A snapshot selects the latest version at or below its step. Delete tombstones, previous rows and retired tablets are retained, so historical reads survive updates, splits and merges. Ordinary commits persist **only changed base/index rows**, not replacement tablet snapshots.

## Transaction flow

1. A coordinator obtains the mediator's contiguous fully-applied snapshot frontier and executes on that snapshot, with read-your-writes inside the transaction. It records logical point/range dependencies, including empty predicates, unique-index value prefixes and base-row dependencies of index lookups.
2. Every involved DataShard—including read-only validation participants—atomically checks for newer versions, **including applied versions above the visible frontier**, and conflicting prepared reservations. Shared read ranges and exclusive changed keys are reserved; conflicts abort/retry immediately, never wait on a tablet-wide lock. Disjoint keys on the **same table and same DataShard** can prepare concurrently.
3. Each shard persists its delta and vote. Once every vote succeeds, the coordinator persists **COMMIT_READY** with the immutable participant/vote manifest. This is irrevocable. Before it, recovery aborts; after it, recovery must finish planning and applying.
4. The mediator idempotently assigns a durable ordered plan step. It does no conflict certification and never waits for remote transaction execution inside its serialized handler. Shards verify the plan certificate, acknowledge delivery, atomically append MVCC deltas and acknowledge application.
5. Delivery and application are separate watermarks. The **visible frontier advances only across a contiguous prefix of fully-applied plans**. A later independent plan may apply behind an earlier hole without exposing partial state. A successful write response waits until its step is visible, preserving subsequent default-read visibility.

Abort markers fence delayed prepare messages; immutable votes/plans and applied markers make retries idempotent. Reservation conflicts identify their owners, allowing another coordinator to recover abandoned work even without an alarm. Coordinator recovery runs through that coordinator's queue rather than racing a live transaction.

**Isolation:** default transactions are serializable under this bounded protocol. Snapshot validation plus retained predicate reservations prevents lost updates, write skew and phantoms; all participants prepare before any reservation is released. Common-step publication makes base/index/catalog changes atomically visible. A write returns 200 only after its publication frontier passes. Explicit historical snapshots intentionally read older committed state.

## Ordered tables and indexes

Base tablets own contiguous **`[lower, upper)` typed primary-key ranges**; null catalog endpoints mean infinity. Splits choose an actual key by estimated UTF-8 row-byte weight; any adjacent small ranges can merge. Defaults: split above 4,096 bytes, merge at/below 1,024, max16 tablets per base/index table. One oversized row remains unsplit. Topology changes use a brief exclusive schema/epoch reservation; ordinary row transactions retain shared schema reads only. Old snapshot catalog mappings and source versions remain readable after retirement.

Physical keys preserve INTEGER/REAL numeric order, BOOLEAN order, NULL-first order and **UTF-8 byte TEXT order**. Signed zero is canonicalized; nonfinite numbers and malformed surrogate text are rejected. Encoded physical keys are limited to 1,800 bytes. Single-statement SELECT `=`, `>`, `>=`, `<`, `<=` and bounded AND prune both tablet routing and local version-key scans. Contradictory ranges issue no data reads. Plans report visited tablets, visible rows and physical versions inspected.

`CREATE [UNIQUE] INDEX name ON table(column)` creates an ordered backing table keyed by **(indexed value, base PK)**; the duplicate PK component is omitted when indexing the PK itself. Indexes have independent ordered tablet maps. Prefix bounds span every PK suffix, including duplicate indexed values split across tablets. Index-to-base point reads are batched by base tablet. NULL entries are stored, but SQL NULL comparisons do not match; uniqueness permits repeated NULL. Index changes, builds and topology changes share the transaction outcome with base rows.

## Deliberate simplifications and bounds

This is **coordinator-assisted prepare/vote → plan/apply**, not YDB's complete peer ReadSet exchange, distributed dependency graph, multiple mediators, speculative execution or replicated tablet storage. The global resolved-prefix frontier is conservative: unrelated holes delay publication of later writes. It is not the same thing as YDB's plan-delivery barrier.

Mutations still materialize full touched-table snapshots for local SQL evaluation/sizing, but their **logical validation footprints are predicate/key scoped and persistence is incremental**. Same-table point writers are not serialized by a hidden full-table reservation. Single-statement SELECT uses physical ordered range access. No MVCC/history/receipt/tombstone GC is implemented.

SQL: single-column inline base PK; CREATE TABLE/INDEX; explicit-column multi-row INSERT/full-row UPSERT; SELECT; UPDATE literals/column references/numeric +/-; DELETE; AND comparisons, one ORDER BY column, LIMIT, same-request BEGIN/COMMIT. Types INTEGER (safe JS integer), REAL, TEXT, BOOLEAN. No joins, aggregates, parameters, DROP/ALTER, CDC or topics.

Limits: 32 statements, 8 touched tables, 2,000 touched rows, 64 participants, 64 reservations per shard, 64 KiB body, 32,768 SQL characters and 100 KB coordinator transaction envelope. No production replication, consensus, backup or availability claim is made. Local durability is only as strong as celld's filesystem.

Public access requires `API_TOKEN` and bearer authorization. Keep `ALLOW_FAULTS=false` outside tests; fault/pause/debug controls are gated. No command here deploys infrastructure.

## YDB mapping and tests

Reference revision `533b58f85b1f332e14088d9c5a597d6e9c6dd18d`: SchemeShard typed partition endpoints and split/merge; `scheme_board/cache.cpp::FillRangePartitioning`; `table_index.cpp::CalcTableImplDescription`; typed cells/prefix borders; DataShard persistent lock validation and versioned reads; coordinator persistent plan manifests and mediator delivery acknowledgments. The role mappings above distinguish implemented behavior from the omitted YDB machinery.

Verified: **63 unit/protocol tests**, **34 actual celld HTTP cases**, **five SIGKILL/restart scenarios**, and a successful Wrangler dry-run of all three actor bindings. No deployment was performed.

Tests include physical range pruning and typed ordering; simultaneous same-table disjoint transactions; write-skew, empty-range phantom and unique races; invisible-version conflicts; old base/index snapshots and routing; lost plan/apply acknowledgments; alarm-only recovery; and real SIGKILL/restart at prepare, COMMIT_READY, partial apply, split and merge. Storage-double tests supplement, not replace, actual celld execution.
