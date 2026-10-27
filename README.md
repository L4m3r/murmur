# Actor Row DB / Murmur

A bounded distributed row-database POC for Cloudflare Workers + Durable Objects, developed with **celld**. Original implementation inspired by YDB's actor separation—not a YDB port.

## Run

```sh
npm ci
npm run setup:celld
npm test
npm run test:e2e   # isolated real celld; includes SIGKILL/restart recovery
npm run test:legacy # v1 upgrade refusal + rollback preserves old data
npm run dev       # 127.0.0.1:9876, persistent .celld/dev
# Another terminal:
node scripts/demo.mjs
```

`PORT` changes the local port; `BASE_URL` changes standalone test/demo targets. Stop a dev server before using the same port for e2e. The harness uses temporary state and never deletes developer data. Fault injection is disabled in the default configuration.

```sh
curl http://127.0.0.1:9876/query -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"example-1",
  "sql":"CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT, balance INTEGER NOT NULL); CREATE UNIQUE INDEX email_idx ON accounts(email); INSERT INTO accounts (id,email,balance) VALUES (1,'\''ada@example.com'\'',100),(2,'\''grace@example.com'\'',100); SELECT * FROM accounts WHERE email='\''ada@example.com'\''"
}'
```

Each `/query` request is one atomic transaction. Supply a unique `idempotencyKey`; replaying identical key/SQL returns the original result, changing SQL returns 409. For a retryable 503, the outcome can be unknown: retry the **same key and SQL**, without any test fault. Results include coordinator identity, routing epochs/tablets, topology changes, participants and query plan.

## Table features

- **Size-based tablets:** deterministic primary-key hash ranges; writes automatically split oversized tablets and merge small aligned siblings. Defaults: split above 4,096 UTF-8 JSON row bytes, merge at/below 1,024. Hysteresis avoids oscillation. Up to 16 tablets/table. New routing, destination rows and source retirement commit together; stale routing epochs are rejected. A single oversized row or one-sided midpoint remains oversized rather than creating empty tablets. This is estimated encoded row size, not physical disk usage.
- **Global indexes:** `CREATE [UNIQUE] INDEX name ON table(column)`. Four independent index DO partitions span all base tablets. Base rows, index buckets and index-build catalog metadata share the transaction decision. Single-statement equality SELECTs on indexed columns perform a point lookup in the index and point reads of matching base rows, reported as `plan.type: "index"`; other queries scan only touched tables. Indexes update on INSERT/UPSERT/UPDATE/DELETE and survive tablet migration. NULLs are not indexed; repeated NULL is allowed by unique indexes. Uniqueness is checked per SQL statement, not deferred to transaction end.
- **Four transaction coordinators:** idempotency keys route deterministically. A durable lock-manager actor atomically grants complete table-lock sets and global fencing versions. Transactions on **different tables genuinely run concurrently**; operations touching the same table serialize. This is table-granular locking, not same-table parallelism or YDB's distributed planning algorithm.

## Durability and recovery

Each table has a catalog actor. Row tablets store individual rows under hashed primary-key storage keys; index actors store index-value → primary-key buckets. Coordinators durably record acquisition intent **before** requesting locks, prepare every participant, then persist COMMIT before any commit RPC. Row updates and participant version/epoch metadata commit atomically.

Locks remain held through all participant acknowledgments. On conflict, the public Worker asks the owner's queued recovery handler to resolve its existing transaction before retrying; coordinators never recursively wait on other coordinators. Before COMMIT, recovery aborts; after COMMIT it must finish. Abort/release tombstones fence late prepare/grant messages, and global versions prevent old commit replay from reverting newer data. Recovery is demand-driven. Unavailable participants block affected tables rather than expose mixed base/index/routing state.

**Within this API:** serializable transactions through strict exclusive table locks, atomic cross-table/cross-tablet changes, durable decisions and replay-safe commits, assuming DO atomic durable storage. Local celld durability depends on its filesystem; independent replication is not implemented. Do not expose internal actor bindings as a separate client API.

### Existing v1 data

V1 used a singleton catalog and fixed shards. V2 detects that catalog and **fails closed with `409 LEGACY_STATE`**, leaving all old data untouched. There is no automatic on-disk migration. Keep the old state directory and run the v1 revision to export data; use a separate fresh state directory for V2. Merely upgrading the source must not silently hide existing rows. Do not run mixed versions against the same actor namespaces.

## SQL and bounds

`CREATE TABLE` with one inline `PRIMARY KEY`; explicit-column multi-row `INSERT` / full-row `UPSERT`; `SELECT`; `UPDATE SET` literals, column references or numeric `+`/`-`; `DELETE`; single-column index creation. WHERE supports comparisons joined by AND; ORDER BY one column; LIMIT; optional same-request `BEGIN; … COMMIT;`.

Types: INTEGER (safe JS integer), REAL (finite number), TEXT, BOOLEAN; NULL unless NOT NULL. No implicit casts, primary-key updates, joins, aggregates, prepared parameters, DROP/ALTER, MVCC, online v1 migration, CDC or topics. UPSERT replaces the full row; omitted nullable columns become NULL.

POC limits: 32 statements, 8 touched tables, 2,000 rows across the touched tables, 16 tablets/table, 64 participants, 64 KiB request body, 32,768 SQL characters and 100 KB durable transaction envelope. Mutations still read full touched-table snapshots and stage full tablet/index snapshots. The lock-manager remains a coordination bottleneck. Idempotency receipts, retired tablet metadata and abort/release tombstones have no GC. This is not production-ready.

Public/non-loopback access requires an `API_TOKEN` secret and bearer authorization. No local command deploys infrastructure. Keep `ALLOW_FAULTS=false` outside tests; test-only faults, pauses and `/debug/*` routes are deliberately gated by it.

## Verification

Verified: **34 unit/protocol tests**, **20 actual celld HTTP checks**, three SIGKILL/restart scenarios, and three V1 upgrade/refusal/rollback scenarios. Wrangler dry-run bundles all five actor bindings; nothing was deployed.

The suite exercises typed CRUD, rollback, idempotency, delayed-message fencing, simultaneous disjoint-table execution, cross-coordinator transfer/read invariants, indexed point lookups and unique conflicts, split/merge and stale epochs. Actual celld tests SIGKILL and restart after prepare, after partial commit, and during combined tablet migration/index build. The deterministic storage-double tests complement—not substitute for—the process-restart tests.

YDB architecture reference: revision `533b58f85b1f332e14088d9c5a597d6e9c6dd18d`, `ydb/core/tx/{schemeshard,coordinator,datashard}`. Replicated storage, mediators and speculative pipelines are not reproduced.
