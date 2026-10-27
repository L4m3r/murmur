# Murmur — ordered actor database POC

A bounded row database on Cloudflare Workers + Durable Objects, developed with **celld**. The table layer follows YDB's **ordered typed-key ranges** and **ordered secondary-index backing tables**, not hash partitioning. This is original POC code, not a YDB port.

## Run

```sh
npm ci
npm run setup:celld
npm test
npm run test:e2e     # isolated real celld, including SIGKILL/restart recovery
npm run dev         # 127.0.0.1:9876
# Another terminal:
node scripts/demo.mjs
```

**V3 is intentionally incompatible:** start with a fresh checkout/state directory. No migration code or compatibility probes are provided. Existing `.celld/dev` data is not deleted automatically. `PORT` changes the server/test port; stop a dev server before testing on the same port. The test harness always creates its own temporary state.

```sh
curl http://127.0.0.1:9876/query -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"example-1",
  "sql":"CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL); INSERT INTO accounts (id,balance) VALUES (-10,20.5),(2,100),(10,200); CREATE INDEX balance_idx ON accounts(balance)"
}'
curl http://127.0.0.1:9876/query -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"example-2",
  "sql":"SELECT * FROM accounts WHERE id > 0 AND id <= 10 ORDER BY id"
}'
```

Every request is one atomic transaction. Replaying an identical `idempotencyKey`/SQL returns the original result; reusing a key with different SQL returns 409. A retryable 503 can mean an unknown outcome: retry the **same key and SQL**, without any test fault.

## Ordered tables and queries

- Base tablets own contiguous **`[lower, upper)` primary-key ranges**; null catalog endpoints mean unbounded. An exact split pivot belongs to the right tablet. The base SQL primary key remains single-column; index physical keys are composite tuples.
- Typed order: numeric INTEGER/REAL values (including negative numbers and decimals), BOOLEAN false/true, and TEXT in **UTF-8 byte order**. Physical NULL sorts first. Malformed surrogate strings and nonfinite numbers are rejected; signed zero is canonicalized.
- Physical row keys are sortable encodings, **not hashes**: transformed IEEE-754 numbers, escaped UTF-8 bytes, boolean/null tags, and concatenated typed tuple components. Bounded `storage.list(start,end)` reads only qualifying physical keys. Encoded physical keys are capped at 1,800 bytes; arbitrary non-key TEXT retains its separate character limit.
- Single-statement SELECT bounds `=`, `>`, `>=`, `<`, `<=`, including bounded AND intersections, prune both tablets and local storage scans. Contradictory bounds perform no row reads. Other predicates are residual filters. `plan` exposes primary/index ranges, visited/total tablets and physical rows read; LIMIT is applied after the bounded scan.
- Size-triggered splits choose an **actual row-key boundary**, balancing estimated UTF-8 serialized row bytes. Adjacent small ranges can merge regardless of their split ancestry. Defaults: split above 4,096 bytes; merge at/below 1,024. Max16 tablets/backing table; one oversized row remains unsplit. Routing, destination rows and source retirement share one durable transaction, with epoch fences rejecting stale routes.

## Global secondary indexes

`CREATE [UNIQUE] INDEX name ON table(column)` creates an ordered backing table whose physical key is **(indexed value, base primary key)**. If the indexed column is already the base PK, the duplicate key component is omitted, as in YDB. Index tablets have their own ordered range map and split/merge independently; there are no fixed hash buckets.

Index equality is a prefix range covering **every primary-key suffix**, even when duplicate indexed values span several tablets. Index inequalities use typed prefix bounds. Matching index entries lead to base-row point reads batched into at most one actor request per base tablet. NULL entries are stored in physical order, but SQL comparisons with NULL never match; uniqueness allows repeated NULL. Base rows, indexes, index-build metadata and tablet topology commit atomically. Uniqueness is checked per SQL statement, not deferred until transaction end.

## Actors and transaction safety

A per-table catalog actor publishes ordered base/index maps. Row and index tablets own durable rows, prepared changes and epochs. Four coordinator actors route requests by idempotency key; hashing is used **only to select a coordinator**, never to partition data.

A durable lock-manager atomically grants all touched-table locks and a global fencing version. Disjoint tables genuinely execute concurrently; the same table serializes. Coordinators record acquisition intent before acquiring locks, prepare all participants, persist COMMIT before commit RPCs, then hold locks through every acknowledgment. Recovery aborts before COMMIT and completes afterward. Durable abort/release tombstones prevent late messages from resurrecting work. Reads recover outstanding owners before observing base/index/routing state.

This is strict table locking plus durable 2PC, **not YDB's full coordinator/mediator planning, MVCC, Calvin-style scheduling or replicated tablet storage**. Local celld persistence depends on its filesystem. Recovery is demand-driven; unavailable participants block affected tables rather than expose partial commits.

## Scope and limits

SQL: CREATE TABLE with one inline PK; CREATE INDEX; explicit-column multi-row INSERT/full-row UPSERT; SELECT; UPDATE literals/column references/numeric +/-; DELETE; AND comparisons, single-column ORDER BY, LIMIT; optional same-request BEGIN/COMMIT. Types INTEGER (safe JS integer), REAL (finite), TEXT, BOOLEAN, nullable unless NOT NULL. No joins, aggregates, parameters, DROP/ALTER, CDC or topics.

The POC still snapshots full **touched tables for mutations and multi-statement requests**. Efficient range reads currently apply to single-statement SELECT. Limits: 32 statements, 8 touched tables, 2,000 rows across touched tables, 64 participants, 64 KiB body, 32,768 SQL characters, 100 KB durable transaction envelope. History/tombstones/retired metadata have no GC. The lock-manager and coarse locks limit throughput. This is not production-ready.

Public access requires an `API_TOKEN` secret and bearer authorization. Keep `ALLOW_FAULTS=false` outside tests; pauses, faults and debug routes are test-only. Nothing is deployed by these commands.

## YDB source mapping and verification

Reference revision `533b58f85b1f332e14088d9c5a597d6e9c6dd18d`:
- `schemeshard_schema.h`: partition range endpoints → catalog tablet boundaries.
- `schemeshard__operation_split_merge.cpp`: typed ordered pivots and consecutive-range merges → ordered split/merge planner.
- `scheme_board/cache.cpp::FillRangePartitioning`: range-border intersection → query tablet pruning.
- `core/base/table_index.cpp::CalcTableImplDescription`: index columns followed by nonduplicate base PK columns → composite backing keys.
- `scheme_tablecell.h` / `scheme_borders.h`: typed comparisons, NULL-first ordering and prefix bounds → shared comparator and bounded scans.

Verified: **51 unit/protocol tests**, **27 actual celld HTTP checks**, and **four SIGKILL/restart scenarios** (including ordered split and merge of base/index tablets). Wrangler dry-run bundles all five actor classes; no deployment was performed.

Tests cover numeric/UTF-8/boolean ordering, pivot inclusivity, physical scan pruning, duplicate index prefixes spanning tablets, unique conflicts, concurrency, split/merge and SIGKILL/restart recovery. Storage-double tests complement actual celld execution; they do not replace it.
