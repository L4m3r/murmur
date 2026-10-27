# Actor Row DB

A working, deliberately small distributed row-database POC for Cloudflare Workers + Durable Objects, developed locally with **celld**. Original implementation inspired by YDB's catalog/coordinator/data-shard separation—not a YDB port or compatibility layer.

## Run locally

```sh
npm ci
npm run setup:celld
npm test
npm run test:e2e  # starts isolated celld, tests, SIGKILLs, restarts and verifies recovery
npm run dev
# Another terminal:
node scripts/demo.mjs
```

The dev server binds `127.0.0.1:9876`. Local state survives server restarts in `.celld/dev`. Keep that directory; deleting it destroys the local database. See `package.json` for commands and `scripts/integration.mjs --restart-seed <file>` / `--restart-verify <file>` for the process-restart recovery test. `BASE_URL` overrides the test/demo URL.

```sh
curl http://127.0.0.1:9876/query -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"hello-1",
  "sql":"CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL); INSERT INTO accounts (id,balance) VALUES (1,100),(2,100); SELECT * FROM accounts ORDER BY id"
}'
```

Every `/query` request is one atomic transaction, including multiple semicolon-separated statements. Supply a unique `idempotencyKey`; replaying the same key/SQL returns the original result, while a different SQL string returns 409. Failed validation is 400. A 503 means the outcome may be unknown: retry the **same** key and SQL, without the optional test fault.

## Actors and transaction protocol

- **Catalog**: durable schemas and fixed four-shard table routing.
- **RowShard**: individual row storage entries keyed by SHA-256 of the typed primary key; durable prepared snapshots and atomic row+version updates.
- **Coordinator**: one global serial queue for **all reads and writes**, durable transaction decisions and idempotency records. This intentionally sacrifices throughput for a simple serializability argument.

The coordinator recovers outstanding work, reads a consistent snapshot, validates/executes all statements on a private copy, writes a PREPARING record, and durably prepares every changed participant. It then persists COMMIT before sending commits. A crash before COMMIT causes abort; after COMMIT, recovery must finish commit. Participants apply commits idempotently; durable abort tombstones fence late prepare messages. The next query cannot inspect data until recovery resolves all participants. There is no external direct shard API.

**Guarantee within this API:** transactions are globally serialized and atomic to readers, with durable decisions and replay-safe commits, assuming Durable Objects' durable atomic storage. Local celld durability is only as strong as its local filesystem; no independent replication is implemented here. Recovery is demand-driven (next query), not a background service. If a participant is unavailable, the database blocks rather than expose a partial result. Do not create a second coordinator or expose participant bindings as an API.

## SQL subset

`CREATE TABLE` with one inline `PRIMARY KEY`; `INSERT INTO` and full-row `UPSERT INTO` with explicit columns and multi-row `VALUES`; `SELECT` columns or `*`; `UPDATE SET` literals/column references/numeric `+` or `-`; `DELETE`. Optional `WHERE` comparisons joined by `AND`, single-column `ORDER BY`, and `LIMIT`. `BEGIN; … COMMIT;` wrappers are accepted within one request, not across requests.

Types: `INTEGER` (safe JS integer), `REAL` (finite number), `TEXT`, `BOOLEAN`, nullable unless `NOT NULL`. No implicit casts. Primary keys cannot be updated. UPSERT replaces the whole row; omitted nullable columns become NULL. NULL comparisons do not match. No joins, indexes beyond primary row identity, aggregates, parameters, migrations, MVCC, resharding, constraints beyond types/PK/nullability, or YQL compatibility.

## Boundaries and security

This is not a production database: queries full-scan all tables, transactions stage full changed-shard snapshots, and a singleton coordinator is a throughput bottleneck. Fixed limits: 16 tables, 2,000 total rows, 32 statements/request, 64 KiB HTTP body, 32,768 SQL characters, 100 KB durable transaction envelope. Idempotency records and abort tombstones have no GC. Full scans and retained history make it unsuitable for large/long-running datasets.

No infrastructure is deployed by the local commands. Public/non-loopback requests require an `API_TOKEN` binding and `Authorization: Bearer …`; configure a strong secret before any deployment. Fault injection is disabled in `wrangler.json`; the isolated test harness enables it only for its test runtime. Keep `ALLOW_FAULTS` false when deploying. Faults `after-prepare`, `after-decision`, and `after-first-commit` exercise recovery and return 503 deliberately. Do not expose the local test server to untrusted clients.

YDB reference: revision `533b58f85b1f332e14088d9c5a597d6e9c6dd18d`, particularly `ydb/core/tx/{schemeshard,coordinator,datashard}`. YDB's replicated tablet storage, mediators, MVCC and distributed planning are deliberately not reproduced.

## Verified

`npm test`: 11 passing SQL/protocol tests. Actual celld v0.6.2: 12 HTTP integration checks, including 30 concurrent cross-shard transfers; process SIGKILL/restart after partial commit recovered the durable decision with no double-apply; SIGKILL/restart before a decision aborted prepared writes. Wrangler 4.151.1 `deploy --dry-run` successfully bundled all three Durable Object bindings; no deployment was performed. The demo transfers balances from 100/100 to 75/125 on different shards.
