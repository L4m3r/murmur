# Frozen v1 runtime fixture

Exact `src/index.js`, `src/sql.js`, and `wrangler.json` from the initial POC tree
(local commit `b05609a`, published parent `dd842e018a3cf669ecac25190448ff922b2d3dfb`).
Kept here so `test:legacy` works from source archives without Git history.
Do not update to v2: this fixture verifies upgrade rejection and rollback preservation.
