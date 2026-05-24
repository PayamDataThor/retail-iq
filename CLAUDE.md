# RetailIQ — Claude Code Instructions

## Project layout

```
notebooks/          Databricks notebooks (serverless, client "2")
apps/retail_insights/  Streamlit Databricks App backed by Lakebase
resources/          DABs resource YAML (job + app)
src/retail_iq/      Pure-Python logic imported by tests
tests/              pytest suite (runs locally, no Spark)
```

Bundle variables: `catalog` (default `main`), `schema` (default `retail_iq`).
Dev target writes to `main.retail_iq`; prod target writes to `main.retail_iq_prod`.

---

## Databricks Apps — non-obvious constraints

**Port is always 8000.**
Databricks injects `STREAMLIT_SERVER_PORT=8000` (and matching vars for Flask,
Uvicorn, etc.) at runtime. `app.yml` must be the minimal form below — any
`--server.port` or `--server.address` flag overrides the injected vars and
causes a 502 Bad Gateway:

```yaml
command:
  - streamlit
  - run
  - app.py
```

**App service principal has no Lakebase access.**
The auto-generated SP is an unknown identity to Lakebase. Lakebase validates
tokens at the Databricks auth layer before any Postgres role check, so
`CREATE ROLE` + Postgres `GRANT` alone are insufficient.

Current solution: the SP fetches the owner's PAT from Databricks Secrets
(`scope=retail-insights`, `key=lakebase-token`), then constructs a user-level
`WorkspaceClient` to generate Lakebase credentials.

**SDK auth conflict when building the user-level client.**
`WorkspaceClient(token=pat)` throws *"more than one authorization method
configured: oauth and pat"* when `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`
are present in the environment. Pop them before the constructor call and restore
immediately after (see `_init_sdk()` in `apps/retail_insights/app.py`).

**Secret scope setup (one-time, already done):**
```bash
databricks secrets create-scope retail-insights
databricks secrets put-secret retail-insights lakebase-token --string-value <PAT>
databricks secrets put-acl retail-insights <SP_CLIENT_ID> READ
```
SP client ID is `b5ccc2cc-1617-4996-b642-8f3028915c51`.
PAT has a 90-day TTL — regenerate and update the secret before expiry.

---

## PostgreSQL type compatibility

Spark syncs float columns as `double precision`. PostgreSQL's two-argument
`ROUND(x, n)` does **not** accept `double precision` — cast first:

```sql
-- wrong:  ROUND(SUM(col), 2)
-- right:  ROUND(SUM(col)::numeric, 2)
```

Apply `::numeric` to the full expression inside every `ROUND(…, n)` call
in `apps/retail_insights/app.py`.

---

## Serverless notebook constraints

- **No `cache()`** — in-memory caching is unsupported on serverless.
- **No `savefig` to `/tmp`** — `/tmp` may be owned by a prior container's UID;
  writes fail with `PermissionError`. Use `plt.show()` only.
- **No `dbutils.fs.mkdirs("file:/tmp/…")`** — blocked under Unity Catalog serverless.
- **Old SDK** — serverless ships a `databricks-sdk` that predates `w.postgres`.
  Any notebook calling `w.postgres.*` must begin with:
  ```python
  # MAGIC %pip install databricks-sdk>=0.81.0 --quiet
  ```
  followed by `dbutils.library.restartPython()`.

---

## Pipeline task order

```
setup → data_generation → silver_transforms → gold_analytics ─┬→ run_tests → lakebase_sync
                                                               ├→ visualization
                                                               └→ dashboard
```

`run_tests` gates before `lakebase_sync` — tests must pass before data reaches
Postgres. Do not move `lakebase_sync` to run in parallel with or before `run_tests`.

---

## Key data integrity rule

`unit_price` in `02_data_generation.py` is derived from the product catalog:
```python
price_map = {p["product_id"]: p["price"] for p in products}
unit_price = round(price_map[order["product_id"]] * random.uniform(0.95, 1.0), 2)
```
Never use `random.uniform(10, 500)` — that makes `gold_product_performance.gross_profit`
meaningless.

---

## Deployment

```bash
databricks bundle deploy
databricks bundle run retail_pipeline   # pipeline; all 8 tasks should go green
databricks bundle run retail_insights   # app; prints the URL when ready
```
