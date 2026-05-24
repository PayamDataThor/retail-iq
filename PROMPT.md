# RetailIQ — Generation Prompt

Build a retail analytics solution on Databricks that demonstrates the full
Lakehouse platform: data engineering, governance, BI, AI, and a custom app.
Work iteratively — propose options before deciding, implement in small
deployable increments, debug against the live environment, and optimize only
after each layer works.

---

## What to build

A Bronze→Silver→Gold medallion pipeline (DABs serverless job) that generates
synthetic retail data, computes KPIs and RFM segments, and serves insights
through four complementary layers:

| Layer | Technology | Audience |
|---|---|---|
| Governed catalog | Unity Catalog — tags, PII masking, comments | Data governance |
| Self-serve BI | Databricks AI/BI Dashboard | Analysts |
| Natural language | Genie Space on Gold tables | Business users |
| Operational app | Streamlit Databricks App via Lakebase | Embedded / ops |

These four layers together show the SA story: not just a pipeline, but a
governed, queryable, AI-enabled Lakehouse.

---

## Non-negotiable constraints

- Databricks Asset Bundles — all resources in YAML, deployed with `databricks bundle`
- Serverless compute (`client: "2"`) throughout
- Unity Catalog (`main` catalog, `retail_iq` schema)
- Lakebase as the serving layer for the Streamlit app — not SQL warehouse

---

## Repo structure

```
retail-iq/
├── databricks.yml
├── CLAUDE.md
├── resources/
│   ├── retail_pipeline_job.yml
│   └── retail_insights_app.yml
├── notebooks/
│   ├── 00_utils.py          # shared tbl(), upsert(), LOAD_TS
│   ├── 01_setup.py          # catalog/schema + UC governance setup
│   ├── 02_data_generation.py
│   ├── 03_silver_transforms.py
│   ├── 04_gold_analytics.py
│   ├── 05_visualization.py
│   ├── 06_lakebase_sync.py
│   ├── 07_maintenance.py    # OPTIMIZE + ANALYZE, runs off critical path
│   └── 08_run_tests.py
├── apps/retail_insights/
│   ├── app.py
│   ├── app.yml
│   └── requirements.txt
├── src/retail_iq/
│   ├── analytics.py         # rfm_segment(), gross_profit() — pure Python
│   └── quality.py           # DQ predicates — pure Python
└── tests/
    ├── conftest.py
    ├── test_gold_analytics.py
    └── test_silver_transforms.py
```

---

## Pipeline job

All tasks use `environment_key: serverless`, `client: "2"`, `max_retries: 2`,
`min_retry_interval_millis: 30000`, `timeout_seconds: 600`.
`lakebase_sync` gets `timeout_seconds: 900`.

Task DAG:
```
setup → data_generation → silver_transforms → gold_analytics ─┬→ run_tests → lakebase_sync
                                                               ├→ visualization
                                                               └→ dashboard
```

`run_tests` gates before `lakebase_sync` — tests must pass before data reaches Postgres.

---

## Notebook 00_utils.py (shared — %run'd by 02–06)

- `tbl(name)` → `` f"`{catalog}`.`{schema}`.`{name}`" ``
- `upsert(source_df, target_table, merge_key) → int` — count rows **before**
  the merge, not after (post-merge `.count()` rescans the source DF)
- `LOAD_TS = F.current_timestamp()`

---

## Notebook 01_setup.py — UC governance

Create catalog/schema, then apply governance to Silver and Gold tables:

**Column tags — mark PII at the schema level:**
```python
spark.sql(f"ALTER TABLE {tbl('silver_customers')} ALTER COLUMN email    SET TAGS ('pii' = 'true')")
spark.sql(f"ALTER TABLE {tbl('silver_customers')} ALTER COLUMN full_name SET TAGS ('pii' = 'true')")
```

**Column mask — non-privileged principals see redacted values:**
```sql
CREATE OR REPLACE FUNCTION main.retail_iq.mask_pii(val STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('retail_iq_analysts') THEN val ELSE '****' END;

ALTER TABLE silver_customers ALTER COLUMN email     SET MASK main.retail_iq.mask_pii;
ALTER TABLE silver_customers ALTER COLUMN full_name SET MASK main.retail_iq.mask_pii;
```

**Table and column comments — enable data discovery:**
```python
spark.sql(f"COMMENT ON TABLE {tbl('gold_customer_rfm')} IS 'RFM segments ...'")
spark.sql(f"ALTER TABLE {tbl('gold_customer_rfm')} ALTER COLUMN rfm_segment COMMENT 'Champions / ...'")
```

Run this after tables exist — use `ALTER TABLE IF EXISTS` so it's idempotent.

---

## Notebook 02_data_generation.py

Bronze Delta tables: `bronze_customers` (1 000), `bronze_products` (100),
`bronze_stores` (20), `bronze_orders` (10 000), `bronze_order_items`, `bronze_dates`.

**Unit price must come from the product catalog:**
```python
price_map = {p["product_id"]: p["price"] for p in products}
unit_price = round(price_map[order["product_id"]] * random.uniform(0.95, 1.0), 2)
```
Do not write `line_total` into the raw dict — compute it in `03_silver_transforms`.

---

## Notebook 03_silver_transforms.py

Validate, deduplicate, enrich Bronze → Silver using `upsert()` from `00_utils`.

Add `line_total` as a derived column on `silver_order_items`:
```python
.withColumn("line_total",
    F.round(F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")), 2))
```

Cluster Silver tables on join keys:

| Table | Cluster columns |
|---|---|
| `silver_orders` | `order_date, customer_id` |
| `silver_order_items` | `order_id, product_id` |
| `silver_customers` | `customer_id` |
| `silver_products` | `product_id, category` |
| `silver_stores` | `store_id, region` |

---

## Notebook 04_gold_analytics.py

Build four Gold CTAS tables. Create a temp view first to avoid scanning the
two largest tables four times:

```sql
CREATE OR REPLACE TEMP VIEW enriched_items AS
SELECT o.order_id, o.customer_id, o.store_id, o.order_date,
       oi.product_id, oi.quantity, oi.unit_price, oi.discount, oi.line_total
FROM {silver_orders} o
JOIN {silver_order_items} oi ON o.order_id = oi.order_id
WHERE o.status = 'completed'
```

All four Gold queries use `FROM enriched_items`.
Enable CDF: `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` in every CTAS.
`REFERENCE_DATE` for RFM = `date_add(MAX(order_date), 1)` — never hardcoded.
No `ORDER BY` in any CTAS. No `OPTIMIZE` here (see `07_maintenance.py`).

**Attach Lakehouse Monitor to `gold_customer_rfm` after creation:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType

w = WorkspaceClient()
w.quality_monitors.create(
    table_name=f"main.retail_iq.gold_customer_rfm",
    inference_log=MonitorInferenceLog(
        problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
        prediction_col="rfm_segment",
        timestamp_col="load_ts",
        model_id_col="rfm_score",
    ),
    assets_dir=f"/Workspace/Users/{w.current_user.me().user_name}/retail-iq/monitors",
    output_schema_name="main.retail_iq",
)
```
This auto-generates data quality and drift metrics tables — zero additional code.

---

## Notebook 06_lakebase_sync.py

Spark JDBC write to staging + atomic SQLAlchemy rename. Never `toPandas()` +
`to_sql()` (driver OOM, row-per-round-trip, table downtime on replace).

```python
# 1. Parallel JDBC write to _staging tables
for table_name in GOLD_TABLES:
    (spark.table(tbl(table_name))
     .write.format("jdbc")
     .option("url", f"jdbc:postgresql://{host}:5432/databricks_postgres?sslmode=require")
     .option("dbtable", f"public.{table_name}_staging")
     .option("user", username).option("password", token)
     .option("batchsize", "10000").option("numPartitions", "4")
     .mode("overwrite").save())

# 2. Atomic rename — zero downtime
with engine.begin() as conn:
    for table_name in GOLD_TABLES:
        conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name}"))
        conn.execute(text(f"ALTER TABLE public.{table_name}_staging RENAME TO {table_name}"))
```

`GOLD_TABLES` discovered dynamically from the catalog — not hardcoded.
Auth: `WorkspaceClient()` in a notebook uses the notebook owner's identity.

---

## AI/BI Dashboard

After the pipeline runs, create a Databricks AI/BI dashboard on the Gold tables.
Use the SDK to provision it programmatically (so it deploys with the bundle):

```python
# In a setup notebook or bundle resource
w.lakeview.create(
    display_name="RetailIQ Executive Dashboard",
    serialized_dashboard=json.dumps({...}),  # dashboard spec
)
```

Cover the same four areas as the Streamlit app: revenue trends, store KPIs,
product performance, RFM distribution. Position the two as complementary:
AI/BI for analysts who self-serve inside the workspace; Streamlit for the
embedded operational use case outside it.

---

## Genie Space

Create a Genie space over the Gold tables so business users can ask natural
language questions without writing SQL:

```python
# Provision via SDK after pipeline succeeds
w.genie.create_space(
    title="RetailIQ Genie",
    description="Ask questions about retail revenue, customers, and products",
    warehouse_id=warehouse_id,
    table_identifiers=[
        "main.retail_iq.gold_revenue_by_category_month",
        "main.retail_iq.gold_store_kpis",
        "main.retail_iq.gold_product_performance",
        "main.retail_iq.gold_customer_rfm",
    ],
)
```

Add curated questions to seed it: "Which category had the highest revenue last
month?", "Show me the top 10 stores by basket size", "How many Champions
customers do we have?". These make the demo self-guiding.

---

## Databricks App — app.yml

```yaml
command:
  - streamlit
  - run
  - app.py
```

No `--server.*` flags. Databricks injects `STREAMLIT_SERVER_PORT=8000`
automatically. Any override causes a 502.

---

## Databricks App — requirements.txt

```
streamlit>=1.35.0
plotly>=5.20.0
pandas>=2.0.0
psycopg2-binary>=2.9.0
databricks-sdk>=0.81.0
```

---

## Databricks App — auth pattern (app.py)

The app's auto-generated SP has no Lakebase access. Store the owner's PAT in
Databricks Secrets; the SP fetches it to generate Lakebase credentials.

**One-time setup:**
```bash
databricks secrets create-scope retail-insights
TOKEN=$(databricks tokens create --comment "lakebase-app" \
  --lifetime-seconds 7776000 -o json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token_value'])")
databricks secrets put-secret retail-insights lakebase-token --string-value "$TOKEN"
databricks secrets put-acl retail-insights <SP_CLIENT_ID> READ
```

**`_init_sdk()` — pop M2M vars before constructing the PAT client:**
```python
@st.cache_resource
def _init_sdk():
    import base64, os
    from databricks.sdk import WorkspaceClient

    sp_w    = WorkspaceClient()
    raw     = sp_w.secrets.get_secret(scope="retail-insights", key="lakebase-token").value
    user_pat = base64.b64decode(raw).decode()

    host = os.environ.get("DATABRICKS_HOST", "")
    _id  = os.environ.pop("DATABRICKS_CLIENT_ID",     None)
    _sec = os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
    try:
        w = WorkspaceClient(host=host, token=user_pat)
    finally:
        if _id:  os.environ["DATABRICKS_CLIENT_ID"]     = _id
        if _sec: os.environ["DATABRICKS_CLIENT_SECRET"] = _sec

    project   = os.environ.get("LAKEBASE_PROJECT", "retail-iq-db")
    branches  = list(w.postgres.list_branches(parent=f"projects/{project}"))
    prod      = next(b for b in branches if b.name.endswith("/production"))
    endpoints = list(w.postgres.list_endpoints(parent=prod.name))
    primary   = next(e for e in endpoints if "primary" in e.name)
    return w, primary.name, primary.status.hosts.host, w.current_user.me().user_name
```

---

## PostgreSQL SQL rules

Spark exports float columns as `double precision`. Cast before every `ROUND`:

```sql
ROUND(SUM(col)::numeric, 2)
ROUND(AVG(col)::numeric, 1)
ROUND((a / NULLIF(b, 0) * 100)::numeric, 1)
```

---

## App content (four tabs)

**Revenue** — stacked area by category × month, revenue-share donut, MoM growth dual-axis.  
**Stores** — top-15 revenue bar, basket size vs order volume scatter, region table.  
**Products** — margin % vs units bubble, revenue & profit by category, sortable table.  
**Customers** — RFM segment donut, avg spend per segment bar, radar chart, lookup table.

Sidebar: category/region multiselects, Refresh button, `st.cache_data(ttl=300)`.

---

## Deployment sequence

```bash
databricks bundle deploy
databricks bundle run retail_pipeline    # all tasks green
# run secrets setup with SP client ID from: databricks apps get retail-insights
databricks bundle run retail_insights    # prints app URL
```

## What this demonstrates as a SA

| Capability | How it shows |
|---|---|
| Data engineering | Medallion pipeline, clustering, CDF, JDBC sync |
| Governance | UC column tags, PII masking, data comments |
| Observability | Lakehouse Monitor on Gold table |
| Self-serve BI | AI/BI Dashboard for analysts |
| AI | Genie Space for natural language queries |
| Custom apps | Streamlit on Lakebase for embedded use case |
| Platform depth | DABs, serverless, secrets, Lakebase, Apps all wired together |
