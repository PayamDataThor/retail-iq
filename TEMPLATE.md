# Databricks Lakehouse Project Template

A step-by-step guide for building a production-grade analytics platform on Databricks
using the same patterns proven in RetailIQ. Replace every `{{PLACEHOLDER}}` with your
own values before running.

---

## Step 0 — Define Your Project Before Writing Code

Answer these five questions first. Every downstream decision flows from them.

| Question | RetailIQ example | Your answer |
|---|---|---|
| What is the domain? | Retail sales | `{{DOMAIN}}` |
| What are the 3–5 key business entities? | Customer, Product, Store, Order | `{{ENTITIES}}` |
| What are the 3–5 Gold aggregations stakeholders want? | Revenue by month, Store KPIs, RFM | `{{GOLD_TABLES}}` |
| Who are the consumers? | Analysts (dashboard), Business (Genie), App (Lakebase) | `{{CONSUMERS}}` |
| What PII columns exist? | email, full_name | `{{PII_COLUMNS}}` |

Write these down — they become your table names, column masks, and Genie questions.

---

## Step 1 — Scaffold the Project

### 1a. Folder structure

```
{{PROJECT_NAME}}/
├── databricks.yml
├── pytest.ini
├── .gitignore
├── README.md
├── CLAUDE.md                       # non-obvious constraints for future sessions
├── notebooks/
│   ├── 00_utils.py
│   ├── 01_setup.py
│   ├── 02_data_generation.py       # or 02_ingest.py if using real sources
│   ├── 03_silver_transforms.py
│   ├── 04_gold_analytics.py
│   ├── 05_visualization.py
│   ├── 06_lakebase_sync.py
│   ├── 07_maintenance.py
│   ├── 08_run_tests.py
│   ├── 09_dashboard.py
│   └── 11_aibi_setup.py
├── apps/
│   └── {{PROJECT_NAME}}_insights/
│       ├── app.py
│       ├── app.yml
│       └── requirements.txt
├── resources/
│   ├── pipeline_job.yml
│   └── maintenance_job.yml
├── src/
│   └── {{PROJECT_NAME}}/
│       ├── __init__.py
│       ├── analytics.py
│       └── quality.py
└── tests/
    ├── conftest.py
    ├── test_gold_analytics.py
    └── test_silver_transforms.py
```

### 1b. `databricks.yml`

```yaml
bundle:
  name: {{PROJECT_NAME}}

variables:
  catalog:
    default: main
  schema:
    default: {{PROJECT_NAME}}

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: {{DATABRICKS_HOST}}

  prod:
    mode: production
    variables:
      schema: {{PROJECT_NAME}}_prod
    workspace:
      host: {{DATABRICKS_HOST}}

include:
  - resources/*.yml
```

### 1c. `.gitignore`

```
__pycache__/
*.pyc
*.pyo
.pytest_cache/
.databricks/
*.egg-info/
dist/
build/
.env
```

### 1d. `pytest.ini`

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
```

---

## Step 2 — Shared Utilities (`00_utils.py`)

Every notebook does `%run ./00_utils` before using these.

```python
# Databricks notebook source

# COMMAND ----------

# catalog and schema are set by the calling notebook before %run
def tbl(name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"

def upsert(source, target_table: str, key: str) -> int:
    from delta.tables import DeltaTable
    n = source.count()
    full = f"{catalog}.{schema}.{target_table}"
    if spark.catalog.tableExists(full):
        (DeltaTable.forName(spark, full).alias("t")
         .merge(source.alias("s"), f"t.{key} = s.{key}")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        source.write.format("delta").saveAsTable(full)
    return n

import pyspark.sql.functions as F
LOAD_TS = F.current_timestamp()
```

---

## Step 3 — Setup (`01_setup.py`)

Creates the schema, enables CDF on Gold tables, and sets up governance objects.

```python
# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "{{PROJECT_NAME}}")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE `{catalog}`.`{schema}`")
print("✓ Schema ready")

# COMMAND ----------
# Enable CDF on Gold tables (idempotent — safe before tables exist)

GOLD_TABLES = [
    # List your Gold table names here
    "{{GOLD_TABLE_1}}",
    "{{GOLD_TABLE_2}}",
]

for t in GOLD_TABLES:
    if spark.catalog.tableExists(f"{catalog}.{schema}.{t}"):
        spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{t}` "
                  f"SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        print(f"  ✓ CDF enabled: {t}")
    else:
        print(f"  – Not yet created: {t}")

# COMMAND ----------
# PII masking function — members of {{PROJECT_NAME}}_analysts see real values

spark.sql(f"""
  CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.mask_pii(val STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_account_group_member('admins')                    THEN val
    WHEN is_account_group_member('{{PROJECT_NAME}}_analysts') THEN val
    ELSE '****'
  END
""")
print("✓ mask_pii function created")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists, ResourceConflict

w = WorkspaceClient()
try:
    group = w.groups.create(display_name="{{PROJECT_NAME}}_analysts")
    print(f"✓ Group created (id={group.id})")
except (ResourceAlreadyExists, ResourceConflict):
    print("– Group already exists")
```

---

## Step 4 — Data Generation / Ingestion (`02_data_generation.py`)

**If generating synthetic data:**

```python
# COMMAND ----------
# MAGIC %pip install faker --quiet

# COMMAND ----------

dbutils.widgets.text("catalog",       "main")
dbutils.widgets.text("schema",        "{{PROJECT_NAME}}")
dbutils.widgets.text("num_records",   "10000")

catalog     = dbutils.widgets.get("catalog")
schema      = dbutils.widgets.get("schema")
num_records = int(dbutils.widgets.get("num_records"))

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

from faker import Faker
import random
fake = Faker()
random.seed(42)

# ── Build your entities ───────────────────────────────────────────────────────
# KEY RULE: any numeric field used in a Gold "profit" or "margin" calculation
# must be derived from a consistent catalog/lookup, not random.uniform().
# Example: unit_price = price_map[product_id] * random.uniform(0.95, 1.0)

{{ENTITY_ROWS}} = [
    {
        "{{ID_FIELD}}": i,
        # ... your fields
    }
    for i in range(1, num_records + 1)
]

# COMMAND ----------
# Write Bronze tables (schema-on-read — no DQ yet)

spark.createDataFrame({{ENTITY_ROWS}}).write.format("delta") \
    .mode("overwrite").saveAsTable(tbl("bronze_{{ENTITY}}"))
print(f"✓ bronze_{{ENTITY}}: {len({{ENTITY_ROWS}}):,} rows")
```

**If ingesting from a real source (Auto Loader pattern):**

```python
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")          # or csv, parquet
 .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/checkpoints/{{ENTITY}}_schema")
 .load("{{SOURCE_PATH}}")
 .writeStream
 .format("delta")
 .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/checkpoints/{{ENTITY}}")
 .trigger(availableNow=True)
 .toTable(tbl("bronze_{{ENTITY}}")))
```

---

## Step 5 — Silver Transforms (`03_silver_transforms.py`)

One pattern for every entity: validate → clean → upsert.

```python
# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "{{PROJECT_NAME}}")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F

# ── {{ENTITY}} ────────────────────────────────────────────────────────────────
raw = spark.table(tbl("bronze_{{ENTITY}}"))

silver = (raw
    # Drop nulls on key columns
    .dropna(subset=["{{ID_FIELD}}", "{{REQUIRED_FIELD}}"])
    # Deduplicate — keep latest by a monotonic key
    .withColumn("_rank", F.row_number().over(
        Window.partitionBy("{{ID_FIELD}}").orderBy(F.col("{{TIMESTAMP_OR_ID}}").desc())
    ))
    .filter(F.col("_rank") == 1).drop("_rank")
    # Domain validation — adjust predicates to your rules
    .filter(F.col("{{NUMERIC_FIELD}}") > 0)
    .withColumn("_load_ts", LOAD_TS)
)

n = upsert(silver, "silver_{{ENTITY}}", "{{ID_FIELD}}")
print(f"✓ silver_{{ENTITY}}: {n:,}")
```

**DQ predicates to keep in `src/{{PROJECT_NAME}}/quality.py`** (testable without Spark):

```python
def is_valid_{{ENTITY}}(row: dict) -> bool:
    """Return True if the row passes all Silver DQ rules."""
    if not row.get("{{ID_FIELD}}"):
        return False
    if row.get("{{NUMERIC_FIELD}}", 0) <= 0:
        return False
    # add your rules
    return True
```

---

## Step 6 — Gold Analytics (`04_gold_analytics.py`)

**Always start with a shared base view** — one physical scan, four logical references.

```python
# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "{{PROJECT_NAME}}")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------
# Shared base view — join your two largest fact tables once

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW base_facts AS
  SELECT
    -- include all columns needed by Gold queries
    f.{{FACT_ID}},
    f.{{DIMENSION_FK}},
    f.{{METRIC_1}},
    f.{{METRIC_2}}
  FROM {tbl('silver_{{FACT_TABLE}}')} f
  WHERE f.{{STATUS_FIELD}} = '{{VALID_STATUS}}'   -- filter once here
""")

# COMMAND ----------
# Gold table 1 — {{AGGREGATION_1_DESCRIPTION}}

spark.sql(f"""
  CREATE OR REPLACE TABLE {tbl('gold_{{AGGREGATION_1}}')}
  CLUSTER BY ({{CLUSTER_COLS}})
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  AS
  SELECT
    d.{{DIMENSION_COL}},
    COUNT(DISTINCT bf.{{FACT_ID}})         AS num_records,
    ROUND(SUM(bf.{{METRIC_1}}), 2)         AS total_{{METRIC_1}},
    ROUND(AVG(bf.{{METRIC_2}}), 2)         AS avg_{{METRIC_2}}
  FROM base_facts bf
  JOIN {tbl('silver_{{DIMENSION_TABLE}}')} d ON bf.{{DIMENSION_FK}} = d.{{DIMENSION_PK}}
  GROUP BY d.{{DIMENSION_COL}}
""")

# COMMAND ----------
# UC Governance — apply AFTER all tables exist

# PII tags (for lineage discovery in UC catalog)
for col in ({{PII_COLUMNS}}):
    spark.sql(f"ALTER TABLE {tbl('silver_{{PII_TABLE}}')} ALTER COLUMN {col} SET TAGS ('pii' = 'true')")

# Drop any residual masks on internal tables (idempotent)
for col in ({{PII_COLUMNS}}):
    try:
        spark.sql(f"ALTER TABLE {tbl('silver_{{PII_TABLE}}')} ALTER COLUMN {col} DROP MASK")
    except Exception:
        pass

# Column masks on the analyst-facing Gold table (not Silver)
for col in ({{PII_COLUMNS}}):
    spark.sql(f"""
      ALTER TABLE {tbl('gold_{{SERVING_TABLE}}')}
      ALTER COLUMN {col}
      SET MASK `{catalog}`.`{schema}`.mask_pii
    """)

# Table comments
table_comments = {
    "gold_{{AGGREGATION_1}}": "{{DESCRIPTION_1}}",
}
for t, comment in table_comments.items():
    safe = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {tbl(t)} IS '{safe}'")

# COMMAND ----------
# Lakehouse Monitor on your primary serving table

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot
from databricks.sdk.errors import ResourceAlreadyExists

w        = WorkspaceClient()
username = w.current_user.me().user_name

try:
    w.quality_monitors.create(
        table_name=f"{catalog}.{schema}.gold_{{SERVING_TABLE}}",
        assets_dir=f"/Workspace/Users/{username}/{{PROJECT_NAME}}/monitors",
        output_schema_name=f"{catalog}.{schema}",
        snapshot=MonitorSnapshot(),
    )
    print("✓ Lakehouse Monitor created")
except ResourceAlreadyExists:
    print("– Monitor already exists")
```

---

## Step 7 — Lakebase Sync (`06_lakebase_sync.py`)

```python
# COMMAND ----------
# MAGIC %pip install databricks-sdk>=0.81.0 --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "{{PROJECT_NAME}}")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

import base64, os
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

w        = WorkspaceClient()
username = w.current_user.me().user_name

# Fetch PAT from secrets — same pattern as the app
raw_b64  = w.secrets.get_secret(scope="{{SECRET_SCOPE}}", key="lakebase-token").value
user_pat = base64.b64decode(raw_b64).decode()
host_env = os.environ.get("DATABRICKS_HOST", "")

_id  = os.environ.pop("DATABRICKS_CLIENT_ID", None)
_sec = os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
try:
    user_w = WorkspaceClient(host=host_env, token=user_pat)
finally:
    if _id:  os.environ["DATABRICKS_CLIENT_ID"]     = _id
    if _sec: os.environ["DATABRICKS_CLIENT_SECRET"] = _sec

project   = "{{LAKEBASE_PROJECT_NAME}}"
branches  = list(user_w.postgres.list_branches(parent=f"projects/{project}"))
prod      = next(b for b in branches if b.name.endswith("/production"))
endpoints = list(user_w.postgres.list_endpoints(parent=prod.name))
primary   = next(e for e in endpoints if "primary" in e.name)
pg_host   = primary.status.hosts.host
token_enc = quote_plus(user_pat)

# COMMAND ----------
# Dynamic Gold table list — excludes monitor output tables

GOLD_TABLES = sorted(
    t.name for t in spark.catalog.listTables(f"{catalog}.{schema}")
    if t.name.startswith("gold_")
    and "_profile_metrics" not in t.name
    and "_drift_metrics"   not in t.name
)
print(f"Tables to sync: {GOLD_TABLES}")

# COMMAND ----------
# Write to staging tables, then atomic rename

jdbc_url = f"jdbc:postgresql://{pg_host}:5432/databricks_postgres?sslmode=require"

for table_name in GOLD_TABLES:
    (spark.table(tbl(table_name))
     .write.format("jdbc")
     .option("url",           jdbc_url)
     .option("dbtable",       f"public.{table_name}_staging")
     .option("user",          username)
     .option("password",      user_pat)
     .option("batchsize",     "10000")
     .option("numPartitions", "4")
     .mode("overwrite")
     .save())
    print(f"  ✓ {table_name} staged")

engine = create_engine(
    f"postgresql+psycopg2://{username}:{token_enc}@{pg_host}:5432/databricks_postgres?sslmode=require"
)
with engine.begin() as conn:
    for table_name in GOLD_TABLES:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{table_name}"'))
        conn.execute(text(f'ALTER TABLE public."{table_name}_staging" RENAME TO "{table_name}"'))
        print(f"  ✓ {table_name} promoted")
engine.dispose()
```

---

## Step 8 — Integration Tests (`08_run_tests.py`)

Write one test per invariant you care about. Minimum required set:

```python
# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "{{PROJECT_NAME}}")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

from pyspark.sql import functions as F

results = []

def run_test(name, fn):
    try:
        fn()
        results.append(("✓", name)); print(f"  ✓  {name}")
    except Exception as exc:
        results.append(("✗", name)); print(f"  ✗  {name}\n       {exc}")

# ── Silver tests ──────────────────────────────────────────────────────────────

def test_silver_{{ENTITY}}_non_empty():
    assert spark.table(tbl("silver_{{ENTITY}}")).count() > 0

def test_silver_{{ENTITY}}_no_null_ids():
    nulls = spark.table(tbl("silver_{{ENTITY}}")).filter(F.col("{{ID_FIELD}}").isNull()).count()
    assert nulls == 0, f"{nulls} null IDs"

def test_silver_{{ENTITY}}_pass_rate():
    bronze = spark.table(tbl("bronze_{{ENTITY}}")).count()
    silver = spark.table(tbl("silver_{{ENTITY}}")).count()
    assert silver / bronze >= 0.95, f"Pass rate {silver/bronze:.1%} < 95%"

# ── Gold tests ────────────────────────────────────────────────────────────────

def test_gold_{{AGGREGATION_1}}_non_empty():
    assert spark.table(tbl("gold_{{AGGREGATION_1}}")).count() > 0

def test_gold_{{AGGREGATION_1}}_metrics_positive():
    bad = spark.table(tbl("gold_{{AGGREGATION_1}}")).filter(F.col("{{METRIC_COL}}") <= 0).count()
    assert bad == 0, f"{bad} rows with non-positive {{METRIC_COL}}"

def test_gold_cdf_enabled():
    props = spark.sql(f"SHOW TBLPROPERTIES {tbl('gold_{{AGGREGATION_1}}')}").collect()
    d = {r["key"]: r["value"] for r in props}
    assert d.get("delta.enableChangeDataFeed") == "true", "CDF not enabled"

for fn in [
    test_silver_{{ENTITY}}_non_empty,
    test_silver_{{ENTITY}}_no_null_ids,
    test_silver_{{ENTITY}}_pass_rate,
    test_gold_{{AGGREGATION_1}}_non_empty,
    test_gold_{{AGGREGATION_1}}_metrics_positive,
    test_gold_cdf_enabled,
]:
    run_test(fn.__name__, fn)

# COMMAND ----------

passed = sum(1 for s, _ in results if s == "✓")
failed = sum(1 for s, _ in results if s == "✗")
print(f"\n{'='*50}\n  {passed}/{len(results)} tests passed\n{'='*50}")
assert failed == 0, f"{failed} integration test(s) failed"
```

---

## Step 9 — AI/BI Dashboard + Genie Space (`11_aibi_setup.py`)

```python
# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "{{PROJECT_NAME}}")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
warehouses = [wh for wh in w.warehouses.list() if wh.state and wh.state.value == "RUNNING"] \
             or list(w.warehouses.list())
if not warehouses:
    raise RuntimeError("No SQL warehouses found")
warehouse = warehouses[0]
host = w.config.host.rstrip("/")

GOLD_TABLES = [
    f"{catalog}.{schema}.gold_{{AGGREGATION_1}}",
    f"{catalog}.{schema}.gold_{{AGGREGATION_2}}",
    # add all your Gold tables
]

# COMMAND ----------
# AI/BI Dashboard ─────────────────────────────────────────────────────────────

DASHBOARD_NAME = f"{{PROJECT_DISPLAY_NAME}} Dashboard [{schema}]"

dashboard_spec = {
    "datasets": [
        {
            "name": "ds_{{AGGREGATION_1}}",
            "displayName": "{{AGGREGATION_1_LABEL}}",
            "query": f"SELECT * FROM {catalog}.{schema}.gold_{{AGGREGATION_1}}",
        },
        # add more datasets
    ],
    "pages": [
        {
            "name": "{{PAGE_1}}",
            "displayName": "{{PAGE_1_LABEL}}",
            "layout": [
                {
                    "widget": {
                        "name": "{{WIDGET_NAME}}",
                        "queries": [{"name": "q", "query": {
                            "datasetName": "ds_{{AGGREGATION_1}}",
                            # IMPORTANT: every field needs BOTH "name" AND "expression"
                            "fields": [
                                {"name": "{{COL_1}}", "expression": "{{COL_1}}"},
                                {"name": "{{COL_2}}", "expression": "{{COL_2}}"},
                            ],
                            "disaggregated": False
                        }}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",    # bar | line | pie | counter | table
                            "encodings": {
                                "x":     {"fieldName": "{{COL_1}}", "scale": {"type": "categorical"}},
                                "y":     {"fieldName": "{{COL_2}}", "scale": {"type": "quantitative"}},
                                "color": {"fieldName": "{{COL_3}}", "scale": {"type": "categorical"}},
                            },
                            "frame": {"title": "{{CHART_TITLE}}", "showTitle": True},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 6},
                },
            ],
        },
    ],
}

existing = next(
    (d for d in w.api_client.do("GET", "/api/2.0/lakeview/dashboards").get("dashboards", [])
     if d.get("display_name") == DASHBOARD_NAME), None
)
if existing:
    dashboard_id = existing["dashboard_id"]
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body={
        "display_name": DASHBOARD_NAME,
        "serialized_dashboard": json.dumps(dashboard_spec),
        "warehouse_id": warehouse.id,
    })
    print(f"✓ Dashboard updated")
else:
    result = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body={
        "display_name":         DASHBOARD_NAME,
        "serialized_dashboard": json.dumps(dashboard_spec),
        "warehouse_id":         warehouse.id,
    })
    dashboard_id = result["dashboard_id"]
    print(f"✓ Dashboard created")

w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", body={
    "warehouse_id": warehouse.id, "embed_credentials": False,
})
print(f"  Dashboard URL: {host}/dashboardsv3/{dashboard_id}")

# COMMAND ----------
# Genie Space ─────────────────────────────────────────────────────────────────
# serialized_space rules (non-obvious — not in official docs):
#   • version must be 2
#   • tables field is "identifier" (not "table_identifier")
#   • tables must be sorted alphabetically by identifier
#   • sample_questions.question is an ARRAY of strings

GENIE_TITLE = f"{{PROJECT_DISPLAY_NAME}} Genie [{schema}]"

SAMPLE_QUESTIONS = [
    "{{SAMPLE_QUESTION_1}}",
    "{{SAMPLE_QUESTION_2}}",
    "{{SAMPLE_QUESTION_3}}",
    # 5-10 questions that represent what users actually ask
]

existing_spaces = (w.api_client.do("GET", "/api/2.0/genie/spaces")
                   .get("spaces") or [])
existing_genie  = next((s for s in existing_spaces if s.get("title") == GENIE_TITLE), None)

if existing_genie:
    space_id = existing_genie["space_id"]
    print(f"– Genie space already exists (id={space_id})")
else:
    serialized_space = json.dumps({
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": f"q{str(i).zfill(31)}", "question": [q]}
                for i, q in enumerate(SAMPLE_QUESTIONS, 1)
            ]
        },
        "data_sources": {
            "tables": [{"identifier": t} for t in sorted(GOLD_TABLES)]
        }
    }, separators=(',', ':'))

    result = w.api_client.do("POST", "/api/2.0/genie/spaces", body={
        "title":            GENIE_TITLE,
        "description":      "{{GENIE_DESCRIPTION}}",
        "warehouse_id":     warehouse.id,
        "serialized_space": serialized_space,
    })
    space_id = result.get("space_id") or result.get("id")
    print(f"✓ Genie space created")

print(f"  Genie URL: {host}/genie/spaces/{space_id}")
```

---

## Step 10 — Streamlit App (`apps/{{PROJECT_NAME}}_insights/`)

### `app.yml` — always minimal, never add flags

```yaml
command:
  - streamlit
  - run
  - app.py
```

### `requirements.txt`

```
streamlit>=1.35.0
plotly>=5.20.0
pandas>=2.0.0
psycopg2-binary>=2.9.0
databricks-sdk>=0.81.0
```

### `app.py` — Lakebase auth pattern

```python
import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_resource
def _init_sdk():
    import base64, os
    from databricks.sdk import WorkspaceClient

    # Step 1: SP fetches PAT from secrets
    sp_w     = WorkspaceClient()
    raw_b64  = sp_w.secrets.get_secret(scope="{{SECRET_SCOPE}}", key="lakebase-token").value
    user_pat = base64.b64decode(raw_b64).decode()
    host_env = os.environ.get("DATABRICKS_HOST", "")

    # Step 2: pop M2M env vars to avoid "more than one auth method" error
    _id  = os.environ.pop("DATABRICKS_CLIENT_ID", None)
    _sec = os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
    try:
        w = WorkspaceClient(host=host_env, token=user_pat)
    finally:
        if _id:  os.environ["DATABRICKS_CLIENT_ID"]     = _id
        if _sec: os.environ["DATABRICKS_CLIENT_SECRET"] = _sec

    # Step 3: resolve Lakebase endpoint
    project   = "{{LAKEBASE_PROJECT_NAME}}"
    branches  = list(w.postgres.list_branches(parent=f"projects/{project}"))
    prod      = next(b for b in branches if b.name.endswith("/production"))
    endpoints = list(w.postgres.list_endpoints(parent=prod.name))
    primary   = next(e for e in endpoints if "primary" in e.name)
    return w, primary.status.hosts.host, w.current_user.me().user_name

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    import psycopg2
    _, pg_host, username = _init_sdk()
    from databricks.sdk import WorkspaceClient
    import base64, os
    sp_w     = WorkspaceClient()
    raw_b64  = sp_w.secrets.get_secret(scope="{{SECRET_SCOPE}}", key="lakebase-token").value
    user_pat = base64.b64decode(raw_b64).decode()
    conn = psycopg2.connect(
        host=pg_host, port=5432, dbname="databricks_postgres",
        user=username, password=user_pat, sslmode="require"
    )
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# ── UI ────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="{{PROJECT_DISPLAY_NAME}}", layout="wide")
st.title("{{PROJECT_DISPLAY_NAME}} Insights")

# IMPORTANT: PostgreSQL ROUND requires ::numeric cast for float columns
# Wrong:  ROUND(SUM(metric), 2)
# Right:  ROUND(SUM(metric)::numeric, 2)

try:
    df = query("""
        SELECT {{DIMENSION}},
               ROUND(SUM({{METRIC}})::numeric, 2) AS total_{{METRIC}}
        FROM public.gold_{{AGGREGATION_1}}
        GROUP BY {{DIMENSION}}
        ORDER BY total_{{METRIC}} DESC
    """)
    fig = px.bar(df, x="{{DIMENSION}}", y="total_{{METRIC}}", title="{{CHART_TITLE}}")
    st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Cannot connect: {e}")
```

---

## Step 11 — Pipeline Job (`resources/pipeline_job.yml`)

```yaml
resources:
  jobs:
    {{PROJECT_NAME}}_pipeline:
      name: "{{PROJECT_DISPLAY_NAME}} Pipeline [${bundle.target}]"

      environments:
        - environment_key: serverless
          spec:
            client: "2"

      tasks:
        - task_key: setup
          environment_key: serverless
          max_retries: 3
          min_retry_interval_millis: 30000
          timeout_seconds: 300
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/01_setup
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"

        - task_key: data_generation          # rename to "ingest" for real sources
          depends_on: [{task_key: setup}]
          environment_key: serverless
          max_retries: 3
          min_retry_interval_millis: 30000
          timeout_seconds: 600
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/02_data_generation
            base_parameters:
              catalog:     "${var.catalog}"
              schema:      "${var.schema}"
              num_records: "10000"

        - task_key: silver_transforms
          depends_on: [{task_key: data_generation}]
          environment_key: serverless
          max_retries: 3
          min_retry_interval_millis: 30000
          timeout_seconds: 600
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/03_silver_transforms
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"

        - task_key: gold_analytics
          depends_on: [{task_key: silver_transforms}]
          environment_key: serverless
          max_retries: 3
          min_retry_interval_millis: 30000
          timeout_seconds: 600
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/04_gold_analytics
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"

        - task_key: run_tests              # quality gate — must pass before sync
          depends_on: [{task_key: gold_analytics}]
          environment_key: serverless
          max_retries: 3
          timeout_seconds: 600
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/08_run_tests
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"

        - task_key: lakebase_sync          # only runs after tests pass
          depends_on: [{task_key: run_tests}]
          environment_key: serverless
          max_retries: 3
          min_retry_interval_millis: 30000
          timeout_seconds: 900
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/06_lakebase_sync
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"

        - task_key: aibi_setup             # parallel with lakebase_sync
          depends_on: [{task_key: gold_analytics}]
          environment_key: serverless
          max_retries: 3
          min_retry_interval_millis: 30000
          timeout_seconds: 300
          notebook_task:
            notebook_path: ${workspace.file_path}/notebooks/11_aibi_setup
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"

      email_notifications:
        on_failure:
          - {{YOUR_EMAIL}}
        no_alert_for_skipped_runs: true

      queue:
        enabled: true
```

---

## Step 12 — Secrets Setup (one-time, run locally)

```bash
# Create secret scope for the app
databricks secrets create-scope {{SECRET_SCOPE}}

# Store owner PAT (base64-encoded — the SDK returns base64 when reading)
echo -n "{{YOUR_PAT}}" | base64 | databricks secrets put-secret {{SECRET_SCOPE}} lakebase-token --string-value "$(echo -n '{{YOUR_PAT}}' | base64)"

# Grant app SP read access
databricks secrets put-acl {{SECRET_SCOPE}} {{APP_SP_CLIENT_ID}} READ
```

> Find the app SP client ID in the App settings page after first deploy.

---

## Step 13 — Deploy and Verify

```bash
# 1. Init git
git init && git add -A && git commit -m "Initial commit: {{PROJECT_NAME}} lakehouse"

# 2. Deploy bundle
databricks bundle deploy

# 3. Run pipeline — all tasks should go green
databricks bundle run {{PROJECT_NAME}}_pipeline

# 4. Verify Gold tables
databricks sql execute --warehouse-id {{WAREHOUSE_ID}} \
  "SELECT COUNT(*) FROM main.{{PROJECT_NAME}}.gold_{{AGGREGATION_1}}"

# 5. Deploy app
databricks bundle run {{PROJECT_NAME}}_insights

# 6. Deploy to prod
databricks bundle deploy --target prod
databricks bundle run {{PROJECT_NAME}}_pipeline --target prod
```

---

## Gotchas Reference

| Symptom | Root cause | Fix |
|---|---|---|
| App returns 502 Bad Gateway | `app.yml` has `--server.port` flag | Remove all `--server.*` flags; use minimal `app.yml` |
| `password authentication failed for user '<SP_CLIENT_ID>'` | App SP has no Lakebase access | Use secret-based PAT pattern (Step 10) |
| `more than one authorization method configured` | SDK sees both `token=` and `DATABRICKS_CLIENT_ID` env var | Pop M2M env vars before `WorkspaceClient(token=pat)`, restore in `finally` |
| `function round(double precision, integer) does not exist` | Spark exports floats as `double precision`; `ROUND(x, n)` requires `numeric` | Cast: `ROUND(expr::numeric, n)` |
| `ResourceConflict` when creating groups | SDK raises `ResourceConflict` (not `ResourceAlreadyExists`) on repeat runs | Catch both: `except (ResourceAlreadyExists, ResourceConflict)` |
| Column mask breaks Silver integration test | Mask was applied to pipeline-internal Silver table | Apply masks to Gold (serving layer) only; drop residual masks on Silver |
| `lakebase_sync` fails with `can't adapt type 'dict'` | Monitor output tables (`_profile_metrics`) have struct columns | Exclude `_profile_metrics` and `_drift_metrics` from sync table list |
| Dashboard `fields[x].expression should not be empty` | Widget query fields only have `"name"` | Every field needs both `"name"` and `"expression"` keys |
| Genie Space `Cannot find field: table_identifier` | Wrong field name in `serialized_space` | Use `"identifier"` not `"table_identifier"` |
| Genie Space `must be sorted by identifier` | Tables not in alphabetical order | Use `sorted(GOLD_TABLES)` before building the list |
| Genie Space `Expected an array for question` | `question` is a string | `question` must be a list: `["text"]` |
| `w.postgres` AttributeError in serverless | Old SDK bundled with serverless | Add `%pip install databricks-sdk>=0.81.0 --quiet` at top of notebook |

---

## Checklist

- [ ] All five planning questions answered before coding
- [ ] `unit_price` (or equivalent margin metric) derived from catalog, not `random.uniform`
- [ ] Silver DQ predicates in `src/` — testable without Spark
- [ ] `enriched_items` (or equivalent base view) created before Gold queries
- [ ] Column masks on Gold serving table, NOT on Silver
- [ ] `DROP MASK` guard before `SET TAGS` on Silver columns
- [ ] `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` on all Gold tables
- [ ] `run_tests` task set as dependency of `lakebase_sync`
- [ ] Monitor output tables excluded from sync list
- [ ] All ROUND calls use `::numeric` cast
- [ ] Dashboard widget fields have both `"name"` and `"expression"`
- [ ] Genie `serialized_space`: `version=2`, `identifier` field, sorted tables, `question` as array
- [ ] App `app.yml` is minimal — no `--server.*` flags
- [ ] Secret scope created; SP granted READ
- [ ] M2M env vars popped before user-level `WorkspaceClient` construction
- [ ] `max_retries: 3` on every task
- [ ] README updated with architecture diagram, pipeline DAG, and governance section
