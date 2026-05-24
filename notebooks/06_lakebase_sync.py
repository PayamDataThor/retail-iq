# Databricks notebook source

# COMMAND ----------
# MAGIC %pip install psycopg2-binary sqlalchemy "databricks-sdk>=0.81.0" --quiet

# COMMAND ----------
# MAGIC %md
# MAGIC # 06 · Lakebase Sync
# MAGIC
# MAGIC Provisions a Lakebase (managed Postgres) project and loads the Gold tables
# MAGIC into it so any app tier can query them over the standard Postgres wire
# MAGIC protocol — no Spark cluster, no Delta dependency.
# MAGIC
# MAGIC **Loading strategy**
# MAGIC 1. Discover Gold tables dynamically from Unity Catalog — no hardcoded list.
# MAGIC 2. Write each Gold table to a `_staging` table in Postgres using Spark JDBC
# MAGIC    (distributed, batched, no driver memory ceiling).
# MAGIC 3. Atomically rename `_staging` → live via a DDL transaction — app queries
# MAGIC    see no downtime gap.
# MAGIC
# MAGIC **Lakebase resource hierarchy**
# MAGIC ```
# MAGIC Project  retail-iq-db
# MAGIC  └── Branch  production   (auto-created)
# MAGIC       └── Endpoint  primary   (auto-created, read-write)
# MAGIC            └── Database  databricks_postgres  (default)
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog",    "main",         "Catalog")
dbutils.widgets.text("schema",     "retail_iq",    "Schema")
dbutils.widgets.text("project_id", "retail-iq-db", "Lakebase project ID")

catalog    = dbutils.widgets.get("catalog")
schema     = dbutils.widgets.get("schema")
project_id = dbutils.widgets.get("project_id")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Discover Gold Tables

# COMMAND ----------

# Derive the Gold table list from the catalog — stays in sync automatically
# as notebook 04 adds or renames tables.
GOLD_TABLES = sorted(
    t.name for t in spark.catalog.listTables(f"{catalog}.{schema}")
    if t.name.startswith("gold_")
)
print(f"Gold tables to sync ({len(GOLD_TABLES)}):")
for t in GOLD_TABLES:
    print(f"  • {t}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Create Lakebase Project

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Project, ProjectSpec

w = WorkspaceClient()

project_resource = f"projects/{project_id}"

existing = {p.name for p in w.postgres.list_projects() if p.name}

if project_resource in existing:
    print(f"✓ Using existing project: {project_id}")
else:
    print(f"Creating Lakebase project '{project_id}' …")
    op = w.postgres.create_project(
        project=Project(spec=ProjectSpec(display_name="RetailIQ Database")),
        project_id=project_id,
    )
    op.wait()
    print(f"✓ Project created: {project_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Resolve Endpoint and Generate OAuth Token

# COMMAND ----------

branches  = list(w.postgres.list_branches(parent=project_resource))
prod      = next(b for b in branches if b.name.endswith("/production"))

endpoints = list(w.postgres.list_endpoints(parent=prod.name))
primary   = next(e for e in endpoints if "primary" in e.name)

# Valid ready states: ACTIVE (serving traffic), IDLE (auto-paused, resumes on connect)
assert primary.status.current_state.value in ("ACTIVE", "IDLE"), \
    f"Endpoint not ready: {primary.status.current_state}"

host     = primary.status.hosts.host
username = w.current_user.me().user_name
token    = w.postgres.generate_database_credential(endpoint=primary.name).token

print(f"✓ Endpoint:  {primary.name}")
print(f"✓ Host:      {host}")
print(f"✓ User:      {username}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Load Gold Tables into Lakebase
# MAGIC
# MAGIC Each Gold table is written to a `_staging` copy in Postgres first, then
# MAGIC atomically renamed so live app queries see no downtime window.
# MAGIC
# MAGIC **Why pandas → to_sql instead of Spark JDBC?**
# MAGIC Databricks Serverless restricts DML to a named set of data sources; the
# MAGIC generic JDBC format (`org.postgresql.Driver`) is not on that list.  The
# MAGIC pandas path works reliably on serverless at Gold-table scale.  `chunksize`
# MAGIC and `method="multi"` batch inserts so the round-trips stay manageable.
# MAGIC On a classic cluster you could switch to `.format("jdbc")` for parallelism.

# COMMAND ----------

import urllib.parse
from sqlalchemy import create_engine, text

token_enc = urllib.parse.quote(token, safe="")
SA_URL    = (f"postgresql+psycopg2://{username}:{token_enc}"
             f"@{host}:5432/databricks_postgres?sslmode=require")
engine    = create_engine(SA_URL, pool_pre_ping=True)

CHUNK = 5_000   # rows per INSERT batch

print("Staging Gold tables …")
for table_name in GOLD_TABLES:
    pdf = spark.table(tbl(table_name)).toPandas()
    pdf.to_sql(
        f"{table_name}_staging", engine,
        schema="public", if_exists="replace",
        index=False, chunksize=CHUNK, method="multi",
    )
    print(f"  ✓ staged: {table_name} ({len(pdf):,} rows)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Atomic Rename: Staging → Live

# COMMAND ----------

print("Promoting staging tables …")
for table_name in GOLD_TABLES:
    with engine.begin() as conn:
        # Rename old live → backup (if it exists), staging → live, drop backup.
        # All three DDL statements are inside one transaction so external readers
        # always see either the old table or the new table — never neither.
        conn.execute(text(
            f"ALTER TABLE IF EXISTS public.{table_name} RENAME TO {table_name}_old"
        ))
        conn.execute(text(
            f"ALTER TABLE public.{table_name}_staging RENAME TO {table_name}"
        ))
        conn.execute(text(
            f"DROP TABLE IF EXISTS public.{table_name}_old"
        ))
    print(f"  ✓ promoted: {table_name}")

engine.dispose()
print("Done.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · (Optional) Lakebase Synced Tables
# MAGIC
# MAGIC Synced tables keep Lakebase automatically in sync with Delta — no manual
# MAGIC reload after each pipeline run.  CDF must be enabled on each Gold table
# MAGIC (notebook 01 and 04 handle this).
# MAGIC
# MAGIC **Create synced table via Databricks CLI:**
# MAGIC ```bash
# MAGIC databricks postgres create-synced-table <LAKEBASE_CATALOG>.retail_iq.gold_store_kpis \
# MAGIC   --json '{
# MAGIC     "spec": {
# MAGIC       "source_table_full_name": "main.retail_iq.gold_store_kpis",
# MAGIC       "primary_key_columns": ["store_id"],
# MAGIC       "scheduling_policy": "TRIGGERED",
# MAGIC       "branch": "projects/retail-iq-db/branches/production",
# MAGIC       "postgres_database": "databricks_postgres",
# MAGIC       "create_database_objects_if_missing": true,
# MAGIC       "new_pipeline_spec": {"storage_catalog": "main", "storage_schema": "retail_iq"}
# MAGIC     }
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC Primary keys: `gold_store_kpis` → `store_id`, `gold_product_performance` → `product_id`,
# MAGIC `gold_customer_rfm` → `customer_id`, `gold_revenue_by_category_month` → composite (year, month, category).

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 · Connection Details for the App Tier

# COMMAND ----------

print("=" * 60)
print("Lakebase Postgres — app tier connection")
print("=" * 60)
print(f"  Host:     {host}")
print(f"  Port:     5432")
print(f"  Database: databricks_postgres")
print(f"  User:     {username}")
print(f"  Auth:     w.postgres.generate_database_credential(endpoint='{primary.name}').token")
print()
print("SQLAlchemy URL (token valid 1 h — implement refresh for prod):")
print(f"  postgresql+psycopg2://{{user}}:{{token}}@{host}:5432/databricks_postgres?sslmode=require")
print()
print("Tables available:")
for t in GOLD_TABLES:
    print(f"  • {t}")
