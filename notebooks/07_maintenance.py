# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 07 · Maintenance
# MAGIC
# MAGIC Runs `OPTIMIZE` and `ANALYZE` on all Gold tables.  Intentionally separated
# MAGIC from the main pipeline (notebook 04) so file compaction does not block the
# MAGIC critical path for visualization and Lakebase sync.
# MAGIC
# MAGIC Scheduled daily at 02:00 UTC via `retail_maintenance_job`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

GOLD_TABLES = sorted(
    t.name for t in spark.catalog.listTables(f"{catalog}.{schema}")
    if t.name.startswith("gold_")
)

print(f"Running OPTIMIZE + ANALYZE on {len(GOLD_TABLES)} Gold tables …")
for t in GOLD_TABLES:
    spark.sql(f"OPTIMIZE {tbl(t)}")
    spark.sql(f"ANALYZE TABLE {tbl(t)} COMPUTE STATISTICS")
    print(f"  ✓ {t}")

print("Maintenance complete.")
