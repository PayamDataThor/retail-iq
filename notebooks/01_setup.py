# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 · Setup
# MAGIC Create the Unity Catalog schema and confirm the Spark + Delta environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE `{catalog}`.`{schema}`")
print("✓ Schema ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Environment Check

# COMMAND ----------

spark.sql("""
  SELECT
    current_catalog()  AS catalog,
    current_database() AS schema,
    version()          AS spark_version
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Enable Change Data Feed on Gold Tables
# MAGIC
# MAGIC CDF is required for Lakebase Synced Tables (continuous Delta → Postgres sync).
# MAGIC This loop is idempotent: safe on first run (tables not yet created) and on
# MAGIC every subsequent run.  The `CREATE OR REPLACE TABLE … TBLPROPERTIES` in
# MAGIC notebook 04 also sets CDF, so this acts as a belt-and-suspenders guard.

# COMMAND ----------

GOLD_TABLES = [
    "gold_revenue_by_category_month",
    "gold_store_kpis",
    "gold_product_performance",
    "gold_customer_rfm",
]

for t in GOLD_TABLES:
    if spark.catalog.tableExists(f"{catalog}.{schema}.{t}"):
        spark.sql(f"""
          ALTER TABLE `{catalog}`.`{schema}`.`{t}`
          SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        print(f"  ✓ CDF enabled:  {t}")
    else:
        print(f"  – Not yet created (will be set during first Gold run): {t}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unity Catalog Governance
# MAGIC
# MAGIC Creates the PII masking function used by `silver_customers`.
# MAGIC Tags and column masks are applied in `04_gold_analytics` after tables exist.

# COMMAND ----------

# Masking function — members of retail_iq_analysts group see real values;
# everyone else sees '****'.  CREATE OR REPLACE makes this idempotent.
spark.sql(f"""
  CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.mask_pii(val STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_account_group_member('retail_iq_analysts') THEN val
    ELSE '****'
  END
""")
print("✓ PII masking function created")
