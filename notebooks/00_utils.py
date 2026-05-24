# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 00 · Shared Utilities
# MAGIC
# MAGIC Import via `%run ./00_utils` **after** `catalog` and `schema` are set in the caller.
# MAGIC
# MAGIC Provides:
# MAGIC - `tbl(name)` — fully-qualified backtick table reference
# MAGIC - `upsert(source, target, key, cluster_cols)` — cache-backed merge with Liquid Clustering
# MAGIC - `LOAD_TS` — current timestamp constant for audit columns

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F, DataFrame

LOAD_TS = F.current_timestamp()

def tbl(name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"

def upsert(source: DataFrame, target_table: str, key: str, cluster_cols: list = None) -> int:
    """
    MERGE source into target on `key`.  Creates the target on first run, applying
    Liquid Clustering if cluster_cols is given.

    Counts source rows before the merge so callers don't need a second scan after.
    Returns the source row count.

    Note: DataFrame.cache() is not supported on Databricks Serverless, so the
    count and merge are two separate source scans.  On classic compute you could
    add source.cache() / source.unpersist() around this block to reduce to one scan.
    """
    n = source.count()
    full_table = f"{catalog}.{schema}.{target_table}"
    if spark.catalog.tableExists(full_table):
        (DeltaTable.forName(spark, full_table)
         .alias("t")
         .merge(source.alias("s"), f"t.`{key}` = s.`{key}`")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        writer = source.write.format("delta")
        if cluster_cols:
            writer = writer.clusterBy(*cluster_cols)
        writer.saveAsTable(full_table)
    return n
