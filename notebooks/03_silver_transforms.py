# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 · Silver: Clean & Validate
# MAGIC
# MAGIC Reads Bronze tables, applies data-quality rules, adds audit metadata, and
# MAGIC writes idempotent Silver tables using the Delta `merge` API.
# MAGIC
# MAGIC **Why MERGE instead of overwrite?**
# MAGIC Overwrite re-processes every row on every run.  MERGE only touches rows that
# MAGIC changed, making incremental runs cheaper and giving us a clean audit trail.
# MAGIC At 1 000× volume this difference is the gap between minutes and hours.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: Customers

# COMMAND ----------

customers = (
    spark.table(tbl("bronze_customers"))
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("email").contains("@"))        # reject malformed emails
    .filter(F.col("age").between(18, 100))
    .withColumn("_load_ts", LOAD_TS)
    .withColumn("_source",  F.lit("bronze"))
)

counts = {}
counts["silver_customers"] = upsert(
    customers, "silver_customers", "customer_id",
    cluster_cols=["customer_id"]
)
print(f"✓ silver_customers: {counts['silver_customers']:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: Products

# COMMAND ----------

products = (
    spark.table(tbl("bronze_products"))
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("cost") > 0)
    .filter(F.col("price") > F.col("cost"))      # price must exceed cost
    .withColumn("margin_pct",
                F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
    .withColumn("_load_ts", LOAD_TS)
    .withColumn("_source",  F.lit("bronze"))
)

counts["silver_products"] = upsert(
    products, "silver_products", "product_id",
    cluster_cols=["product_id", "category"]
)
print(f"✓ silver_products: {counts['silver_products']:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: Stores

# COMMAND ----------

stores = (
    spark.table(tbl("bronze_stores"))
    .filter(F.col("store_id").isNotNull())
    .withColumn("_load_ts", LOAD_TS)
    .withColumn("_source",  F.lit("bronze"))
)

counts["silver_stores"] = upsert(
    stores, "silver_stores", "store_id",
    cluster_cols=["store_id", "region"]
)
print(f"✓ silver_stores: {counts['silver_stores']:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: Orders

# COMMAND ----------

orders = (
    spark.table(tbl("bronze_orders"))
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("order_date").isNotNull())
    .filter(F.col("status").isin("completed", "returned", "cancelled"))
    .withColumn("_load_ts", LOAD_TS)
    .withColumn("_source",  F.lit("bronze"))
)

counts["silver_orders"] = upsert(
    orders, "silver_orders", "order_id",
    cluster_cols=["order_date", "customer_id"]
)
print(f"✓ silver_orders: {counts['silver_orders']:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: Order Items
# MAGIC
# MAGIC `line_total` is derived here rather than carried from Bronze, so it is always
# MAGIC consistent with the stored `unit_price`, `quantity`, and `discount` values.

# COMMAND ----------

order_items = (
    spark.table(tbl("bronze_order_items"))
    .filter(F.col("order_item_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    .filter(F.col("discount").between(0, 1))
    .withColumn("line_total",
                F.round(F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")), 2))
    .withColumn("_load_ts", LOAD_TS)
    .withColumn("_source",  F.lit("bronze"))
)

counts["silver_order_items"] = upsert(
    order_items, "silver_order_items", "order_item_id",
    cluster_cols=["order_id", "product_id"]
)
print(f"✓ silver_order_items: {counts['silver_order_items']:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Report
# MAGIC
# MAGIC Silver counts come from the upsert pass (no re-scan).
# MAGIC Bronze counts are fetched once here for the pass-rate calculation.

# COMMAND ----------

pairs = [
    ("bronze_customers",   "silver_customers"),
    ("bronze_products",    "silver_products"),
    ("bronze_stores",      "silver_stores"),
    ("bronze_orders",      "silver_orders"),
    ("bronze_order_items", "silver_order_items"),
]

print(f"{'Table':<30} {'Bronze':>10} {'Silver':>10} {'Pass %':>8}")
print("-" * 60)
for b, s in pairs:
    bc = spark.table(tbl(b)).count()   # Bronze scan (needed for DQ ratio)
    sc = counts[s]                     # Silver count from upsert — no re-scan
    print(f"  {s:<28} {bc:>10,} {sc:>10,} {sc/bc*100:>7.1f}%")
