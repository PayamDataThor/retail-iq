# Databricks notebook source

# COMMAND ----------
# MAGIC %pip install pytest --quiet

# COMMAND ----------
# MAGIC %md
# MAGIC # 08 · Integration Tests
# MAGIC
# MAGIC Runs Spark-dependent tests that cannot execute locally (no Java).
# MAGIC Uses the live Gold and Silver tables written by the pipeline.
# MAGIC
# MAGIC Run this notebook manually or add it as an optional pipeline task
# MAGIC after `gold_analytics` to catch regressions on every deploy.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test helpers

# COMMAND ----------

import pytest
import sys

def assert_eq(actual, expected, msg=""):
    assert actual == expected, f"{msg} — expected {expected!r}, got {actual!r}"

def assert_gt(actual, threshold, msg=""):
    assert actual > threshold, f"{msg} — expected > {threshold}, got {actual}"

def assert_df_not_empty(df, msg=""):
    n = df.count()
    assert n > 0, f"{msg} — DataFrame was empty"

PASS = "✓"
FAIL = "✗"
results = []

def run_test(name, fn):
    try:
        fn()
        results.append((PASS, name))
        print(f"  {PASS}  {name}")
    except Exception as exc:
        results.append((FAIL, name))
        print(f"  {FAIL}  {name}")
        print(f"       {exc}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver layer: row counts

# COMMAND ----------

def test_silver_customers_non_empty():
    assert_df_not_empty(spark.table(tbl("silver_customers")), "silver_customers")

def test_silver_customers_pass_rate():
    """At least 95 % of Bronze customers should pass Silver filters."""
    bc = spark.table(tbl("bronze_customers")).count()
    sc = spark.table(tbl("silver_customers")).count()
    assert sc / bc >= 0.95, f"Pass rate {sc/bc:.1%} < 95 %"

def test_silver_customers_no_null_ids():
    from pyspark.sql import functions as F
    nulls = (spark.table(tbl("silver_customers"))
             .filter(F.col("customer_id").isNull())
             .count())
    assert nulls == 0, f"{nulls} rows with null customer_id in silver_customers"

def test_silver_customers_all_emails_valid():
    from pyspark.sql import functions as F
    invalid = (spark.table(tbl("silver_customers"))
               .filter(~F.col("email").contains("@"))
               .count())
    assert invalid == 0, f"{invalid} customers with malformed email in silver_customers"

def test_silver_customers_all_ages_in_range():
    from pyspark.sql import functions as F
    out_of_range = (spark.table(tbl("silver_customers"))
                    .filter(~F.col("age").between(18, 100))
                    .count())
    assert out_of_range == 0, f"{out_of_range} customers with age outside [18, 100]"

def test_silver_products_price_exceeds_cost():
    from pyspark.sql import functions as F
    bad = (spark.table(tbl("silver_products"))
           .filter(F.col("price") <= F.col("cost"))
           .count())
    assert bad == 0, f"{bad} products where price <= cost in silver_products"

def test_silver_orders_valid_statuses_only():
    from pyspark.sql import functions as F
    bad = (spark.table(tbl("silver_orders"))
           .filter(~F.col("status").isin("completed", "returned", "cancelled"))
           .count())
    assert bad == 0, f"{bad} orders with invalid status in silver_orders"

def test_silver_order_items_line_total_correct():
    """Recomputed line_total must match stored value within 1 cent."""
    from pyspark.sql import functions as F
    bad = (spark.table(tbl("silver_order_items"))
           .withColumn("expected_lt",
                       F.round(F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")), 2))
           .filter(F.abs(F.col("line_total") - F.col("expected_lt")) > 0.01)
           .count())
    assert bad == 0, f"{bad} order items where line_total != unit_price * quantity * (1 - discount)"

def test_silver_order_items_unit_price_bounded():
    """
    unit_price must be within [0.95, 1.0] × the catalog price.
    Verifies the Step 1 fix is live in the persisted data.
    """
    from pyspark.sql import functions as F
    bad = (spark.table(tbl("silver_order_items")).alias("oi")
           .join(spark.table(tbl("silver_products")).alias("p"),
                 F.col("oi.product_id") == F.col("p.product_id"))
           .filter(
               (F.col("oi.unit_price") > F.col("p.price") * 1.001) |     # above catalog
               (F.col("oi.unit_price") < F.col("p.price") * 0.949)       # below 95 % floor
           )
           .count())
    assert bad == 0, f"{bad} order items with unit_price outside [0.95×price, 1.0×price]"

for fn in [
    test_silver_customers_non_empty, test_silver_customers_pass_rate,
    test_silver_customers_no_null_ids, test_silver_customers_all_emails_valid,
    test_silver_customers_all_ages_in_range, test_silver_products_price_exceeds_cost,
    test_silver_orders_valid_statuses_only, test_silver_order_items_line_total_correct,
    test_silver_order_items_unit_price_bounded,
]:
    run_test(fn.__name__, fn)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold layer: correctness

# COMMAND ----------

def test_gold_revenue_has_all_categories():
    cats = {row["category"] for row in spark.table(tbl("gold_revenue_by_category_month")).select("category").distinct().collect()}
    expected = {"Electronics", "Clothing", "Food & Bev", "Home & Garden", "Sports"}
    assert expected == cats, f"Missing categories: {expected - cats}"

def test_gold_revenue_gross_revenue_positive():
    from pyspark.sql import functions as F
    bad = spark.table(tbl("gold_revenue_by_category_month")).filter(F.col("gross_revenue") <= 0).count()
    assert bad == 0, f"{bad} rows with non-positive gross_revenue"

def test_gold_product_gross_profit_mostly_positive():
    """
    After the unit_price fix, the majority of products should have positive gross_profit.
    A small fraction may be negative due to high-discount / low-margin combinations.
    """
    from pyspark.sql import functions as F
    total    = spark.table(tbl("gold_product_performance")).count()
    negative = spark.table(tbl("gold_product_performance")).filter(F.col("gross_profit") <= 0).count()
    pct_bad  = negative / total if total > 0 else 1
    assert pct_bad < 0.10, f"{pct_bad:.1%} of products have non-positive gross_profit (threshold 10 %)"

def test_gold_store_kpis_revenue_equals_sum_of_order_items():
    """
    gold_store_kpis.total_revenue should equal SUM(silver_order_items.line_total)
    for completed orders, joined through silver_orders.
    """
    from pyspark.sql import functions as F

    kpi_total = (spark.table(tbl("gold_store_kpis"))
                 .agg(F.sum("total_revenue"))
                 .collect()[0][0] or 0)

    actual_total = (
        spark.table(tbl("silver_order_items")).alias("oi")
        .join(spark.table(tbl("silver_orders")).alias("o")
              .filter(F.col("status") == "completed"),
              F.col("oi.order_id") == F.col("o.order_id"))
        .agg(F.round(F.sum("oi.line_total"), 2))
        .collect()[0][0] or 0
    )

    assert abs(kpi_total - actual_total) < 1.0, (
        f"gold_store_kpis total_revenue ({kpi_total:,.2f}) differs from "
        f"silver source ({actual_total:,.2f}) by more than $1"
    )

def test_gold_rfm_covers_all_customers_with_orders():
    """Every customer who placed a completed order should appear in gold_customer_rfm."""
    from pyspark.sql import functions as F

    customers_with_orders = (
        spark.table(tbl("silver_orders"))
        .filter(F.col("status") == "completed")
        .select("customer_id").distinct().count()
    )
    rfm_customers = spark.table(tbl("gold_customer_rfm")).select("customer_id").distinct().count()
    assert rfm_customers == customers_with_orders, (
        f"RFM has {rfm_customers} customers; expected {customers_with_orders}"
    )

def test_gold_rfm_segments_are_exhaustive():
    """Every row must map to one of the 7 defined segments."""
    valid_segments = {
        "Champions", "Loyal", "Recent", "Frequent",
        "Big Spenders", "At Risk", "Needs Attention",
    }
    actual = {row["rfm_segment"] for row in spark.table(tbl("gold_customer_rfm")).select("rfm_segment").distinct().collect()}
    unknown = actual - valid_segments
    assert not unknown, f"Unknown RFM segments found: {unknown}"

def test_gold_rfm_r_score_range():
    from pyspark.sql import functions as F
    bad = spark.table(tbl("gold_customer_rfm")).filter(~F.col("r_score").between(1, 5)).count()
    assert bad == 0, f"{bad} rows with r_score outside [1, 5]"

def test_gold_rfm_cdf_enabled():
    """Change Data Feed must be enabled on Gold tables for Synced Tables to work."""
    from pyspark.sql import functions as F
    props = spark.sql(f"SHOW TBLPROPERTIES {tbl('gold_store_kpis')}").collect()
    prop_dict = {row["key"]: row["value"] for row in props}
    cdf = prop_dict.get("delta.enableChangeDataFeed", "false")
    assert cdf == "true", "delta.enableChangeDataFeed is not set to 'true' on gold_store_kpis"

for fn in [
    test_gold_revenue_has_all_categories, test_gold_revenue_gross_revenue_positive,
    test_gold_product_gross_profit_mostly_positive,
    test_gold_store_kpis_revenue_equals_sum_of_order_items,
    test_gold_rfm_covers_all_customers_with_orders,
    test_gold_rfm_segments_are_exhaustive, test_gold_rfm_r_score_range,
    test_gold_rfm_cdf_enabled,
]:
    run_test(fn.__name__, fn)

# COMMAND ----------
# MAGIC %md
# MAGIC ## upsert() idempotency

# COMMAND ----------

def test_upsert_creates_table_on_first_run():
    """upsert() should create the table and return the correct row count."""
    from pyspark.sql import Row
    test_table = "_test_upsert_create"
    full = f"{catalog}.{schema}.{test_table}"

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

    df = spark.createDataFrame([Row(id=1, val="a"), Row(id=2, val="b")])
    n  = upsert(df, test_table, "id")

    assert n == 2, f"Expected 2 rows, got {n}"
    assert spark.table(tbl(test_table)).count() == 2

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

def test_upsert_merges_existing_rows():
    """A second upsert with updated values should update existing rows."""
    from pyspark.sql import Row
    test_table = "_test_upsert_merge"

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

    first  = spark.createDataFrame([Row(id=1, val="original"), Row(id=2, val="stays")])
    upsert(first, test_table, "id")

    second = spark.createDataFrame([Row(id=1, val="updated")])
    n      = upsert(second, test_table, "id")

    rows = {r["id"]: r["val"] for r in spark.table(tbl(test_table)).collect()}
    assert rows[1] == "updated", f"Row 1 should be updated; got {rows[1]!r}"
    assert rows[2] == "stays",   f"Row 2 should be unchanged; got {rows[2]!r}"
    assert n == 1

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

def test_upsert_inserts_new_rows():
    """A second upsert with a new key should insert without touching existing rows."""
    from pyspark.sql import Row
    test_table = "_test_upsert_insert"

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

    upsert(spark.createDataFrame([Row(id=1, val="a")]), test_table, "id")
    upsert(spark.createDataFrame([Row(id=2, val="b")]), test_table, "id")

    assert spark.table(tbl(test_table)).count() == 2

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

def test_upsert_returns_source_row_count():
    """Return value must equal the number of rows in the source DataFrame."""
    from pyspark.sql import Row
    test_table = "_test_upsert_count"

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

    df = spark.createDataFrame([Row(id=i) for i in range(7)])
    n  = upsert(df, test_table, "id")
    assert n == 7

    spark.sql(f"DROP TABLE IF EXISTS {tbl(test_table)}")

for fn in [
    test_upsert_creates_table_on_first_run, test_upsert_merges_existing_rows,
    test_upsert_inserts_new_rows, test_upsert_returns_source_row_count,
]:
    run_test(fn.__name__, fn)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

passed = sum(1 for s, _ in results if s == PASS)
failed = sum(1 for s, _ in results if s == FAIL)
total  = len(results)

print(f"\n{'='*55}")
print(f"  {passed}/{total} tests passed")
if failed:
    print(f"  FAILED ({failed}):")
    for status, name in results:
        if status == FAIL:
            print(f"    • {name}")
print(f"{'='*55}")

assert failed == 0, f"{failed} integration test(s) failed — see output above"
