# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 04 · Gold: Business Aggregations
# MAGIC
# MAGIC Builds four Gold tables consumed by dashboards, the Genie space, and the
# MAGIC Lakebase app layer.
# MAGIC
# MAGIC | Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `gold_revenue_by_category_month` | category × year-month | Revenue trend |
# MAGIC | `gold_store_kpis` | store | Revenue, orders, basket size |
# MAGIC | `gold_product_performance` | product | Revenue, units, margin |
# MAGIC | `gold_customer_rfm` | customer | RFM segmentation |
# MAGIC
# MAGIC **Performance design**
# MAGIC - A single `enriched_items` temp view pre-joins `silver_orders` + `silver_order_items`
# MAGIC   with the `status = 'completed'` filter, so all four Gold queries share one physical
# MAGIC   scan of the two largest Silver tables instead of four independent scans.
# MAGIC - Gold tables use `CLUSTER BY` (Liquid Clustering).  Unlike static `PARTITION BY`,
# MAGIC   Liquid Clustering rebalances files automatically as data grows.
# MAGIC - `OPTIMIZE` is intentionally omitted here; it runs in the nightly maintenance job
# MAGIC   (`07_maintenance`) so it does not block the critical pipeline path.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"USE `{catalog}`.`{schema}`")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## Shared base view: enriched_items
# MAGIC
# MAGIC All four Gold queries read from this temp view.  Spark executes the join and
# MAGIC filter once; subsequent references read from the cached plan.

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW enriched_items AS
  SELECT
    o.order_id,
    o.customer_id,
    o.store_id,
    o.order_date,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount,
    oi.line_total
  FROM {tbl('silver_orders')}      o
  JOIN {tbl('silver_order_items')} oi ON o.order_id = oi.order_id
  WHERE o.status = 'completed'
""")
print("✓ enriched_items view created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Revenue by Category × Month

# COMMAND ----------

# Reference date: one day after the latest order so recency scores are stable
# regardless of when the job runs.
ref_row   = spark.sql(f"SELECT date_add(MAX(order_date), 1) FROM {tbl('silver_orders')}").collect()[0]
REFERENCE_DATE = str(ref_row[0])
print(f"✓ RFM reference date: {REFERENCE_DATE}")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {tbl('gold_revenue_by_category_month')}
  CLUSTER BY (year, month, category)
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  AS
  SELECT
    d.year,
    d.month,
    d.month_name,
    p.category,
    COUNT(DISTINCT ei.order_id)                                        AS num_orders,
    SUM(ei.quantity)                                                   AS units_sold,
    ROUND(SUM(ei.line_total), 2)                                       AS gross_revenue,
    ROUND(SUM(ei.line_total) / COUNT(DISTINCT ei.order_id), 2)         AS avg_order_value
  FROM enriched_items                        ei
  JOIN {tbl('silver_products')} p  ON ei.product_id = p.product_id
  JOIN {tbl('bronze_dates')}    d  ON ei.order_date  = d.date
  GROUP BY d.year, d.month, d.month_name, p.category
""")
print(f"✓ gold_revenue_by_category_month: {spark.table(tbl('gold_revenue_by_category_month')).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Store KPIs

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {tbl('gold_store_kpis')}
  CLUSTER BY (region, store_id)
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  AS
  SELECT
    s.store_id,
    s.name                                                             AS store_name,
    s.city,
    s.state,
    s.region,
    COUNT(DISTINCT ei.order_id)                                        AS total_orders,
    COUNT(DISTINCT ei.customer_id)                                     AS unique_customers,
    ROUND(SUM(ei.line_total), 2)                                       AS total_revenue,
    ROUND(SUM(ei.line_total) / COUNT(DISTINCT ei.order_id), 2)         AS avg_basket_size
  FROM enriched_items                   ei
  JOIN {tbl('silver_stores')} s ON ei.store_id = s.store_id
  GROUP BY s.store_id, s.name, s.city, s.state, s.region
""")
print(f"✓ gold_store_kpis: {spark.table(tbl('gold_store_kpis')).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Product Performance

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {tbl('gold_product_performance')}
  CLUSTER BY (category, product_id)
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  AS
  SELECT
    p.product_id,
    p.name                                                             AS product_name,
    p.category,
    p.brand,
    p.price                                                            AS list_price,
    p.cost,
    p.margin_pct,
    COUNT(DISTINCT ei.order_id)                                        AS num_orders,
    SUM(ei.quantity)                                                   AS units_sold,
    ROUND(SUM(ei.line_total), 2)                                       AS gross_revenue,
    ROUND(SUM(ei.line_total) - p.cost * SUM(ei.quantity), 2)           AS gross_profit,
    ROUND(AVG(ei.discount) * 100, 1)                                   AS avg_discount_pct
  FROM enriched_items                      ei
  JOIN {tbl('silver_products')} p ON ei.product_id = p.product_id
  GROUP BY p.product_id, p.name, p.category, p.brand, p.price, p.cost, p.margin_pct
""")
print(f"✓ gold_product_performance: {spark.table(tbl('gold_product_performance')).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customer RFM Segmentation
# MAGIC
# MAGIC RFM scores each customer 1–5 on three dimensions:
# MAGIC - **Recency (R)** — days since last purchase; lower is better
# MAGIC - **Frequency (F)** — number of completed orders; higher is better
# MAGIC - **Monetary (M)** — total spend; higher is better
# MAGIC
# MAGIC `NTILE(5)` partitions customers into equal quintiles so scores are always
# MAGIC relative to the full customer base, not absolute thresholds.

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {tbl('gold_customer_rfm')}
  CLUSTER BY (rfm_segment, customer_id)
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  AS
  WITH order_summary AS (
    SELECT
      customer_id,
      MAX(order_date)                                                  AS last_order_date,
      COUNT(DISTINCT order_id)                                         AS frequency,
      ROUND(SUM(line_total), 2)                                        AS monetary
    FROM enriched_items
    GROUP BY customer_id
  ),
  rfm_scores AS (
    SELECT
      customer_id,
      DATEDIFF(DATE '{REFERENCE_DATE}', last_order_date)               AS recency_days,
      frequency,
      monetary,
      -- Lower recency_days = more recent = higher score, so order DESC
      NTILE(5) OVER (ORDER BY DATEDIFF(DATE '{REFERENCE_DATE}', last_order_date) DESC) AS r_score,
      NTILE(5) OVER (ORDER BY frequency)                                                AS f_score,
      NTILE(5) OVER (ORDER BY monetary)                                                 AS m_score
    FROM order_summary
  )
  SELECT
    s.customer_id,
    c.full_name,
    c.email,
    c.segment                              AS bronze_segment,
    s.recency_days,
    s.frequency,
    s.monetary,
    s.r_score,
    s.f_score,
    s.m_score,
    s.r_score + s.f_score + s.m_score      AS rfm_score,
    CASE
      WHEN s.r_score >= 4 AND s.f_score >= 4 THEN 'Champions'
      WHEN s.r_score >= 3 AND s.f_score >= 3 THEN 'Loyal'
      WHEN s.r_score >= 4                    THEN 'Recent'
      WHEN s.f_score >= 4                    THEN 'Frequent'
      WHEN s.m_score >= 4                    THEN 'Big Spenders'
      WHEN s.r_score <= 2 AND s.f_score <= 2 THEN 'At Risk'
      ELSE                                        'Needs Attention'
    END                                    AS rfm_segment
  FROM rfm_scores s
  JOIN {tbl('silver_customers')} c ON s.customer_id = c.customer_id
""")
print(f"✓ gold_customer_rfm: {spark.table(tbl('gold_customer_rfm')).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quick Sanity Check

# COMMAND ----------

print("=== Top 5 categories by revenue ===")
spark.sql(f"""
  SELECT category, ROUND(SUM(gross_revenue), 0) AS total_revenue
  FROM {tbl('gold_revenue_by_category_month')}
  GROUP BY category ORDER BY total_revenue DESC LIMIT 5
""").display()

print("=== RFM segment distribution ===")
spark.sql(f"""
  SELECT rfm_segment, COUNT(*) AS customers,
         ROUND(AVG(monetary), 0) AS avg_spend
  FROM {tbl('gold_customer_rfm')}
  GROUP BY rfm_segment ORDER BY customers DESC
""").display()

print("=== gross_profit sanity (should all be > 0) ===")
spark.sql(f"""
  SELECT COUNT(*) AS negative_profit_products
  FROM {tbl('gold_product_performance')}
  WHERE gross_profit <= 0
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unity Catalog Governance
# MAGIC
# MAGIC Applies PII tags, column masks, and descriptive comments now that all tables exist.
# MAGIC Every statement is idempotent — safe to rerun.

# COMMAND ----------

# ── PII tags on silver_customers ─────────────────────────────────────────────
for col in ("email", "full_name"):
    spark.sql(f"""
      ALTER TABLE {tbl('silver_customers')}
      ALTER COLUMN {col} SET TAGS ('pii' = 'true')
    """)
print("✓ PII tags set on silver_customers.email and full_name")

# ── Column masks (requires masking function from 01_setup) ───────────────────
for col in ("email", "full_name"):
    spark.sql(f"""
      ALTER TABLE {tbl('silver_customers')}
      ALTER COLUMN {col}
      SET MASK `{catalog}`.`{schema}`.mask_pii
    """)
print("✓ Column masks applied — non-analysts see '****'")

# ── Table-level comments ─────────────────────────────────────────────────────
table_comments = {
    "gold_revenue_by_category_month": "Monthly gross revenue, order count, and AOV aggregated by product category. Primary source for revenue trend analysis.",
    "gold_store_kpis":                "Store-level KPIs: total revenue, orders, unique customers, and avg basket size. Refreshed each pipeline run.",
    "gold_product_performance":       "Product-level performance: units sold, gross revenue, gross profit, and avg discount. Use for margin analysis.",
    "gold_customer_rfm":              "Customer RFM segmentation (Recency, Frequency, Monetary). Segments: Champions, Loyal, Recent, Frequent, Big Spenders, At Risk, Needs Attention.",
}
for t, comment in table_comments.items():
    safe = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {tbl(t)} IS '{safe}'")
print("✓ Table comments applied to all Gold tables")

# ── Column comments on gold_customer_rfm ────────────────────────────────────
col_comments = {
    "rfm_segment":  "Derived segment label based on r/f/m quintile scores.",
    "rfm_score":    "Sum of r_score + f_score + m_score (3 to 15). Higher = more valuable customer.",
    "recency_days": "Days since the last completed order for this customer.",
    "frequency":    "Total number of completed orders.",
    "monetary":     "Total revenue from completed orders (net of discounts).",
}
for col, comment in col_comments.items():
    safe = comment.replace("'", "''")   # escape single quotes for SQL string literals
    spark.sql(f"ALTER TABLE {tbl('gold_customer_rfm')} ALTER COLUMN {col} COMMENT '{safe}'")
print("✓ Column comments applied to gold_customer_rfm")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lakehouse Monitor
# MAGIC
# MAGIC Attaches a Databricks Lakehouse Monitor to `gold_customer_rfm` to track
# MAGIC data quality and RFM segment distribution drift over time.
# MAGIC Idempotent — skips creation if the monitor already exists.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot
from databricks.sdk.errors import ResourceAlreadyExists, NotFound

w        = WorkspaceClient()
username = w.current_user.me().user_name
table_fqn = f"{catalog}.{schema}.gold_customer_rfm"

try:
    w.quality_monitors.create(
        table_name=table_fqn,
        assets_dir=f"/Workspace/Users/{username}/retail-iq/monitors",
        output_schema_name=f"{catalog}.{schema}",
        snapshot=MonitorSnapshot(),
    )
    print(f"✓ Lakehouse Monitor created for {table_fqn}")
except ResourceAlreadyExists:
    print(f"– Monitor already exists for {table_fqn} (skipping)")
except Exception as e:
    print(f"⚠ Monitor creation skipped: {e}")
