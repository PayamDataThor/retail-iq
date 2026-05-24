# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 05 · Visualization
# MAGIC Five business charts rendered inline from Gold tables using matplotlib.
# MAGIC Charts are also saved to `/tmp/retail-iq-charts/` for reference.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

import os
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

charts_dir = "/tmp/retail-iq-charts"
os.makedirs(charts_dir, exist_ok=True)

plt.rcParams.update({
    "figure.figsize":    (12, 5),
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "font.size":         11,
})

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Monthly Revenue Trend

# COMMAND ----------

trend = spark.sql(f"""
  SELECT
    MAKE_DATE(year, month, 1)                  AS period,
    ROUND(SUM(gross_revenue) / 1000, 1)        AS revenue_k
  FROM {tbl('gold_revenue_by_category_month')}
  GROUP BY year, month
  ORDER BY period
""").toPandas()

fig, ax = plt.subplots()
ax.plot(trend["period"], trend["revenue_k"], marker="o", linewidth=2, color="#1F77B4")
ax.fill_between(trend["period"], trend["revenue_k"], alpha=0.12, color="#1F77B4")
ax.set_title("Monthly Gross Revenue (completed orders)", fontweight="bold")
ax.set_ylabel("Revenue ($k)")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.0fk"))
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
fig.savefig(f"{charts_dir}/01_monthly_revenue_trend.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Revenue by Product Category

# COMMAND ----------

cats = spark.sql(f"""
  SELECT category, ROUND(SUM(gross_revenue) / 1000, 1) AS revenue_k
  FROM {tbl('gold_revenue_by_category_month')}
  GROUP BY category ORDER BY revenue_k DESC
""").toPandas()

fig, ax = plt.subplots()
bars = ax.barh(cats["category"], cats["revenue_k"], color="#2CA02C")
ax.bar_label(bars, fmt="$%.0fk", padding=4)
ax.set_title("Total Revenue by Product Category", fontweight="bold")
ax.set_xlabel("Revenue ($k)")
ax.invert_yaxis()
plt.tight_layout()
fig.savefig(f"{charts_dir}/02_revenue_by_category.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Top 10 Products by Revenue

# COMMAND ----------

CAT_COLORS = {
    "Electronics":   "#1F77B4",
    "Clothing":      "#FF7F0E",
    "Food & Bev":    "#2CA02C",
    "Home & Garden": "#D62728",
    "Sports":        "#9467BD",
}

prods = spark.sql(f"""
  SELECT product_name, category,
         ROUND(gross_revenue / 1000, 1) AS revenue_k
  FROM {tbl('gold_product_performance')}
  ORDER BY gross_revenue DESC LIMIT 10
""").toPandas()

fig, ax = plt.subplots()
bars = ax.barh(
    prods["product_name"], prods["revenue_k"],
    color=[CAT_COLORS.get(c, "#888") for c in prods["category"]]
)
ax.bar_label(bars, fmt="$%.0fk", padding=4)
ax.set_title("Top 10 Products by Gross Revenue", fontweight="bold")
ax.set_xlabel("Revenue ($k)")
ax.invert_yaxis()

from matplotlib.patches import Patch
ax.legend(handles=[Patch(color=v, label=k) for k, v in CAT_COLORS.items()],
          title="Category", loc="lower right", fontsize=9)
plt.tight_layout()
fig.savefig(f"{charts_dir}/03_top10_products.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Customer RFM Segment Distribution

# COMMAND ----------

SEG_COLORS = {
    "Champions":      "#2CA02C",
    "Loyal":          "#1F77B4",
    "Recent":         "#17BECF",
    "Frequent":       "#BCBD22",
    "Big Spenders":   "#FF7F0E",
    "Needs Attention":"#9467BD",
    "At Risk":        "#D62728",
}

rfm = spark.sql(f"""
  SELECT rfm_segment, COUNT(*) AS customers
  FROM {tbl('gold_customer_rfm')}
  GROUP BY rfm_segment ORDER BY customers DESC
""").toPandas()

fig, ax = plt.subplots()
bars = ax.bar(
    rfm["rfm_segment"], rfm["customers"],
    color=[SEG_COLORS.get(s, "#888") for s in rfm["rfm_segment"]]
)
ax.bar_label(bars, padding=3)
ax.set_title("Customer RFM Segment Distribution", fontweight="bold")
ax.set_ylabel("# Customers")
plt.xticks(rotation=20, ha="right")
plt.tight_layout()
fig.savefig(f"{charts_dir}/04_rfm_segments.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Store Revenue vs Orders by Region

# COMMAND ----------

stores = spark.sql(f"""
  SELECT store_name, region,
         ROUND(total_revenue / 1000, 1) AS revenue_k,
         total_orders
  FROM {tbl('gold_store_kpis')}
""").toPandas()

REGION_COLORS = {
    "West": "#1F77B4", "East": "#FF7F0E", "South": "#2CA02C",
    "Midwest": "#D62728", "Mountain": "#9467BD"
}

fig, ax = plt.subplots()
for region, grp in stores.groupby("region"):
    ax.scatter(grp["total_orders"], grp["revenue_k"],
               label=region, color=REGION_COLORS.get(region, "#888"),
               s=80, alpha=0.8)

ax.set_title("Store Performance: Revenue vs Orders by Region", fontweight="bold")
ax.set_xlabel("Total Orders")
ax.set_ylabel("Revenue ($k)")
ax.legend(title="Region")
plt.tight_layout()
fig.savefig(f"{charts_dir}/05_store_performance.png", dpi=150, bbox_inches="tight")
plt.show()

# COMMAND ----------

print(f"Charts saved to: {charts_dir}")
for f in sorted(os.listdir(charts_dir)):
    print(f"  {f}")
