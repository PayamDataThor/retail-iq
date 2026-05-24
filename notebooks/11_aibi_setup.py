# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · AI/BI Dashboard + Genie Space
# MAGIC
# MAGIC Creates two self-serve layers on top of the Gold tables:
# MAGIC
# MAGIC | Layer | Audience | How |
# MAGIC |---|---|---|
# MAGIC | **AI/BI Dashboard** | Analysts inside the workspace | Databricks Lakeview — curated charts |
# MAGIC | **Genie Space** | Business users | Natural language → SQL over Gold tables |

# COMMAND ----------

dbutils.widgets.text("catalog", "main",      "Catalog")
dbutils.widgets.text("schema",  "retail_iq", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

w = WorkspaceClient()

# ── Resolve a running SQL warehouse ──────────────────────────────────────────
warehouses = [wh for wh in w.warehouses.list() if wh.state and wh.state.value == "RUNNING"]
if not warehouses:
    warehouses = list(w.warehouses.list())   # take any if none running
if not warehouses:
    raise RuntimeError("No SQL warehouses found — create one in the workspace first")

warehouse = warehouses[0]
print(f"Using warehouse: {warehouse.name}  (id={warehouse.id})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## AI/BI Dashboard (Lakeview)

# COMMAND ----------

DASHBOARD_NAME = f"RetailIQ Executive Dashboard [{schema}]"

dashboard_spec = {
    "datasets": [
        {
            "name": "ds_revenue",
            "displayName": "Revenue by Category × Month",
            "query": f"""
                SELECT year, month,
                       CAST(year AS STRING) || '-' || LPAD(CAST(month AS STRING), 2, '0') AS period,
                       category,
                       ROUND(SUM(gross_revenue)::numeric, 0)  AS gross_revenue,
                       SUM(num_orders)                         AS num_orders
                FROM {catalog}.{schema}.gold_revenue_by_category_month
                GROUP BY year, month, period, category
                ORDER BY period
            """,
        },
        {
            "name": "ds_stores",
            "displayName": "Store KPIs",
            "query": f"""
                SELECT store_name, city, region,
                       ROUND(total_revenue::numeric, 0) AS total_revenue,
                       total_orders,
                       ROUND(avg_basket_size::numeric, 2) AS avg_basket_size
                FROM {catalog}.{schema}.gold_store_kpis
                ORDER BY total_revenue DESC
            """,
        },
        {
            "name": "ds_rfm",
            "displayName": "RFM Segments",
            "query": f"""
                SELECT rfm_segment,
                       COUNT(*)                            AS customers,
                       ROUND(AVG(monetary)::numeric, 0)   AS avg_spend,
                       ROUND(AVG(frequency)::numeric, 1)  AS avg_orders
                FROM {catalog}.{schema}.gold_customer_rfm
                GROUP BY rfm_segment
                ORDER BY customers DESC
            """,
        },
        {
            "name": "ds_products",
            "displayName": "Product Performance",
            "query": f"""
                SELECT category,
                       ROUND(SUM(gross_revenue)::numeric, 0) AS gross_revenue,
                       ROUND(SUM(gross_profit)::numeric, 0)  AS gross_profit,
                       SUM(units_sold)                        AS units_sold
                FROM {catalog}.{schema}.gold_product_performance
                GROUP BY category
                ORDER BY gross_revenue DESC
            """,
        },
    ],
    "pages": [
        {
            "name":        "revenue",
            "displayName": "Revenue",
            "layout": [
                {
                    "widget": {
                        "name": "revenue_trend",
                        "queries": [{"name": "q", "query": {"datasetName": "ds_revenue", "fields": [{"name": "period", "expression": "period"}, {"name": "gross_revenue", "expression": "gross_revenue"}, {"name": "category", "expression": "category"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x":     {"fieldName": "period",        "scale": {"type": "categorical"}, "displayName": "Month"},
                                "y":     {"fieldName": "gross_revenue",  "scale": {"type": "quantitative"}, "displayName": "Revenue ($)"},
                                "color": {"fieldName": "category",       "scale": {"type": "categorical"}, "displayName": "Category"},
                            },
                            "frame": {"title": "Revenue by Category & Month", "showTitle": True},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 4, "height": 6},
                },
                {
                    "widget": {
                        "name": "revenue_by_category_pie",
                        "queries": [{"name": "q", "query": {"datasetName": "ds_products", "fields": [{"name": "category", "expression": "category"}, {"name": "gross_revenue", "expression": "gross_revenue"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "gross_revenue", "scale": {"type": "quantitative"}, "displayName": "Revenue"},
                                "color": {"fieldName": "category",      "scale": {"type": "categorical"}, "displayName": "Category"},
                            },
                            "frame": {"title": "Revenue Share by Category", "showTitle": True},
                        },
                    },
                    "position": {"x": 4, "y": 0, "width": 2, "height": 6},
                },
            ],
        },
        {
            "name":        "stores",
            "displayName": "Stores",
            "layout": [
                {
                    "widget": {
                        "name": "top_stores",
                        "queries": [{"name": "q", "query": {"datasetName": "ds_stores", "fields": [{"name": "store_name", "expression": "store_name"}, {"name": "total_revenue", "expression": "total_revenue"}, {"name": "region", "expression": "region"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x":     {"fieldName": "total_revenue", "scale": {"type": "quantitative"}, "displayName": "Revenue ($)"},
                                "y":     {"fieldName": "store_name",    "scale": {"type": "categorical"}, "displayName": "Store"},
                                "color": {"fieldName": "region",        "scale": {"type": "categorical"}, "displayName": "Region"},
                            },
                            "frame": {"title": "Top Stores by Revenue", "showTitle": True},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 8},
                },
            ],
        },
        {
            "name":        "customers",
            "displayName": "Customers",
            "layout": [
                {
                    "widget": {
                        "name": "rfm_segments",
                        "queries": [{"name": "q", "query": {"datasetName": "ds_rfm", "fields": [{"name": "rfm_segment", "expression": "rfm_segment"}, {"name": "customers", "expression": "customers"}, {"name": "avg_spend", "expression": "avg_spend"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x":     {"fieldName": "rfm_segment", "scale": {"type": "categorical"}, "displayName": "Segment"},
                                "y":     {"fieldName": "customers",   "scale": {"type": "quantitative"}, "displayName": "Customers"},
                                "color": {"fieldName": "rfm_segment", "scale": {"type": "categorical"}},
                            },
                            "frame": {"title": "Customers by RFM Segment", "showTitle": True},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 3, "height": 6},
                },
                {
                    "widget": {
                        "name": "avg_spend_by_segment",
                        "queries": [{"name": "q", "query": {"datasetName": "ds_rfm", "fields": [{"name": "rfm_segment", "expression": "rfm_segment"}, {"name": "avg_spend", "expression": "avg_spend"}], "disaggregated": False}}],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x":     {"fieldName": "rfm_segment", "scale": {"type": "categorical"}, "displayName": "Segment"},
                                "y":     {"fieldName": "avg_spend",   "scale": {"type": "quantitative"}, "displayName": "Avg Spend ($)"},
                                "color": {"fieldName": "rfm_segment", "scale": {"type": "categorical"}},
                            },
                            "frame": {"title": "Avg Lifetime Spend by Segment", "showTitle": True},
                        },
                    },
                    "position": {"x": 3, "y": 0, "width": 3, "height": 6},
                },
            ],
        },
    ],
}

# ── Create or update the dashboard via REST API ───────────────────────────────
# Using api_client.do() directly — the SDK wrapper's keyword-arg signature
# varies across installed versions.
host = w.config.host.rstrip("/")

existing_list = w.api_client.do("GET", "/api/2.0/lakeview/dashboards").get("dashboards", [])
existing = next((d for d in existing_list if d.get("display_name") == DASHBOARD_NAME), None)

if existing:
    dashboard_id = existing["dashboard_id"]
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body={
        "display_name":          DASHBOARD_NAME,
        "serialized_dashboard":  json.dumps(dashboard_spec),
        "warehouse_id":          warehouse.id,
    })
    print(f"✓ Dashboard updated: {DASHBOARD_NAME}")
else:
    result = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body={
        "display_name":         DASHBOARD_NAME,
        "serialized_dashboard": json.dumps(dashboard_spec),
        "warehouse_id":         warehouse.id,
    })
    dashboard_id = result["dashboard_id"]
    print(f"✓ Dashboard created: {DASHBOARD_NAME}")

# ── Publish ───────────────────────────────────────────────────────────────────
try:
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", body={
        "warehouse_id":      warehouse.id,
        "embed_credentials": True,
    })
    print("✓ Dashboard published")
except Exception as e:
    print(f"– Publish skipped: {e}")

print(f"\n  Dashboard URL: {host}/dashboardsv3/{dashboard_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Genie Space

# COMMAND ----------

GENIE_TITLE = f"RetailIQ Genie [{schema}]"

GOLD_TABLES = [
    f"{catalog}.{schema}.gold_revenue_by_category_month",
    f"{catalog}.{schema}.gold_store_kpis",
    f"{catalog}.{schema}.gold_product_performance",
    f"{catalog}.{schema}.gold_customer_rfm",
]

CURATED_QUESTIONS = [
    "Which product category had the highest revenue last month?",
    "Show me the top 10 stores by total revenue",
    "How many Champion customers do we have and what is their average spend?",
    "What is the month-over-month revenue trend for Electronics?",
    "Which stores have the highest average basket size?",
    "Show me products with negative gross profit",
    "What percentage of customers are At Risk?",
]

# ── Create Genie space via REST API ──────────────────────────────────────────
# serialized_space rules (discovered empirically — not fully documented):
#   - Must be a JSON string (not an object)
#   - version must be 2
#   - data_sources.tables uses field "identifier", not "table_identifier"
#   - data_sources.tables must be sorted alphabetically by identifier
#   - config.sample_questions.question must be an array of strings
try:
    resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
    existing_spaces = resp.get("spaces") or resp.get("genie_spaces") or []
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
                    for i, q in enumerate(CURATED_QUESTIONS, 1)
                ]
            },
            "data_sources": {
                # tables must be sorted alphabetically by identifier
                "tables": [{"identifier": t} for t in sorted(GOLD_TABLES)]
            }
        }, separators=(',', ':'))

        result = w.api_client.do("POST", "/api/2.0/genie/spaces", body={
            "title":            GENIE_TITLE,
            "description":      "Ask questions about retail revenue, store performance, product margins, and customer segments in plain English.",
            "warehouse_id":     warehouse.id,
            "serialized_space": serialized_space,
        })
        space_id = result.get("space_id") or result.get("id")
        print(f"✓ Genie space created: {GENIE_TITLE}")

    if space_id:
        print(f"\n  Genie URL: {host}/genie/spaces/{space_id}")

except Exception as e:
    print(f"⚠ Genie setup skipped: {e}")
    print("  You can create the Genie space manually via: workspace → New → Genie space")
    print(f"  Tables to add: {', '.join(GOLD_TABLES)}")
