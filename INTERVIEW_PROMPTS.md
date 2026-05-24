# RetailIQ — 10-Step Build Prompts

Paste each prompt in order into a fresh Claude Code session.
Bold values are the only things you change per project.

---

## Step 1 — Scaffold

```
Create a Databricks Asset Bundle project called retail-iq with this structure:

databricks.yml — two targets:
  dev (default, mode: development, schema: retail_iq)
  prod (mode: production, schema: retail_iq_prod)
  both on host https://dbc-85cda355-cf23.cloud.databricks.com
  catalog variable defaults to "main"

notebooks/00_utils.py — shared helpers loaded via %run:
  tbl(name) → "`{catalog}`.`{schema}`.`{name}`"
  upsert(source_df, target_table, key_col) → int
    uses Delta MERGE when table exists, saveAsTable on first run
    returns source.count() before the merge (one scan, not two)
  LOAD_TS = F.current_timestamp()

notebooks/01_setup.py — widgets: catalog (main), schema (retail_iq)
  CREATE SCHEMA IF NOT EXISTS
  USE catalog.schema
  Loop over gold table names — if table exists: ALTER TABLE SET TBLPROPERTIES CDF=true
  CREATE OR REPLACE FUNCTION mask_pii(val STRING): admins + retail_iq_analysts see real value, else ****
  Create group retail_iq_analysts — catch both ResourceAlreadyExists AND ResourceConflict

pytest.ini, .gitignore (exclude __pycache__ .databricks .env dist build)
requirements-test.txt: pytest faker
```

---

## Step 2 — Bronze Data Generation

```
Create notebooks/02_data_generation.py.
Widgets: catalog, schema, num_customers=1000, num_products=100, num_stores=20, num_orders=10000.
%run ./00_utils

Generate with Faker + random (seed 42):

bronze_customers: customer_id, full_name, email, age(18-80), segment(Premium/Standard/Budget), city, state, join_date
bronze_products:  product_id, name, category(Electronics/Clothing/Food & Bev/Home & Garden/Sports), brand, cost(10-200), price(cost*1.2 to cost*2.5), margin_pct
bronze_stores:    store_id, name, city, state, region(North/South/East/West)
bronze_dates:     date, year, month, month_name, quarter, day_of_week — full range of order dates
bronze_orders:    order_id, customer_id, store_id, order_date, status(70% completed/15% returned/15% cancelled)
bronze_order_items: order_item_id, order_id, product_id, quantity(1-5), unit_price, discount(0-0.3)

CRITICAL: build price_map = {p["product_id"]: p["price"] for p in products}
unit_price = round(price_map[product_id] * random.uniform(0.95, 1.0), 2)
Never use random.uniform(10, 500) — it makes gross_profit meaningless.

Do NOT compute line_total here — derive it in Silver.

Write each list as a Spark DataFrame and saveAsTable using overwrite mode.
Print row counts.
```

---

## Step 3 — Silver Transforms

```
Create:

src/retail_iq/__init__.py (empty)
src/retail_iq/quality.py — pure Python predicates (no Spark), one function per entity:
  is_valid_customer(row): email contains @, 18<=age<=100, no null id
  is_valid_product(row):  price > cost > 0, margin_pct > 0
  is_valid_order(row):    status in {completed, returned, cancelled}
  is_valid_order_item(row): quantity > 0, 0 <= discount < 1, unit_price > 0
src/retail_iq/analytics.py — pure Python functions:
  rfm_segment(r, f, m): returns Champions/Loyal/Recent/Frequent/Big Spenders/At Risk/Needs Attention
    Champions: r>=4 and f>=4; Loyal: r>=3 and f>=3; Recent: r>=4; Frequent: f>=4
    Big Spenders: m>=4; At Risk: r<=2 and f<=2; else Needs Attention
  line_total(unit_price, quantity, discount): round(unit_price * quantity * (1 - discount), 2)
  gross_profit(line_total, cost, quantity): round(line_total - cost * quantity, 2)

notebooks/03_silver_transforms.py — widgets: catalog, schema. %run ./00_utils
For each entity: read bronze, apply DQ filters (dropna + .filter()), dedup with row_number(),
add _load_ts = LOAD_TS, call upsert(), print count.
For silver_order_items: derive line_total = round(unit_price * quantity * (1 - discount), 2)
Liquid Clustering:
  silver_orders → order_date, customer_id
  silver_order_items → order_id, product_id
  silver_customers → customer_id
  silver_products → product_id, category
  silver_stores → store_id, region
Add clustering in the upsert first-run branch: source.write.format("delta").clusterBy(*cols).saveAsTable(...)
Print a DQ report: bronze count vs silver count vs pass rate for each table.
```

---

## Step 4 — Gold Analytics

```
Create notebooks/04_gold_analytics.py. Widgets: catalog, schema. %run ./00_utils

PERFORMANCE RULE: create ONE shared temp view before any Gold query:
  CREATE OR REPLACE TEMP VIEW enriched_items AS
  SELECT o.order_id, o.customer_id, o.store_id, o.order_date,
         oi.product_id, oi.quantity, oi.unit_price, oi.discount, oi.line_total
  FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
  WHERE o.status = 'completed'
All four Gold queries read from enriched_items — not from Silver directly.

Build four Gold tables (each with CLUSTER BY and TBLPROPERTIES CDF=true):

gold_revenue_by_category_month — grain: category × year × month
  CLUSTER BY (year, month, category)
  columns: year, month, month_name, category, num_orders, units_sold, gross_revenue, avg_order_value

gold_store_kpis — grain: store
  CLUSTER BY (region, store_id)
  columns: store_id, store_name, city, state, region, total_orders, unique_customers, total_revenue, avg_basket_size

gold_product_performance — grain: product
  CLUSTER BY (category, product_id)
  columns: product_id, product_name, category, brand, list_price, cost, margin_pct,
           num_orders, units_sold, gross_revenue, gross_profit, avg_discount_pct

gold_customer_rfm — grain: customer
  CLUSTER BY (rfm_segment, customer_id)
  Use NTILE(5) OVER (ORDER BY ...) for r_score, f_score, m_score
  REFERENCE_DATE = date_add(MAX(order_date), 1) — compute dynamically, not hardcoded
  r_score: ORDER BY recency_days DESC (more recent = higher score)
  columns: customer_id, full_name, email, bronze_segment, recency_days, frequency, monetary,
           r_score, f_score, m_score, rfm_score, rfm_segment

Print row counts for all four tables.
```

---

## Step 5 — Visualization

```
Create notebooks/05_visualization.py. Widgets: catalog, schema. %run ./00_utils

Using matplotlib (already available in Databricks), create 5 charts from Gold tables.
Use plt.show() — do NOT save to /tmp (PermissionError in serverless).
No cache() — not supported in serverless.

Chart 1: Stacked bar — monthly gross revenue by category (gold_revenue_by_category_month)
Chart 2: Horizontal bar — top 10 stores by total_revenue (gold_store_kpis)
Chart 3: Scatter — gross_profit vs units_sold, colored by category (gold_product_performance)
Chart 4: Bar — customer count per rfm_segment (gold_customer_rfm)
Chart 5: Bar — avg monetary per rfm_segment (gold_customer_rfm)

For each chart: load data with spark.table(tbl(...)).toPandas(), plot, plt.show(), print summary stats.
```

---

## Step 6 — Unity Catalog Governance + Lakehouse Monitor

```
Add a governance section at the END of notebooks/04_gold_analytics.py (after all Gold tables exist).

Section 1 — PII tags on silver_customers (for lineage, not for masking):
  ALTER TABLE silver_customers ALTER COLUMN email SET TAGS ('pii' = 'true')
  ALTER TABLE silver_customers ALTER COLUMN full_name SET TAGS ('pii' = 'true')

Section 2 — Drop any residual masks from silver_customers (idempotent guard):
  for col in (email, full_name): try: ALTER TABLE silver_customers ALTER COLUMN {col} DROP MASK; except: pass
  REASON: masks on Silver break the email-validity integration test; masks belong on Gold

Section 3 — Column masks on gold_customer_rfm (the analyst-facing serving layer):
  ALTER TABLE gold_customer_rfm ALTER COLUMN email SET MASK `{catalog}`.`{schema}`.mask_pii
  ALTER TABLE gold_customer_rfm ALTER COLUMN full_name SET MASK `{catalog}`.`{schema}`.mask_pii

Section 4 — Table comments on all four Gold tables (escape ' as '' in SQL strings):
  gold_revenue_by_category_month: "Monthly gross revenue, order count, and AOV by product category."
  gold_store_kpis: "Store-level KPIs: revenue, orders, unique customers, avg basket size."
  gold_product_performance: "Product-level: units sold, gross revenue, gross profit, avg discount."
  gold_customer_rfm: "Customer RFM segmentation. Segments: Champions, Loyal, Recent, Frequent, Big Spenders, At Risk, Needs Attention."

Section 5 — Column comments on gold_customer_rfm:
  rfm_segment, rfm_score, recency_days, frequency, monetary — one-line definition each

Section 6 — Lakehouse Monitor on gold_customer_rfm:
  from databricks.sdk.service.catalog import MonitorSnapshot
  w.quality_monitors.create(table_name=..., assets_dir=.../monitors, output_schema_name=..., snapshot=MonitorSnapshot())
  catch ResourceAlreadyExists
```

---

## Step 7 — Tests

```
Create the full test suite:

tests/conftest.py — pytest fixtures, no Spark dependency
tests/test_silver_transforms.py — test quality.py predicates:
  valid/invalid cases for each is_valid_* function (~40 tests)
tests/test_gold_analytics.py — test analytics.py functions:
  rfm_segment: all 7 segments, boundary conditions (~40 tests)
  line_total, gross_profit: arithmetic correctness (~10 tests)
tests/test_utils.py — test tbl() format string (5 tests)

notebooks/08_run_tests.py — Spark integration tests against live tables:
  Widgets: catalog, schema. %run ./00_utils
  Silver: non_empty, pass_rate>=95%, no_null_ids, all_emails_valid, ages_in_range,
          price_exceeds_cost, valid_statuses, line_total_correct,
          unit_price_bounded (within [0.949*price, 1.001*price])
  Gold:   all_categories_present, gross_revenue_positive, gross_profit_mostly_positive (<10% negative),
          store_kpis_revenue_matches_source (within $1), rfm_covers_all_customers,
          rfm_segments_exhaustive (exactly 7), r_score_range [1-5], cdf_enabled
  upsert: creates_table, merges_existing, inserts_new, returns_source_count

  Run each test with run_test(name, fn), collect results, print summary.
  assert failed == 0 at the end — this gates lakebase_sync.
```

---

## Step 8 — Pipeline Job + Maintenance

```
Create:

resources/retail_pipeline_job.yml — DABs job with 8 tasks, all on serverless client "2":
  setup → data_generation → silver_transforms → gold_analytics
  gold_analytics → visualization      (parallel)
  gold_analytics → run_tests          (parallel)
  gold_analytics → dashboard          (parallel)  ← notebooks/09_dashboard.py (print Gold stats)
  gold_analytics → aibi_setup         (parallel)  ← notebooks/11_aibi_setup.py (placeholder for now)
  run_tests      → lakebase_sync

  Every task: max_retries: 3, min_retry_interval_millis: 30000
  lakebase_sync timeout: 900s; all others: 300-600s
  email on_failure: p.amani@gmail.com
  queue enabled: true
  base_parameters: catalog + schema on every task

resources/retail_maintenance_job.yml — separate job:
  Single task: optimize_gold — runs notebooks/07_maintenance
  Schedule: 02:00 UTC daily (quartz: "0 0 2 * * ?")
  max_retries: 3

notebooks/07_maintenance.py:
  OPTIMIZE + ANALYZE TABLE on all four Gold tables
  Print timing for each.

notebooks/09_dashboard.py:
  Print top-5 rows for each Gold table.
  Print gross_profit<=0 count (should be 0 after unit_price fix).

Now run:
  databricks bundle deploy
  databricks bundle run retail_pipeline
Report each task status.
```

---

## Step 9 — Lakebase Sync + Streamlit App

```
Create notebooks/06_lakebase_sync.py. Widgets: catalog, schema. %run ./00_utils

Auth pattern (required — app SP cannot access Lakebase directly):
  Fetch PAT from Databricks Secrets: scope=retail-insights, key=lakebase-token
  Pop DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET before WorkspaceClient(host, token=pat)
  Restore env vars in finally block
  Resolve project "retail-iq-db" → production branch → primary endpoint

Dynamic Gold table list (exclude monitor output tables):
  GOLD_TABLES = sorted(t.name for t in spark.catalog.listTables(...)
    if t.name.startswith("gold_")
    and "_profile_metrics" not in t.name
    and "_drift_metrics" not in t.name)

Sync pattern — zero downtime:
  For each table: spark JDBC write to {table}_staging (batchsize=10000, numPartitions=4, mode=overwrite)
  Then: SQLAlchemy transaction — DROP {table} IF EXISTS; ALTER TABLE {table}_staging RENAME TO {table}

---

Create apps/retail_insights/:

app.yml — exactly:
  command: [streamlit, run, app.py]
  (NO --server.* flags — Databricks injects PORT=8000; any flag causes 502)

requirements.txt: streamlit>=1.35.0, plotly>=5.20.0, pandas>=2.0.0, psycopg2-binary>=2.9.0, databricks-sdk>=0.81.0

app.py — @st.cache_resource for connection, @st.cache_data(ttl=300) per query:
  4 tabs: Revenue Trends | Store Performance | Product Analysis | Customer Segments
  PostgreSQL ROUND rule: ROUND(expr::numeric, 2) — never ROUND(float_col, 2) (type error)
  Handle connection errors gracefully with st.error()

Add apps/retail_insights to databricks.yml as a Databricks App resource.

Print secret setup commands for me to run once manually.
```

---

## Step 10 — AI/BI Dashboard + Genie Space

```
Create notebooks/11_aibi_setup.py. Widgets: catalog, schema.

Resolve any running SQL warehouse via w.warehouses.list().

--- AI/BI DASHBOARD ---
Use REST API (not SDK wrapper — keyword args vary by SDK version):
  POST /api/2.0/lakeview/dashboards with serialized_dashboard = json.dumps(spec)
  Check for existing dashboard by display_name before creating (idempotent)
  Publish after create/update

Dashboard name: "RetailIQ Executive Dashboard [{schema}]"
3 pages:
  Revenue: bar chart (revenue by category×month) + pie (revenue share by category)
  Stores:  horizontal bar (top stores by revenue, colored by region)
  Customers: bar (customers per RFM segment) + bar (avg spend per segment)

CRITICAL — every widget query field needs BOTH keys:
  {"name": "field_name", "expression": "field_name"}
  Omitting "expression" causes: InvalidParameterValue: fields[x].expression should not be empty

--- GENIE SPACE ---
Use REST API: POST /api/2.0/genie/spaces
  List existing: GET /api/2.0/genie/spaces → key is "spaces" (not "genie_spaces")
  
serialized_space rules (not in docs — discovered empirically):
  Must be a JSON string (not object): json.dumps({...}, separators=(',',':'))
  "version": 2
  tables field: "identifier" (NOT "table_identifier")
  tables must be sorted alphabetically by identifier
  sample_questions.question is an ARRAY: ["text"], not a string "text"

Genie title: "RetailIQ Genie [{schema}]"
Tables: all 4 gold tables (sorted)
7 sample questions:
  "Which product category had the highest revenue last month?"
  "Show me the top 10 stores by total revenue"
  "How many Champion customers do we have and what is their average spend?"
  "What is the month-over-month revenue trend for Electronics?"
  "Which stores have the highest average basket size?"
  "Show me products with negative gross profit"
  "What percentage of customers are At Risk?"

Print Dashboard URL and Genie URL at the end.

Then deploy and run:
  databricks bundle deploy
  databricks bundle run retail_pipeline
All 8 tasks must go green.
```

---

## Run Order

```
Step 1  → Step 2 → Step 3 → Step 4 → Step 5    (build)
Step 6  → Step 7                                 (quality + governance)
Step 8                                           (wire + first full run)
Step 9  → Step 10                                (serve + self-serve)
```

Each step ends with a verifiable output.
Steps 8 and 10 each trigger a full pipeline run.
