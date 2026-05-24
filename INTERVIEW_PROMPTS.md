# 10 Prompts — Copy-Paste in Order

---

### Prompt 1

```
Create a Databricks lakehouse project for retail analytics.

Structure:
- databricks.yml: bundle name retail-iq, catalog=main, schema=retail_iq (dev default) / retail_iq_prod (prod), host https://dbc-85cda355-cf23.cloud.databricks.com, dev mode=development, prod mode=production
- notebooks/00_utils.py: tbl(name) helper returning "`{catalog}`.`{schema}`.`{name}`"; upsert(df, table, key) that does Delta MERGE when table exists else saveAsTable, returns source row count (count before merge — one scan only); LOAD_TS = F.current_timestamp()
- notebooks/01_setup.py: widgets catalog+schema, CREATE SCHEMA IF NOT EXISTS, USE, loop over gold table names to enable CDF if they exist, CREATE OR REPLACE FUNCTION mask_pii that shows real value to admins and retail_iq_analysts group else ****, create retail_iq_analysts group (catch ResourceAlreadyExists AND ResourceConflict)
- pytest.ini, .gitignore, requirements-test.txt (pytest, faker)
```

---

### Prompt 2

```
Create notebooks/02_data_generation.py for a retail business.

Widgets: catalog, schema, num_customers=1000, num_products=100, num_stores=20, num_orders=10000
%run ./00_utils

Use Faker + random seed 42 to generate:
- bronze_customers: customer_id, full_name, email, age(18-80), segment(Premium/Standard/Budget), city, state, join_date
- bronze_products: product_id, name, category(Electronics/Clothing/Food & Bev/Home & Garden/Sports), brand, cost(10-200), price(cost × 1.2 to 2.5), margin_pct
- bronze_stores: store_id, name, city, state, region(North/South/East/West)
- bronze_dates: one row per calendar date covering the order date range, with year/month/month_name/quarter
- bronze_orders: order_id, customer_id, store_id, order_date, status (70% completed, 15% returned, 15% cancelled)
- bronze_order_items: order_item_id, order_id, product_id, quantity(1-5), unit_price, discount(0-0.3)

Critical: build price_map from products before generating orders. Set unit_price = price_map[product_id] × random.uniform(0.95, 1.0). Never use random.uniform(10,500) — it makes gross_profit meaningless.
Do not compute line_total here — derive it in Silver.

Write each as a Delta table with mode overwrite. Print row counts.
```

---

### Prompt 3

```
Create the Silver layer.

src/retail_iq/__init__.py (empty)
src/retail_iq/quality.py — pure Python, no Spark:
  is_valid_customer(row): email has @, 18<=age<=100, no null id
  is_valid_product(row): price > cost > 0
  is_valid_order(row): status in {completed, returned, cancelled}
  is_valid_order_item(row): quantity>0, 0<=discount<1, unit_price>0
src/retail_iq/analytics.py — pure Python, no Spark:
  rfm_segment(r,f,m): Champions(r>=4,f>=4) > Loyal(r>=3,f>=3) > Recent(r>=4) > Frequent(f>=4) > Big Spenders(m>=4) > At Risk(r<=2,f<=2) > Needs Attention
  line_total(unit_price, quantity, discount): round(unit_price * quantity * (1-discount), 2)
  gross_profit(line_total, cost, quantity): round(line_total - cost*quantity, 2)

notebooks/03_silver_transforms.py — widgets catalog+schema, %run ./00_utils:
  Read each bronze table, dropna on key columns, row_number() dedup, apply domain filters, add _load_ts, call upsert()
  For silver_order_items: derive line_total = round(unit_price * quantity * (1 - discount), 2)
  On first-run (saveAsTable branch): clusterBy the table — orders:(order_date,customer_id), order_items:(order_id,product_id), customers:(customer_id), products:(product_id,category), stores:(store_id,region)
  Print DQ report: bronze count, silver count, pass rate for each table
```

---

### Prompt 4

```
Create notebooks/04_gold_analytics.py — widgets catalog+schema, %run ./00_utils

First, build one shared temp view before all Gold queries:
  CREATE OR REPLACE TEMP VIEW enriched_items AS
  SELECT o.order_id, o.customer_id, o.store_id, o.order_date, oi.product_id, oi.quantity, oi.unit_price, oi.discount, oi.line_total
  FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
  WHERE o.status = 'completed'
All four Gold queries read from enriched_items — not from Silver directly. This turns 4 scans into 1.

Build four tables, each with CLUSTER BY and TBLPROPERTIES delta.enableChangeDataFeed=true:
1. gold_revenue_by_category_month — grain: category×year×month, CLUSTER BY (year,month,category), cols: year,month,month_name,category,num_orders,units_sold,gross_revenue,avg_order_value
2. gold_store_kpis — grain: store, CLUSTER BY (region,store_id), cols: store_id,store_name,city,state,region,total_orders,unique_customers,total_revenue,avg_basket_size
3. gold_product_performance — grain: product, CLUSTER BY (category,product_id), cols: product_id,product_name,category,brand,list_price,cost,margin_pct,num_orders,units_sold,gross_revenue,gross_profit,avg_discount_pct
4. gold_customer_rfm — grain: customer, CLUSTER BY (rfm_segment,customer_id)
  REFERENCE_DATE = date_add(MAX(order_date),1) from silver_orders — compute dynamically
  NTILE(5) OVER for r_score (ORDER BY recency_days DESC), f_score (ORDER BY frequency), m_score (ORDER BY monetary)
  Segment label using CASE on r/f/m scores matching the rfm_segment() logic in analytics.py
  cols: customer_id,full_name,email,bronze_segment,recency_days,frequency,monetary,r_score,f_score,m_score,rfm_score,rfm_segment

Print row counts.
```

---

### Prompt 5

```
Create notebooks/05_visualization.py — widgets catalog+schema, %run ./00_utils

5 charts using matplotlib. Use plt.show() only — do not save to /tmp (PermissionError in serverless). No cache() — unsupported in serverless.

1. Stacked bar: monthly gross_revenue by category — from gold_revenue_by_category_month, x=period(year-month), y=gross_revenue, color=category
2. Horizontal bar: top 10 stores by total_revenue — from gold_store_kpis
3. Scatter: gross_profit vs units_sold colored by category — from gold_product_performance
4. Bar: customer count per rfm_segment — from gold_customer_rfm
5. Bar: avg monetary per rfm_segment — from gold_customer_rfm

Load each Gold table with spark.table(tbl(...)).toPandas(). Print a one-line summary stat under each chart.
```

---

### Prompt 6

```
Add a Unity Catalog governance section at the END of notebooks/04_gold_analytics.py (after all four Gold tables exist).

1. Drop residual masks from silver_customers (idempotent, swallow errors — masks on Silver break integration tests):
   for col in (email, full_name): try ALTER TABLE silver_customers ALTER COLUMN {col} DROP MASK except pass

2. PII tags on silver_customers for lineage (not masking — masking belongs on Gold):
   ALTER TABLE silver_customers ALTER COLUMN email SET TAGS ('pii'='true')
   ALTER TABLE silver_customers ALTER COLUMN full_name SET TAGS ('pii'='true')

3. Column masks on gold_customer_rfm (the analyst-facing layer, not Silver):
   ALTER TABLE gold_customer_rfm ALTER COLUMN email SET MASK `{catalog}`.`{schema}`.mask_pii
   ALTER TABLE gold_customer_rfm ALTER COLUMN full_name SET MASK `{catalog}`.`{schema}`.mask_pii

4. Table comments on all four Gold tables. Escape single quotes as '' in SQL strings.

5. Column comments on gold_customer_rfm: rfm_segment, rfm_score, recency_days, frequency, monetary.

6. Lakehouse Monitor on gold_customer_rfm:
   w.quality_monitors.create(table_name, assets_dir=/Workspace/Users/{username}/retail-iq/monitors, output_schema_name, snapshot=MonitorSnapshot())
   Catch ResourceAlreadyExists.
```

---

### Prompt 7

```
Create the test suite.

tests/conftest.py — minimal pytest config, no Spark
tests/test_silver_transforms.py — unit tests for every function in src/retail_iq/quality.py: valid cases, null cases, boundary cases (~40 tests total)
tests/test_gold_analytics.py — unit tests for src/retail_iq/analytics.py: all 7 rfm_segment outcomes with boundary values, line_total arithmetic, gross_profit arithmetic (~50 tests)
tests/test_utils.py — test tbl() output format (5 tests)

notebooks/08_run_tests.py — Spark integration tests against live tables, widgets catalog+schema, %run ./00_utils:
  Silver: non_empty, pass_rate>=95%, no_null_ids, all_emails_valid (email contains @), ages_in_range, price_exceeds_cost, valid_statuses_only, line_total_correct (within 1 cent), unit_price_bounded (within [0.949×price, 1.001×price])
  Gold: all_categories_present, gross_revenue_positive, gross_profit_mostly_positive (<10% negative), store_kpis_revenue_matches_silver_source (within $1), rfm_covers_all_customers_with_orders, rfm_segments_exhaustive (exactly 7 valid values), r_score_range (1-5), cdf_enabled
  upsert: creates_table, merges_existing_rows, inserts_new_rows, returns_source_row_count
  Collect results with run_test(name, fn). Print summary. assert failed == 0.
```

---

### Prompt 8

```
Wire everything into a DABs pipeline.

resources/retail_pipeline_job.yml — serverless environment client "2", 8 tasks:
  setup → data_generation → silver_transforms → gold_analytics
  gold_analytics → visualization     (parallel)
  gold_analytics → run_tests         (parallel)
  gold_analytics → dashboard         (parallel)
  gold_analytics → aibi_setup        (parallel, notebook path notebooks/11_aibi_setup, placeholder for now)
  run_tests → lakebase_sync
  Every task: max_retries=3, min_retry_interval_millis=30000
  lakebase_sync timeout=900, all others 300-600
  email on_failure: p.amani@gmail.com, queue enabled

resources/retail_maintenance_job.yml — separate job, nightly 02:00 UTC, max_retries=3:
  single task running notebooks/07_maintenance

notebooks/07_maintenance.py: OPTIMIZE then ANALYZE TABLE on all four Gold tables. Print timing.
notebooks/09_dashboard.py: top-5 rows per Gold table, gross_profit<=0 count.

Then:
  databricks bundle deploy
  databricks bundle run retail_pipeline
Report pass/fail for each task.
```

---

### Prompt 9

```
Create the Lakebase sync notebook and the Streamlit app.

notebooks/06_lakebase_sync.py — widgets catalog+schema, %run ./00_utils:
  Auth: fetch PAT from Databricks Secrets (scope=retail-insights, key=lakebase-token), base64-decode it, pop DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET env vars, create WorkspaceClient(host, token=pat), restore env vars in finally — this avoids "more than one auth method" SDK error
  Resolve Lakebase: project retail-iq-db → production branch → primary endpoint
  Gold table list: all tables starting with gold_ excluding _profile_metrics and _drift_metrics (monitor output — has struct columns that psycopg2 can't adapt)
  Sync: for each table JDBC-write to {table}_staging (batchsize=10000, numPartitions=4, overwrite), then SQLAlchemy transaction: DROP TABLE IF EXISTS {table}; ALTER TABLE {table}_staging RENAME TO {table}

apps/retail_insights/app.yml — exactly: command: [streamlit, run, app.py]
  No --server.* flags — Databricks injects PORT=8000 at runtime; any override causes 502 Bad Gateway.
apps/retail_insights/requirements.txt: streamlit>=1.35.0, plotly>=5.20.0, pandas>=2.0.0, psycopg2-binary>=2.9.0, databricks-sdk>=0.81.0
apps/retail_insights/app.py:
  Same auth pattern as sync notebook (secret PAT + env var pop)
  @st.cache_resource for connection init, @st.cache_data(ttl=300) per query
  4 tabs: Revenue Trends, Store Performance, Product Analysis, Customer Segments
  PostgreSQL ROUND rule: ROUND(expr::numeric, 2) — Spark exports floats as double precision, ROUND(double,int) doesn't exist in Postgres

Add the app to databricks.yml as a Databricks App resource.
Print one-time secret setup commands for me to run.
```

---

### Prompt 10

```
Create notebooks/11_aibi_setup.py — widgets catalog+schema.

Resolve a running SQL warehouse via w.warehouses.list().

AI/BI Dashboard — use w.api_client.do() not SDK wrapper (keyword args vary by version):
  Name: "RetailIQ Executive Dashboard [{schema}]"
  Check existing by display_name before creating (idempotent). Publish after create/update.
  3 pages backed by 4 datasets (gold_revenue_by_category_month, gold_store_kpis, gold_customer_rfm, gold_product_performance):
    Revenue: bar (gross_revenue by category×month) + pie (revenue share by category)
    Stores: horizontal bar (total_revenue by store, colored by region)
    Customers: bar (customer count by rfm_segment) + bar (avg monetary by rfm_segment)
  Every widget query field MUST have both "name" and "expression" keys — omitting "expression" causes InvalidParameterValue validation failure.

Genie Space — use POST /api/2.0/genie/spaces:
  List existing: GET /api/2.0/genie/spaces — response key is "spaces" not "genie_spaces"
  Title: "RetailIQ Genie [{schema}]"
  serialized_space must be a JSON string (not object) with these exact rules:
    "version": 2
    tables use field "identifier" not "table_identifier"
    tables must be sorted alphabetically by identifier
    each sample question: {"id": "<32-char-hex>", "question": ["text as array not string"]}
  7 sample questions about revenue by category, top stores, Champion customers, Electronics trend, basket size, negative gross profit, At Risk percentage.

Print Dashboard URL and Genie URL.

Then:
  databricks bundle deploy
  databricks bundle run retail_pipeline
All 8 tasks must go green. Print final confirmation.
```
