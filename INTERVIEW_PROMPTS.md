# 1-Hour Vibe-Coding Interview — Prompt Playbook

Copy each prompt block verbatim. Fill in every `[BRACKET]` before sending.
Each prompt is designed to run without follow-up questions.

---

## Before You Start (2 min)

Fill in your variables once here — you'll paste them into every prompt:

```
DOMAIN:          [e.g. "HR analytics", "e-commerce", "supply chain"]
BUSINESS_CASE:   [e.g. "track employee performance and attrition risk"]
CATALOG:         main
SCHEMA:          [e.g. hr_iq, ecomm_iq, supply_iq]
DATABRICKS_HOST: [e.g. https://dbc-xxxxx.cloud.databricks.com]
YOUR_EMAIL:      [your@email.com]

ENTITIES (3-5):  [e.g. employees, departments, roles, performance_reviews]
GOLD_TABLES (3-5):
  - [e.g. gold_attrition_risk  — one row per employee, risk score + segment]
  - [e.g. gold_dept_kpis       — one row per dept, headcount/turnover/avg_tenure]
  - [e.g. gold_performance     — one row per employee, review scores + trend]

PII_COLUMNS:     [e.g. email, full_name, phone]
SERVING_TABLE:   [Gold table that contains PII, e.g. gold_attrition_risk]
KEY_METRIC:      [e.g. attrition_risk_score, revenue, churn_probability]

SAMPLE_QUESTIONS (5-7 things a business user would ask):
  - [e.g. "Which departments have the highest attrition risk?"]
  - [e.g. "Show me employees with the lowest performance scores"]
  - ...
```

---

## PROMPT 1 — Plan (0:00–0:05)

> **Goal:** Align on architecture before writing a line of code.
> **Expected output:** Confirmed DAG, table names, governance plan. Say "proceed" to move on.

```
I'm building a [DOMAIN] analytics platform on Databricks in one hour.

Business case: [BUSINESS_CASE]

Tech stack (all non-negotiable):
- Databricks serverless compute (client "2"), Unity Catalog
- Medallion architecture: Bronze → Silver → Gold
- Lakebase (Databricks-managed Postgres) as the app serving layer
- Streamlit Databricks App connecting to Lakebase
- AI/BI Dashboard (Lakeview) + Genie Space on Gold tables
- DABs (databricks.yml) for deployment
- catalog: main, schema: [SCHEMA]

Entities: [ENTITIES]

Gold tables I need:
- [GOLD_TABLE_1]: [one-line description and grain]
- [GOLD_TABLE_2]: [one-line description and grain]
- [GOLD_TABLE_3]: [one-line description and grain]

PII columns: [PII_COLUMNS] (on [SERVING_TABLE])

Before building, show me:
1. The pipeline DAG with task names and dependencies
2. The full table list (Bronze + Silver + Gold)
3. The UC governance plan (which table gets masks, which gets tags)
4. One risk or non-obvious constraint I should know about

Keep the response to one page. I'll say "proceed" when I'm happy.
```

---

## PROMPT 2 — Build the Full Pipeline (0:05–0:25)

> **Goal:** All notebooks + DABs config written and deployed in one shot.
> **Expected output:** All files on disk, `databricks bundle deploy` succeeds, pipeline runs green.

```
Build the complete Databricks lakehouse pipeline for [DOMAIN].

catalog: main  schema: [SCHEMA]  host: [DATABRICKS_HOST]

Create these files exactly:

notebooks/00_utils.py       — tbl(), upsert() with Delta MERGE, LOAD_TS constant
notebooks/01_setup.py       — schema creation, CDF on Gold tables, mask_pii function,
                               create [SCHEMA]_analysts group
                               (catch both ResourceAlreadyExists AND ResourceConflict)
notebooks/02_data_generation.py — synthetic data with Faker for:
  [ENTITY_1]: [NUM] rows, fields: [FIELDS_1]
  [ENTITY_2]: [NUM] rows, fields: [FIELDS_2]
  [ENTITY_3]: [NUM] rows, fields: [FIELDS_3]
  CRITICAL: any numeric field used in a profit/margin/score Gold calc
  must come from a lookup, not random.uniform() — else Gold metrics are meaningless
notebooks/03_silver_transforms.py — DQ filter + dedup + MERGE into Silver;
  derive any computed columns here (e.g. line_total, tenure_days)
notebooks/04_gold_analytics.py — all [N] Gold tables using a single shared
  base_facts temp view (one scan, not N scans); include UC governance at the
  end: DROP MASK on Silver, PII tags on Silver, column masks on Gold,
  table + column comments; add Lakehouse Monitor on [SERVING_TABLE]
notebooks/07_maintenance.py  — OPTIMIZE + ANALYZE on all Gold tables
notebooks/08_run_tests.py    — integration tests; Silver non-empty + pass-rate + no-nulls;
  Gold metrics positive; CDF enabled; run_tests must be a dependency of lakebase_sync
notebooks/09_dashboard.py    — print row counts and top-5 for each Gold table

databricks.yml               — dev (default) + prod (schema: [SCHEMA]_prod)
resources/pipeline_job.yml   — tasks: setup → data_generation → silver_transforms →
  gold_analytics → [run_tests + visualization in parallel] → lakebase_sync
  Also: gold_analytics → dashboard (parallel)
  max_retries: 3 on every task; email [YOUR_EMAIL] on failure
resources/maintenance_job.yml — 02:00 UTC daily, max_retries: 3

pytest.ini, .gitignore, requirements-test.txt

After writing all files:
1. databricks bundle deploy
2. databricks bundle run [SCHEMA]_pipeline
Report what passed and what failed.
```

---

## PROMPT 3 — Self-Serve Analytics (0:25–0:35)

> **Goal:** AI/BI Dashboard + Genie Space live and accessible.
> **Expected output:** Dashboard URL + Genie URL printed.

```
Create notebooks/11_aibi_setup.py for [SCHEMA] and add it as a pipeline task.

AI/BI Dashboard — 3 pages, backed by live Gold tables:
  Page 1 "[PAGE_1_NAME]": [describe 1-2 charts, e.g. "bar chart of KEY_METRIC by DIMENSION"]
  Page 2 "[PAGE_2_NAME]": [describe 1-2 charts]
  Page 3 "[PAGE_3_NAME]": [describe 1-2 charts]

Genie Space — natural language over all Gold tables:
  Sample questions:
  - "[SAMPLE_QUESTION_1]"
  - "[SAMPLE_QUESTION_2]"
  - "[SAMPLE_QUESTION_3]"
  - "[SAMPLE_QUESTION_4]"
  - "[SAMPLE_QUESTION_5]"

Non-obvious API rules you MUST follow (these are not in the docs):
- Dashboard: every widget query field needs BOTH "name" AND "expression" keys
- Genie: POST /api/2.0/genie/spaces requires "serialized_space" as a JSON string with
  version=2; tables use field "identifier" (not "table_identifier"); tables must be
  sorted alphabetically; "question" is an array ["text"], not a string
- Use w.api_client.do() for both APIs — SDK wrapper methods have version mismatches
- List spaces: response key is "spaces", not "genie_spaces"

After writing the notebook:
1. Add aibi_setup task to pipeline_job.yml (depends on gold_analytics, parallel with lakebase_sync)
2. databricks bundle deploy
3. Run just the aibi_setup task: databricks bundle run [SCHEMA]_pipeline --task aibi_setup
Print both URLs.
```

---

## PROMPT 4 — Streamlit App on Lakebase (0:35–0:43)

> **Goal:** App deployed and connecting to Lakebase.
> **Expected output:** App URL + all four tabs rendering data.

```
Create the Streamlit Databricks App for [SCHEMA].

app.yml — MUST be this exact minimal form (no --server.* flags — Databricks injects
PORT=8000 automatically; any flag overrides it and causes 502):
  command: [streamlit, run, app.py]

apps/[SCHEMA]_insights/app.py — 4 tabs:
  Tab 1: [GOLD_TABLE_1] — [describe key chart]
  Tab 2: [GOLD_TABLE_2] — [describe key chart]
  Tab 3: [GOLD_TABLE_3] — [describe key chart]
  Tab 4: [SERVING_TABLE] — top 20 rows with PII visible for analysts

Lakebase auth pattern (the app SP cannot access Lakebase directly — use this exactly):
  1. SP fetches owner PAT from Databricks Secrets (scope=[SCHEMA]-insights, key=lakebase-token)
  2. Pop DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET env vars before WorkspaceClient(host, token=pat)
  3. Restore env vars in finally block
  4. Resolve project "[SCHEMA]-db" → production branch → primary endpoint

PostgreSQL ROUND rule: Spark exports floats as double precision.
ROUND(x, n) requires numeric. Always write ROUND(expr::numeric, n).

requirements.txt:
  streamlit>=1.35.0, plotly>=5.20.0, pandas>=2.0.0,
  psycopg2-binary>=2.9.0, databricks-sdk>=0.81.0

After writing files:
1. Run the one-time secret setup commands (print them for me to run with !)
2. databricks bundle deploy
3. databricks bundle run [SCHEMA]_insights
```

---

## PROMPT 5 — Optimize for Cost and Performance (0:43–0:52)

> **Goal:** Quantify inefficiencies, apply top-3 fixes, show before/after.
> **Expected output:** Code changes + a comparison table.

```
Audit the [SCHEMA] pipeline for cost and performance inefficiencies.

For each issue found, give me:
- What the problem is and why it matters at scale
- The fix (code change or config)
- Estimated improvement (e.g. "eliminates 3 of 4 table scans")

Then implement the top 3 highest-impact fixes right now.

I know these patterns already — do not repeat them, look for others:
- enriched_items temp view to avoid repeated scans (already done)
- OPTIMIZE in nightly maintenance job (already done)
- max_retries: 3 (already done)

Focus on:
1. Serverless DBU cost per pipeline run
2. Lakebase sync throughput and downtime
3. Liquid Clustering vs partition choice for the actual query patterns
4. Any redundant work across the 8 tasks
5. Cold-start latency in the Streamlit app

After the fixes, show a before/after table:
| Metric | Before | After |
```

---

## PROMPT 6 — Scale to 1 Million × Current Volume (0:52–1:00)

> **Goal:** Design review + concrete recommendations for 1M× scale. Show what breaks and what doesn't.
> **Expected output:** Prioritized list of architecture changes with trade-offs.

```
The current [SCHEMA] pipeline runs on [N] records. Design it for 1,000,000×:
- [N * 1M] entity records
- [N_ORDERS * 1M] fact records  
- 1,000 concurrent Streamlit app users
- Pipeline must complete in under 30 minutes
- App queries must return in under 2 seconds

For each layer, tell me:
1. What breaks first at this scale
2. The single highest-leverage fix
3. The trade-off (cost / complexity / latency)

Layers to cover:
Bronze ingestion    — batch replace vs streaming vs Auto Loader
Silver transforms   — MERGE performance at scale, Z-ORDER vs Liquid Clustering
Gold analytics      — partition strategy, incremental vs full recompute
Lakebase sync       — toPandas ceiling, JDBC parallelism, Synced Tables vs manual sync
App serving         — connection pooling, query pushdown, caching strategy
Governance          — row-level security at scale vs column masks

Then answer:
- Which Databricks features are free-tier constraints vs genuine architectural limits?
- What is the minimum change to get from current scale to 10× without a rewrite?
- At what point does Lakebase Postgres become the bottleneck, and what replaces it?

Keep each answer to 2-3 sentences. End with a prioritized action list.
```

---

## Emergency Fixes (use if a task fails mid-interview)

### Pipeline task failing — unknown test

```
The pipeline failed on task [TASK_NAME]. Here is the error:
[PASTE ERROR]

Diagnose the root cause in one sentence, apply the minimal fix,
redeploy, and re-run just the failing task:
databricks bundle run [SCHEMA]_pipeline --task [TASK_NAME]
```

### SDK / API error

```
I'm getting this error: [PASTE ERROR]

This is a Databricks SDK or REST API issue.
Check if the problem is: wrong field name, wrong key in response,
SDK version mismatch, or auth conflict.
Apply the fix and verify it works before showing me the code.
```

### App not loading

```
The Streamlit app returns [ERROR/502/blank screen].
The most common causes in order are:
1. app.yml has --server.* flags (remove them)
2. Lakebase auth — SP can't connect (use secret-based PAT pattern)
3. PostgreSQL ROUND type error (add ::numeric cast)
4. Missing package in requirements.txt
Diagnose which one this is and fix it.
```

---

## Demo Talking Points (1-min each, use at end)

After the build, hit these to show SA depth:

1. **Governance** — *"Show TBLPROPERTIES on gold_[SERVING_TABLE] and explain what delta.enableChangeDataFeed enables"*
2. **Quality gate** — *"Why does run_tests depend-on before lakebase_sync, and what does that prevent in production?"*
3. **Scan optimization** — *"How many times does the pipeline read silver_[FACT_TABLE]? Why?"*
4. **Mask placement** — *"Why is the column mask on the Gold table, not Silver?"*
5. **Genie** — *Open the Genie Space, ask one sample question, show the generated SQL*
6. **Monitor** — *Open the auto-generated Lakehouse Monitor dashboard, point to the segment distribution*
7. **Scale** — *"What breaks first at 100× and what's the one-line fix?"*

---

## Time Budget

```
0:00–0:05   Prompt 1 — Plan + align
0:05–0:25   Prompt 2 — Full pipeline build + first run
0:25–0:35   Prompt 3 — AI/BI Dashboard + Genie Space
0:35–0:43   Prompt 4 — Streamlit app
0:43–0:52   Prompt 5 — Cost + performance optimization
0:52–1:00   Prompt 6 — Scale design discussion + demo talking points
```

If the pipeline run takes longer than 5 minutes (it won't with serverless), start
Prompt 3 while it runs — the notebooks are already deployed.
