# 10 Prompts — Paste One at a Time

---

### 1. Project Setup

```
I want to build a [domain] analytics project on Databricks Free Edition.

The business context: [2-3 sentences describing what the business does and what insights are needed].

Use a medallion architecture (Bronze → Silver → Gold) with Delta Lake, Unity Catalog,
and Databricks Asset Bundles for deployment. Set up the project structure, bundle config
with dev and prod targets, and a shared utilities notebook. Deploy to
[your Databricks host].
```

---

### 2. Data Layer

```
Generate realistic synthetic data for the [domain] use case.

Entities: [list your 4-6 business entities, e.g. customers, products, stores, orders, order items].
Scale: [e.g. 1000 customers, 100 products, 10000 transactions].

Create a Bronze data generation notebook using Faker. Make sure all numeric fields used
in financial or performance calculations are internally consistent — for example,
selling prices must be derived from cost prices, not generated independently.
Write all entities as Delta tables.
```

---

### 3. Silver Layer

```
Add a Silver transformation layer that validates, cleans, and enriches the Bronze data.

For each entity, define and enforce data quality rules (nulls, ranges, referential integrity).
Derive any computed fields here (e.g. line totals, margins, durations).
Use Delta MERGE for idempotent upserts. Apply Liquid Clustering on the join keys
each Gold query will use. Print a quality report showing pass rates.

Also extract the business logic (quality predicates, calculation functions) into a
pure Python module under src/ so it can be unit tested without Spark.
```

---

### 4. Gold Analytics Layer

```
Build the Gold aggregation layer for [domain] analytics.

Create these business-level tables:
- [Table 1]: [grain and purpose, e.g. "revenue by category and month for trend analysis"]
- [Table 2]: [grain and purpose, e.g. "KPIs per store: revenue, orders, basket size"]
- [Table 3]: [grain and purpose, e.g. "product performance: units, revenue, profit, discount"]
- [Table 4]: [grain and purpose, e.g. "customer segmentation by recency, frequency, spend"]

Optimize for query performance — avoid redundant table scans.
Enable Change Data Feed on all Gold tables.
```

---

### 5. Tests and Evaluation

```
Add a comprehensive test suite and evaluate the solution.

Write unit tests for all business logic in src/ — no Spark required, fast to run locally.
Write integration tests as a Databricks notebook that runs against the live tables and
acts as a quality gate in the pipeline.

Then evaluate the full solution for: data quality, pipeline performance, cost on serverless
compute, and scalability. Write a README with Mermaid architecture diagrams.
Be honest about limitations and what would break at 10× or 100× current data volume.
```

---

### 6. Governance

```
Add enterprise data governance to the project.

The [entity] table contains PII columns: [list columns, e.g. email, full_name].
Set up Unity Catalog column-level security so that only members of an analysts group
see real values — everyone else sees masked output.
Tag PII columns for data catalog discovery.
Add table and column descriptions to all Gold tables.
Attach a Lakehouse Monitor to the primary Gold table to detect data drift over time.
```

---

### 7. Pipeline Orchestration

```
Wire all notebooks into a production pipeline using Databricks Asset Bundles.

The pipeline should run in the right order with proper dependencies.
Tests must pass before data is promoted to the serving layer.
Tasks that don't depend on each other should run in parallel.
Add a separate nightly maintenance job for OPTIMIZE and ANALYZE — keep it off
the critical pipeline path.
Set up retry logic, timeouts, and email alerts on failure.
Deploy and run the full pipeline. All tasks must go green.
```

---

### 8. Serving Layer and App

```
Add a live serving layer so business users can explore the data through a web app.

Sync the Gold tables to Lakebase (Databricks-managed Postgres) after each pipeline run.
The sync should be zero-downtime — app queries must never see a gap between runs.

Build a Streamlit Databricks App that connects to Lakebase and visualises the key
Gold tables with interactive charts. Deploy it to Databricks Apps.
The app should handle connection errors gracefully and cache queries for performance.
```

---

### 9. Self-Serve Analytics

```
Add self-serve analytics so business users can explore data without writing SQL.

Create an AI/BI Dashboard (Lakeview) with curated charts covering the most important
views of the Gold data — at minimum: trend over time, performance by dimension, and
customer or entity segmentation.

Create a Genie Space over all Gold tables with sample questions that represent what
a business user would actually ask. Pre-load it with 5-7 example questions.

Both should be created programmatically and idempotently as a pipeline task.
```

---

### 10. Optimization and Scale

```
Review the full solution and optimise it.

First, identify and fix the top inefficiencies in cost (DBU usage per run) and
performance (query latency, pipeline duration). Show a before/after comparison.

Then design for 1 million times the current data volume:
- What breaks first in each layer?
- What is the minimal change to handle 10× without a rewrite?
- At what point does each component (Delta, Lakebase, the app) become the bottleneck?
- What Databricks-native features replace or upgrade each layer at that scale?

Prioritise recommendations by impact and implementation effort.
```
