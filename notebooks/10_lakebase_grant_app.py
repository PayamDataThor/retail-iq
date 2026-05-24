# Databricks notebook source
# MAGIC %md
# MAGIC ## Grant App Service Principal Lakebase Access
# MAGIC
# MAGIC Run this notebook **once** as yourself (p.amani@gmail.com) to grant the
# MAGIC retail-insights app's service principal SELECT access to all Gold tables in Postgres.
# MAGIC
# MAGIC The app SP client ID is: `b5ccc2cc-1617-4996-b642-8f3028915c51`

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk>=0.81.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient

# ── Resolve Lakebase endpoint as yourself ─────────────────────────────────────

w = WorkspaceClient()

LAKEBASE_PROJECT = "retail-iq-db"
parent = f"projects/{LAKEBASE_PROJECT}"
branches  = list(w.postgres.list_branches(parent=parent))
prod      = next(b for b in branches if b.name.endswith("/production"))
endpoints = list(w.postgres.list_endpoints(parent=prod.name))
primary   = next(e for e in endpoints if "primary" in e.name)

host    = primary.status.hosts.host
ep_name = primary.name
me      = w.current_user.me().user_name
token   = w.postgres.generate_database_credential(endpoint=ep_name).token

print(f"Connecting as: {me}")
print(f"Host: {host}")

# COMMAND ----------

# ── Grant the app SP access ───────────────────────────────────────────────────

APP_SP = "b5ccc2cc-1617-4996-b642-8f3028915c51"

conn = psycopg2.connect(
    host=host, port=5432, dbname="databricks_postgres",
    user=me, password=token, sslmode="require", connect_timeout=15,
)
conn.autocommit = True
cur = conn.cursor()

# Create the Postgres role for the SP if it doesn't exist yet
cur.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{APP_SP}') THEN
            CREATE ROLE "{APP_SP}" WITH LOGIN NOINHERIT;
            RAISE NOTICE 'Created role %', '{APP_SP}';
        ELSE
            RAISE NOTICE 'Role % already exists', '{APP_SP}';
        END IF;
    END $$;
""")

cur.execute(f'GRANT CONNECT ON DATABASE databricks_postgres TO "{APP_SP}";')
cur.execute(f'GRANT USAGE ON SCHEMA public TO "{APP_SP}";')
cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{APP_SP}";')

print(f"✓ Granted SP [{APP_SP}] SELECT on all public tables")

# Verify
cur.execute(f"""
    SELECT table_name
    FROM information_schema.role_table_grants
    WHERE grantee = '{APP_SP}' AND privilege_type = 'SELECT'
    ORDER BY table_name;
""")
rows = cur.fetchall()
print(f"\nTables accessible to SP ({len(rows)}):")
for (t,) in rows:
    print(f"  • {t}")

cur.close()
conn.close()
