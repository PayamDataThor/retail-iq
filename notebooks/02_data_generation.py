# Databricks notebook source

# COMMAND ----------
# MAGIC %pip install faker==24.0.0 --quiet

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 · Bronze: Synthetic Data Generation
# MAGIC
# MAGIC Generates a realistic retail dataset using Faker and writes six Delta tables
# MAGIC that form the Bronze layer of a medallion architecture.
# MAGIC
# MAGIC **Data model**
# MAGIC | Table | Type | Key |
# MAGIC |---|---|---|
# MAGIC | bronze_customers | dimension | customer_id |
# MAGIC | bronze_products | dimension | product_id |
# MAGIC | bronze_stores | dimension | store_id |
# MAGIC | bronze_dates | dimension | date_id |
# MAGIC | bronze_orders | fact | order_id |
# MAGIC | bronze_order_items | fact | order_item_id |

# COMMAND ----------

dbutils.widgets.text("catalog",       "main",      "Catalog")
dbutils.widgets.text("schema",        "retail_iq", "Schema")
dbutils.widgets.text("num_customers", "1000",      "# Customers")
dbutils.widgets.text("num_products",  "100",       "# Products")
dbutils.widgets.text("num_stores",    "20",        "# Stores")
dbutils.widgets.text("num_orders",    "10000",     "# Orders")

catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")
NUM_CUSTOMERS = int(dbutils.widgets.get("num_customers"))
NUM_PRODUCTS  = int(dbutils.widgets.get("num_products"))
NUM_STORES    = int(dbutils.widgets.get("num_stores"))
NUM_ORDERS    = int(dbutils.widgets.get("num_orders"))

# COMMAND ----------
# MAGIC %run ./00_utils

# COMMAND ----------

import random
from datetime import date, timedelta

import pandas as pd
from faker import Faker
from pyspark.sql import functions as F

fake = Faker()
Faker.seed(42)
random.seed(42)

START_DATE = date(2023, 1, 1)
END_DATE   = date(2024, 12, 31)

def rand_date(start=START_DATE, end=END_DATE) -> str:
    return fake.date_between(start, end).isoformat()

def write_bronze(pdf: pd.DataFrame, table: str, date_cols: list = None):
    """Write a pandas DataFrame to a Bronze Delta table, casting date columns."""
    df = spark.createDataFrame(pdf)
    if date_cols:
        for col in date_cols:
            df = df.withColumn(col, F.to_date(col, "yyyy-MM-dd"))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.{schema}.{table}"))
    print(f"  ✓ {table}: {len(pdf):,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Customers

# COMMAND ----------

SEGMENTS = ["High Value", "Regular", "At Risk", "New", "Dormant"]

customers = [
    {
        "customer_id": i + 1,
        "full_name":   fake.name(),
        "email":       fake.email(),
        "city":        fake.city(),
        "state":       fake.state_abbr(),
        "signup_date": rand_date(date(2020, 1, 1), END_DATE),
        "segment":     random.choices(SEGMENTS, weights=[10, 40, 15, 20, 15])[0],
        "age":         random.randint(18, 75),
    }
    for i in range(NUM_CUSTOMERS)
]

write_bronze(pd.DataFrame(customers), "bronze_customers", date_cols=["signup_date"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Products
# MAGIC
# MAGIC Names are built from `tier × base_item` so every combination is unique and
# MAGIC meaningful (e.g. "Pro Laptop", "Elite Yoga Mat").  Price ranges are set per
# MAGIC category to reflect realistic retail economics.

# COMMAND ----------

CATEGORIES = {
    #  category         base items                     (cost_min, cost_max)
    "Electronics":   (["Laptop", "Smartphone", "Tablet", "Headphones", "Smart Watch"],   (80,  900)),
    "Clothing":      (["T-Shirt", "Jeans", "Jacket", "Running Shoes", "Dress"],          (8,   120)),
    "Food & Bev":    (["Coffee Blend", "Green Tea", "Protein Bar", "Energy Drink", "Dark Chocolate"], (3, 30)),
    "Home & Garden": (["LED Lamp", "Throw Pillow", "Area Rug", "Scented Candle", "Plant Pot"],        (10, 200)),
    "Sports":        (["Yoga Mat", "Dumbbell Set", "Water Bottle", "Resistance Bands", "Bike Helmet"], (15, 300)),
}
TIERS = ["Pro", "Elite", "Ultra", "Max", "Plus"]

# Build all 125 unique (tier, category, base_item) combinations
combos = [
    (tier, cat, base)
    for tier in TIERS
    for cat, (base_items, _) in CATEGORIES.items()
    for base in base_items
]

products = []
for i in range(NUM_PRODUCTS):
    tier, cat, base = combos[i % len(combos)]
    _, (cost_min, cost_max) = CATEGORIES[cat]
    cost = round(random.uniform(cost_min, cost_max), 2)
    products.append({
        "product_id": i + 1,
        "name":       f"{tier} {base}",
        "category":   cat,
        "brand":      fake.company(),
        "cost":       cost,
        "price":      round(cost * random.uniform(1.3, 2.5), 2),
    })

write_bronze(pd.DataFrame(products), "bronze_products")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Stores

# COMMAND ----------

REGIONS = {
    "West":     ["CA", "WA", "OR"],
    "East":     ["NY", "FL", "MA"],
    "South":    ["TX", "GA", "NC"],
    "Midwest":  ["IL", "OH", "MI"],
    "Mountain": ["CO", "AZ", "NV"],
}

stores = []
for i in range(NUM_STORES):
    region = random.choice(list(REGIONS))
    stores.append({
        "store_id":    i + 1,
        "name":        f"{fake.city()} Store",
        "city":        fake.city(),
        "state":       random.choice(REGIONS[region]),
        "region":      region,
        "opened_date": rand_date(date(2015, 1, 1), START_DATE),
    })

write_bronze(pd.DataFrame(stores), "bronze_stores", date_cols=["opened_date"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension: Dates

# COMMAND ----------

dates = []
d = START_DATE
while d <= END_DATE:
    dates.append({
        "date_id":      int(d.strftime("%Y%m%d")),
        "date":         d.isoformat(),
        "year":         d.year,
        "quarter":      (d.month - 1) // 3 + 1,
        "month":        d.month,
        "month_name":   d.strftime("%B"),
        "week_of_year": d.isocalendar()[1],
        "day_of_week":  d.weekday(),   # 0 = Monday
        "day_name":     d.strftime("%A"),
        "is_weekend":   d.weekday() >= 5,
    })
    d += timedelta(days=1)

write_bronze(pd.DataFrame(dates), "bronze_dates", date_cols=["date"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fact: Orders
# MAGIC
# MAGIC **Scale note:** At this volume (~10 k rows) partitioning by `order_date` would
# MAGIC create ~700 tiny files and hurt performance.  At production scale (>100 M rows)
# MAGIC you would either partition by month or — better — use Delta Liquid Clustering
# MAGIC on `(order_date, customer_id)` to get the same data-skipping benefit without
# MAGIC static partition overhead.

# COMMAND ----------

STATUSES = ["completed", "completed", "completed", "returned", "cancelled"]
CHANNELS = ["online", "online", "in-store", "mobile"]

orders = [
    {
        "order_id":    i + 1,
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "store_id":    random.randint(1, NUM_STORES),
        "order_date":  rand_date(),
        "status":      random.choice(STATUSES),
        "channel":     random.choice(CHANNELS),
    }
    for i in range(NUM_ORDERS)
]

write_bronze(pd.DataFrame(orders), "bronze_orders", date_cols=["order_date"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fact: Order Items
# MAGIC
# MAGIC `unit_price` is sourced from the product catalog (with ±5 % noise to simulate
# MAGIC real-world price variation such as promotions).  `line_total` is intentionally
# MAGIC omitted here and derived in Silver so it is always consistent with the stored
# MAGIC unit_price, quantity, and discount values.

# COMMAND ----------

# Build a price lookup so unit_price is grounded in the product catalog
price_map = {p["product_id"]: p["price"] for p in products}

ITEM_COUNT_WEIGHTS = [30, 35, 20, 10, 5]   # probability of 1–5 items per order
DISCOUNTS          = [0, 0, 0, 0.05, 0.10, 0.15, 0.20]

order_items = []
item_id = 1
for order in orders:
    n_items = random.choices(range(1, 6), weights=ITEM_COUNT_WEIGHTS)[0]
    for _ in range(n_items):
        product_id = random.randint(1, NUM_PRODUCTS)
        # Small noise factor simulates promotions / price adjustments
        unit_price = round(price_map[product_id] * random.uniform(0.95, 1.0), 2)
        qty        = random.randint(1, 4)
        discount   = random.choice(DISCOUNTS)
        order_items.append({
            "order_item_id": item_id,
            "order_id":      order["order_id"],
            "product_id":    product_id,
            "quantity":      qty,
            "unit_price":    unit_price,
            "discount":      discount,
            # line_total derived in Silver to keep Bronze raw/immutable
        })
        item_id += 1

write_bronze(pd.DataFrame(order_items), "bronze_order_items")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\nBronze layer — row counts:")
for t in ["bronze_customers", "bronze_products", "bronze_stores",
          "bronze_dates", "bronze_orders", "bronze_order_items"]:
    n = spark.table(f"`{catalog}`.`{schema}`.`{t}`").count()
    print(f"  {t:<30} {n:>10,}")
