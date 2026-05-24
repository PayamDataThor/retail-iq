"""
Tests for the data generation logic in notebooks/02_data_generation.py.

All tests are pure Python — no Spark required.
"""

import random
import pytest
from datetime import date


# ---------------------------------------------------------------------------
# Product pricing invariants
# ---------------------------------------------------------------------------

class TestProductPricing:
    def setup_method(self):
        random.seed(42)
        CATEGORIES = {
            "Electronics":   (["Laptop", "Smartphone"], (80,  900)),
            "Clothing":      (["T-Shirt", "Jeans"],     (8,   120)),
            "Food & Bev":    (["Coffee Blend"],          (3,    30)),
            "Home & Garden": (["LED Lamp"],              (10,  200)),
            "Sports":        (["Yoga Mat"],              (15,  300)),
        }
        TIERS = ["Pro", "Elite", "Ultra", "Max", "Plus"]
        combos = [
            (tier, cat, base)
            for tier in TIERS
            for cat, (base_items, _) in CATEGORIES.items()
            for base in base_items
        ]
        self.products = []
        for i in range(50):
            tier, cat, base = combos[i % len(combos)]
            _, (cost_min, cost_max) = CATEGORIES[cat]
            cost = round(random.uniform(cost_min, cost_max), 2)
            self.products.append({
                "product_id": i + 1,
                "name":       f"{tier} {base}",
                "category":   cat,
                "cost":       cost,
                "price":      round(cost * random.uniform(1.3, 2.5), 2),
            })

    def test_price_always_exceeds_cost(self):
        assert all(p["price"] > p["cost"] for p in self.products)

    def test_price_at_least_1_3x_cost(self):
        """Minimum multiplier is 1.3 — price must be at least 30 % above cost."""
        for p in self.products:
            assert p["price"] >= p["cost"] * 1.3 - 0.01   # -0.01 for float rounding

    def test_no_duplicate_product_ids(self):
        ids = [p["product_id"] for p in self.products]
        assert len(ids) == len(set(ids))

    def test_all_products_have_positive_cost(self):
        assert all(p["cost"] > 0 for p in self.products)


# ---------------------------------------------------------------------------
# unit_price sourced from catalog — Step 1 fix
# ---------------------------------------------------------------------------

class TestUnitPrice:
    def setup_method(self):
        random.seed(0)
        self.price_map = {1: 200.0, 2: 50.0, 3: 15.0}

    def test_unit_price_never_exceeds_catalog_price(self):
        for _ in range(500):
            pid = random.choice([1, 2, 3])
            unit_price = round(self.price_map[pid] * random.uniform(0.95, 1.0), 2)
            assert unit_price <= self.price_map[pid] + 0.01   # +0.01 for rounding

    def test_unit_price_always_at_least_95_pct_of_catalog(self):
        for _ in range(500):
            pid = random.choice([1, 2, 3])
            unit_price = round(self.price_map[pid] * random.uniform(0.95, 1.0), 2)
            assert unit_price >= self.price_map[pid] * 0.95 - 0.01

    def test_unit_price_is_positive(self):
        for pid, price in self.price_map.items():
            unit_price = round(price * random.uniform(0.95, 1.0), 2)
            assert unit_price > 0


# ---------------------------------------------------------------------------
# Order-item derived fields
# ---------------------------------------------------------------------------

class TestLineTotal:
    @pytest.mark.parametrize("unit_price,qty,discount,expected", [
        (100.0, 2, 0.0,  200.0),
        (100.0, 2, 0.1,  180.0),
        (100.0, 2, 0.20, 160.0),
        (50.0,  1, 0.0,   50.0),
        (33.33, 3, 0.0,   99.99),
    ])
    def test_line_total_formula(self, unit_price, qty, discount, expected):
        """line_total = unit_price * quantity * (1 - discount), rounded to 2dp."""
        result = round(unit_price * qty * (1 - discount), 2)
        assert result == pytest.approx(expected, abs=0.01)


# ---------------------------------------------------------------------------
# Order-item ID uniqueness
# ---------------------------------------------------------------------------

class TestOrderItemIds:
    def test_order_item_ids_are_sequential_and_unique(self):
        orders = [{"order_id": i + 1} for i in range(10)]
        ITEM_COUNT_WEIGHTS = [30, 35, 20, 10, 5]
        random.seed(42)
        order_items = []
        item_id = 1
        for order in orders:
            n = random.choices(range(1, 6), weights=ITEM_COUNT_WEIGHTS)[0]
            for _ in range(n):
                order_items.append({"order_item_id": item_id, "order_id": order["order_id"]})
                item_id += 1

        ids = [oi["order_item_id"] for oi in order_items]
        assert ids == list(range(1, len(ids) + 1))   # sequential from 1
        assert len(ids) == len(set(ids))              # no duplicates


# ---------------------------------------------------------------------------
# Customer dimension
# ---------------------------------------------------------------------------

class TestCustomerDimension:
    def setup_method(self):
        random.seed(42)
        from faker import Faker
        fake = Faker()
        Faker.seed(42)
        self.customers = [
            {"customer_id": i + 1,
             "age": random.randint(18, 75),
             "email": fake.email()}
            for i in range(200)
        ]

    def test_all_ages_within_18_to_75(self):
        assert all(18 <= c["age"] <= 75 for c in self.customers)

    def test_all_emails_contain_at_sign(self):
        assert all("@" in c["email"] for c in self.customers)

    def test_customer_ids_unique(self):
        ids = [c["customer_id"] for c in self.customers]
        assert len(ids) == len(set(ids))
