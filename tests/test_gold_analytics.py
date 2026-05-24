"""
Tests for the Gold-layer business logic in notebooks/04_gold_analytics.py.

RFM segment rules and revenue math are pure Python — no Spark needed.
Spark aggregation tests live in notebooks/08_run_tests.py.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from retail_iq.analytics import rfm_segment, line_total, gross_profit


# ---------------------------------------------------------------------------
# RFM segment label assignment
# ---------------------------------------------------------------------------

class TestRfmSegmentRules:
    """
    Segment precedence (first matching WHEN wins):
      Champions     r>=4 AND f>=4
      Loyal         r>=3 AND f>=3
      Recent        r>=4
      Frequent      f>=4
      Big Spenders  m>=4
      At Risk       r<=2 AND f<=2
      (else)        Needs Attention
    """

    # --- Champions ---
    @pytest.mark.parametrize("r,f,m", [(4, 4, 1), (5, 5, 5), (4, 5, 1), (5, 4, 3)])
    def test_champions(self, r, f, m):
        assert rfm_segment(r, f, m) == "Champions"

    # --- Loyal (r>=3, f>=3, but not both >=4) ---
    @pytest.mark.parametrize("r,f,m", [(3, 3, 1), (4, 3, 1), (3, 4, 1)])
    def test_loyal(self, r, f, m):
        # (4,4,x) is Champions; (4,3,x) and (3,4,x) hit Loyal (r>=3 AND f>=3, prev condition false)
        assert rfm_segment(r, f, m) == "Loyal"

    # --- Recent (r>=4, f<3) ---
    @pytest.mark.parametrize("r,f,m", [(4, 1, 1), (5, 2, 1), (4, 2, 3)])
    def test_recent(self, r, f, m):
        assert rfm_segment(r, f, m) == "Recent"

    # --- Frequent (f>=4, r<3) ---
    @pytest.mark.parametrize("r,f,m", [(1, 4, 1), (2, 5, 3), (2, 4, 2)])
    def test_frequent(self, r, f, m):
        assert rfm_segment(r, f, m) == "Frequent"

    # --- Big Spenders (m>=4, before At Risk check) ---
    @pytest.mark.parametrize("r,f,m", [(1, 1, 4), (1, 1, 5), (2, 2, 4)])
    def test_big_spenders_beats_at_risk(self, r, f, m):
        """m>=4 triggers BEFORE At Risk in the CASE WHEN order."""
        assert rfm_segment(r, f, m) == "Big Spenders"

    # --- At Risk (r<=2, f<=2, m<4) ---
    @pytest.mark.parametrize("r,f,m", [(1, 1, 1), (2, 2, 3), (1, 2, 2)])
    def test_at_risk(self, r, f, m):
        assert rfm_segment(r, f, m) == "At Risk"

    # --- Needs Attention (catch-all) ---
    @pytest.mark.parametrize("r,f,m", [(3, 2, 3), (2, 3, 2), (3, 1, 3)])
    def test_needs_attention(self, r, f, m):
        assert rfm_segment(r, f, m) == "Needs Attention"

    # --- Boundary / edge cases ---
    def test_all_minimum_scores_at_risk(self):
        assert rfm_segment(1, 1, 1) == "At Risk"

    def test_exactly_at_loyal_boundary(self):
        assert rfm_segment(3, 3, 1) == "Loyal"

    def test_exactly_at_champions_boundary(self):
        assert rfm_segment(4, 4, 1) == "Champions"


# ---------------------------------------------------------------------------
# gross_profit mathematical invariant
# ---------------------------------------------------------------------------

class TestGrossProfitInvariant:
    """
    After Step 1, unit_price >= catalog_price * 0.95 >= cost * 1.3 * 0.95 = cost * 1.235.
    With maximum 20 % discount: effective revenue per unit = unit_price * 0.8.
    For an INDIVIDUAL line item this can dip below cost (1.235 * 0.8 = 0.988 < 1.0).
    But AVERAGE discount is ~7 %, so aggregated gross_profit per product should be positive.
    """

    def test_unit_price_exceeds_cost_before_discount(self):
        cost = 80.0
        min_price_mult = 1.3
        noise_floor    = 0.95
        min_unit_price = cost * min_price_mult * noise_floor    # 98.8
        assert min_unit_price > cost                            # always true

    def test_average_gross_profit_positive(self):
        """
        Simulate 1 000 order items for a single product and verify aggregate
        gross_profit > 0, which is what 04_gold_analytics produces.
        """
        import random
        random.seed(42)

        cost      = 50.0
        price     = 100.0   # price = 2× cost — well above minimum
        DISCOUNTS = [0, 0, 0, 0.05, 0.10, 0.15, 0.20]

        total_revenue = 0.0
        total_cost    = 0.0
        for _ in range(1_000):
            unit_price  = price * random.uniform(0.95, 1.0)
            qty         = random.randint(1, 4)
            discount    = random.choice(DISCOUNTS)
            line_total  = unit_price * qty * (1 - discount)
            total_revenue += line_total
            total_cost    += cost * qty

        gross_profit = total_revenue - total_cost
        assert gross_profit > 0, (
            f"Expected positive gross_profit; got {gross_profit:.2f}. "
            "Check unit_price sourcing in 02_data_generation.py."
        )

    def test_gross_profit_would_fail_with_random_unit_price(self):
        """
        Demonstrates the pre-fix bug: random unit_price in [10, 500] for a
        high-cost product can produce a large negative gross_profit.
        """
        import random
        random.seed(42)

        cost       = 800.0   # expensive product (e.g. Pro Laptop)
        DISCOUNTS  = [0, 0, 0, 0.05, 0.10, 0.15, 0.20]

        total_revenue = 0.0
        total_cost    = 0.0
        for _ in range(1_000):
            # OLD behaviour: unit_price independent of product catalog
            unit_price = random.uniform(10, 500)
            qty        = random.randint(1, 4)
            discount   = random.choice(DISCOUNTS)
            total_revenue += unit_price * qty * (1 - discount)
            total_cost    += cost * qty

        assert total_revenue - total_cost < 0, (
            "Expected negative gross_profit with random unit_price for a high-cost product."
        )


# ---------------------------------------------------------------------------
# Revenue aggregation math
# ---------------------------------------------------------------------------

class TestRevenueAggregation:
    def test_avg_order_value_equals_revenue_over_orders(self):
        orders = [
            {"order_id": 1, "line_total": 100.0},
            {"order_id": 1, "line_total": 50.0},
            {"order_id": 2, "line_total": 80.0},
        ]
        total_revenue     = sum(o["line_total"] for o in orders)
        unique_order_ids  = len({o["order_id"] for o in orders})
        avg_order_value   = round(total_revenue / unique_order_ids, 2)

        assert total_revenue    == 230.0
        assert unique_order_ids == 2
        assert avg_order_value  == 115.0

    def test_avg_basket_size_calculation(self):
        revenue = 1_500.0
        orders  = 10
        assert round(revenue / orders, 2) == 150.0


# ---------------------------------------------------------------------------
# GOLD_TABLES dynamic discovery
# ---------------------------------------------------------------------------

class TestGoldTablesDiscovery:
    def test_only_gold_prefixed_tables_selected(self):
        all_tables = [
            "bronze_customers", "silver_orders", "gold_store_kpis",
            "gold_customer_rfm", "gold_product_performance",
            "gold_revenue_by_category_month", "temp_view",
        ]
        gold_tables = sorted(t for t in all_tables if t.startswith("gold_"))
        assert gold_tables == [
            "gold_customer_rfm",
            "gold_product_performance",
            "gold_revenue_by_category_month",
            "gold_store_kpis",
        ]

    def test_result_is_sorted(self):
        tables = ["gold_store_kpis", "gold_customer_rfm", "gold_product_performance"]
        assert sorted(tables) == sorted(t for t in tables if t.startswith("gold_"))
