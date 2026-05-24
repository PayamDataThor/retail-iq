"""
Tests for the Silver data-quality filter rules in notebooks/03_silver_transforms.py.

Each test exercises a single filter predicate using the Python mirror functions
defined in conftest.py, keeping tests fast and Spark-free.

Spark-based tests (actual DataFrame .filter() and upsert() calls) are in
notebooks/08_run_tests.py.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from retail_iq.quality import (
    customer_passes_silver,
    product_passes_silver,
    order_passes_silver,
    order_item_passes_silver,
)


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

class TestSilverCustomerFilters:
    def test_valid_customer_passes(self):
        assert customer_passes_silver({"customer_id": 1, "email": "a@b.com", "age": 30})

    def test_null_customer_id_rejected(self):
        assert not customer_passes_silver({"customer_id": None, "email": "a@b.com", "age": 30})

    def test_missing_at_sign_rejected(self):
        assert not customer_passes_silver({"customer_id": 1, "email": "notanemail", "age": 30})

    def test_email_with_at_sign_accepted(self):
        assert customer_passes_silver({"customer_id": 1, "email": "user@domain.co.uk", "age": 30})

    @pytest.mark.parametrize("age", [17, 101, 0, -5, 200])
    def test_out_of_range_age_rejected(self, age):
        assert not customer_passes_silver({"customer_id": 1, "email": "a@b.com", "age": age})

    @pytest.mark.parametrize("age", [18, 50, 100])
    def test_boundary_ages_accepted(self, age):
        assert customer_passes_silver({"customer_id": 1, "email": "a@b.com", "age": age})


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

class TestSilverProductFilters:
    def test_valid_product_passes(self):
        assert product_passes_silver({"product_id": 1, "cost": 10.0, "price": 25.0})

    def test_null_product_id_rejected(self):
        assert not product_passes_silver({"product_id": None, "cost": 10.0, "price": 25.0})

    def test_zero_cost_rejected(self):
        assert not product_passes_silver({"product_id": 1, "cost": 0.0, "price": 25.0})

    def test_negative_cost_rejected(self):
        assert not product_passes_silver({"product_id": 1, "cost": -5.0, "price": 25.0})

    def test_price_equal_to_cost_rejected(self):
        assert not product_passes_silver({"product_id": 1, "cost": 10.0, "price": 10.0})

    def test_price_less_than_cost_rejected(self):
        assert not product_passes_silver({"product_id": 1, "cost": 10.0, "price": 8.0})

    def test_price_just_above_cost_accepted(self):
        assert product_passes_silver({"product_id": 1, "cost": 10.0, "price": 10.01})


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

class TestSilverOrderFilters:
    @pytest.mark.parametrize("status", ["completed", "returned", "cancelled"])
    def test_valid_statuses_accepted(self, status):
        assert order_passes_silver({"order_id": 1, "order_date": "2024-01-01", "status": status})

    @pytest.mark.parametrize("status", ["pending", "processing", "shipped", "", "COMPLETED"])
    def test_invalid_statuses_rejected(self, status):
        assert not order_passes_silver({"order_id": 1, "order_date": "2024-01-01", "status": status})

    def test_null_order_id_rejected(self):
        assert not order_passes_silver({"order_id": None, "order_date": "2024-01-01", "status": "completed"})

    def test_null_order_date_rejected(self):
        assert not order_passes_silver({"order_id": 1, "order_date": None, "status": "completed"})


# ---------------------------------------------------------------------------
# Order items
# ---------------------------------------------------------------------------

class TestSilverOrderItemFilters:
    def test_valid_order_item_passes(self):
        assert order_item_passes_silver(
            {"order_item_id": 1, "quantity": 2, "unit_price": 50.0, "discount": 0.1}
        )

    def test_null_order_item_id_rejected(self):
        assert not order_item_passes_silver(
            {"order_item_id": None, "quantity": 2, "unit_price": 50.0, "discount": 0.0}
        )

    def test_zero_quantity_rejected(self):
        assert not order_item_passes_silver(
            {"order_item_id": 1, "quantity": 0, "unit_price": 50.0, "discount": 0.0}
        )

    def test_negative_quantity_rejected(self):
        assert not order_item_passes_silver(
            {"order_item_id": 1, "quantity": -1, "unit_price": 50.0, "discount": 0.0}
        )

    def test_zero_unit_price_rejected(self):
        assert not order_item_passes_silver(
            {"order_item_id": 1, "quantity": 1, "unit_price": 0.0, "discount": 0.0}
        )

    def test_negative_discount_rejected(self):
        assert not order_item_passes_silver(
            {"order_item_id": 1, "quantity": 1, "unit_price": 10.0, "discount": -0.01}
        )

    def test_discount_above_one_rejected(self):
        assert not order_item_passes_silver(
            {"order_item_id": 1, "quantity": 1, "unit_price": 10.0, "discount": 1.01}
        )

    @pytest.mark.parametrize("discount", [0.0, 0.05, 0.10, 0.15, 0.20, 1.0])
    def test_boundary_discounts_accepted(self, discount):
        assert order_item_passes_silver(
            {"order_item_id": 1, "quantity": 1, "unit_price": 10.0, "discount": discount}
        )
