"""
Data-quality filter predicates for the Silver layer.

These are the canonical definitions.  notebooks/03_silver_transforms.py applies
the equivalent Spark .filter() calls; tests/test_silver_transforms.py verifies
these Python mirrors agree with the notebook rules.

If you change a filter rule in the notebook, update the matching function here
and vice versa so the unit tests stay honest.
"""


def customer_passes_silver(row: dict) -> bool:
    """Mirror of 03_silver_transforms.py customer filters."""
    cid   = row.get("customer_id")
    email = row.get("email", "")
    age   = row.get("age", 0)
    return cid is not None and "@" in email and 18 <= age <= 100


def product_passes_silver(row: dict) -> bool:
    """Mirror of 03_silver_transforms.py product filters."""
    pid   = row.get("product_id")
    cost  = row.get("cost", 0)
    price = row.get("price", 0)
    return pid is not None and cost > 0 and price > cost


def order_passes_silver(row: dict) -> bool:
    """Mirror of 03_silver_transforms.py order filters."""
    valid_statuses = {"completed", "returned", "cancelled"}
    return (row.get("order_id") is not None
            and row.get("order_date") is not None
            and row.get("status") in valid_statuses)


def order_item_passes_silver(row: dict) -> bool:
    """Mirror of 03_silver_transforms.py order_item filters."""
    return (row.get("order_item_id") is not None
            and row.get("quantity", 0) > 0
            and row.get("unit_price", 0) > 0
            and 0 <= row.get("discount", -1) <= 1)
