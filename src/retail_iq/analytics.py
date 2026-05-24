"""
Pure-Python analytics helpers.

These mirror the SQL business logic in 04_gold_analytics.py so it can be
tested locally without a Spark session.

If you change the CASE WHEN segment rules in the notebook, update rfm_segment()
here as well.
"""


def rfm_segment(r: int, f: int, m: int) -> str:
    """
    Mirror of the CASE WHEN segment assignment in 04_gold_analytics.py.

    Precedence (first matching rule wins):
      Champions     r >= 4 AND f >= 4
      Loyal         r >= 3 AND f >= 3
      Recent        r >= 4
      Frequent      f >= 4
      Big Spenders  m >= 4
      At Risk       r <= 2 AND f <= 2
      (else)        Needs Attention

    Note: Big Spenders is evaluated BEFORE At Risk, so a dormant high-spender
    (r=1, f=1, m=5) is classified as 'Big Spenders', not 'At Risk'.
    """
    if r >= 4 and f >= 4:
        return "Champions"
    if r >= 3 and f >= 3:
        return "Loyal"
    if r >= 4:
        return "Recent"
    if f >= 4:
        return "Frequent"
    if m >= 4:
        return "Big Spenders"
    if r <= 2 and f <= 2:
        return "At Risk"
    return "Needs Attention"


def line_total(unit_price: float, quantity: int, discount: float) -> float:
    """Mirror of the line_total derivation in 03_silver_transforms.py."""
    return round(unit_price * quantity * (1 - discount), 2)


def gross_profit(total_line_revenue: float, cost: float, total_units: int) -> float:
    """Mirror of gross_profit in 04_gold_analytics.py gold_product_performance."""
    return round(total_line_revenue - cost * total_units, 2)
