"""
Shared fixtures and helpers.

Business-logic helpers are imported from src/retail_iq/ — the canonical
definitions.  Tests verify the src/ functions; notebooks/03 and 04 apply
the equivalent Spark logic.

Pure-Python tests need nothing special.
Spark tests are tagged @pytest.mark.spark and live in notebooks/08_run_tests.py.
"""

import os
import sys
import types

# Make src/ importable without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Re-export so test files can import directly from conftest if they prefer
from retail_iq.quality import (          # noqa: F401
    customer_passes_silver,
    product_passes_silver,
    order_passes_silver,
    order_item_passes_silver,
)
from retail_iq.analytics import rfm_segment  # noqa: F401


# ---------------------------------------------------------------------------
# Notebook loader
# ---------------------------------------------------------------------------

def _strip_magic(source: str) -> str:
    """Remove Databricks notebook magic lines so the file can be exec'd."""
    clean = []
    for line in source.splitlines():
        s = line.strip()
        if s in ("# Databricks notebook source", "# COMMAND ----------"):
            continue
        if s.startswith("# MAGIC"):
            continue
        clean.append(line)
    return "\n".join(clean)


def _load_notebook(path: str, extra_globals: dict = None) -> types.ModuleType:
    """
    Exec a Databricks notebook .py file in an isolated module namespace.
    extra_globals are injected before execution (e.g. spark, catalog, schema).
    """
    with open(path) as fh:
        source = _strip_magic(fh.read())

    module = types.ModuleType(path)
    if extra_globals:
        module.__dict__.update(extra_globals)
    exec(compile(source, path, "exec"), module.__dict__)
    return module
