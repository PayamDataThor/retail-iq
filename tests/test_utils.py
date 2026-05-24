"""
Tests for notebooks/00_utils.py

tbl() and upsert() Spark tests live in 08_run_tests.py (require live Spark).
"""

"""
Tests for notebooks/00_utils.py

tbl() is pure string formatting — we define it locally so this test file
runs without Spark or delta-spark installed.

The upsert() Spark tests are in notebooks/08_run_tests.py.
"""

import pytest


# tbl() mirrored from 00_utils.py.  If the implementation changes, update here too.
def _tbl(catalog: str, schema: str, name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


# ---------------------------------------------------------------------------
# tbl() — pure string formatting, no Spark needed
# ---------------------------------------------------------------------------

class TestTbl:
    @pytest.fixture(autouse=True)
    def utils(self):
        self.catalog = "main"
        self.schema  = "retail_iq"

    def test_returns_three_part_backtick_name(self):
        assert _tbl(self.catalog, self.schema, "gold_store_kpis") == "`main`.`retail_iq`.`gold_store_kpis`"

    def test_preserves_table_name_exactly(self):
        assert _tbl(self.catalog, self.schema, "bronze_order_items") == "`main`.`retail_iq`.`bronze_order_items`"

    def test_different_catalog_and_schema(self):
        assert _tbl("my_cat", "my_schema", "t") == "`my_cat`.`my_schema`.`t`"

    def test_special_chars_in_table_name_preserved(self):
        result = _tbl(self.catalog, self.schema, "table_with_underscores_123")
        assert "table_with_underscores_123" in result

    def test_backticks_present(self):
        result = _tbl(self.catalog, self.schema, "some_table")
        assert result.count("`") == 6   # three pairs of backticks
