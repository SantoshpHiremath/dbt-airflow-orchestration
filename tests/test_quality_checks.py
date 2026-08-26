"""
Unit tests for include/quality_checks.py, run against a small, hand-built
in-memory DuckDB database (not the real dbt-produced warehouse) -- fast,
isolated tests of the check LOGIC itself. Separately,
tests/test_dag_end_to_end.py runs the real dbt project + real quality
checks together via `airflow dags test`, which is where the integration
is actually proven live.
"""
import duckdb
import pytest

from include.quality_checks import (
    DataQualityError,
    check_dedup_loss_within_expected_range,
    check_no_nulls_in_column,
    check_row_count_in_expected_range,
)


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE raw_orders (order_id INTEGER, region VARCHAR)")
    connection.execute(
        "INSERT INTO raw_orders VALUES (1, 'EU'), (2, 'EU'), (2, 'EU'), (3, NULL), (4, 'US')"
    )
    connection.execute("CREATE TABLE stg_orders (order_id INTEGER, region VARCHAR)")
    connection.execute(
        "INSERT INTO stg_orders VALUES (1, 'EU'), (2, 'EU'), (4, 'US')"
    )
    yield connection
    connection.close()


class TestCheckRowCountInExpectedRange:
    def test_passes_when_count_within_range(self, con):
        count = check_row_count_in_expected_range(con, "raw_orders", min_rows=1, max_rows=10)
        assert count == 5

    def test_raises_when_count_below_min(self, con):
        with pytest.raises(DataQualityError):
            check_row_count_in_expected_range(con, "raw_orders", min_rows=100, max_rows=200)

    def test_raises_when_count_above_max(self, con):
        with pytest.raises(DataQualityError):
            check_row_count_in_expected_range(con, "raw_orders", min_rows=0, max_rows=1)


class TestCheckNoNullsInColumn:
    def test_passes_when_no_nulls(self, con):
        check_no_nulls_in_column(con, "raw_orders", "order_id")  # should not raise

    def test_raises_when_nulls_present(self, con):
        with pytest.raises(DataQualityError, match="region"):
            check_no_nulls_in_column(con, "raw_orders", "region")


class TestCheckDedupLossWithinExpectedRange:
    def test_passes_when_loss_within_range(self, con):
        # raw=5, staging=3 -> 40% loss
        loss = check_dedup_loss_within_expected_range(
            con, "raw_orders", "stg_orders", min_loss_pct=30, max_loss_pct=50,
        )
        assert loss == pytest.approx(40.0)

    def test_raises_when_loss_too_high(self, con):
        """Simulates a broken dedup rule that discards way more than
        expected -- exactly the class of bug this check exists to catch,
        which dbt's own not_null/unique tests on stg_orders alone
        wouldn't detect (an over-filtered table can still pass those)."""
        with pytest.raises(DataQualityError, match="loss was"):
            check_dedup_loss_within_expected_range(
                con, "raw_orders", "stg_orders", min_loss_pct=0, max_loss_pct=10,
            )

    def test_raises_when_loss_too_low(self, con):
        """Simulates a dedup rule that isn't actually deduplicating
        anything -- the opposite failure mode, also worth catching."""
        with pytest.raises(DataQualityError, match="loss was"):
            check_dedup_loss_within_expected_range(
                con, "raw_orders", "stg_orders", min_loss_pct=90, max_loss_pct=100,
            )

    def test_raises_on_empty_raw_table(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE empty_raw (x INTEGER)")
        con.execute("CREATE TABLE empty_staging (x INTEGER)")
        with pytest.raises(DataQualityError, match="empty"):
            check_dedup_loss_within_expected_range(
                con, "empty_raw", "empty_staging", min_loss_pct=0, max_loss_pct=100,
            )
        con.close()
