"""
Post-run data-quality checks executed by the Airflow DAG's PythonOperator
task, AFTER dbt run/test have already passed. This is deliberately a
different, complementary kind of check from dbt's own schema tests:

- dbt's tests (not_null, unique, accepted_values) check STRUCTURAL
  correctness of individual columns/tables.
- These checks look at whole-pipeline OUTCOMES: did the run actually
  process a sane amount of data, is the loss from raw to staging within
  an expected range (catching a silent over- or under-filtering bug that
  a purely structural test wouldn't catch), and are the mart tables
  actually populated.

This is pipeline-performance monitoring that flags issues, done as real,
runnable Python against the actual DuckDB warehouse the dbt run just
produced -- not a mocked check.
"""
from __future__ import annotations

import duckdb


class DataQualityError(Exception):
    """Raised when a post-run quality check fails -- in a real Airflow
    deployment this would fail the task and could trigger an alert
    (OpsGenie, Slack, etc.); here it's a plain Python exception the DAG's
    PythonOperator lets propagate, which is exactly how Airflow expects
    a task to signal failure.
    """


def check_row_count_in_expected_range(con: duckdb.DuckDBPyConnection, table: str, min_rows: int, max_rows: int) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if not (min_rows <= count <= max_rows):
        raise DataQualityError(
            f"{table} has {count} rows, expected between {min_rows} and {max_rows}."
        )
    return count


def check_no_nulls_in_column(con: duckdb.DuckDBPyConnection, table: str, column: str) -> None:
    null_count = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
    if null_count > 0:
        raise DataQualityError(f"{table}.{column} has {null_count} unexpected NULL values.")


def check_dedup_loss_within_expected_range(
    con: duckdb.DuckDBPyConnection,
    raw_table: str,
    cleaned_table: str,
    min_loss_pct: float,
    max_loss_pct: float,
) -> float:
    """Confirms the raw->staging row-count drop (from deduplication) is
    within an expected range -- if it were, say, 90% instead of the
    expected ~1.5%, that's a strong signal the dedup logic broke and is
    silently discarding good data, which none of dbt's own per-column
    tests would catch (they'd all still pass on an over-filtered table).
    """
    raw_count = con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    cleaned_count = con.execute(f"SELECT COUNT(*) FROM {cleaned_table}").fetchone()[0]
    if raw_count == 0:
        raise DataQualityError(f"{raw_table} is empty — cannot compute loss percentage.")
    loss_pct = (raw_count - cleaned_count) / raw_count * 100
    if not (min_loss_pct <= loss_pct <= max_loss_pct):
        raise DataQualityError(
            f"Raw->staging loss was {loss_pct:.2f}% ({raw_count} -> {cleaned_count} rows), "
            f"expected between {min_loss_pct}% and {max_loss_pct}%."
        )
    return loss_pct


def run_all_quality_checks(duckdb_path: str) -> dict:
    """The function the Airflow PythonOperator task actually calls.
    Opens a fresh, read-only-intent connection to the warehouse the dbt
    run just built, runs every check, and returns a summary dict (which
    Airflow would store via XCom in a real deployment for downstream
    tasks or alerting to consume).
    """
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        raw_rows = check_row_count_in_expected_range(con, "main.raw_orders", min_rows=4000, max_rows=6000)
        staging_rows = check_row_count_in_expected_range(con, "main.stg_orders", min_rows=4000, max_rows=6000)
        check_no_nulls_in_column(con, "main.stg_orders", "order_id")
        check_no_nulls_in_column(con, "main.stg_orders", "region")
        loss_pct = check_dedup_loss_within_expected_range(
            con, "main.raw_orders", "main.stg_orders", min_loss_pct=0.5, max_loss_pct=5.0,
        )
        mart_rows = check_row_count_in_expected_range(con, "main.mart_order_summary", min_rows=1, max_rows=1000)
        return {
            "raw_rows": raw_rows,
            "staging_rows": staging_rows,
            "dedup_loss_pct": round(loss_pct, 3),
            "mart_rows": mart_rows,
            "status": "PASS",
        }
    finally:
        con.close()
