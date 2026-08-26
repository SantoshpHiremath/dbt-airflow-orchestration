"""
Runs the real dbt project (seed -> run -> test) via subprocess, then runs
run_all_quality_checks() against the actual DuckDB warehouse it produces
-- the integration test that proves quality_checks.py works against real
dbt output, not just a hand-built fixture (see test_quality_checks.py for
the fast, isolated unit tests of the check logic itself).
"""
import os
import subprocess

import pytest

from include.quality_checks import run_all_quality_checks

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project")
DBT_PROFILES_DIR = os.path.join(DBT_PROJECT_DIR, "profiles")
DUCKDB_PATH = os.path.join(DBT_PROJECT_DIR, "order_management.duckdb")


@pytest.fixture(scope="module")
def real_warehouse():
    """Actually runs dbt seed/run/test via subprocess against the real
    project bundled in this repo, producing a real DuckDB file — this is
    NOT a mock; if dbt itself is broken, this fixture fails and every
    test in this file fails with it.
    """
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)

    env = dict(os.environ)
    env["DBT_PROFILES_DIR"] = DBT_PROFILES_DIR
    env["DBT_DUCKDB_PATH"] = DUCKDB_PATH

    for cmd in (["dbt", "seed"], ["dbt", "run"], ["dbt", "test"]):
        result = subprocess.run(
            cmd, cwd=DBT_PROJECT_DIR, env=env,
            capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 0, f"{' '.join(cmd)} failed:\n{result.stdout}\n{result.stderr}"

    yield DUCKDB_PATH


class TestQualityChecksAgainstRealWarehouse:
    def test_runs_without_error_against_real_dbt_output(self, real_warehouse):
        result = run_all_quality_checks(real_warehouse)
        assert result["status"] == "PASS"

    def test_raw_and_staging_row_counts_match_known_dataset(self, real_warehouse):
        result = run_all_quality_checks(real_warehouse)
        assert result["raw_rows"] == 5080
        assert result["staging_rows"] == 5000

    def test_dedup_loss_matches_known_duplicate_count(self, real_warehouse):
        """80 duplicate order_ids out of 5,080 raw rows is a documented,
        deliberately-injected figure (see dbt_project/README.md) -- this
        confirms the quality check's computed loss percentage lines up
        with that known ground truth, not just "some number came out"."""
        result = run_all_quality_checks(real_warehouse)
        assert result["dedup_loss_pct"] == pytest.approx(1.575, abs=0.01)

    def test_mart_has_rows(self, real_warehouse):
        result = run_all_quality_checks(real_warehouse)
        assert result["mart_rows"] > 0
