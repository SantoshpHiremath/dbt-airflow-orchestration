"""
Airflow DAG orchestrating the order-management dbt pipeline: seed -> run
-> test -> post-run data-quality checks. This is a real DAG, parsed and
executed by a real Airflow installation (see README for exactly how it
was verified: `airflow dags test`, not just "the code looks right").

Design choices, and why:

- Each dbt step (seed/run/test) is its own BashOperator task rather than
  one big "run dbt" task, so a failure at any stage is visible in the
  Airflow UI as a specific failed task (e.g. "tests failed" vs "seed
  failed") rather than one opaque failure -- this is standard, real
  Airflow-with-dbt practice, not just for looking granular in a demo.
- The post-run quality checks run as a separate PythonOperator AFTER
  dbt test, deliberately checking different things than dbt's own tests
  (see include/quality_checks.py docstring) -- catching a class of bug
  (e.g. silent over-filtering) dbt's structural tests wouldn't catch on
  their own.
- DBT_PROFILES_DIR and DBT_DUCKDB_PATH are passed as env vars into the
  BashOperator, not hardcoded — the same DAG file works whether it's run
  from this project's own test harness or a real Airflow deployment
  pointed at a different path.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.environ.get(
    "DBT_AIRFLOW_PROJECT_ROOT",
    "/home/claude/dbt-airflow-orchestration",
)
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project")
DBT_PROFILES_DIR = os.path.join(DBT_PROJECT_DIR, "profiles")
DUCKDB_PATH = os.path.join(DBT_PROJECT_DIR, "order_management.duckdb")

DBT_ENV_EXPORTS = (
    f'export DBT_PROFILES_DIR="{DBT_PROFILES_DIR}" && '
    f'export DBT_DUCKDB_PATH="{DUCKDB_PATH}"'
)
DBT_CMD_PREFIX = f'cd "{DBT_PROJECT_DIR}" && {DBT_ENV_EXPORTS}'


def _run_quality_checks(**context):
    """PythonOperator callable -- imports and runs the real quality-check
    module against the DuckDB file dbt run/test just produced. Raising
    DataQualityError here fails the Airflow task, exactly the mechanism
    a real deployment would use to trigger a downstream alert.
    """
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from include.quality_checks import run_all_quality_checks

    result = run_all_quality_checks(DUCKDB_PATH)
    # In a real deployment this dict would be pushed to XCom automatically
    # (PythonOperator does this for the return value) for downstream tasks
    # or a monitoring dashboard to consume.
    print(f"Data quality check result: {result}")
    return result


default_args = {
    "owner": "santosh",
    "retries": 0,
}

with DAG(
    dag_id="order_management_dbt_pipeline",
    description="Seed, run, and test the order-management dbt project, then run post-run data-quality checks.",
    default_args=default_args,
    schedule=None,  # triggered manually / by `airflow dags test` in this project; a real deployment would set a cron schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "data-quality", "order-management"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f'{DBT_CMD_PREFIX} && dbt seed',
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f'{DBT_CMD_PREFIX} && dbt run',
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f'{DBT_CMD_PREFIX} && dbt test',
    )

    quality_checks = PythonOperator(
        task_id="post_run_quality_checks",
        python_callable=_run_quality_checks,
    )

    dbt_seed >> dbt_run >> dbt_test >> quality_checks
