# dbt + Airflow Orchestration: Order Management Pipeline

A tested Airflow DAG that orchestrates a dbt-core pipeline. It takes an existing, already-tested dbt project (`order-management-dbt`) and wraps it in an Airflow DAG: `dbt seed → dbt run → dbt test → post-run data-quality checks`, verified end to end with the actual `airflow dags test` command.

## What it does

- Runs the dbt project (`dbt_project/`) as four ordered Airflow tasks.
- Runs a post-run data-quality-check module that inspects the warehouse dbt just built.
- Includes a test suite that exercises the DAG's real execution, not a mock of one.

## Data

The dbt project is a copy of the independently verified `order-management-dbt` project. The order-management data is synthetic, with realistic injected data-quality problems (80 duplicate order IDs, 60 missing order values, 25 negative processing-day values out of 5,080 raw rows). The new work here is the orchestration layer: the Airflow DAG, the post-run data-quality checks, and the tests.

DuckDB is the warehouse (as in the original dbt project). The orchestration pattern, with Airflow scheduling and monitoring a dbt run, is the same whichever warehouse dbt points at, so Redshift or Athena can be swapped in as the target.

## The DAG (`dags/order_management_dag.py`)

Four tasks in a strict linear chain: `dbt_seed >> dbt_run >> dbt_test >> post_run_quality_checks`. The three dbt steps are separate `BashOperator` tasks (not one combined "run dbt" task), so a failure at any stage shows up in Airflow as a specific, identifiable failed task, which is standard Airflow-with-dbt practice. `schedule=None`: the DAG is triggered by `airflow dags test` in this project's own verification rather than by a live scheduler.

## Post-run data-quality checks (`include/quality_checks.py`)

These check different things than dbt's own schema tests:

- dbt's tests (`not_null`, `unique`) check structural correctness of individual columns.
- These checks look at whole-pipeline outcomes, such as whether the raw-to-staging row-count loss is within an expected range. That catches a silently broken dedup rule that discards far more, or far less, than expected: an over- or under-filtered table can still pass every `not_null`/`unique` check on its own.

It is pipeline monitoring that flags issues, implemented as runnable Python against the actual warehouse the pipeline just built.

## Results

`airflow dags test order_management_dbt_pipeline 2026-01-01` exits 0, with all four tasks (`dbt_seed`, `dbt_run`, `dbt_test`, `post_run_quality_checks`) reporting SUCCESS. The quality-check task reads real numbers back from the DuckDB file the dbt tasks just built: 5,080 raw rows, 5,000 staging rows (1.575% dedup loss, the known, expected figure for this dataset), 25 mart rows.

## Tests

26 tests across 4 files:

- `tests/test_quality_checks.py`: fast, isolated unit tests of the check logic against a small hand-built in-memory DuckDB database.
- `tests/test_dag_structure.py`: fast structural tests of the DAG definition (task IDs, dependency order, no accidental retries).
- `tests/test_quality_checks_against_real_warehouse.py`: runs the real dbt project via subprocess and confirms the quality checks work against its actual output, including the known, documented dedup-loss figure (1.575%).
- `tests/test_dag_end_to_end.py`: invokes `airflow dags test` as a subprocess and confirms the whole DAG succeeds end to end (see Notes for why this uses the CLI rather than Airflow's Python test API).

## Running it

```bash
pip install -r requirements.txt

# Fast tests (unit, structural, warehouse-integration):
pytest tests/test_quality_checks.py tests/test_dag_structure.py tests/test_quality_checks_against_real_warehouse.py -v

# Real end-to-end DAG execution:
export AIRFLOW_HOME=/tmp/airflow_home && airflow db init
export AIRFLOW_HOME_TEST=/tmp/airflow_home
pytest tests/test_dag_end_to_end.py -v

# Or run the DAG directly via the Airflow CLI:
export AIRFLOW_HOME=/tmp/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export DBT_AIRFLOW_PROJECT_ROOT=$(pwd)
airflow dags test order_management_dbt_pipeline 2026-01-01
```

## Notes

Getting the DAG to pass `airflow dags test`, not just parse without import errors, surfaced three bugs during development, each found by running it:

**Bug 1: env vars not reaching dbt.** The first version of the DAG's `BashOperator` commands built a shell string like `DBT_PROFILES_DIR="..." DBT_DUCKDB_PATH="..." && dbt seed`, which in plain bash sets those variables only for the (empty) command before `&&`, not for `dbt seed` after it. The task reported SUCCESS (dbt fell back to its default profile resolution and ran fine on its own), but the DuckDB file never appeared at the path the downstream quality-check task expected, which only surfaced as a failure several steps later. Fixed by using `export VAR=...` before the actual command, joined with `&&` throughout.

**Bug 2: a stray `;` before `&&` produced a bash syntax error.** An intermediate fix used `export ...; export ...; && dbt seed`; the trailing `;` immediately before `&&` is invalid bash. Fixed by using `&&` consistently as the only separator.

**Bug 3: a 2-minute-plus stall traced to `retries: 1`.** With the two bugs above still present, the DAG appeared to hang. The `post_run_quality_checks` task failed (because of Bug 1), and Airflow's retry mechanism marked it `up_for_retry` and waited for the configured retry delay under `airflow dags test`'s local test executor, printing "No tasks to run" once a second until the wrapping shell timeout killed it. This is why the DAG's `default_args` set `retries: 0`, a deliberate choice covered by `tests/test_dag_structure.py::test_no_retries_configured`, so it doesn't silently regress.

**CLI vs. Python API.** Airflow's Python `dag.test()` API (as opposed to the `airflow dags test` CLI command) intermittently failed in my development environment with a SQLAlchemy error (`NoReferencedTableError` on a Flask-AppBuilder `ab_user` auth table), unrelated to this project's own DAG or task code. I traced it to FAB auth-manager tables that a bare `airflow db init`/`db migrate` doesn't create in this Airflow/provider combination. The CLI command, run as a subprocess against the same initialized metadata database, does not hit that code path and passed reliably across multiple repeated runs. `tests/test_dag_end_to_end.py` therefore invokes the real `airflow dags test` CLI via subprocess and documents this reasoning in its docstring.

## Possible extensions

- Point dbt at Redshift or Athena instead of DuckDB.
- Run the DAG on a live Airflow deployment with a cron schedule, a scheduler, and a webserver.
- Add alerting (Slack, email) on `DataQualityError`.
