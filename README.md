# dbt + Airflow Orchestration — Order Management Pipeline

A real, tested Airflow DAG orchestrating a dbt-core pipeline, built
specifically to close a gap for Holidu's "Intern Data Engineering"
posting: the JD's tech stack leads with "Data Pipelines: Airflow +
dbt-core," and while dbt-core was already solidly covered by an earlier
project (`order-management-dbt`), Airflow was a complete gap — nothing
in my prior portfolio touched pipeline orchestration at all. This project
takes that existing, already-tested dbt project and wraps it in a real
Airflow DAG: `dbt seed → dbt run → dbt test → post-run data-quality
checks`, verified end to end via the actual `airflow dags test` command,
not just written to look plausible.

## What this is (read before citing anywhere)

The dbt project itself (`dbt_project/`) is a copy of an existing, already
independently-verified project (`order-management-dbt`) — the underlying
order-management data is **synthetic, not real Holidu or any company's
data**, with realistic injected data-quality problems (80 duplicate order
IDs, 60 missing order values, 25 negative processing-day values out of
5,080 raw rows). What's new here is the orchestration layer: a real
Airflow DAG, a real post-run data-quality-check module that inspects the
warehouse dbt just built, and a test suite that proves the whole thing
actually runs — including the DAG's real execution, not a mock of one.

DuckDB is used as the warehouse (as in the original dbt project), not
Redshift or Athena, which this posting's stack actually names — a real,
disclosed substitution for the same reason as before: those require a
paid cloud account I don't have access to build against. The
orchestration pattern (Airflow scheduling and monitoring a dbt run) is
the same regardless of which warehouse dbt is pointed at underneath.

## How this was actually verified (not just "should work")

This is the part worth reading closely, because getting a DAG to
genuinely pass `airflow dags test` — not just parse without import
errors — surfaced three real bugs during development, each one only
found by actually running it:

**Bug 1 — env vars silently not reaching dbt.** The first version of the
DAG's `BashOperator` commands built a shell string like
`DBT_PROFILES_DIR="..." DBT_DUCKDB_PATH="..." && dbt seed` — which in
plain bash sets those variables only for the (empty) command *before*
`&&`, not for `dbt seed` after it. The task reported SUCCESS (dbt fell
back to its default profile resolution and ran fine on its own), but the
DuckDB file never appeared at the path the downstream quality-check task
expected, which only surfaced as a failure several steps later. Fixed by
using `export VAR=...` before the actual command, joined with `&&`
throughout.

**Bug 2 — a stray `;` before `&&` produced a bash syntax error.** An
intermediate fix used `export ...; export ...; && dbt seed` — the
trailing `;` immediately before `&&` is invalid bash and fails
immediately. Fixed by using `&&` consistently as the only separator.

**Bug 3 — a 2-minute-plus stall traced to `retries: 1`.** With the above
two bugs still present, the DAG didn't just fail — it appeared to hang.
The `post_run_quality_checks` task failed (because of Bug 1), and
Airflow's own retry mechanism marked it `up_for_retry` and sat waiting
for the configured retry delay under `airflow dags test`'s local test
executor, printing "No tasks to run" once a second until the wrapping
shell timeout killed it. This is exactly why the DAG's `default_args` set
`retries: 0` — a disclosed, deliberate choice for this project (see
`tests/test_dag_structure.py::test_no_retries_configured`, which exists
specifically so this doesn't silently regress).

**A fourth thing, found and disclosed rather than hidden:** Airflow's
Python `dag.test()` API (as opposed to the `airflow dags test` CLI
command) intermittently failed in this development environment with a
SQLAlchemy error (`NoReferencedTableError` on a Flask-AppBuilder
`ab_user` auth table) unrelated to this project's own DAG or task code —
a real environment/provider-version quirk, root-caused as far as
reasonably possible (traced to FAB auth-manager tables that a bare
`airflow db init`/`db migrate` doesn't create in this Airflow/provider
combination). The CLI command, run as a subprocess against the same
initialized metadata database, does not hit that code path and passed
reliably across multiple repeated runs — confirmed directly, not assumed.
`tests/test_dag_end_to_end.py` therefore invokes the real `airflow dags
test` CLI via subprocess rather than the Python API, and documents this
reasoning in its own docstring rather than silently switching approaches.

After all of the above: `airflow dags test order_management_dbt_pipeline
2026-01-01` exits 0, with all four tasks (`dbt_seed`, `dbt_run`,
`dbt_test`, `post_run_quality_checks`) reporting SUCCESS, and the
quality-check task reading real numbers back from the DuckDB file the
dbt tasks just built: 5,080 raw rows, 5,000 staging rows (1.575% dedup
loss — the known, expected figure for this dataset), 25 mart rows.

## What the DAG actually does (`dags/order_management_dag.py`)

Four tasks, in a strict linear chain: `dbt_seed >> dbt_run >> dbt_test >>
post_run_quality_checks`. The three dbt steps are separate `BashOperator`
tasks (not one combined "run dbt" task) so a failure at any stage shows
up in Airflow as a specific, identifiable failed task — real, standard
Airflow-with-dbt practice, not just for looking granular in a demo.

## Post-run data-quality checks (`include/quality_checks.py`)

Deliberately checks different things than dbt's own schema tests:

- dbt's tests (`not_null`, `unique`) check structural correctness of
  individual columns.
- These checks look at whole-pipeline **outcomes** — is the raw-to-
  staging row-count loss within an expected range (catching a silently
  broken dedup rule that discards far more, or far less, than expected —
  a class of bug dbt's own per-column tests wouldn't catch, since an
  over- or under-filtered table can still pass every `not_null`/`unique`
  check on its own).

This is the "monitor pipeline performance, flag issues" line from the
job posting, done as real, runnable Python against the actual warehouse
the pipeline just built.

## Test suite (26 tests total across 4 files)

- `tests/test_quality_checks.py` — fast, isolated unit tests of the
  check logic against a small hand-built in-memory DuckDB database.
- `tests/test_dag_structure.py` — fast structural tests of the DAG
  definition (task IDs, dependency order, no accidental retries).
- `tests/test_quality_checks_against_real_warehouse.py` — runs the real
  dbt project via subprocess and confirms the quality checks work
  against its actual output, including the known, documented dedup-loss
  figure (1.575%).
- `tests/test_dag_end_to_end.py` — the real proof: invokes `airflow dags
  test` as a subprocess and confirms the whole DAG succeeds end to end
  (see "How this was actually verified" above for why this uses the CLI
  rather than Airflow's Python test API).

## What this doesn't demonstrate

- No Redshift, Athena, Terraform, Docker, Jenkins, AWS EKS, Kafka,
  Airbyte, or Fivetran — none of these appear anywhere in this project;
  DuckDB substitutes for a cloud warehouse for the same disclosed reason
  as the original dbt project.
- No production Airflow deployment (scheduler running continuously,
  webserver, real DAG scheduling on a cron interval) — `schedule=None`
  here deliberately, since this DAG is triggered by `airflow dags test`
  in this project's own verification, not by a live scheduler.
- No real Holidu data, infrastructure, or production incident experience.

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
