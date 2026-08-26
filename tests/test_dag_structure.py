"""
Fast structural tests of the DAG definition -- import it, check task
count/order/dependencies. These don't require an initialized Airflow
metadata database, so they run quickly as part of the normal test suite.
The actual live execution proof (every task really runs, in order,
against the real dbt project) is in test_dag_end_to_end.py, which does
require a real Airflow DB and is documented separately since it's slower
and has more setup (see README's "How this was actually verified"
section for the exact commands used).
"""
import importlib
import sys


def _load_dag_module():
    sys.path.insert(0, "dags")
    if "order_management_dag" in sys.modules:
        importlib.reload(sys.modules["order_management_dag"])
    else:
        import order_management_dag  # noqa: F401
    return sys.modules["order_management_dag"]


class TestDagStructure:
    def test_dag_has_expected_task_ids(self):
        module = _load_dag_module()
        task_ids = set(module.dag.task_ids)
        assert task_ids == {"dbt_seed", "dbt_run", "dbt_test", "post_run_quality_checks"}

    def test_dag_id_is_set(self):
        module = _load_dag_module()
        assert module.dag.dag_id == "order_management_dbt_pipeline"

    def test_task_dependencies_are_linear_in_correct_order(self):
        module = _load_dag_module()
        dag = module.dag

        seed = dag.get_task("dbt_seed")
        run = dag.get_task("dbt_run")
        test = dag.get_task("dbt_test")
        checks = dag.get_task("post_run_quality_checks")

        assert run.task_id in [t.task_id for t in seed.downstream_list]
        assert test.task_id in [t.task_id for t in run.downstream_list]
        assert checks.task_id in [t.task_id for t in test.downstream_list]

    def test_dbt_seed_has_no_upstream_dependencies(self):
        module = _load_dag_module()
        seed = module.dag.get_task("dbt_seed")
        assert seed.upstream_list == []

    def test_quality_checks_task_has_no_downstream(self):
        module = _load_dag_module()
        checks = module.dag.get_task("post_run_quality_checks")
        assert checks.downstream_list == []

    def test_bash_tasks_reference_the_correct_dbt_commands(self):
        module = _load_dag_module()
        assert "dbt seed" in module.dag.get_task("dbt_seed").bash_command
        assert "dbt run" in module.dag.get_task("dbt_run").bash_command
        assert "dbt test" in module.dag.get_task("dbt_test").bash_command

    def test_no_retries_configured(self):
        """Documented, deliberate choice for this demo DAG (see README) --
        this test exists so a future change to default_args is caught
        rather than silently reintroducing the retry-wait behavior that
        caused a 2-minute `airflow dags test` hang during development."""
        module = _load_dag_module()
        assert module.dag.default_args.get("retries") == 0
