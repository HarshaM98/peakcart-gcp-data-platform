"""DAG integrity tests for all Cloud Composer DAGs in this repository.

This is the cheapest and highest-value Airflow test there is: it proves each
DAG file actually *parses* and produces a valid DAG object. A DAG with a typo,
a bad import, or a cycle doesn't fail loudly in Composer -- it silently
disappears from the UI, which is much harder to notice than a red task.

Covers all three DAGs:
  - project-02-cloud-migration/dags/project02_migration_dag.py
  - project-03-customer-360/dags/customer_360_dag.py
  - project-04-realtime-ops/dags/project04_streaming_rollup_dag.py

Requires `apache-airflow` and `apache-airflow-providers-google` (see
tests/requirements.txt). If Airflow isn't installed the whole module skips
rather than failing, so this never breaks a run in an environment that only
needs the Beam/dbt/Vertex dependencies.

Run with:
    python3 -m unittest discover -s tests -v
"""

import os
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DAG_DIRS = [
    os.path.join(REPO_ROOT, "project-02-cloud-migration", "dags"),
    os.path.join(REPO_ROOT, "project-03-customer-360", "dags"),
    os.path.join(REPO_ROOT, "project-04-realtime-ops", "dags"),
]

EXPECTED_DAG_IDS = {
    "project02_migration_bulk_load",
    "customer_360_pipeline",
    "project04_streaming_rollup",
}

try:
    from airflow.models import DagBag

    AIRFLOW_AVAILABLE = True
except ImportError:  # pragma: no cover
    AIRFLOW_AVAILABLE = False


def _load_all_dags():
    """Collect DAGs and import errors across every project's dags/ folder."""
    dags = {}
    import_errors = {}
    for dag_dir in DAG_DIRS:
        # include_examples=False keeps Airflow's bundled example DAGs out of
        # the results, which would otherwise swamp the assertions below.
        bag = DagBag(dag_folder=dag_dir, include_examples=False)
        dags.update(bag.dags)
        import_errors.update(bag.import_errors)
    return dags, import_errors


@unittest.skipUnless(
    AIRFLOW_AVAILABLE,
    "apache-airflow not installed -- see tests/requirements.txt",
)
class DagIntegrityTest(unittest.TestCase):
    """Structural checks that hold for every DAG in the repo."""

    @classmethod
    def setUpClass(cls):
        cls.dags, cls.import_errors = _load_all_dags()

    def test_no_import_errors(self):
        """Every DAG file parses cleanly.

        This is the assertion that catches the failure mode Composer hides:
        a DAG that raises on import just never appears, with no red task to
        alert on.
        """
        self.assertEqual(
            self.import_errors,
            {},
            f"DAG import errors found: {self.import_errors}",
        )

    def test_expected_dags_present(self):
        """All three known DAGs are discovered, none silently renamed away."""
        self.assertEqual(
            EXPECTED_DAG_IDS,
            set(self.dags.keys()),
            f"Expected {EXPECTED_DAG_IDS}, found {set(self.dags.keys())}",
        )

    def test_dags_have_tasks(self):
        """No DAG is accidentally empty."""
        for dag_id, dag in self.dags.items():
            with self.subTest(dag_id=dag_id):
                self.assertGreater(
                    len(dag.tasks), 0, f"{dag_id} has no tasks"
                )

    def test_no_cycles(self):
        """Task dependency graphs are acyclic."""
        from airflow.utils.dag_cycle_tester import check_cycle

        for dag_id, dag in self.dags.items():
            with self.subTest(dag_id=dag_id):
                check_cycle(dag)  # raises AirflowDagCycleException on failure

    def test_required_default_args(self):
        """Each DAG sets the operational defaults this repo standardizes on.

        `retries` and `owner` are the two that actually matter in Composer:
        without retries a transient GCP blip fails the run outright, and
        without an owner the UI gives no routing information.
        """
        for dag_id, dag in self.dags.items():
            with self.subTest(dag_id=dag_id):
                self.assertIn("retries", dag.default_args, f"{dag_id} sets no retries")
                self.assertIsNotNone(dag.owner, f"{dag_id} has no owner")

    def test_tags_present(self):
        """Tags are how these DAGs are filtered in a shared environment."""
        for dag_id, dag in self.dags.items():
            with self.subTest(dag_id=dag_id):
                self.assertTrue(dag.tags, f"{dag_id} has no tags")


@unittest.skipUnless(
    AIRFLOW_AVAILABLE,
    "apache-airflow not installed -- see tests/requirements.txt",
)
class DagFileDiscoveryTest(unittest.TestCase):
    """Guards against a DAG file existing but never being picked up."""

    def test_every_dag_file_yields_a_dag(self):
        dag_files = []
        for dag_dir in DAG_DIRS:
            if not os.path.isdir(dag_dir):
                continue
            for name in os.listdir(dag_dir):
                # profiles.yml and __pycache__ live alongside the DAGs
                if name.endswith("_dag.py"):
                    dag_files.append(os.path.join(dag_dir, name))

        self.assertEqual(
            len(dag_files),
            len(EXPECTED_DAG_IDS),
            f"Found {len(dag_files)} *_dag.py files but expect "
            f"{len(EXPECTED_DAG_IDS)} DAGs: {dag_files}",
        )


if __name__ == "__main__":
    unittest.main()
