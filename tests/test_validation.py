from airflow.models import DagBag

import unittest


class TestDAGValidation(unittest.TestCase):

    def setUp(self):
        self.dag_bag = DagBag()
        self.expected_dags = ["make_operations", "vector_operations"]

    def test_import_dags(self):
        self.assertFalse(
            len(self.dag_bag.import_errors),
            f'DAG has an import Error {self.dag_bag.import_errors}'
        )

    def test_dags_exists(self):
        dags = list()
        for dag_id, _ in self.dag_bag.dags.items():
            dags.append(dag_id)

        self.assertListEqual(sorted(dags), sorted(self.expected_dags))
