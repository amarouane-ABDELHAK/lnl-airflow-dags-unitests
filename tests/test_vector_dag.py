import datetime
import pendulum
from airflow.models import DagBag
import uuid
import pytest
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.utils.trigger_rule import TriggerRule
DATA_INTERVAL_START = pendulum.now(tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

import unittest
unittest.TestLoader.sortTestMethodsUsing = None


class TestOperatoinsDAG(unittest.TestCase):

    def setUp(self):
        dag_bag = DagBag(include_examples=False)
        dag = dag_bag.get_dag(dag_id="vector_operations")
        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
            run_id=str(uuid.uuid4()),
        )
        self.dag = dag
        self.dagrun = dagrun

    def tearDown(self) -> None:
        pass

    def test_DAG_tags(self):
        """
        Test that tasks properly take start/end dates from DAGs
        """
        self.assertEqual(self.dag.tags, ["Custom", "Operator", "Vectors"])


    def test_hello_task(self, task_id="sample-task"):
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS

        ti.task.name = "This is a test"
        ti.run()
        return_val = ti.xcom_pull(task_ids=task_id, key="return_value")
        self.assertEqual(ti.state,TaskInstanceState.SUCCESS)
        self.assertEqual(return_val, "Hello This is a test!")
    #
    def test_not_supported_vector_operation(self, task_id="add_vectors"):
        self.dagrun.execution_date = pendulum.now(tz="UTC") + datetime.timedelta(days=1)
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        ti.task.vector1 = (3, 1)
        ti.task.vector2 = (4, 2)
        ti.task.action = "multiplications"
        with pytest.raises(Exception, match=r"We only support addition and subtraction"):
            ti.run()
    #
    def test_not_supported_multi_dimension_vector_operation(self, task_id="add_vectors"):
        self.dagrun.execution_date = pendulum.now(tz="UTC") + datetime.timedelta(days=1)
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        ti.task.vector1 = (3, 1, 1)
        ti.task.vector2 = (4, 2, 1)
        ti.task.action = "addition"
        with pytest.raises(Exception, match=r"We only deal with 2D vectors"):
            ti.run()
    #
    def test_add_two_vectors(self, task_id="add_vectors"):
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        ti.task.vector1 = (10, 12)
        ti.task.vector2 = (40, 21)
        ti.task.action = "addition"
        ti.run()
        return_val = ti.xcom_pull(task_ids=task_id, key="return_value")
        self.assertEqual(ti.state,TaskInstanceState.SUCCESS)
        self.assertEqual(return_val, [50, 33])
    #
    def test_subtract_two_vectors(self, task_id="subtract_vectors"):
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        ti.task.vector1 = (10, 12)
        ti.task.vector2 = (4, 6)
        ti.task.action = "subtraction"
        ti.run()
        return_val = ti.xcom_pull(task_ids=task_id, key="return_value")
        self.assertEqual(ti.state,TaskInstanceState.SUCCESS)
        self.assertEqual(return_val, [6, 6])

