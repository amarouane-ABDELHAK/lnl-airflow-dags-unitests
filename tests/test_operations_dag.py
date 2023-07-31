import datetime
import pendulum
from airflow.models import DagBag
import uuid
import pytest
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.xcom import XCom
DATA_INTERVAL_START = pendulum.now(tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

import unittest
unittest.TestLoader.sortTestMethodsUsing = None


class TestOperatoinsDAG(unittest.TestCase):

    def setUp(self):
        dag_bag = DagBag()
        dag = dag_bag.get_dag(dag_id="make_operations")
        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
            run_id=str(uuid.uuid4()),
        )
        self.dag = dag
        self.dagrun = dagrun
        self.number_of_tasks = 5

    def tearDown(self) -> None:
        pass

    def test_DAG_tags(self):
        """
        Test that DAG contains the correct TAG
        """
        self.assertEqual(self.dag.tags, ["Kris", "Operations", "Addition", "Multiplication"])

    def test_DAG_tasks_count(self):
        self.assertEqual(len(self.dag.tasks), self.number_of_tasks)
    #
    def test_dependencies(self):
        """
        Rest the upstream and down stream of the DAG
        :return:
        """
        pass
    #
    #
    def test_addition_task(self, task_id="addition"):
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        ti.task.op_kwargs = {"a": 50, "b": 5}
        ti.run()
        return_val = ti.xcom_pull(task_ids=task_id, key="return_value")
        self.assertEqual(ti.state,TaskInstanceState.SUCCESS)
        self.assertEqual(return_val, 55)
    #
    def test_addition_non_int(self, task_id="addition"):
        self.dagrun.execution_date = pendulum.now(tz="UTC") + datetime.timedelta(days=1)
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        ti.task.op_kwargs = {"a": 50, "b": "Abdelhak"}
        with pytest.raises(Exception, match=r".*natural.*"):
            ti.run()
    #
    def test_multiply_10_task(self, task_id="multiplication"):
        ti = self.dagrun.get_task_instance(task_id=task_id)
        ti.task = self.dag.get_task(task_id=task_id)
        ti.task.trigger_rule = TriggerRule.ALWAYS
        # ti.xcom_pull(task_ids="addition", key="result_of_addition")
        XCom.set(
            key="result_of_addition",
            value=80,
            task_id="addition",
            dag_id=self.dag.dag_id,
            run_id=self.dagrun.run_id,
        )
    #
        ti.run()
        return_val = ti.xcom_pull(task_ids=task_id, key="return_value")
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)
        self.assertEqual(return_val, 800)

