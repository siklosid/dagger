from airflow.operators.dummy_operator import DummyOperator
from dagger.dag_creator.airflow.operator_creator import OperatorCreator


class DummyCreator(OperatorCreator):
    ref_name = "dummy"

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _create_operator(self, **kwargs):
        params = {**kwargs}

        return DummyOperator(dag=self._dag, task_id=self._task.name, **params)
