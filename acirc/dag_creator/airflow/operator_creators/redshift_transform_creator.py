from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from airflow.operators.dummy_operator import DummyOperator


class RedshiftTransformCreator(OperatorCreator):
    ref_name = 'redshift_transform'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def create_operator(self):
        return DummyOperator(dag=self._dag, task_id=self._task.name)
