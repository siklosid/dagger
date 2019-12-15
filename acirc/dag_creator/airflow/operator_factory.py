from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from acirc.dag_creator.airflow.operator_creators import (
    batch_creator,
    dummy_creator,
    redshift_load_creator,
    redshift_transform_creator,
    redshift_unload_creator,
    spark_creator,
    sqoop_creator,
)

from airflow.operators.dummy_operator import DummyOperator

from circ.utils.operator_factories import make_control_flow


class DataOperator(DummyOperator):
    ui_color = '#e8f7e4'

    def __init__(self, *args, **kwargs):
        super(DataOperator, self).__init__(*args, **kwargs)


class OperatorFactory:
    def __init__(self):
        self.factory = dict()

        for cls in OperatorCreator.__subclasses__():
            self.factory[cls.ref_name] = cls

    def create_operator(self, task, dag):
        cls = self.factory.get(task.ref_name, dummy_creator.DummyCreator)

        return cls(task, dag).create_operator()

    @staticmethod
    def create_control_flow_operator(env, dag):
        return make_control_flow(env, dag)

    @staticmethod
    def create_dataset_operator(data_id, dag):
        return DataOperator(dag=dag, task_id=data_id)

