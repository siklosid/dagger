from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from acirc.dag_creator.airflow.operator_creators import (
    batch_creator,
    dummy_creator,
    redshift_load_creator,
    redshift_transform_creator,
    redshift_unload_creator,
    spark_creator,
)


class OperatorFactory:
    def __init__(self):
        self.factory = dict()

        for cls in OperatorCreator.__subclasses__():
            self.factory[cls.ref_name] = cls

    def create_operator(self, task, dag):
        cls = self.factory.get(task.ref_name, dummy_creator.DummyCreator)

        return cls(task, dag).create_operator()
