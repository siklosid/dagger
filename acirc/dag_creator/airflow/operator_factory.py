from acirc.pipeline.task import Task
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG


class OperatorFactory:
    @staticmethod
    def create_operator(task: Task, dag: DAG):
        return DummyOperator(dag=dag, task_id=task.name)
