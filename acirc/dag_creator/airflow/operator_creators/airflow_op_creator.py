from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from acirc import conf
from airflow.operators.python_operator import BranchPythonOperator
import importlib

from os import path


class AirflowOpCreator(OperatorCreator):
    ref_name = 'airflow_operator'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _create_operator(self, **kwargs):
        params = {**kwargs}
        del params['description']
        airflow_operator_module = importlib.import_module(self._task.module)
        operator_class = getattr(airflow_operator_module, self._task.class_name)

        if self._task.python:
            python_file = path.relpath(path.join(self._task.pipeline.directory, self._task.python), conf.AIRFLOW_HOME)
            python_module = path.splitext(python_file)[0].replace('/', '.')
            python_function = getattr(importlib.import_module(python_module), self._task.function)
            params['python_callable'] = python_function
            params['provide_context'] = True
            params['op_kwargs'] = self._template_parameters

        batch_op = operator_class(
            dag=self._dag,
            task_id=self._task.name,
            **params,
        )

        return batch_op
