import importlib
from os import path

from airflow.operators.python_operator import PythonOperator
from dagger import conf
from dagger.dag_creator.airflow.operator_creator import OperatorCreator


class PythonCreator(OperatorCreator):
    ref_name = "python"

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _create_operator(self, **kwargs):
        params = {**kwargs}

        python_file = path.relpath(
            path.join(self._task.pipeline.directory, self._task.python),
            conf.AIRFLOW_HOME,
        )
        python_module = path.splitext(python_file)[0].replace("/", ".")
        python_function = getattr(
            importlib.import_module(python_module), self._task.function
        )

        batch_op = PythonOperator(
            dag=self._dag,
            task_id=self._task.name,
            python_callable=python_function,
            provide_context=True,
            op_kwargs=self._template_parameters,
            **params,
        )

        return batch_op
