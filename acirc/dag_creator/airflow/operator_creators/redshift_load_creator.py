from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.postgres_operator import PostgresOperator

from os.path import join


REDSHIFT_LOAD_CMD = \
"""
copy {table_name}{columns}  
from '{input_path}' 
iam_role '{iam_role}'
{extra_parameters}
"""


class RedshiftLoadCreator(OperatorCreator):
    ref_name = 'redshift_load'

    CONN_ID_DEFAULT_REDSHIFT = 'redshift_default'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _get_load_command(self):
        input_path = self._task.inputs[0].rendered_name
        table_to = self._task.outputs[0].rendered_name

        extra_parameters =\
            "\n".join(["{} {}".format(key, value) for key, value in self._task.extra_parameters.items()])

        unload_cmd = REDSHIFT_LOAD_CMD.format(
            table_name=table_to,
            columns="({})".format(self._task.columns) if self._task.columns else "",
            input_path=input_path,
            iam_role=self._task.iam_role,
            extra_parameters=extra_parameters,
        )

        return unload_cmd

    def _create_operator(self):
        load_cmd = self._get_load_command()

        redshift_op = PostgresOperator(
            task_id=self._task.name,
            sql=load_cmd,
            pool='redshift',
            postgres_conn_id=self.CONN_ID_DEFAULT_REDSHIFT,
            params=self._template_parameters,
            **self._task.airflow_parameters,
        )

        return redshift_op
