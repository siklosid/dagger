from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.postgres_operator import PostgresOperator

from os.path import join


REDSHIFT_LOAD_CMD = \
"""
{delete_cmd};
copy {table_name}{columns}  
from '{input_path}' 
iam_role '{iam_role}'
{extra_parameters}
"""


class RedshiftLoadCreator(OperatorCreator):
    ref_name = 'redshift_load'

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._input_path = self._task.inputs[0].rendered_name
        self._table_to = self._task.outputs[0].rendered_name

    def _get_delete_cmd(self):
        if self._task.incremental:
            return "DELETE FROM {table} WHERE {condition}"\
                .format(table=self._table_to, condition=self._task.delete_condition)
        else:
            return "TRUNCATE TABLE {table}".format(table=self._table_to)

    def _get_load_command(self):

        extra_parameters =\
            "\n".join(["{} {}".format(key, value) for key, value in self._task.extra_parameters.items()])

        unload_cmd = REDSHIFT_LOAD_CMD.format(
            delete_cmd=self._get_delete_cmd(),
            table_name=self._table_to,
            columns="({})".format(self._task.columns) if self._task.columns else "",
            input_path=self._input_path,
            iam_role=self._task.iam_role,
            extra_parameters=extra_parameters,
        )

        return unload_cmd

    def _create_operator(self, **kwargs):
        load_cmd = self._get_load_command()

        redshift_op = PostgresOperator(
            task_id=self._task.name,
            sql=load_cmd,
            postgres_conn_id=self._task.postgres_conn_id,
            params=self._template_parameters,
            **kwargs,
        )

        return redshift_op
