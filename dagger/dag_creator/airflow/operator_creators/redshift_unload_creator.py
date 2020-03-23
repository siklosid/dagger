from os.path import join

from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.dag_creator.airflow.operators.postgres_operator import PostgresOperator

REDSHIFT_UNLOAD_CMD = """
unload ('{sql_string}')
to '{output_path}'
iam_role '{iam_role}'
{extra_parameters}
"""


class RedshiftUnloadCreator(OperatorCreator):
    ref_name = "redshift_unload"

    def __init__(self, task, dag):
        super().__init__(task, dag)

    @staticmethod
    def _read_sql(directory, file_path):
        full_path = join(directory, file_path)

        with open(full_path, "r") as f:
            sql_string = f.read()

        return sql_string

    def _default_sql(self):
        from_table = self._task.inputs[0].rendered_name
        return "SELECT * FROM {}".format(from_table)

    def _get_unload_command(self, sql_string):
        output_path = self._task.outputs[0].rendered_name
        extra_parameters = "\n".join(
            [
                "{} {}".format(key, value)
                for key, value in self._task.extra_parameters.items()
            ]
        )

        unload_cmd = REDSHIFT_UNLOAD_CMD.format(
            sql_string=sql_string,
            output_path=output_path,
            iam_role=self._task.iam_role,
            extra_parameters=extra_parameters,
        )

        return unload_cmd

    def _create_operator(self, **kwargs):
        if self._task.sql_file:
            sql_string = self._read_sql(
                self._task.pipeline.directory, self._task.sql_file
            )
        else:
            sql_string = self._default_sql()

        unload_cmd = self._get_unload_command(sql_string)

        redshift_op = PostgresOperator(
            dag=self._dag,
            task_id=self._task.name,
            sql=unload_cmd,
            postgres_conn_id=self._task.postgres_conn_id,
            params=self._template_parameters,
            **kwargs,
        )

        return redshift_op
