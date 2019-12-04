from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.postgres_operator import PostgresOperator

from os.path import join


class RedshiftTransformCreator(OperatorCreator):
    ref_name = 'redshift_transform'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    @staticmethod
    def _read_sql(directory, file_path):
        full_path = join(directory, file_path)

        with open(full_path, "r") as f:
            sql_string = f.read()

        return sql_string

    def _create_operator(self, **kwargs):
        sql_string = self._read_sql(self._task.pipeline.directory, self._task.sql_file)

        redshift_op = PostgresOperator(
            dag=self._dag,
            task_id=self._task.name,
            sql=sql_string,
            postgres_conn_id=self._task.postgres_conn_id,
            params=self._template_parameters,
            **kwargs,
        )

        return redshift_op
