from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.postgres_operator import PostgresOperator

from os.path import join


class RedshiftTransformCreator(OperatorCreator):
    ref_name = 'redshift_transform'

    CONN_ID_DEFAULT_REDSHIFT = 'redshift_default'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    @staticmethod
    def _read_sql(directory, file_path):
        full_path = join(directory, file_path)

        with open(full_path, "r") as f:
            sql_string = f.read()

        return sql_string

    def _create_operator(self):
        sql_string = self._read_sql(self._task.pipeline.directory, self._task.sql_file)

        redshift_op = PostgresOperator(
            task_id=self._task.name,
            sql=sql_string,
            pool='redshift',
            postgres_conn_id=self.CONN_ID_DEFAULT_REDSHIFT,
            params=self._template_parameters,
        )

        return redshift_op
