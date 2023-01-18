from os.path import join

from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.dag_creator.airflow.operators.aws_athena_operator import AWSAthenaOperator


class AthenaTransformCreator(OperatorCreator):
    ref_name = "athena_transform"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        athena_output = task._outputs[0]
        self._output_database = athena_output.schema
        self._output_table = athena_output.table

    @staticmethod
    def _read_sql(directory, file_path):
        full_path = join(directory, file_path)

        with open(full_path, "r") as f:
            sql_string = f.read()

        return sql_string

    def _create_operator(self, **kwargs):
        sql_string = self._read_sql(self._task.pipeline.directory, self._task.sql_file)

        athena_op = AWSAthenaOperator(
            dag=self._dag,
            task_id=self._task.name,
            query=sql_string,
            aws_conn_id=self._task.aws_conn_id,
            database=self._output_database,
            s3_tmp_results_location=self._task.s3_tmp_results_location,
            s3_output_location=self._task.s3_output_location,
            output_table=self._output_table,
            is_incremental=self._task.is_incremental,
            partitioned_by=self._task.partitioned_by,
            output_format=self._task.output_format,
            blue_green_deployment=self._task.blue_green_deployment,
            workgroup=self._task.workgroup,
            params=self._template_parameters,
            **kwargs,
        )

        return athena_op
