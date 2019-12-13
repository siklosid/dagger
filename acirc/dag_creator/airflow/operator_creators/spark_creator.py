from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.spark_submit_operator import SparkSubmitOperator


class SparkCreator(OperatorCreator):
    ref_name = 'spark'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _generate_command(self):
        command = []
        for param_name, param_value in self._template_parameters.items():
            command.append("--{name}={value}".format(name=param_name, value=param_value))

        return command

    def _generate_spark_args(self):
        args = []
        for spark_arg in self._task.spark_args:
            args.append("--{}".format(spark_arg))

        return args

    def _create_operator(self, **kwargs):
        batch_op = SparkSubmitOperator(
            dag=self._dag,
            task_id=self._task.name,
            job_file=self._task.job_file,
            job_args=self._generate_command(),
            spark_args=self._generate_spark_args(),
            s3_files_bucket=self._task.s3_files_bucket,
            extra_py_files=self._task.extra_py_files,
            emr_master=self._task.emr_master,
            **kwargs,
        )

        return batch_op
