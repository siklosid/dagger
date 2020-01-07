from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.spark_submit_operator import SparkSubmitOperator
from circ.operators.awsbatch_operator import AWSBatchOperator

from os.path import basename, dirname
import shlex


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
        for key, value in self._task.spark_args.items():
            args.append(f"--{key}={value}")

        return shlex.split(" ".join(args))

    def _create_operator(self, **kwargs):
        if self._task.spark_engine == 'emr':
            spark_op = SparkSubmitOperator(
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
        elif self._task.spark_engine == 'batch':
            job_name = "{}".format(dirname(self._task.job_file).replace('/', '-'))
            executable = basename(self._task.job_file)

            command = []
            command += ["spark-submit"] + self._generate_spark_args()
            command += [executable] + self._generate_command()

            overrides = {
                'command': command
            }

            spark_op = AWSBatchOperator(
                dag=self._dag,
                task_id=self._task.name,
                job_name=job_name,
                region_name=self._task.region_name,
                job_queue=self._task.job_queue,
                overrides=overrides,
                **kwargs,
            )

        return spark_op
