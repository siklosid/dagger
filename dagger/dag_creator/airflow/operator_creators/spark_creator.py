from os.path import basename, dirname
from shlex import shlex

from dagger import conf
from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.dag_creator.airflow.operators.aws_glue_job_operator import AwsGlueJobOperator
from dagger.dag_creator.airflow.operators.awsbatch_operator import AWSBatchOperator
from dagger.dag_creator.airflow.operators.spark_submit_operator import (
    SparkSubmitOperator,
)


def _parse_args(job_args):
    if job_args is None:
        return None
    command = []
    for param_name, param_value in job_args.items():
        command.append(
            "--{name}={value}".format(name=param_name, value=param_value)
        )

    return "".join(command)

def _parse_spark_args(job_args):
    if job_args is None:
        return None
    command = []
    for param_name, param_value in job_args.items():
        command.append(
            "--{name} {value}".format(name=param_name, value=param_value)
        )

    return "".join(command)


class SparkCreator(OperatorCreator):
    ref_name = "spark"

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _generate_command(self):
        command = []
        for param_name, param_value in self._template_parameters.items():
            command.append(
                "--{name}={value}".format(name=param_name, value=param_value)
            )

        return command

    def _generate_spark_args(self):
        args = []
        for key, value in self._task.spark_args.items():
            args.append(f"--{key}={value}")

        return shlex.split(" ".join(args))

    @staticmethod
    def _convert_size_text_to_megabytes(size):
        multipliers = {"m": 1, "g": 1024}

        for suffix in multipliers:
            if size.lower().endswith(suffix):
                return int(size[0: -len(suffix)]) * multipliers[suffix]

        return None

    def _calculate_memory_from_spark_args(self):
        spark_args = self._task.spark_args
        driver_memory = spark_args["conf spark.driver.memory"]
        executor_memory = spark_args["conf spark.executor.memory"]

        return int(
            conf.SPARK_OVERHEAD_MULTIPLIER
            * max(
                self._convert_size_text_to_megabytes(driver_memory),
                self._convert_size_text_to_megabytes(executor_memory),
            )
        )

    def _create_operator(self, **kwargs):
        if self._task.spark_engine == "emr":
            spark_op = SparkSubmitOperator(
                dag=self._dag,
                task_id=self._task.name,
                job_file=self._task.job_file,
                cluster_name=self._task.cluster_name,
                job_args=_parse_args(self._template_parameters),
                spark_args=_parse_spark_args(self._task.spark_args),
                extra_py_files=self._task.extra_py_files,
                **kwargs,
            )
        elif self._task.spark_engine == "batch":
            overrides = {"memory": self._calculate_memory_from_spark_args()}
            overrides.update(self._task.overrides)

            job_name = "{}".format(dirname(self._task.job_file).replace("/", "-"))
            executable = basename(self._task.job_file)

            command = []
            command += ["spark-submit"] + self._generate_spark_args()
            command += [executable] + self._generate_command()

            overrides.update({"command": command})

            spark_op = AWSBatchOperator(
                dag=self._dag,
                task_id=self._task.name,
                job_name=job_name,
                region_name=self._task.region_name,
                job_queue=self._task.job_queue,
                overrides=overrides,
                **kwargs,
            )
        elif self._task.spark_engine == "glue":
            parameters = {
                f"--{parameter}": value for parameter, value in self._template_parameters.items()
            }

            spark_op = AwsGlueJobOperator(
                dag=self._dag,
                task_id=self._task.name,
                job_name=self._task.name,
                script_args=parameters,
                region_name=self._task.region_name,
                **kwargs
            )

        return spark_op
