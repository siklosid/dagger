import os
import signal
import time

import boto3
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from dagger import conf
from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator

ENV = os.environ["ENV"].lower()
ENV_SUFFIX = "dev/" if ENV == "local" else ""


class SparkSubmitOperator(DaggerBaseOperator):
    ui_color = "bisque"
    template_fields = ("job_args",)

    @apply_defaults
    def __init__(
            self,
            job_file,
            cluster_name,
            aws_conn_id='aws_default',
            emr_conn_id='emr_default',
            job_args=None,
            spark_args=None,
            extra_py_files=None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.job_file = job_file
        self.job_args = job_args
        self.spark_args = spark_args
        self.extra_py_files = extra_py_files
        self.cluster_id = self.emr_hook.get_cluster_id_by_name(cluster_name, ["WAITING", "RUNNING"])
        self.emr_hook = EmrHook(aws_conn_id=aws_conn_id, emr_conn_id=emr_conn_id)
        self.emr_master_instance_id = \
            self.emr_client.list_instances(ClusterId=self.cluster_id, InstanceGroupTypes=["MASTER"],
                                           InstanceStates=["RUNNING"])["Instances"][0]["Ec2InstanceId"]

    @property
    def ssm_client(self):
        return boto3.client("ssm")

    @property
    def spark_submit_cmd(self):
        spark_submit_cmd = "spark-submit --master yarn --deploy-mode cluster"
        if self.spark_args is not None:
            spark_submit_cmd += " " + self.spark_args
        if self.extra_py_files is not None:
            spark_submit_cmd += " " + f"--py-files {self.extra_py_files}"

        spark_submit_cmd += " " + self.job_file

        if self.job_args is not None:
            spark_submit_cmd += " " + self.job_args
        return spark_submit_cmd

    def execute(self, context):
        """
        See `execute` method from airflow.operators.bash_operator
        """
        response = self.ssm_client.send_command(
            InstanceIds=[self.emr_master_instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [self.spark_submit_cmd]},
        )
        command_id = response['Command']['CommandId']
        status = 'InProgress'
        while status == 'InProgress':
            time.sleep(30)
            status = \
                self.ssm_client.get_command_invocation(CommandId=command_id, InstanceId=self.emr_master_instance_id)[
                    'StatusDetails']
        self.log.info(
            self.ssm_client.get_command_invocation(CommandId=command_id, InstanceId=self.emr_master_instance_id)[
                'StandardErrorContent'])

        if status != 'Success':
            raise AirflowException("Spark command failed")

    def on_kill(self):
        self.log.info("Sending SIGTERM signal to bash process group")
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
