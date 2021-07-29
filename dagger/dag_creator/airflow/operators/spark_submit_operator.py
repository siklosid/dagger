import logging
import os
import signal
import boto3
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
        job_args=None,
        spark_args=None,
        s3_files_bucket=conf.SPARK_S3_FILES_BUCKET,
        extra_py_files=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.job_file = job_file
        self.job_args = job_args or []
        self.spark_args = spark_args or [
            "--conf spark.driver.memory=512m",
            "--conf spark.executor.memory=512m",
            "--conf spark.cores.max=1",
            f"--conf spark.scheduler.pool={ENV}",
        ]
        self.s3_files_bucket = s3_files_bucket
        self.extra_py_files = extra_py_files or []
        self.application_id = None
        self.ssm_client = boto3.client("ssm")
        self.emr_client = boto3.client("emr")
        self.cluster_id = self.emr_client.list_clusters(ClusterStates=["WAITING", "RUNNING"])["Clusters"][0]["Id"]
        self.emr_master_instance_id = \
            self.emr_client.list_instances(ClusterId=self.cluster_id, InstanceGroupTypes=["MASTER"],
                                           InstanceStates=["RUNNING"])["Instances"][0]["Ec2InstanceId"]

    @property
    def s3_file_path(self):
        return os.path.join(
            "s3://", self.s3_files_bucket, f"{ENV_SUFFIX}airflow/dags/{self.job_file}"
        )

    @property
    def s3_bundle_path(self):
        return os.path.join(
            "s3://", self.s3_files_bucket, f"{ENV_SUFFIX}", conf.SPARK_S3_LIBS_SUFFIX
        )

    @property
    def spark_submit_cmd(self):
        spark_submit_cmd = [
            "spark-submit --master yarn --deploy-mode cluster",
            " ".join(self.spark_args),
            f"--py-files {self.s3_bundle_path},",
            ",".join(self.extra_py_files),
            self.s3_file_path,
            " ".join(self.job_args),
        ]
        return " ".join(spark_submit_cmd)

    def execute(self, context):
        """
        See `execute` method from airflow.operators.bash_operator
        """
        response = self.ssm_client.send_command(
            InstanceIds=[self.emr_master_instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [self.spark_submit_cmd]},
        )
        logging.INFO(response)

    def on_kill(self):
        self.log.info("Sending SIGTERM signal to bash process group")
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
