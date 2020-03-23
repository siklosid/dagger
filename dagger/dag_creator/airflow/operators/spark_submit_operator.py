import os
import re
import signal
from subprocess import PIPE, STDOUT, Popen

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
        job_args=None,
        spark_args=None,
        s3_files_bucket=conf.SPARK_S3_FILES_BUCKET,
        extra_py_files=None,
        emr_master=conf.SPARK_EMR_MASTER,
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
        self.emr_master = emr_master
        self.application_id = None

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
            "spark-submit --master yarn --deploy-mode client",
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
        cmd = " ".join(
            [
                f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
                f"hadoop@{self.emr_master} -tt",
                self.spark_submit_cmd,
            ]
        )

        def pre_exec():
            for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                if hasattr(signal, sig):
                    signal.signal(getattr(signal, sig), signal.SIG_DFL)
            os.setsid()

        self.log.info(f"Running command: {cmd}")
        self.sp = Popen(cmd.split(), stdout=PIPE, stderr=STDOUT, preexec_fn=pre_exec)
        application_pattern = re.compile("(?<=proxy/)(.*)(?=/)")
        pyspark_logs_only = False

        while True:
            line = self.sp.stdout.readline().decode("utf-8")
            if line == "" and self.sp.poll() is not None:
                break
            if line:
                if "tracking URL:" in line:
                    self.application_id = application_pattern.findall(line)[0]
                if "(state: FINISHED)" in line:
                    pyspark_logs_only = False
                if pyspark_logs_only:
                    if "- pyspark - " not in line:
                        continue
                if "(state: RUNNING)" in line:
                    pyspark_logs_only = True
                self.log.info(line)

        rc = self.sp.poll()

        self.log.info(f"Command exited with return code {rc}")

        if self.sp.returncode:
            raise AirflowException("Bash command failed")

    def on_kill(self):
        self.log.info("Sending SIGTERM signal to bash process group")
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
