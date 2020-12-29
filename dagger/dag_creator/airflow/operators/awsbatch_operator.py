from pathlib import Path
from time import sleep

from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator
from dagger.dag_creator.airflow.utils.decorators import lazy_property


class AWSBatchOperator(DaggerBaseOperator):
    """
    Execute a job on AWS Batch Service

    .. warning: the queue parameter was renamed to job_queue to segregate the
                internal CeleryExecutor queue from the AWS Batch internal queue.

    :param job_name: the name for the job that will run on AWS Batch
    :type job_name: str
    :param job_definition: the job definition name on AWS Batch
    :type job_definition: str
    :param job_queue: the queue name on AWS Batch
    :type job_queue: str
    :param overrides: the same parameter that boto3 will receive on
        containerOverrides (templated):
        http://boto3.readthedocs.io/en/latest/reference/services/batch.html#submit_job
    :type overrides: dict
    :param max_retries: exponential backoff retries while waiter is not
        merged, 4200 = 48 hours
    :type max_retries: int
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str
    :param cluster_name: Batch cluster short name or arn
    :type region_name: str

    """

    ui_color = "#c3dae0"
    client = None
    arn = None
    template_fields = ("overrides",)

    @apply_defaults
    def __init__(
        self,
        job_name,
        job_queue,
        overrides=None,
        job_definition=None,
        aws_conn_id=None,
        region_name=None,
        cluster_name=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.job_name = self._validate_job_name(job_name)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.cluster_name = cluster_name
        self.job_definition = job_definition or job_name
        self.job_queue = job_queue
        self.overrides = overrides or {}
        self.job_id = None

    @lazy_property
    def batch_client(self):
        return AwsHook(aws_conn_id=self.aws_conn_id, client_type="batch").get_client_type(
            "batch", region_name=self.region_name
        )

    @lazy_property
    def logs_client(self):
        return AwsHook(aws_conn_id=self.aws_conn_id, client_type="batch").get_client_type(
            "logs", region_name=self.region_name
        )

    @lazy_property
    def ecs_client(self):
        return AwsHook(aws_conn_id=self.aws_conn_id, client_type="batch").get_client_type(
            "ecs", region_name=self.region_name
        )

    def _validate_job_name(self, job_name):
        job_path = Path().home() / "dags" / job_name.replace("-", "/")
        assert (
            job_path.is_dir()
        ), f"Job name `{job_name}`, points to a non-existing folder `{job_path}`"
        return job_name

    def execute(self, context):
        self.task_instance = context["ti"]
        res = self.batch_client.submit_job(
            jobName=self.job_name,
            jobQueue=self.job_queue,
            jobDefinition=self.job_definition,
            containerOverrides=self.overrides,
        )
        job_path = self.job_name.replace("-", "/")
        self.job_id = res["jobId"]
        self.log.info(
            "\n"
            f"\n\tJob name: {self.job_name}"
            f"\n\tJob definition: {self.job_definition}"
            f"\n\tJob id: {self.job_id}"
            "\n"
        )
        self.poll_task()

    def poll_task(self):
        log_offset = 0
        print_logs_url = True

        while True:
            res = self.batch_client.describe_jobs(jobs=[self.job_id])

            if len(res["jobs"]) == 0:
                sleep(3)
                continue

            job = res["jobs"][0]
            job_status = job["status"]
            log_stream_name = job["container"].get("logStreamName")

            if print_logs_url and log_stream_name:
                print_logs_url = False
                self.log.info(
                    "\n"
                    f"\n\tLogs at: https://{self.region_name}.console.aws.amazon.com/cloudwatch/home?"
                    f"region={self.region_name}#logEventViewer:group=/aws/batch/job;stream={log_stream_name}"
                    "\n"
                )

            if job_status in ("RUNNING", "FAILED", "SUCCEEDED") and log_stream_name:
                try:
                    log_offset = self.print_logs(log_stream_name, log_offset)
                except self.logs_client.exceptions.ResourceNotFoundException:
                    pass
            else:
                self.log.info(f"Job status: {job_status}")

            if job_status == "FAILED":
                status_reason = res["jobs"][0]["statusReason"]
                exit_code = res["jobs"][0]["container"].get("exitCode")
                reason = res["jobs"][0]["container"].get("reason", "")
                failure_msg = f"Status: {status_reason} | Exit code: {exit_code} | Reason: {reason}"
                container_instance_arn = job["container"]["containerInstanceArn"]
                self.retry_check(container_instance_arn)
                raise AirflowException(failure_msg)

            if job_status == "SUCCEEDED":
                self.log.info("AWS Batch Job has been successfully executed")
                return

            sleep(7.5)

    def retry_check(self, container_instance_arn):
        res = self.ecs_client.describe_container_instances(
            cluster=self.cluster_name, containerInstances=[container_instance_arn]
        )
        instance_status = res["containerInstances"][0]["status"]
        if instance_status != "ACTIVE":
            self.log.warning(
                f"Instance in {instance_status} state: setting the task up for retry..."
            )
            self.retries += self.task_instance.try_number + 1
            self.task_instance.max_tries = self.retries

    def print_logs(self, log_stream_name, log_offset):
        logs = self.logs_client.get_log_events(
            logGroupName="/aws/batch/job",
            logStreamName=log_stream_name,
            startFromHead=True,
        )

        for event in logs["events"][log_offset:]:
            self.log.info(event["message"])

        log_offset = len(logs["events"])
        return log_offset

    def on_kill(self):
        res = self.batch_client.terminate_job(
            jobId=self.job_id, reason="Task killed by the user"
        )
        self.log.info(res)
