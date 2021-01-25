#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Optional

from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException

from dagger.dag_creator.airflow.utils.decorators import lazy_property
from dagger.dag_creator.airflow.hooks.aws_glue_hook import AwsGlueJobHook

OUTPUT_LOG_GROUP = "/aws-glue/jobs/output"
ERROR_LOG_GROUP = "/aws-glue/jobs/error"


class AwsGlueJobOperator(DaggerBaseOperator):
    """
    Creates an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala

    :param job_name: unique job name per AWS Account
    :type job_name: Optional[str]
    :param script_args: etl script arguments and AWS Glue arguments
    :type script_args: dict
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    """

    template_fields = ("script_args",)
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        job_name: str = 'aws_glue_default_job',
        script_args: Optional[dict] = None,
        aws_conn_id: str = 'aws_default',
        region_name: Optional[str] = None,
        **kwargs,
    ):  # pylint: disable=too-many-arguments
        super().__init__(**kwargs)
        self.job_name = job_name
        self.script_args = script_args or {}
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    @lazy_property
    def logs_client(self):
        return AwsHook(aws_conn_id=self.aws_conn_id, client_type="logs").get_client_type(
            "logs", region_name=self.region_name
        )

    def execute(self, context):
        """
        Executes AWS Glue Job from Airflow

        :return: the id of the current glue job.
        """
        glue_job = AwsGlueJobHook(
            job_name=self.job_name,
            region_name=self.region_name,
        )
        self.log.info("Initializing AWS Glue Job: %s", self.job_name)
        glue_job_run = glue_job.initialize_job(self.script_args)
        glue_job_run = glue_job.job_completion(self.job_name, glue_job_run['JobRunId'])
        self.log.info(
            "AWS Glue Job: %s status: %s. Run Id: %s",
            self.job_name,
            glue_job_run['JobRunState'],
            glue_job_run['JobRunId'],
        )
        return glue_job_run['JobRunId'], glue_job_run['JobRunState']

    def post_execute(self, context, result):
        job_run_id, job_state = result

        if job_run_id is None:
            return

        self.log.info("Job Output Logs")
        self.print_logs(OUTPUT_LOG_GROUP, job_run_id)
        if job_state != "SUCCEEDED":
            self.log.info("Job Error Logs")
            self.print_logs(ERROR_LOG_GROUP, job_run_id)
            raise AirflowException("JOB FAILED")

    def print_logs(self, log_group_name, log_stream_name):
        logs = self.logs_client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            startFromHead=True,
        )

        while True:
            next_token = logs['nextForwardToken']

            for event in logs["events"]:
                self.log.info(event["message"])

            logs = self.logs_client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startFromHead=True,
                nextToken=next_token
            )

            if next_token == logs['nextForwardToken']:
                break
