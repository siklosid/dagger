# -*- coding: utf-8 -*-
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
#

from uuid import uuid4
from os import path

from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator
from airflow.utils.decorators import apply_defaults
from dagger.dag_creator.airflow.hooks.aws_athena_hook import AWSAthenaHook
from os import path


class AWSAthenaOperator(DaggerBaseOperator):
    """
    An operator that submit presto query to athena.

    If ``do_xcom_push`` is True, the QueryExecutionID assigned to the
    query will be pushed to an XCom when it successfuly completes.

    :param query: Presto to be run on athena. (templated)
    :type query: str
    :param database: Database to select. (templated)
    :type database: str
    :param output_location: s3 path to write the query results into. (templated)
    :type output_location: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param sleep_time: Time to wait between two consecutive call to check query status on athena
    :type sleep_time: int
    :param max_tries: Number of times to poll for query state before function exits
    :type max_triex: int
    """

    ui_color = '#44b5e2'
    template_fields = ('query', 'database', 'output_location')
    template_ext = ('.sql', )

    @apply_defaults
    def __init__(self, query, database, s3_tmp_results_location, s3_output_location, output_table, is_incremental,
                 partitioned_by=None, output_format=None, aws_conn_id='aws_default', client_request_token=None,
                 query_execution_context=None, result_configuration=None, sleep_time=30, max_tries=None,
                 workgroup='primary',
                 *args, **kwargs):
        super(AWSAthenaOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.database = database
        # Make sure that all queries are writing to a different path to avoid Slow Down exceptions
        self.s3_tmp_results_location = path.join(s3_tmp_results_location, self. str(uuid4().hex))
        self.s3_output_location = s3_output_location
        self.s3_output_bucket = s3_output_location.split('/')[2]
        self.s3_output_path = '/'.join(s3_output_location.split('/')[3:])
        self.output_table = output_table

        self.is_incremental = is_incremental
        self.partitioned_by = partitioned_by
        self.output_format = output_format
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.max_tries = max_tries
        self.query_execution_id = None
        self.hook = None

    def get_hook(self):
        return AWSAthenaHook(self.aws_conn_id, self.sleep_time, client_type="athena")

    def build_insert_into_query(self):
        return f"""\
INSERT INTO {self.database}.{self.output_table}
{self.query}
                """

    def build_ctas_query(self):
        s3_table_location = path.join(self.s3_output_location, self.database, self.output_table)
        with_parameters_dict = {
            "external_location": f"'{s3_table_location}/'",
        }

        if self.partitioned_by:
            partitioned_by_str = ','.join([f"'{column}'" for column in self.partitioned_by])
            with_parameters_dict['partitioned_by'] = f"ARRAY[{partitioned_by_str}]"

        if self.output_format:
            with_parameters_dict['format'] = f"'{self.output_format}'"

        with_parameters_expression =\
            ",\n    ".join([f"{parameter} = {value}" for parameter, value in with_parameters_dict.items()])

        return f"""\
CREATE TABLE {self.database}.{self.output_table}
WITH (
    {with_parameters_expression}
)
AS {self.query}
        """

    def extend_query(self, query):
        if self.is_incremental and self.hook.check_table_exists(self.database, self.output_table):
            return self.build_insert_into_query()
        else:
            return self.build_ctas_query()

    def execute(self, context):
        """
        Run Presto Query on Athena
        """
        self.hook = self.get_hook()

        self.log.info(f"""

            is_incremental: {self.is_incremental}
            output_database: {self.database}
            output_table: {self.output_table}
            s3_output_location: {self.s3_output_location}

        """)

        if not self.is_incremental:
            self.log.info(f"Dropping table: {self.database}.{self.output_table}")
            self.hook.drop_table(self.database, self.output_table)
            self.log.info(f"Deleting s3 location: s3://{self.s3_output_bucket}/{self.s3_output_path}/{self.database}/{self.output_table}")
            self.hook.delete_s3_location(self.s3_output_bucket, self.s3_output_path, self.database, self.output_table)

        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.s3_tmp_results_location
        query = self.extend_query(self.query)
        self.log.info(f"Running query\n{query}")
        self.query_execution_id = self.hook.run_query(query, self.query_execution_context,
                                                      self.result_configuration, self.client_request_token,
                                                      self.workgroup)
        query_status = self.hook.poll_query_status(self.query_execution_id, self.max_tries)

        if query_status in AWSAthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                'Final state of Athena job is {}, query_execution_id is {}. Error: {}'
                .format(query_status, self.query_execution_id, error_message))
        elif not query_status or query_status in AWSAthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                'Final state of Athena job is {}. '
                'Max tries of poll status exceeded, query_execution_id is {}.'
                .format(query_status, self.query_execution_id))

        return self.query_execution_id

    def on_kill(self):
        """
        Cancel the submitted athena query
        """
        if self.query_execution_id:
            self.log.info('⚰️⚰️⚰️ Received a kill Signal. Time to Die')
            self.log.info(
                'Stopping Query with executionId - %s', self.query_execution_id
            )
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response['ResponseMetadata']['HTTPStatusCode']
            except Exception as ex:
                self.log.error('Exception while cancelling query', ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error('Unable to request query cancel on athena. Exiting')
                else:
                    self.log.info(
                        'Polling Athena for query with id %s to reach final state', self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id)
