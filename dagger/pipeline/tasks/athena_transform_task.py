from dagger import conf
from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute
from dagger.pipeline.ios.s3_io import S3IO
from os import path


class AthenaTransformTask(Task):
    ref_name = "athena_transform"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="sql",
                    parent_fields=["task_parameters"],
                    comment="Relative path to sql file",
                ),
                Attribute(
                    attribute_name="aws_conn_id",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="s3_tmp_results_location",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="s3_output_bucket",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="s3_output_path",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="workgroup",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="is_incremental",
                    required=True,
                    validator=bool,
                    comment="""If set yes then SQL going to be an INSERT INTO\
                               statement, otherwise a DROP TABLE; CTAS statement""",
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="partitioned_by",
                    required=False,
                    validator=str,
                    comment="The list of fields to partition by. These fields should come last in the select statement",
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="output_format",
                    required=False,
                    validator=str,
                    comment="Output file format. One of PARQUET/ORC/JSON/CSV",
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="blue_green_deployment",
                    required=False,
                    validator=bool,
                    comment="Set to true for blue green deployment. Only works with non incremental transformations.",
                    parent_fields=["task_parameters"],
                )
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute("sql")
        self._aws_conn_id = (
            self.parse_attribute("aws_conn_id") or conf.ATHENA_AWS_CONN_ID
        )
        self._s3_tmp_results_bucket = \
            self.parse_attribute("s3_tmp_results_location") or conf.ATHENA_S3_TMP_RESULTS_LOCATION
        self._s3_output_bucket = self.parse_attribute("s3_output_bucket") or conf.ATHENA_DEFAULT_S3_OUTPUT_BUCKET
        self._s3_output_path = self.parse_attribute("s3_output_path") or conf.ATHENA_DEFAULT_S3_OUTPUT_PATH
        self._workgroup = self.parse_attribute("workgroup") or conf.ATHENA_DEFAULT_WORKGROUP
        self._is_incremental = self.parse_attribute("is_incremental")
        self._partitioned_by = self.parse_attribute("partitioned_by")
        self._output_format = self.parse_attribute("output_format")
        self._blue_green_deployment = self.parse_attribute("blue_green_deployment") or False

        self._add_hidden_s3_output()

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def aws_conn_id(self):
        return self._aws_conn_id

    @property
    def s3_tmp_results_location(self):
        return self._s3_tmp_results_bucket

    @property
    def s3_output_location(self):
        return f"s3://{path.join(self._s3_output_bucket, self._s3_output_path)}"

    @property
    def workgroup(self):
        return self._workgroup

    @property
    def is_incremental(self):
        return self._is_incremental

    @property
    def partitioned_by(self):
        return self._partitioned_by

    @property
    def output_format(self):
        return self._output_format

    @property
    def blue_green_deployment(self):
        return self._blue_green_deployment

    def _add_hidden_s3_output(self):
        output_athena = self._outputs[0]
        output_s3 = {
            'type': 's3',
            'name': f"{output_athena.table}_s3",
            'bucket': f"{self._s3_output_bucket}",
            'path': f"{path.join(self._s3_output_path, output_athena.schema, output_athena.table)}"
        }

        self.process_outputs([output_s3])
