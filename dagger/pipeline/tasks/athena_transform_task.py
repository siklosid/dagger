from dagger import conf
from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


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
                    attribute_name="s3_output_location",
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
                    validator=list,
                    comment="The list of fields to partition by. These fields should come last in the select statement",
                    parent_fields=["task_parameters"],
                ),
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute("sql")
        self._aws_conn_id = (
            self.parse_attribute("aws_conn_id") or conf.ATHENA_AWS_CONN_ID
        )
        self._s3_tmp_results_location = self.parse_attribute("s3_tmp_results_location") or conf.ATHENA_S3_TMP_RESULTS_LOCATION
        self._s3_output_location = self.parse_attribute("s3_output_location") or conf.ATHENA_DEFAULT_S3_OUTPUT_LOCATION
        self._workgroup = self.parse_attribute("workgroup") or conf.ATHENA_DEFAULT_WORKGROUP
        self._is_incremental = self.parse_attribute("is_incremental")
        self._partitioned_by = self.parse_attribute("partitioned_by")

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def aws_conn_id(self):
        return self._aws_conn_id

    @property
    def s3_tmp_results_location(self):
        return self._s3_tmp_results_location

    @property
    def s3_output_location(self):
        return self._s3_output_location

    @property
    def workgroup(self):
        return self._workgroup

    @property
    def is_incremental(self):
        return self._is_incremental

    @property
    def partitioned_by(self):
        return self._partitioned_by
