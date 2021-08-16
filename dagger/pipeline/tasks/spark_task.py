from dagger.utilities.exceptions import DaggerMissingFieldException

from dagger import conf
from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


class SparkTask(Task):
    ref_name = "spark"
    default_pool = "spark"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="spark_engine",
                    parent_fields=["task_parameters"],
                    required=True,
                    comment="Where to run spark job. Accepted values: emr, batch, glue",
                ),
                Attribute(attribute_name="job_file", parent_fields=["task_parameters"], required=True),
                Attribute(attribute_name="cluster_name", parent_fields=["task_parameters"], required=False),
                Attribute(attribute_name="job_bucket", parent_fields=["task_parameters"], required=False),
                Attribute(
                    attribute_name="spark_args",
                    parent_fields=["task_parameters"],
                    required=False,
                    format_help="Dictionary",
                ),
                Attribute(
                    attribute_name="job_args",
                    parent_fields=["task_parameters"],
                    required=False,
                    format_help="Dictionary",
                ),
                Attribute(
                    attribute_name="job_file",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
                Attribute(
                    attribute_name="extra_py_files",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
                Attribute(
                    attribute_name="emr_conn_id",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
                Attribute(
                    attribute_name="overrides",
                    parent_fields=["task_parameters"],
                    required=False,
                    validator=dict,
                    comment="Batch overrides dictionary: https://docs.aws.amazon.com/sdkforruby/api/Aws/Batch/Types/ContainerOverrides.html",
                ),
                Attribute(
                    attribute_name="aws_conn_id",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
                Attribute(
                    attribute_name="region_name",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
                Attribute(
                    attribute_name="job_queue",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
                Attribute(
                    attribute_name="max_retries",
                    parent_fields=["task_parameters"],
                    required=False,
                ),
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)
        self._spark_engine = self.parse_attribute("spark_engine")
        job_file_path = self.parse_attribute("job_file") or ''
        job_bucket = self.parse_attribute("job_bucket") or conf.SPARK_JOB_BUCKET
        self._cluster_name = self.parse_attribute("cluster_name") or conf.SPARK_CLUSTER_NAME
        self._extra_py_files = self.parse_attribute("extra_py_files")
        if job_file_path == '' and self._spark_engine == 'emr':
            raise DaggerMissingFieldException("Job file is mandatory for emr")
        elif job_file_path == '':
            self._job_file = job_file_path
        else:
            self._job_file = f"s3://{job_bucket}/{job_file_path}"
        self._spark_args = self.parse_attribute("spark_args")
        self._overrides = self.parse_attribute("overrides") or {}
        self._aws_conn_id = self.parse_attribute("aws_conn_id")
        self._region_name = self.parse_attribute("region_name") or conf.BATCH_AWS_REGION
        self._job_queue = self.parse_attribute("job_queue") or conf.BATCH_DEFAULT_QUEUE
        self._max_retries = self.parse_attribute("max_retries") or 4200

    @property
    def spark_engine(self):
        return self._spark_engine

    @property
    def job_file(self):
        return self._job_file

    @property
    def spark_args(self):
        return self._spark_args

    @property
    def extra_py_files(self):
        return self._extra_py_files

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def overrides(self):
        return self._overrides

    @property
    def aws_conn_id(self):
        return self._aws_conn_id

    @property
    def region_name(self):
        return self._region_name

    @property
    def job_queue(self):
        return self._job_queue

    @property
    def max_retries(self):
        return self._max_retries
