import os

from dagger import conf
from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


class BatchTask(Task):
    ref_name = "batch"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="executable",
                    parent_fields=["task_parameters"],
                    comment="E.g.: my_code.py",
                ),
                Attribute(
                    attribute_name="executable_prefix",
                    nullable=True,
                    parent_fields=["task_parameters"],
                    comment="E.g.: python",
                ),
                Attribute(attribute_name="job_name", parent_fields=["task_parameters"], required=False),
                Attribute(attribute_name="absolute_job_name", parent_fields=["task_parameters"], required=False),
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
                    attribute_name="cluster_name",
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

        self._executable = self.parse_attribute("executable")
        self._executable_prefix = self.parse_attribute("executable_prefix") or ""
        job_path = f"""{pipeline.directory}/{self.parse_attribute("job_name")}"""
        job_name = str(os.path.relpath(job_path, conf.DAGS_DIR)).replace("/", "-")
        self._job_name = job_name
        self._absolute_job_name = self.parse_attribute("absolute_job_name")
        self._overrides = self.parse_attribute("overrides") or {}
        self._aws_conn_id = self.parse_attribute("aws_conn_id") or conf.BATCH_AWS_CONN_ID
        self._region_name = self.parse_attribute("region_name") or conf.BATCH_AWS_REGION
        self._cluster_name = self.parse_attribute("cluster_name") or conf.BATCH_CLUSTER_NAME
        self._job_queue = self.parse_attribute("job_queue") or conf.BATCH_DEFAULT_QUEUE
        self._max_retries = self.parse_attribute("max_retries") or 4200

    @property
    def executable(self):
        return self._executable

    @property
    def executable_prefix(self):
        return self._executable_prefix

    @property
    def absolute_job_name(self):
        return self._absolute_job_name

    @property
    def job_name(self):
        return self._job_name

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
    def cluster_name(self):
        return self._cluster_name

    @property
    def job_queue(self):
        return self._job_queue

    @property
    def max_retries(self):
        return self._max_retries
