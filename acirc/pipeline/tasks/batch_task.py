from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task


class BatchTask(Task):
    ref_name = "batch"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='executable', parent_fields=['task_parameters'], comment="E.g.: my_code.py"),
            Attribute(attribute_name='executable_prefix', nullable=True, parent_fields=['task_parameters'],
                      comment="E.g.: python"),
            Attribute(attribute_name='job_name', parent_fields=['task_parameters']),
            Attribute(attribute_name='aws_conn_id', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='region_name', parent_fields=['task_parameters'],
                      required=False),
            Attribute(attribute_name='job_queue', parent_fields=['task_parameters'],
                      required=False),
            Attribute(attribute_name='max_retries', parent_fields=['task_parameters'],
                      required=False),
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._executable = self.parse_attribute('executable')
        self._executable_prefix = self.parse_attribute('executable_prefix') or ""
        job_name = "{}-{}".format(pipeline.name, self.parse_attribute('job_name'))
        self._job_name = job_name
        self._aws_conn_id = self.parse_attribute('aws_conn_id')
        self._region_name = self.parse_attribute('region_name') or 'eu-central-1'
        self._job_queue = self.parse_attribute('job_queue') or 'airflow-prio1'
        self._max_retries = self.parse_attribute('max_retries') or 4200

    @property
    def executable(self):
        return self._executable

    @property
    def executable_prefix(self):
        return self._executable_prefix

    @property
    def job_name(self):
        return self._job_name

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
