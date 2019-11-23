from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task


class RedshiftTransformTask(Task):
    ref_name = "redshift_transform"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='sql', parent_fields=['task_parameters'], comment="Relative path to sql file"),
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute('sql')

    @property
    def sql_file(self):
        return self._sql_file
