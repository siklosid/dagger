from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task


class RedshiftTransformTask(Task):
    ref_name = "redshift_transform"

    s_attributes = []
    @classmethod
    def init_attributes_once(cls):
        if len(cls.s_attributes):
            return

        Task.init_attributes_once()
        cls.init_attributes()
        cls.s_attributes = Task.s_attributes + cls.s_attributes

    @staticmethod
    def init_attributes():
        RedshiftTransformTask.s_attributes = [
            Attribute(attribute_name='sql', parent_fields=['task_parameters'], comment="Relative path to sql file"),
        ]

    def __init__(self, name, pipeline_name, pipeline, job_config):
        RedshiftTransformTask.init_attributes_once()
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute('sql')

    @property
    def sql_file(self):
        return self._sql_file
