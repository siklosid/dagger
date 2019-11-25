from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task
from acirc import conf


class RedshiftTransformTask(Task):
    ref_name = "redshift_transform"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='sql', parent_fields=['task_parameters'], comment="Relative path to sql file"),
            Attribute(attribute_name='postgres_conn_id', required=False, parent_fields=['task_parameters']),
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute('sql')
        self._postgres_conn_id = self.parse_attribute('postgres_conn_id') or conf.REDSHIFT_CONN_ID

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def postgres_conn_id(self):
        return self._postgres_conn_id
