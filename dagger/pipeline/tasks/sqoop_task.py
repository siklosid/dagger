from dagger.utilities.config_validator import Attribute
from dagger.pipeline.task import Task
from dagger import conf


class SqoopTask(Task):
    ref_name = "sqoop"
    default_pool = "spark"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='sql', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='where', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='columns', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='num_mappers', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='split_by', parent_fields=['task_parameters'], required=True),
            Attribute(attribute_name='delete_target_dir', parent_fields=['task_parameters'], required=False,
                      validator=bool),
            Attribute(attribute_name='format', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='emr_master', parent_fields=['task_parameters'], required=False)
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        db_input = self._inputs[0]
        self._conn_id = db_input.conn_id
        self._table = db_input.table
        self._sql = self.parse_attribute('sql')
        if self._sql:
            self._table = None
        self._where = self.parse_attribute('where')
        self._columns = self.parse_attribute('columns')
        self._num_mappers = self.parse_attribute('num_mappers')
        self._split_by = self.parse_attribute('split_by')
        self._delete_target_dir = self.parse_attribute('delete_target_dir') or False
        self._target_dir = self._outputs[0].rendered_name
        self._format = self.parse_attribute('format') or conf.SQOOP_DEFAULT_FORMAT
        self._emr_master = self.parse_attribute('emr_master') or conf.SPARK_EMR_MASTER

    @property
    def conn_id(self):
        return self._conn_id

    @property
    def table(self):
        return self._table

    @property
    def sql(self):
        return self._sql

    @property
    def where(self):
        return self._where

    @property
    def columns(self):
        return self._columns

    @property
    def num_mappers(self):
        return self._num_mappers

    @property
    def split_by(self):
        return self._split_by

    @property
    def delete_target_dir(self):
        return self._delete_target_dir

    @property
    def target_dir(self):
        return self._target_dir

    @property
    def format(self):
        return self._format

    @property
    def emr_master(self):
        return self._emr_master
