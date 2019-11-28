from acirc.pipeline.task import Task
from acirc.utilities.config_validator import ConfigValidator, Attribute
from acirc import conf

from datetime import datetime
from os.path import (
    relpath,
    join,
)


class Pipeline(ConfigValidator):

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='owner', validator=str, format_help="<team|person>@circ.com"),
            Attribute(attribute_name='description', validator=str),
            Attribute(attribute_name='schedule', format_help="crontab e.g.: 0 3 * * *"),
            Attribute(attribute_name='start_date', format_help="2019-11-01T03:00",
                      validator=lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M')),
            Attribute(attribute_name='slack_alert', auto_value='off', validator=bool),
            Attribute(attribute_name='airflow_parameters'),
            Attribute(attribute_name='default_args', required=True, nullable=True, validator=dict,
                      parent_fields=['airflow_parameters'], format_help="dictionary"),
            Attribute(attribute_name='dag_parameters', required=True, nullable=True, validator=dict,
                      parent_fields=['airflow_parameters'], format_help="dictionary")
        ])

    def __init__(self, directory: str, config: dict):
        super().__init__(join(directory, 'pipeline.yaml'), config)

        self._directory = directory
        self._name = relpath(directory, conf.DAGS_DIR).replace('/', '-')

        self._owner = self.parse_attribute(attribute_name='owner')
        self._description = self.parse_attribute(attribute_name='description')
        self._default_args = self.parse_attribute(attribute_name='default_args') or {}
        self._schedule = self.parse_attribute(attribute_name='schedule')
        self._start_date = self.parse_attribute(attribute_name='start_date')
        self._slack_alert = self.parse_attribute('slack_alert')
        self._parameters = self.parse_attribute(attribute_name='dag_parameters') or {}

        self._tasks = []

    @property
    def directory(self):
        return self._directory

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    @property
    def schedule(self):
        return self._schedule

    @property
    def start_date(self):
        return self._start_date

    @property
    def default_args(self):
        return self._default_args

    @property
    def slack_alert(self):
        return self._slack_alert

    @property
    def parameters(self):
        return self._parameters

    @property
    def tasks(self):
        return self._tasks

    def add_task(self, task: Task):
        self._tasks.append(task)
